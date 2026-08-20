use pir_protocol::{YpirScenario, ZcashNetwork, DATASET_VERSION, IRONWOOD_POOL};
use serde::Deserialize;
use thiserror::Error;
use witness_pir::*;
use ypir::client::YPIRClient;
use ypir::params::params_for_scenario_simplepir;
use ypir::serialize::ToBytes;

pub use crate::reconstruct;

#[derive(Error, Debug)]
pub enum WitnessClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("server unavailable (503)")]
    ServerUnavailable,
    #[error("invalid params from server: {0}")]
    InvalidParams(String),
    #[error("query failed: {0}")]
    QueryFailed(String),
    #[error("position {0} is outside the server's PIR window (shards {1}..{2})")]
    PositionOutsideWindow(u64, u32, u32),
    #[error("witness verification failed for position {0}: computed root does not match anchor")]
    VerificationFailed(u64),
}

pub type Result<T> = std::result::Result<T, WitnessClientError>;

#[derive(Deserialize)]
struct Metadata {
    zcash_network: ZcashNetwork,
    commitment_pool: String,
    dataset_version: u32,
}

pub struct WitnessClient {
    http: reqwest::Client,
    base_url: String,
    #[allow(dead_code)]
    scenario: YpirScenario,
    broadcast: BroadcastData,
    ypir_client: YPIRClient,
}

impl WitnessClient {
    /// Connect to a witness-server, fetch params and broadcast data, initialize
    /// the PIR client. The broadcast download is ~104 KB and cached for the
    /// lifetime of this client.
    pub async fn connect(url: &str, zcash_network: ZcashNetwork) -> Result<Self> {
        let t0 = std::time::Instant::now();
        let base_url = url.trim_end_matches('/').to_string();
        let http = reqwest::Client::new();

        let metadata: Metadata = http
            .get(format!("{base_url}/metadata"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if metadata.zcash_network != zcash_network
            || metadata.commitment_pool != IRONWOOD_POOL
            || metadata.dataset_version != DATASET_VERSION
        {
            return Err(WitnessClientError::InvalidParams(
                "server is not the expected Ironwood witness dataset".into(),
            ));
        }

        let scenario: YpirScenario = http
            .get(format!("{base_url}/params"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if scenario.num_items != L0_DB_ROWS as u64
            || scenario.item_size_bits != (SUBSHARD_ROW_BYTES * 8) as u64
        {
            return Err(WitnessClientError::InvalidParams(
                "unexpected Ironwood witness PIR geometry".into(),
            ));
        }
        tracing::info!(elapsed_ms = t0.elapsed().as_millis(), "fetched /params");

        let t1 = std::time::Instant::now();
        let broadcast_resp = http.get(format!("{base_url}/broadcast")).send().await?;
        if broadcast_resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(WitnessClientError::ServerUnavailable);
        }
        let broadcast: BroadcastData = broadcast_resp.error_for_status()?.json().await?;
        tracing::info!(
            elapsed_ms = t1.elapsed().as_millis(),
            broadcast_bytes = serde_json::to_vec(&broadcast).map(|v| v.len()).unwrap_or(0),
            "fetched /broadcast",
        );

        let t2 = std::time::Instant::now();
        let ypir_client = {
            let params = params_for_scenario_simplepir(scenario.num_items, scenario.item_size_bits);
            YPIRClient::new(&params)
        };
        tracing::info!(
            elapsed_ms = t2.elapsed().as_millis(),
            num_items = scenario.num_items,
            item_size_bits = scenario.item_size_bits,
            "PIR client initialized",
        );

        tracing::info!(
            base_url,
            total_connect_ms = t0.elapsed().as_millis(),
            anchor_height = broadcast.anchor_height,
            window_start = broadcast.window_start_shard,
            window_count = broadcast.window_shard_count,
            cap_shards = broadcast.cap.shard_roots.len(),
            "connected to witness-server",
        );

        Ok(Self {
            http,
            base_url,
            scenario,
            broadcast,
            ypir_client,
        })
    }

    /// Fetch a note commitment witness for the given tree position.
    ///
    /// Issues a single PIR query to retrieve the
    /// subshard row containing the note's leaf. Combines the PIR response with
    /// the cached broadcast data to reconstruct the full 32-level authentication
    /// path. Self-verifies the witness before returning.
    pub async fn get_witness(&self, position: u64) -> Result<PirWitness> {
        let t0 = std::time::Instant::now();
        let (shard_idx, subshard_idx, leaf_idx) = decompose_position(position);
        let window_end = self
            .broadcast
            .window_start_shard
            .checked_add(self.broadcast.window_shard_count)
            .ok_or_else(|| WitnessClientError::InvalidParams("invalid PIR window".into()))?;

        if shard_idx < self.broadcast.window_start_shard || shard_idx >= window_end {
            return Err(WitnessClientError::PositionOutsideWindow(
                position,
                self.broadcast.window_start_shard,
                window_end,
            ));
        }

        let row_idx =
            physical_row_index(shard_idx, subshard_idx, self.broadcast.window_start_shard);

        let t1 = std::time::Instant::now();
        let (query_bytes, seed) = {
            let (query, seed) = self.ypir_client.generate_query_simplepir(row_idx);
            (query.to_bytes(), seed)
        };
        tracing::info!(
            elapsed_ms = t1.elapsed().as_millis(),
            query_bytes = query_bytes.len(),
            row_idx,
            position,
            "query generated",
        );

        let t2 = std::time::Instant::now();
        let resp = self
            .http
            .post(format!("{}/query", self.base_url))
            .body(query_bytes)
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(WitnessClientError::ServerUnavailable);
        }

        let response_bytes = resp
            .error_for_status()
            .map_err(|e| WitnessClientError::QueryFailed(e.to_string()))?
            .bytes()
            .await?;
        tracing::info!(
            elapsed_ms = t2.elapsed().as_millis(),
            response_bytes = response_bytes.len(),
            "server response received",
        );

        let t3 = std::time::Instant::now();
        let decoded_row = self
            .ypir_client
            .decode_response_simplepir(seed, &response_bytes);
        tracing::info!(
            elapsed_ms = t3.elapsed().as_millis(),
            decoded_elements = decoded_row.len(),
            "response decoded",
        );

        let t4 = std::time::Instant::now();
        let witness = reconstruct::reconstruct_witness(
            position,
            shard_idx,
            subshard_idx,
            leaf_idx,
            &decoded_row,
            &self.broadcast,
        )?;
        tracing::info!(
            elapsed_ms = t4.elapsed().as_millis(),
            total_ms = t0.elapsed().as_millis(),
            position,
            "witness reconstructed",
        );

        Ok(witness)
    }

    /// Re-fetch broadcast data from the server (new anchor, updated tree).
    pub async fn refresh_broadcast(&mut self) -> Result<()> {
        let resp = self
            .http
            .get(format!("{}/broadcast", self.base_url))
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(WitnessClientError::ServerUnavailable);
        }

        self.broadcast = resp.error_for_status()?.json().await?;
        Ok(())
    }

    pub fn anchor_height(&self) -> u64 {
        self.broadcast.anchor_height
    }

    pub fn broadcast(&self) -> &BroadcastData {
        &self.broadcast
    }
}

/// Blocking wrapper for use from synchronous FFI contexts.
pub struct WitnessClientBlocking {
    rt: tokio::runtime::Runtime,
    client: WitnessClient,
}

impl WitnessClientBlocking {
    pub fn connect(url: &str, zcash_network: ZcashNetwork) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| WitnessClientError::QueryFailed(e.to_string()))?;
        let client = rt.block_on(WitnessClient::connect(url, zcash_network))?;
        Ok(Self { rt, client })
    }

    /// Fetch witnesses for a batch of positions.
    /// Returns a `Vec<PirWitness>` parallel to the input positions.
    /// Calls `progress` after each query with fraction complete (0.0..=1.0).
    pub fn get_witnesses(
        &self,
        positions: &[u64],
        progress: impl Fn(f64),
    ) -> Result<Vec<PirWitness>> {
        let total = positions.len();
        let mut results = Vec::with_capacity(total);
        for (i, &pos) in positions.iter().enumerate() {
            let witness = self.rt.block_on(self.client.get_witness(pos))?;
            results.push(witness);
            progress((i + 1) as f64 / total as f64);
        }
        Ok(results)
    }

    pub fn anchor_height(&self) -> u64 {
        self.client.anchor_height()
    }

    pub fn broadcast(&self) -> &BroadcastData {
        self.client.broadcast()
    }
}
