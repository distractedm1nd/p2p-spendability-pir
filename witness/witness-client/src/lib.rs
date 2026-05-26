#[cfg(feature = "ipir")]
use ipir_sp::serialize::serialize_packing_keys;
#[cfg(feature = "ipir")]
use ipir_sp::{params_for_simplepir, IPIRClient, IPIRSeed};
use pir_types::YpirScenario;
#[cfg(feature = "ipir")]
use pir_types::IPIR_SETUP_SEED;
use thiserror::Error;
use witness_types::*;
#[cfg(not(feature = "ipir"))]
use ypir::client::YPIRClient;
#[cfg(not(feature = "ipir"))]
use ypir::params::params_for_scenario_simplepir;
#[cfg(not(feature = "ipir"))]
use ypir::serialize::ToBytes;

pub mod reconstruct;

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

pub struct WitnessClient {
    http: reqwest::Client,
    base_url: String,
    #[allow(dead_code)]
    scenario: YpirScenario,
    broadcast: BroadcastData,
    #[cfg(feature = "ipir")]
    ipir_client: IpirClientState,
    #[cfg(not(feature = "ipir"))]
    ypir_client: YPIRClient,
}

#[cfg(feature = "ipir")]
struct IpirClientState {
    client: IPIRClient,
    offline_query_polys: Vec<Vec<u64>>,
}

impl WitnessClient {
    /// Connect to a witness-server, fetch params and broadcast data, initialize
    /// the PIR client. The broadcast download is ~104 KB and cached for the
    /// lifetime of this client.
    pub async fn connect(url: &str) -> Result<Self> {
        let t0 = std::time::Instant::now();
        let base_url = url.trim_end_matches('/').to_string();
        let http = reqwest::Client::new();

        let scenario: YpirScenario = http
            .get(format!("{base_url}/params"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
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
        #[cfg(feature = "ipir")]
        let ipir_client = {
            let (rlwe, ypir) = params_for_simplepir(scenario.num_items, scenario.item_size_bits)
                .map_err(|e| WitnessClientError::InvalidParams(e.to_string()))?;
            let client = IPIRClient::new(&rlwe, &ypir);
            let offline_query_polys =
                client.generate_public_query_setup_simplepir_from_seed(IPIR_SETUP_SEED);
            IpirClientState {
                client,
                offline_query_polys,
            }
        };
        #[cfg(not(feature = "ipir"))]
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
            #[cfg(feature = "ipir")]
            ipir_client,
            #[cfg(not(feature = "ipir"))]
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
        let window_end = self.broadcast.window_start_shard + self.broadcast.window_shard_count;

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
        #[cfg(feature = "ipir")]
        let (query_bytes, seed) = self.generate_ipir_query(row_idx)?;
        #[cfg(not(feature = "ipir"))]
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
        #[cfg(feature = "ipir")]
        let decoded_row = self.decode_ipir_response(seed, &response_bytes);
        #[cfg(not(feature = "ipir"))]
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

    #[cfg(feature = "ipir")]
    fn generate_ipir_query(&self, row_idx: usize) -> Result<(Vec<u8>, IPIRSeed)> {
        let (query, packing_keys, seed) = self
            .ipir_client
            .client
            .generate_fresh_query_simplepir(&self.ipir_client.offline_query_polys, row_idx);
        let mut query_bytes =
            serialize_packing_keys(self.ipir_client.client.rlwe_params(), &packing_keys)
                .map_err(|e| WitnessClientError::QueryFailed(e.to_string()))?;
        query_bytes.extend(query.to_packed_bytes(self.ipir_client.client.rlwe_params().q));
        Ok((query_bytes, seed))
    }

    #[cfg(feature = "ipir")]
    fn decode_ipir_response(&self, seed: IPIRSeed, response_bytes: &[u8]) -> Vec<u8> {
        self.ipir_client
            .client
            .decode_response_simplepir(seed, response_bytes)
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
    pub fn connect(url: &str) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| WitnessClientError::QueryFailed(e.to_string()))?;
        let client = rt.block_on(WitnessClient::connect(url))?;
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
