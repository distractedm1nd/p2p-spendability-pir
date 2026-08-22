use crate::{
    p2p::{P2pClientError, P2pPirSession},
    transport::{Operation, PirTransport, TransportError},
};
use pir_protocol::p2p::P2PError;
use pir_protocol::{YpirScenario, ZcashNetwork, DATASET_VERSION, IRONWOOD_POOL};
use serde::Deserialize;
use std::collections::{hash_map::Entry, HashMap};
use thiserror::Error;
use witness_pir::*;
use ypir::client::YPIRClient;
use ypir::params::{params_for_scenario_simplepir_with_config, YPIRSPConfig};
use ypir::serialize::ToBytes;

pub use crate::reconstruct;

#[derive(Error, Debug)]
pub enum WitnessClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("P2P error: {0}")]
    P2p(#[from] P2pClientError),
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
    transport: PirTransport,
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
        Self::connect_with_transport(PirTransport::http(url), zcash_network).await
    }

    pub async fn connect_p2p(session: P2pPirSession, zcash_network: ZcashNetwork) -> Result<Self> {
        Self::connect_with_transport(PirTransport::Zakura(session), zcash_network).await
    }

    async fn connect_with_transport(
        transport: PirTransport,
        zcash_network: ZcashNetwork,
    ) -> Result<Self> {
        let t0 = std::time::Instant::now();
        let metadata: Metadata = serde_json::from_slice(
            &transport
                .request(Operation::WitnessMetadata, vec![])
                .await
                .map_err(map_transport_error)?,
        )
        .map_err(|error| WitnessClientError::InvalidParams(error.to_string()))?;
        if metadata.zcash_network != zcash_network
            || metadata.commitment_pool != IRONWOOD_POOL
            || metadata.dataset_version != DATASET_VERSION
        {
            return Err(WitnessClientError::InvalidParams(
                "server is not the expected Ironwood witness dataset".into(),
            ));
        }

        let scenario: YpirScenario = serde_json::from_slice(
            &transport
                .request(Operation::WitnessParams, vec![])
                .await
                .map_err(map_transport_error)?,
        )
        .map_err(|error| WitnessClientError::InvalidParams(error.to_string()))?;
        if scenario.num_items != L0_DB_ROWS as u64
            || scenario.item_size_bits != (SUBSHARD_ROW_BYTES * 8) as u64
            || scenario.poly_len != YPIR_POLY_LEN
        {
            return Err(WitnessClientError::InvalidParams(
                "unexpected Ironwood witness PIR geometry".into(),
            ));
        }
        tracing::info!(elapsed_ms = t0.elapsed().as_millis(), "fetched /params");

        let t1 = std::time::Instant::now();
        let broadcast: BroadcastData = serde_json::from_slice(
            &transport
                .request(Operation::WitnessBroadcast, vec![])
                .await
                .map_err(map_transport_error)?,
        )
        .map_err(|error| WitnessClientError::InvalidParams(error.to_string()))?;
        tracing::info!(
            elapsed_ms = t1.elapsed().as_millis(),
            broadcast_bytes = serde_json::to_vec(&broadcast).map(|v| v.len()).unwrap_or(0),
            "fetched /broadcast",
        );

        let t2 = std::time::Instant::now();
        let ypir_client = {
            let params = params_for_scenario_simplepir_with_config(
                scenario.num_items,
                scenario.item_size_bits,
                YPIRSPConfig::for_poly_len(scenario.poly_len),
            );
            YPIRClient::new(&params)
        };
        tracing::info!(
            elapsed_ms = t2.elapsed().as_millis(),
            num_items = scenario.num_items,
            item_size_bits = scenario.item_size_bits,
            poly_len = scenario.poly_len,
            "PIR client initialized",
        );

        tracing::info!(
            total_connect_ms = t0.elapsed().as_millis(),
            anchor_height = broadcast.anchor_height,
            window_start = broadcast.window_start_shard,
            window_count = broadcast.window_shard_count,
            cap_shards = broadcast.cap.shard_roots.len(),
            "connected to witness-server",
        );

        Ok(Self {
            transport,
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
        Ok(self
            .get_witnesses(&[position])
            .await?
            .pop()
            .expect("one position produces one witness"))
    }

    /// Fetch witnesses for multiple positions, querying each PIR row only once.
    pub async fn get_witnesses(&self, positions: &[u64]) -> Result<Vec<PirWitness>> {
        let window_end = self
            .broadcast
            .window_start_shard
            .checked_add(self.broadcast.window_shard_count)
            .ok_or_else(|| WitnessClientError::InvalidParams("invalid PIR window".into()))?;
        let mut decoded_rows = HashMap::new();
        let mut witnesses = Vec::with_capacity(positions.len());

        for &position in positions {
            let t0 = std::time::Instant::now();
            let (shard_idx, subshard_idx, leaf_idx) = decompose_position(position);
            if shard_idx < self.broadcast.window_start_shard || shard_idx >= window_end {
                return Err(WitnessClientError::PositionOutsideWindow(
                    position,
                    self.broadcast.window_start_shard,
                    window_end,
                ));
            }

            let row_idx =
                physical_row_index(shard_idx, subshard_idx, self.broadcast.window_start_shard);

            if let Entry::Vacant(row) = decoded_rows.entry(row_idx) {
                row.insert(self.query_row(row_idx).await?);
            }

            let t4 = std::time::Instant::now();
            let witness = reconstruct::reconstruct_witness(
                position,
                shard_idx,
                subshard_idx,
                leaf_idx,
                decoded_rows.get(&row_idx).expect("queried row is cached"),
                &self.broadcast,
            )?;
            tracing::info!(
                elapsed_ms = t4.elapsed().as_millis(),
                total_ms = t0.elapsed().as_millis(),
                position,
                "witness reconstructed",
            );
            witnesses.push(witness);
        }

        Ok(witnesses)
    }

    async fn query_row(&self, row_idx: usize) -> Result<Vec<u8>> {
        let t0 = std::time::Instant::now();
        let (query, seed) = self.ypir_client.generate_query_simplepir(row_idx);
        let query_bytes = query.to_bytes();
        tracing::info!(
            elapsed_ms = t0.elapsed().as_millis(),
            query_bytes = query_bytes.len(),
            row_idx,
            "query generated",
        );

        let t1 = std::time::Instant::now();
        let response = self
            .transport
            .request(Operation::WitnessQuery, query_bytes)
            .await
            .map_err(map_transport_error)?;
        tracing::info!(
            elapsed_ms = t1.elapsed().as_millis(),
            response_bytes = response.len(),
            "server response received",
        );

        let t2 = std::time::Instant::now();
        let row = self.ypir_client.decode_response_simplepir(seed, &response);
        tracing::info!(
            elapsed_ms = t2.elapsed().as_millis(),
            decoded_elements = row.len(),
            "response decoded",
        );
        Ok(row)
    }

    /// Re-fetch broadcast data from the server (new anchor, updated tree).
    pub async fn refresh_broadcast(&mut self) -> Result<()> {
        self.broadcast = serde_json::from_slice(
            &self
                .transport
                .request(Operation::WitnessBroadcast, vec![])
                .await
                .map_err(map_transport_error)?,
        )
        .map_err(|error| WitnessClientError::InvalidParams(error.to_string()))?;
        Ok(())
    }

    pub fn anchor_height(&self) -> u64 {
        self.broadcast.anchor_height
    }

    pub fn broadcast(&self) -> &BroadcastData {
        &self.broadcast
    }
}

fn map_transport_error(error: TransportError) -> WitnessClientError {
    match error {
        TransportError::Http(error) => WitnessClientError::Http(error),
        TransportError::Unavailable
        | TransportError::P2p(P2pClientError::Remote(P2PError::ServiceUnavailable)) => {
            WitnessClientError::ServerUnavailable
        }
        TransportError::P2p(error) => WitnessClientError::P2p(error),
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
