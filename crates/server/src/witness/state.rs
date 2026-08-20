use arc_swap::ArcSwap;
use pir_protocol::{PirEngine, ServerPhase, YpirScenario, ZcashNetwork};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use witness_pir::*;

pub const DEFAULT_WINDOW_SHARD_LIMIT: usize = L0_MAX_SHARDS;

/// Metadata exposed via `/metadata` and attached to PIR state snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WitnessMetadata {
    pub zcash_network: ZcashNetwork,
    pub commitment_pool: String,
    pub dataset_version: u32,
    pub anchor_height: u64,
    pub tree_size: u64,
    pub window_start_shard: u32,
    pub window_shard_count: u32,
    pub populated_shards: u32,
    pub phase: ServerPhase,
}

/// Live PIR state: engine state + broadcast data + metadata, swapped atomically.
pub struct PirState<P: PirEngine> {
    pub engine_state: P::ServerState,
    pub broadcast: BroadcastData,
    pub metadata: WitnessMetadata,
}

/// Shared application state accessible from all Axum handlers.
pub struct AppState<P: PirEngine> {
    pub live_pir: ArcSwap<Option<PirState<P>>>,
    pub phase: ArcSwap<ServerPhase>,
    pub scenario: YpirScenario,
    pub engine: Arc<P>,
    pub config: ServerConfig,
}

impl<P: PirEngine> AppState<P> {
    pub fn new(config: ServerConfig, engine: Arc<P>) -> Self {
        Self {
            live_pir: ArcSwap::from_pointee(None),
            phase: ArcSwap::from_pointee(ServerPhase::Syncing {
                current_height: 0,
                target_height: 0,
            }),
            scenario: YpirScenario {
                num_items: L0_DB_ROWS as u64,
                item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
            },
            engine,
            config,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub zcash_network: ZcashNetwork,
    pub snapshot_interval: u64,
    pub data_dir: PathBuf,
    pub lwd_urls: Vec<String>,
    pub listen_addr: SocketAddr,
    pub window_shard_limit: usize,
}

impl ServerConfig {
    pub fn effective_window_shard_limit(&self) -> usize {
        self.window_shard_limit.clamp(1, L0_MAX_SHARDS)
    }
}
