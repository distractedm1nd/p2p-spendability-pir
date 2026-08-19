use arc_swap::ArcSwap;
use spend_types::{
    PirEngine, ServerPhase, SpendabilityMetadata, YpirScenario, ZcashNetwork, BUCKET_BYTES,
    NUM_BUCKETS,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

pub struct PirState<P: PirEngine> {
    pub engine_state: P::ServerState,
    pub metadata: SpendabilityMetadata,
}

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
                num_items: NUM_BUCKETS as u64,
                item_size_bits: (BUCKET_BYTES * 8) as u64,
            },
            engine,
            config,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub zcash_network: ZcashNetwork,
    pub target_size: usize,
    pub confirmation_depth: u64,
    pub snapshot_interval: u64,
    pub data_dir: PathBuf,
    pub lwd_urls: Vec<String>,
    pub listen_addr: SocketAddr,
}
