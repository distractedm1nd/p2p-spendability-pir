use arc_swap::ArcSwap;
use pir_types::{PirEngine, ServerPhase, YpirScenario};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use witness_types::*;

pub const DEFAULT_WINDOW_SHARD_LIMIT: usize = L0_MAX_SHARDS;
pub const FRONTIER_HISTORY_LIMIT: usize = 2_000;

/// Metadata exposed via `/metadata` and attached to PIR state snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WitnessMetadata {
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
    frontier_updates: RwLock<VecDeque<FrontierUpdate>>,
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
            frontier_updates: RwLock::new(VecDeque::with_capacity(FRONTIER_HISTORY_LIMIT)),
        }
    }

    pub fn push_frontier(&self, update: FrontierUpdate) {
        let mut updates = self
            .frontier_updates
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let keep = updates.partition_point(|last| last.height < update.height);
        updates.truncate(keep);
        updates.push_back(update);
        while updates.len() > FRONTIER_HISTORY_LIMIT {
            updates.pop_front();
        }
    }

    pub fn rollback_frontiers(&self, height: u64) {
        let mut updates = self
            .frontier_updates
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let keep = updates.partition_point(|update| update.height <= height);
        updates.truncate(keep);
    }

    /// Return an inclusive, gap-free height range, or `None` when it has been
    /// evicted or has not arrived yet.
    pub fn frontier_range(&self, from: u64, to: u64) -> Option<Vec<FrontierUpdate>> {
        let expected = to.checked_sub(from)?.checked_add(1)?;
        if expected > FRONTIER_HISTORY_LIMIT as u64 {
            return None;
        }
        let expected = expected as usize;
        let updates = self
            .frontier_updates
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let selected: Vec<_> = updates
            .iter()
            .filter(|update| (from..=to).contains(&update.height))
            .cloned()
            .collect();
        (selected.len() == expected).then_some(selected)
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pir_stub::StubPirEngine;

    fn state() -> AppState<StubPirEngine> {
        AppState::new(
            ServerConfig {
                snapshot_interval: 100,
                data_dir: PathBuf::new(),
                lwd_urls: vec![],
                listen_addr: "127.0.0.1:0".parse().unwrap(),
                window_shard_limit: DEFAULT_WINDOW_SHARD_LIMIT,
            },
            Arc::new(StubPirEngine),
        )
    }

    fn update(height: u64) -> FrontierUpdate {
        FrontierUpdate {
            height,
            tree_size: height,
            root: [height as u8; 32],
            rightmost_nodes: [[height as u8; 32]; TREE_DEPTH],
        }
    }

    #[test]
    fn frontier_range_requires_complete_history_and_handles_reorgs() {
        let state = state();
        state.push_frontier(update(10));
        state.push_frontier(update(11));
        state.push_frontier(update(12));

        assert_eq!(
            state
                .frontier_range(10, 12)
                .unwrap()
                .iter()
                .map(|update| update.height)
                .collect::<Vec<_>>(),
            vec![10, 11, 12]
        );
        assert!(state.frontier_range(9, 12).is_none());

        state.rollback_frontiers(10);
        state.push_frontier(update(11));
        assert!(state.frontier_range(10, 12).is_none());
        assert!(state.frontier_range(10, 11).is_some());
    }

    #[test]
    fn frontier_history_is_bounded() {
        let state = state();
        for height in 0..=FRONTIER_HISTORY_LIMIT as u64 {
            state.push_frontier(update(height));
        }
        assert!(state.frontier_range(0, 0).is_none());
        assert!(state
            .frontier_range(1, FRONTIER_HISTORY_LIMIT as u64)
            .is_some());
    }
}
