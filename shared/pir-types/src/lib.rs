//! Shared PIR types used by both the nullifier and witness subsystems.
//!
//! Contains the [`PirEngine`] trait (abstracting YPIR for tests),
//! YPIR scenario parameters, server lifecycle phases, and chain
//! constants shared across all PIR services.

use serde::{Deserialize, Serialize};

/// Blocks behind the tip at which the PIR server anchors its database state.
/// Shared by both nullifier and witness PIR servers. Deep enough (10) to survive
/// typical reorgs while still being fresh enough for practical spending.
pub const CONFIRMATION_DEPTH: u64 = 10;

/// Mainnet activation height for NU5 (Orchard). No Orchard data exists below
/// this height, so PIR ingest starts here.
pub const NU5_MAINNET_ACTIVATION: u64 = 1_687_104;

/// Server lifecycle phase, reported via `/metadata` endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServerPhase {
    /// Catching up to the chain tip during initial sync.
    Syncing {
        current_height: u64,
        target_height: u64,
    },
    /// Fully synced and serving PIR queries.
    Serving,
}

/// SimplePIR scenario parameters describing the database geometry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YpirScenario {
    /// Number of rows in the PIR database.
    pub num_items: u64,
    /// Size of each row in bits.
    pub item_size_bits: u64,
}

/// Abstraction over the PIR engine, allowing stub implementations for testing
/// and the real YPIR engine in production.
pub trait PirEngine: Send + Sync {
    type ServerState: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Offline precomputation: build server state from raw DB bytes and scenario.
    fn setup(
        &self,
        db_bytes: &[u8],
        scenario: &YpirScenario,
    ) -> Result<Self::ServerState, Self::Error>;

    /// Online computation: answer a single encrypted client query.
    fn answer_query(
        &self,
        state: &Self::ServerState,
        query_bytes: &[u8],
    ) -> Result<Vec<u8>, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_phase_serde_roundtrip() {
        let syncing = ServerPhase::Syncing {
            current_height: 100,
            target_height: 200,
        };
        let json = serde_json::to_string(&syncing).unwrap();
        let decoded: ServerPhase = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, syncing);

        let serving = ServerPhase::Serving;
        let json = serde_json::to_string(&serving).unwrap();
        let decoded: ServerPhase = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, serving);
    }

    #[test]
    fn ypir_scenario_serde_roundtrip() {
        let scenario = YpirScenario {
            num_items: 16_384,
            item_size_bits: 28_672,
        };
        let json = serde_json::to_string(&scenario).unwrap();
        let decoded: YpirScenario = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.num_items, 16_384);
        assert_eq!(decoded.item_size_bits, 28_672);
    }
}
