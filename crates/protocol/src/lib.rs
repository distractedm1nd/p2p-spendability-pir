//! Shared PIR types used by both the nullifier and witness subsystems.
//!
//! Contains the [`PirEngine`] trait (abstracting YPIR for tests),
//! YPIR scenario parameters, server lifecycle phases, and chain
//! constants shared across all PIR services.

use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

pub mod p2p;

/// Blocks behind the tip at which the PIR server anchors its database state.
/// Shared by both nullifier and witness PIR servers. Deep enough (10) to survive
/// typical reorgs while still being fresh enough for practical spending.
pub const CONFIRMATION_DEPTH: u64 = 10;

/// Shielded pool represented by newly-created PIR datasets.
pub const IRONWOOD_POOL: &str = "ironwood";

/// Version of the Ironwood PIR dataset contract.
pub const DATASET_VERSION: u32 = 2;

/// Zcash network represented by a PIR dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ZcashNetwork {
    Main,
    Test,
}

impl ZcashNetwork {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::Test => "test",
        }
    }
}

impl fmt::Display for ZcashNetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ZcashNetwork {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "main" => Ok(Self::Main),
            "test" => Ok(Self::Test),
            _ => Err(format!(
                "unsupported Zcash network {value:?}; expected main or test"
            )),
        }
    }
}

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
    /// RLWE polynomial degree.
    pub poly_len: usize,
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
            poly_len: 4_096,
        };
        let json = serde_json::to_string(&scenario).unwrap();
        let decoded: YpirScenario = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.num_items, 16_384);
        assert_eq!(decoded.item_size_bits, 28_672);
        assert_eq!(decoded.poly_len, 4_096);
        assert!(serde_json::from_str::<YpirScenario>(
            r#"{"num_items":16384,"item_size_bits":28672}"#
        )
        .is_err());
    }

    #[test]
    fn ironwood_dataset_identity_matches_vote_nullifier_pir() {
        assert_eq!(IRONWOOD_POOL, "ironwood");
        assert_eq!(DATASET_VERSION, 2);
        assert_eq!(ZcashNetwork::Main.to_string(), "main");
        assert_eq!("test".parse(), Ok(ZcashNetwork::Test));
    }
}
