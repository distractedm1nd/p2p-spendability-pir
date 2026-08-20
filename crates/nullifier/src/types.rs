use serde::{Deserialize, Serialize};

pub use pir_protocol::{
    PirEngine, ServerPhase, YpirScenario, ZcashNetwork, CONFIRMATION_DEPTH, DATASET_VERSION,
    IRONWOOD_POOL,
};

pub type Nullifier = [u8; 32];

pub const TARGET_SIZE: usize = 1_000_000;
pub const NUM_BUCKETS: usize = 16_384;
pub const BUCKET_CAPACITY: usize = 112;
pub const ENTRY_BYTES: usize = 32;
pub const BUCKET_BYTES: usize = BUCKET_CAPACITY * ENTRY_BYTES;
pub const DB_BYTES: usize = NUM_BUCKETS * BUCKET_BYTES;
pub const YPIR_POLY_LEN: usize = 2_048;

pub fn hash_to_bucket(nf: &Nullifier) -> u32 {
    u32::from_le_bytes(nf[..4].try_into().unwrap()) % NUM_BUCKETS as u32
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpendabilityMetadata {
    pub zcash_network: ZcashNetwork,
    pub nullifier_pool: String,
    pub dataset_version: u32,
    pub earliest_height: u64,
    pub latest_height: u64,
    pub num_nullifiers: u64,
    pub num_buckets: u64,
    pub phase: ServerPhase,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ironwood_geometry_meets_simplepir_minimum_exactly() {
        assert_eq!(ENTRY_BYTES, 32);
        assert_eq!(BUCKET_BYTES * 8, 28_672);
        assert_eq!(DB_BYTES, NUM_BUCKETS * BUCKET_BYTES);
    }

    #[test]
    fn bucket_mapping_is_deterministic() {
        let nf = [42; 32];
        assert_eq!(hash_to_bucket(&nf), hash_to_bucket(&nf));
        assert!((hash_to_bucket(&nf) as usize) < NUM_BUCKETS);
    }
}
