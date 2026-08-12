use serde::{Deserialize, Serialize};

// Re-export shared PIR types so existing consumers (spend-server, nf-ingest)
// continue to compile without import changes.
pub use pir_types::{
    PirEngine, ServerPhase, YpirScenario, CONFIRMATION_DEPTH, NU5_MAINNET_ACTIVATION,
};

pub const TARGET_SIZE: usize = 1_000_000;
pub const NUM_BUCKETS: usize = 16_384; // 2^14
pub const BUCKET_CAPACITY: usize = 112;
pub const ENTRY_BYTES: usize = 41;
pub const BUCKET_BYTES: usize = BUCKET_CAPACITY * ENTRY_BYTES; // 4,592
pub const DB_BYTES: usize = NUM_BUCKETS * BUCKET_BYTES; // ~72 MB

/// Map a nullifier to its bucket index.
/// Nullifiers are cryptographically random, so the first 4 bytes give uniform distribution.
pub fn hash_to_bucket(nf: &[u8; 32]) -> u32 {
    let raw = u32::from_le_bytes([nf[0], nf[1], nf[2], nf[3]]);
    raw % (NUM_BUCKETS as u32)
}

/// Nullifier with per-transaction metadata, produced by the block parser.
/// `spend_height` is not included — it comes from the block height at insert time.
#[derive(Debug, Clone)]
pub struct NullifierWithMeta {
    pub nullifier: [u8; 32],
    pub first_output_position: u32,
    pub action_count: u8,
}

/// Full 41-byte entry stored in each bucket slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NullifierEntry {
    pub nullifier: [u8; 32],
    pub spend_height: u32,
    pub first_output_position: u32,
    pub action_count: u8,
}

impl NullifierEntry {
    pub const ZERO: Self = Self {
        nullifier: [0u8; 32],
        spend_height: 0,
        first_output_position: 0,
        action_count: 0,
    };

    pub fn is_empty(&self) -> bool {
        self.nullifier == [0u8; 32]
    }

    pub fn to_bytes(self) -> [u8; ENTRY_BYTES] {
        let mut buf = [0u8; ENTRY_BYTES];
        buf[..32].copy_from_slice(&self.nullifier);
        buf[32..36].copy_from_slice(&self.spend_height.to_le_bytes());
        buf[36..40].copy_from_slice(&self.first_output_position.to_le_bytes());
        buf[40] = self.action_count;
        buf
    }

    pub fn from_bytes(buf: [u8; ENTRY_BYTES]) -> Self {
        let mut nullifier = [0u8; 32];
        nullifier.copy_from_slice(&buf[..32]);
        Self {
            nullifier,
            spend_height: u32::from_le_bytes(buf[32..36].try_into().unwrap()),
            first_output_position: u32::from_le_bytes(buf[36..40].try_into().unwrap()),
            action_count: buf[40],
        }
    }
}

/// Metadata returned by the client on a nullifier match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpendMetadata {
    pub spend_height: u32,
    pub first_output_position: u32,
    pub action_count: u8,
}

impl SpendMetadata {
    pub fn from_entry_tail(buf: &[u8; 9]) -> Self {
        Self {
            spend_height: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            first_output_position: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            action_count: buf[8],
        }
    }
}

#[derive(Debug, Clone)]
pub enum ChainEvent {
    NewBlock {
        height: u64,
        hash: [u8; 32],
        prev_hash: [u8; 32],
        nullifiers: Vec<NullifierWithMeta>,
    },
    Reorg {
        orphaned: Vec<OrphanedBlock>,
        new_blocks: Vec<NewBlock>,
    },
}

#[derive(Debug, Clone)]
pub struct OrphanedBlock {
    pub height: u64,
    pub hash: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct NewBlock {
    pub height: u64,
    pub hash: [u8; 32],
    pub prev_hash: [u8; 32],
    pub nullifiers: Vec<NullifierWithMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpendabilityMetadata {
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
    fn test_hash_to_bucket_in_range() {
        let nf = [0xffu8; 32];
        let bucket = hash_to_bucket(&nf);
        assert!(bucket < NUM_BUCKETS as u32);
    }

    #[test]
    fn test_hash_to_bucket_deterministic() {
        let nf = [42u8; 32];
        assert_eq!(hash_to_bucket(&nf), hash_to_bucket(&nf));
    }

    #[test]
    fn test_hash_to_bucket_distribution() {
        use std::collections::HashMap;
        let mut counts: HashMap<u32, usize> = HashMap::new();
        for i in 0u32..10_000 {
            let mut nf = [0u8; 32];
            nf[0..4].copy_from_slice(&i.to_le_bytes());
            let bucket = hash_to_bucket(&nf);
            *counts.entry(bucket).or_default() += 1;
        }
        let max_count = counts.values().max().copied().unwrap_or(0);
        assert!(max_count < 10, "max bucket count {max_count} is too high");
    }

    #[test]
    fn test_hash_to_bucket_different_inputs() {
        let nf_a = [1u8; 32];
        let nf_b = [2u8; 32];
        let a = hash_to_bucket(&nf_a);
        let b = hash_to_bucket(&nf_b);
        assert_ne!(a, b);
    }

    #[test]
    fn test_constants_consistency() {
        assert_eq!(ENTRY_BYTES, 41);
        assert_eq!(BUCKET_BYTES, BUCKET_CAPACITY * ENTRY_BYTES);
        assert_eq!(DB_BYTES, NUM_BUCKETS * BUCKET_BYTES);
        assert!(NUM_BUCKETS.is_power_of_two());
    }

    #[test]
    fn test_nullifier_entry_roundtrip() {
        let entry = NullifierEntry {
            nullifier: [0xAA; 32],
            spend_height: 2_800_000,
            first_output_position: 12_345_678,
            action_count: 4,
        };
        let bytes = entry.to_bytes();
        assert_eq!(bytes.len(), ENTRY_BYTES);
        let restored = NullifierEntry::from_bytes(bytes);
        assert_eq!(entry, restored);
    }

    #[test]
    fn test_nullifier_entry_zero_is_empty() {
        assert!(NullifierEntry::ZERO.is_empty());
        let non_zero = NullifierEntry {
            nullifier: [1; 32],
            ..NullifierEntry::ZERO
        };
        assert!(!non_zero.is_empty());
    }

    #[test]
    fn test_spend_metadata_from_entry_tail() {
        let entry = NullifierEntry {
            nullifier: [0xFF; 32],
            spend_height: 100,
            first_output_position: 5000,
            action_count: 2,
        };
        let bytes = entry.to_bytes();
        let tail: [u8; 9] = bytes[32..41].try_into().unwrap();
        let meta = SpendMetadata::from_entry_tail(&tail);
        assert_eq!(meta.spend_height, 100);
        assert_eq!(meta.first_output_position, 5000);
        assert_eq!(meta.action_count, 2);
    }
}
