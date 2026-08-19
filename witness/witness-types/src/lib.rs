//! Types and constants for the witness PIR subsystem.
//!
//! Defines the Orchard note commitment tree geometry, the [`PirWitness`] type
//! returned by the witness client, and broadcast metadata structures. Uses raw
//! byte arrays ([`Hash`] = `[u8; 32]`) rather than `orchard` crate types so
//! this crate stays lightweight; conversions to/from `MerkleHashOrchard` happen
//! in the tree and client crates where those dependencies exist.

use serde::{Deserialize, Serialize};

/// Orchard note commitment tree depth (32 levels, 2^32 leaf positions).
pub const TREE_DEPTH: usize = 32;

/// Number of levels per shard (top tier). Each shard covers 2^16 = 65,536 leaves.
pub const SHARD_HEIGHT: usize = 16;

/// Number of levels per sub-shard within a shard. Each sub-shard covers 2^8 = 256 leaves.
pub const SUBSHARD_HEIGHT: usize = 8;

/// Number of leaves in a single sub-shard.
pub const SUBSHARD_LEAVES: usize = 1 << SUBSHARD_HEIGHT; // 256

/// Number of sub-shards per shard.
pub const SUBSHARDS_PER_SHARD: usize = 1 << SUBSHARD_HEIGHT; // 256

/// Number of leaves per shard.
pub const SHARD_LEAVES: usize = 1 << SHARD_HEIGHT; // 65,536

/// Number of bytes per sub-shard PIR row (256 leaves x 32 bytes).
pub const SUBSHARD_ROW_BYTES: usize = SUBSHARD_LEAVES * 32; // 8,192

/// Padded PIR database row count for L0 (32 shards x 256 sub-shards).
pub const L0_DB_ROWS: usize = 8_192;

/// L0 PIR database size in bytes.
pub const L0_DB_BYTES: usize = L0_DB_ROWS * SUBSHARD_ROW_BYTES; // 64 MB

/// Maximum number of shards in L0 before eviction/tiering.
pub const L0_MAX_SHARDS: usize = 32;

/// Raw 32-byte hash used throughout the witness system.
/// Represents a Sinsemilla hash (Orchard `MerkleHashOrchard`) in serialized form.
pub type Hash = [u8; 32];

/// Complete witness bundle for a note at a specific tree position.
///
/// Contains all 32 sibling hashes (leaf-to-root), the anchor height, and the
/// expected root for self-verification. Convertible to
/// `incrementalmerkletree::MerklePath<MerkleHashOrchard>` at the integration
/// boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PirWitness {
    /// Leaf position in the commitment tree (0-indexed).
    pub position: u64,
    /// Authentication path siblings, ordered leaf-to-root (level 0 first).
    pub siblings: [Hash; TREE_DEPTH],
    /// Block height at which this witness is anchored.
    pub anchor_height: u64,
    /// Expected tree root at `anchor_height` (for self-verification).
    pub anchor_root: Hash,
}

impl PirWitness {
    /// Shard index for this witness's position (top 16 bits).
    pub fn shard_index(&self) -> u32 {
        (self.position >> SHARD_HEIGHT) as u32
    }

    /// Sub-shard index within the shard (middle 8 bits).
    pub fn subshard_index(&self) -> u8 {
        ((self.position >> SUBSHARD_HEIGHT) & 0xFF) as u8
    }

    /// Leaf index within the sub-shard (bottom 8 bits).
    pub fn leaf_index(&self) -> u8 {
        (self.position & 0xFF) as u8
    }
}

/// Decompose a tree position into (shard_index, subshard_index, leaf_index).
pub fn decompose_position(position: u64) -> (u32, u8, u8) {
    let shard_index = (position >> SHARD_HEIGHT) as u32;
    let subshard_index = ((position >> SUBSHARD_HEIGHT) & 0xFF) as u8;
    let leaf_index = (position & 0xFF) as u8;
    (shard_index, subshard_index, leaf_index)
}

/// Compute the physical PIR row index from a logical position and window offset.
pub fn physical_row_index(shard_index: u32, subshard_index: u8, window_start_shard: u32) -> usize {
    ((shard_index - window_start_shard) as usize) * SUBSHARDS_PER_SHARD + subshard_index as usize
}

/// Cap tree data: all populated shard roots, used by the client to reconstruct
/// the top 16 levels of the authentication path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapData {
    /// Shard roots indexed by shard number. The vec length equals the number of
    /// populated shards (completed + frontier).
    pub shard_roots: Vec<Hash>,
}

/// Sub-shard roots for a single shard (256 roots, one per sub-shard).
/// Uses `Vec<Hash>` rather than a fixed array for serde compatibility.
/// Consumers should verify `len() == SUBSHARDS_PER_SHARD`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSubRoots {
    pub roots: Vec<Hash>,
}

/// Full broadcast payload downloaded periodically by the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastData {
    /// All populated shard roots for the cap tree.
    pub cap: CapData,
    /// Sub-shard roots for each shard in the active PIR window.
    /// Outer vec indexed by `shard_index - window_start_shard`.
    /// Each inner `ShardSubRoots` contains `SUBSHARDS_PER_SHARD` entries.
    pub subshard_roots: Vec<ShardSubRoots>,
    /// First shard index in the PIR database window.
    pub window_start_shard: u32,
    /// Number of shards in the PIR database window.
    pub window_shard_count: u32,
    /// Block height at which this broadcast is anchored.
    pub anchor_height: u64,
}

/// Public data needed to advance an existing witness to a newer block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrontierUpdate {
    /// Block height represented by this update.
    pub height: u64,
    /// Orchard commitment tree size after this block.
    pub tree_size: u64,
    /// Orchard commitment tree root after this block.
    pub root: Hash,
    /// Hash of the rightmost populated node at each level, leaf-to-root.
    pub rightmost_nodes: [Hash; TREE_DEPTH],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrontierRange {
    pub from: u64,
    pub to: u64,
}

/// Chain event specific to the witness ingest pipeline.
#[derive(Debug, Clone)]
pub enum WitnessChainEvent {
    /// A new block with note commitments was ingested.
    NewBlock {
        height: u64,
        hash: [u8; 32],
        prev_hash: [u8; 32],
        /// Orchard note commitments (`cmx`) extracted from this block, in order.
        commitments: Vec<Hash>,
        /// Orchard commitment tree size at the start of this block (from the
        /// previous block's `ChainMetadata.orchardCommitmentTreeSize`).
        /// `None` for the first block or when metadata is unavailable.
        prior_tree_size: Option<u32>,
    },
    /// A reorg was detected; roll back to the given height (exclusive).
    Reorg { rollback_to: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geometry_consistency() {
        assert_eq!(SHARD_HEIGHT + SUBSHARD_HEIGHT + SUBSHARD_HEIGHT, TREE_DEPTH);
        assert_eq!(SUBSHARD_LEAVES, 256);
        assert_eq!(SUBSHARDS_PER_SHARD, 256);
        assert_eq!(SHARD_LEAVES, 65_536);
        assert_eq!(SUBSHARD_ROW_BYTES, 8_192);
        assert_eq!(L0_DB_ROWS, 8_192);
        assert_eq!(L0_DB_BYTES, 64 * 1024 * 1024);
        assert_eq!(L0_MAX_SHARDS, 32);
        assert_eq!(L0_MAX_SHARDS * SUBSHARDS_PER_SHARD, L0_DB_ROWS);
    }

    #[test]
    fn decompose_position_roundtrip() {
        let position: u64 = (5u64 << 16) | (200u64 << 8) | 42;
        let (shard, subshard, leaf) = decompose_position(position);
        assert_eq!(shard, 5);
        assert_eq!(subshard, 200);
        assert_eq!(leaf, 42);

        let reconstructed = (shard as u64) << 16 | (subshard as u64) << 8 | leaf as u64;
        assert_eq!(reconstructed, position);
    }

    #[test]
    fn decompose_position_boundaries() {
        let (s, ss, l) = decompose_position(0);
        assert_eq!((s, ss, l), (0, 0, 0));

        let (s, ss, l) = decompose_position(255);
        assert_eq!((s, ss, l), (0, 0, 255));

        let (s, ss, l) = decompose_position(256);
        assert_eq!((s, ss, l), (0, 1, 0));

        let (s, ss, l) = decompose_position(65_535);
        assert_eq!((s, ss, l), (0, 255, 255));

        let (s, ss, l) = decompose_position(65_536);
        assert_eq!((s, ss, l), (1, 0, 0));
    }

    #[test]
    fn physical_row_index_basic() {
        assert_eq!(physical_row_index(0, 0, 0), 0);
        assert_eq!(physical_row_index(0, 1, 0), 1);
        assert_eq!(physical_row_index(0, 255, 0), 255);
        assert_eq!(physical_row_index(1, 0, 0), 256);
        assert_eq!(physical_row_index(1, 1, 0), 257);
    }

    #[test]
    fn physical_row_index_with_window_offset() {
        assert_eq!(physical_row_index(10, 0, 10), 0);
        assert_eq!(physical_row_index(10, 5, 10), 5);
        assert_eq!(physical_row_index(11, 0, 10), 256);
    }

    #[test]
    fn pir_witness_decomposition() {
        let witness = PirWitness {
            position: (3u64 << 16) | (100u64 << 8) | 77,
            siblings: [[0u8; 32]; TREE_DEPTH],
            anchor_height: 2_000_000,
            anchor_root: [0xAA; 32],
        };
        assert_eq!(witness.shard_index(), 3);
        assert_eq!(witness.subshard_index(), 100);
        assert_eq!(witness.leaf_index(), 77);
    }

    #[test]
    fn cap_data_serde_roundtrip() {
        let cap = CapData {
            shard_roots: vec![[1u8; 32], [2u8; 32], [3u8; 32]],
        };
        let json = serde_json::to_string(&cap).unwrap();
        let decoded: CapData = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.shard_roots.len(), 3);
        assert_eq!(decoded.shard_roots[0], [1u8; 32]);
    }

    #[test]
    fn broadcast_data_serde_roundtrip() {
        let roots = ShardSubRoots {
            roots: vec![[0xBB; 32]; SUBSHARDS_PER_SHARD],
        };
        let broadcast = BroadcastData {
            cap: CapData {
                shard_roots: vec![[0xAA; 32]],
            },
            subshard_roots: vec![roots],
            window_start_shard: 0,
            window_shard_count: 1,
            anchor_height: 2_500_000,
        };
        let json = serde_json::to_string(&broadcast).unwrap();
        let decoded: BroadcastData = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.window_start_shard, 0);
        assert_eq!(decoded.window_shard_count, 1);
        assert_eq!(decoded.anchor_height, 2_500_000);
        assert_eq!(decoded.subshard_roots.len(), 1);
        assert_eq!(decoded.subshard_roots[0].roots.len(), SUBSHARDS_PER_SHARD);
    }

    #[test]
    fn frontier_update_serde_roundtrip() {
        let update = FrontierUpdate {
            height: 2_500_001,
            tree_size: 123_456,
            root: [0xAA; 32],
            rightmost_nodes: [[0xBB; 32]; TREE_DEPTH],
        };
        let json = serde_json::to_string(&update).unwrap();
        let decoded: FrontierUpdate = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, update);
    }

    #[test]
    fn witness_chain_event_variants() {
        let new_block = WitnessChainEvent::NewBlock {
            height: 100,
            hash: [1u8; 32],
            prev_hash: [0u8; 32],
            commitments: vec![[0xCC; 32], [0xDD; 32]],
            prior_tree_size: Some(500),
        };
        if let WitnessChainEvent::NewBlock {
            commitments,
            prior_tree_size,
            ..
        } = &new_block
        {
            assert_eq!(commitments.len(), 2);
            assert_eq!(*prior_tree_size, Some(500));
        } else {
            panic!("expected NewBlock");
        }

        let reorg = WitnessChainEvent::Reorg { rollback_to: 99 };
        if let WitnessChainEvent::Reorg { rollback_to } = reorg {
            assert_eq!(rollback_to, 99);
        } else {
            panic!("expected Reorg");
        }
    }
}
