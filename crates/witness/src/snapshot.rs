//! Snapshot serialization and deserialization for [`CommitmentTreeDb`].
//!
//! Ironwood binary format v5 (all integers little-endian):
//!
//! | Field                 | Size     | Description                                    |
//! |-----------------------|----------|------------------------------------------------|
//! | magic                 | 8 bytes  | `0x434D_5452_4545_0005` (version 5)            |
//! | block_count           | 8 bytes  | Number of block records                        |
//! | leaf_offset           | 8 bytes  | Global position of first stored leaf            |
//! | prefetched_count      | 8 bytes  | Number of prefetched shard roots                |
//! | prefetched_roots      | variable | `prefetched_count` × 32 bytes                   |
//! | block_records         | variable | `block_count` × (height: 8, hash: 32, n_cmx: 4)|
//! | leaf_data             | variable | `local_leaves` × 32 bytes                      |
//! | cached_count          | 8 bytes  | Number of cached sub-shard root entries         |
//! | cached_entries        | variable | `cached_count` × (slot: 4, root: 32)            |
//! | checksum              | 8 bytes  | xxHash64 over everything preceding              |

use super::{BlockRecord, CommitmentTreeDb, TreeError};
use crate::L0_DB_ROWS;
use xxhash_rust::xxh64::xxh64;

const SNAPSHOT_MAGIC: u64 = 0x434D_5452_4545_0005;
const BLOCK_RECORD_SIZE: usize = 8 + 32 + 4; // height + hash + num_commitments

impl CommitmentTreeDb {
    /// Serialize the current Ironwood tree state.
    pub fn to_snapshot(&self) -> Vec<u8> {
        let block_count = self.blocks().len();
        let local_leaves = self.leaves().len();
        let prefetched_count = self.prefetched_shard_roots().len();
        let estimated = 8
            + 8
            + 8
            + 8
            + prefetched_count * 32
            + block_count * BLOCK_RECORD_SIZE
            + local_leaves * 32
            + 8;
        let mut buf = Vec::with_capacity(estimated);

        // Header
        buf.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
        buf.extend_from_slice(&(block_count as u64).to_le_bytes());
        buf.extend_from_slice(&self.leaf_offset().to_le_bytes());
        buf.extend_from_slice(&(prefetched_count as u64).to_le_bytes());

        for root in self.prefetched_shard_roots() {
            buf.extend_from_slice(root);
        }

        // Block records
        for block in self.blocks() {
            buf.extend_from_slice(&block.height.to_le_bytes());
            buf.extend_from_slice(&block.hash);
            buf.extend_from_slice(&block.num_commitments.to_le_bytes());
        }

        // Leaf data
        for leaf in self.leaves() {
            buf.extend_from_slice(leaf);
        }

        let cached_count = self.ss_root_cache().iter().flatten().count();
        buf.extend_from_slice(&(cached_count as u64).to_le_bytes());
        for (slot, root) in self
            .ss_root_cache()
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| opt.map(|root| (i as u32, root)))
        {
            buf.extend_from_slice(&slot.to_le_bytes());
            buf.extend_from_slice(&root);
        }

        // Checksum
        let checksum = xxh64(&buf, 0);
        buf.extend_from_slice(&checksum.to_le_bytes());

        buf
    }

    /// Restore a tree from an Ironwood v5 snapshot.
    pub fn from_snapshot(data: &[u8]) -> Result<Self, TreeError> {
        let min_size = 8 + 8; // magic + at least a checksum
        if data.len() < min_size {
            return Err(TreeError::SnapshotCorrupted {
                reason: "data too short".into(),
            });
        }

        // Verify checksum
        let payload = &data[..data.len() - 8];
        let stored_checksum =
            u64::from_le_bytes(data[data.len() - 8..].try_into().map_err(|_| {
                TreeError::SnapshotCorrupted {
                    reason: "checksum read failed".into(),
                }
            })?);
        let computed_checksum = xxh64(payload, 0);
        if stored_checksum != computed_checksum {
            return Err(TreeError::SnapshotCorrupted {
                reason: "checksum mismatch".into(),
            });
        }

        let mut pos = 0;
        let read_u64 = |pos: &mut usize| -> Result<u64, TreeError> {
            if *pos + 8 > payload.len() {
                return Err(TreeError::SnapshotCorrupted {
                    reason: "unexpected EOF reading u64".into(),
                });
            }
            let val = u64::from_le_bytes(payload[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            Ok(val)
        };

        let magic = read_u64(&mut pos)?;
        match magic {
            SNAPSHOT_MAGIC => Self::decode_snapshot(payload, pos),
            _ => Err(TreeError::SnapshotCorrupted {
                reason: format!("bad magic: {magic:#018x}"),
            }),
        }
    }

    fn decode_snapshot(payload: &[u8], mut pos: usize) -> Result<Self, TreeError> {
        let read_u64 = |pos: &mut usize| -> Result<u64, TreeError> {
            if *pos + 8 > payload.len() {
                return Err(TreeError::SnapshotCorrupted {
                    reason: "unexpected EOF reading u64".into(),
                });
            }
            let val = u64::from_le_bytes(payload[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            Ok(val)
        };

        let block_count = read_u64(&mut pos)? as usize;
        let leaf_offset = read_u64(&mut pos)?;
        let prefetched_count = read_u64(&mut pos)? as usize;

        let mut prefetched_shard_roots = Vec::with_capacity(prefetched_count);
        for _ in 0..prefetched_count {
            if pos + 32 > payload.len() {
                return Err(TreeError::SnapshotCorrupted {
                    reason: "prefetched roots truncated".into(),
                });
            }
            let mut root = [0u8; 32];
            root.copy_from_slice(&payload[pos..pos + 32]);
            pos += 32;
            prefetched_shard_roots.push(root);
        }

        let (blocks, local_leaves) = Self::read_block_records(payload, &mut pos, block_count)?;
        let leaves = Self::read_leaves(payload, &mut pos, local_leaves)?;

        // Read sub-shard root cache
        let cached_count = read_u64(&mut pos)? as usize;
        let mut ss_root_cache = vec![None; L0_DB_ROWS];
        for _ in 0..cached_count {
            if pos + 4 + 32 > payload.len() {
                return Err(TreeError::SnapshotCorrupted {
                    reason: "cache entry truncated".into(),
                });
            }
            let slot = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let mut root = [0u8; 32];
            root.copy_from_slice(&payload[pos..pos + 32]);
            pos += 32;
            if slot < L0_DB_ROWS {
                ss_root_cache[slot] = Some(root);
            }
        }

        if prefetched_shard_roots
            .iter()
            .chain(&leaves)
            .chain(ss_root_cache.iter().flatten())
            .any(|hash| !CommitmentTreeDb::is_canonical_hash(hash))
        {
            return Err(TreeError::SnapshotCorrupted {
                reason: "non-canonical Ironwood tree hash".into(),
            });
        }

        let mut tree = CommitmentTreeDb::with_offset(leaf_offset, prefetched_shard_roots);
        tree.leaves = leaves;
        tree.blocks = blocks;
        tree.ss_root_cache = ss_root_cache;
        Ok(tree)
    }

    fn read_block_records(
        payload: &[u8],
        pos: &mut usize,
        count: usize,
    ) -> Result<(Vec<BlockRecord>, usize), TreeError> {
        let end = count
            .checked_mul(BLOCK_RECORD_SIZE)
            .and_then(|bytes| pos.checked_add(bytes))
            .ok_or_else(|| TreeError::SnapshotCorrupted {
                reason: "block record size overflow".into(),
            })?;
        if end > payload.len() {
            return Err(TreeError::SnapshotCorrupted {
                reason: "block records truncated".into(),
            });
        }

        let mut blocks = Vec::with_capacity(count);
        let mut total = 0usize;
        for _ in 0..count {
            let height = u64::from_le_bytes(payload[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&payload[*pos..*pos + 32]);
            *pos += 32;
            let num_commitments = u32::from_le_bytes(payload[*pos..*pos + 4].try_into().unwrap());
            *pos += 4;
            total += num_commitments as usize;
            blocks.push(BlockRecord {
                height,
                hash,
                num_commitments,
            });
        }
        Ok((blocks, total))
    }

    fn read_leaves(
        payload: &[u8],
        pos: &mut usize,
        count: usize,
    ) -> Result<Vec<[u8; 32]>, TreeError> {
        let end = count
            .checked_mul(32)
            .and_then(|bytes| pos.checked_add(bytes))
            .ok_or_else(|| TreeError::SnapshotCorrupted {
                reason: "leaf data size overflow".into(),
            })?;
        if end > payload.len() {
            return Err(TreeError::SnapshotCorrupted {
                reason: "leaf data truncated".into(),
            });
        }
        let mut leaves = Vec::with_capacity(count);
        for _ in 0..count {
            let mut leaf = [0u8; 32];
            leaf.copy_from_slice(&payload[*pos..*pos + 32]);
            *pos += 32;
            leaves.push(leaf);
        }
        Ok(leaves)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Hash;

    fn make_leaf(byte: u8) -> Hash {
        let mut h = [0u8; 32];
        h[0] = byte;
        h
    }

    #[test]
    fn empty_snapshot_roundtrip() {
        let tree = CommitmentTreeDb::new();
        let snap = tree.to_snapshot();
        let restored = CommitmentTreeDb::from_snapshot(&snap).unwrap();

        assert_eq!(restored.tree_size(), 0);
        assert!(restored.blocks().is_empty());
        assert_eq!(restored.tree_root(), tree.tree_root());
    }

    #[test]
    fn populated_snapshot_roundtrip() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [0xAA; 32], &[make_leaf(1), make_leaf(2)]);
        tree.append_commitments(101, [0xBB; 32], &[make_leaf(3)]);

        let snap = tree.to_snapshot();
        let restored = CommitmentTreeDb::from_snapshot(&snap).unwrap();

        assert_eq!(restored.tree_size(), 3);
        assert_eq!(restored.blocks().len(), 2);
        assert_eq!(restored.latest_height(), Some(101));
        assert_eq!(restored.latest_block_hash(), Some([0xBB; 32]));
        assert_eq!(restored.tree_root(), tree.tree_root());
    }

    #[test]
    fn snapshot_detects_corruption() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1)]);

        let mut snap = tree.to_snapshot();
        let mid = snap.len() / 2;
        snap[mid] ^= 0xFF;

        let result = CommitmentTreeDb::from_snapshot(&snap);
        assert!(result.is_err());
    }

    #[test]
    fn snapshot_detects_truncation() {
        let result = CommitmentTreeDb::from_snapshot(&[0u8; 10]);
        assert!(result.is_err());
    }

    #[test]
    fn snapshot_preserves_rollback_ability() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1), make_leaf(2)]);
        tree.append_commitments(101, [2u8; 32], &[make_leaf(3)]);

        let snap = tree.to_snapshot();
        let mut restored = CommitmentTreeDb::from_snapshot(&snap).unwrap();
        let root_before = restored.tree_root();

        restored.rollback_to(100);
        assert_eq!(restored.tree_size(), 2);
        assert_eq!(restored.latest_height(), Some(100));
        assert_ne!(restored.tree_root(), root_before);
    }

    #[test]
    fn snapshot_preserves_cache() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [0xAA; 32], &[make_leaf(1), make_leaf(2)]);
        tree.build_pir_db_and_broadcast(100);

        // Cache should be warm for slot 0
        assert!(tree.ss_root_cache()[0].is_some());
        let cached_root = tree.ss_root_cache()[0].unwrap();

        let snap = tree.to_snapshot();
        let restored = CommitmentTreeDb::from_snapshot(&snap).unwrap();

        assert!(
            restored.ss_root_cache()[0].is_some(),
            "cache must survive snapshot roundtrip"
        );
        assert_eq!(restored.ss_root_cache()[0].unwrap(), cached_root);
        assert_eq!(restored.tree_size(), tree.tree_size());
    }

    #[test]
    fn windowed_snapshot_roundtrip() {
        let prefetched = vec![make_leaf(0xAA), make_leaf(0xBB), make_leaf(0xCC)];
        let offset = 3 * crate::SHARD_LEAVES as u64;
        let mut tree = CommitmentTreeDb::with_offset(offset, prefetched.clone());
        tree.append_commitments(200, [0xDD; 32], &[make_leaf(1), make_leaf(2)]);

        let snap = tree.to_snapshot();
        let restored = CommitmentTreeDb::from_snapshot(&snap).unwrap();

        assert_eq!(restored.tree_size(), tree.tree_size());
        assert_eq!(restored.leaf_offset(), offset);
        assert_eq!(restored.prefetched_shard_roots().len(), 3);
        assert_eq!(restored.prefetched_shard_roots()[0], make_leaf(0xAA));
        assert_eq!(restored.prefetched_shard_roots()[2], make_leaf(0xCC));
        assert_eq!(restored.leaves().len(), 2);
        assert_eq!(restored.latest_height(), Some(200));
        assert_eq!(restored.window_start_shard(), 3);
    }
}
