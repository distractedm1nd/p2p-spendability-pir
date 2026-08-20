//! In-memory Ironwood note commitment tree with shard/sub-shard decomposition.
//!
//! Maintains an append-only leaf store with per-block rollback support.
//! Computes shard roots, sub-shard roots, and serializes PIR database rows
//! using protocol-correct Sinsemilla hashing via [`MerkleHashOrchard`].
//!
//! # Windowed mode
//!
//! When constructed via [`CommitmentTreeDb::with_offset`], only leaves within
//! the PIR window are stored. Shard roots for earlier shards are provided via
//! `prefetched_shard_roots` (from lightwalletd's `GetSubtreeRoots`), avoiding
//! the need to sync the entire chain from NU6.3 activation.
//!
//! # Operations
//!
//! - [`CommitmentTreeDb::append_commitments`] — extend the tree with a block's commitments
//! - [`CommitmentTreeDb::rollback_to`] — handle chain reorgs
//! - [`CommitmentTreeDb::shard_roots`] — all populated shard roots for cap construction
//! - [`CommitmentTreeDb::subshard_roots`] — 256 sub-shard roots for a given shard
//! - [`CommitmentTreeDb::subshard_leaves`] — 256 leaf commitments for a given sub-shard
//! - [`CommitmentTreeDb::build_pir_db`] — row-major bytes for YPIR setup
//! - [`CommitmentTreeDb::broadcast_data`] — full broadcast payload
//! - Snapshot/restore via [`CommitmentTreeDb::to_snapshot`] / [`from_snapshot`]

#[path = "snapshot.rs"]
mod snapshot;

use crate::*;
use incrementalmerkletree::{Hashable, Level};
use orchard::tree::MerkleHashOrchard;

/// Per-block record tracking how many commitments each block contributed.
/// Used for rollback support: removing a block removes its leaves from the end.
#[derive(Debug, Clone)]
pub struct BlockRecord {
    pub height: u64,
    pub hash: [u8; 32],
    pub num_commitments: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum TreeError {
    #[error("shard index {shard_idx} out of range (populated: {populated})")]
    ShardOutOfRange { shard_idx: u32, populated: u32 },

    #[error("snapshot corrupted: {reason}")]
    SnapshotCorrupted { reason: String },
}

/// In-memory Ironwood note commitment tree.
///
/// Stores leaf commitments within the PIR window and per-block metadata for
/// rollback. Hash computations use [`MerkleHashOrchard`] (Sinsemilla) with
/// level-dependent empty-subtree sentinels from the Orchard protocol.
///
/// In windowed mode (`leaf_offset > 0`), leaves before the window are not
/// stored; their shard roots come from `prefetched_shard_roots`.
pub struct CommitmentTreeDb {
    /// Leaf commitments in append order. `leaves[i]` corresponds to global
    /// tree position `leaf_offset + i`.
    leaves: Vec<Hash>,
    /// Per-block records ordered by height, for rollback support.
    blocks: Vec<BlockRecord>,
    /// Precomputed empty subtree roots for levels `0..=TREE_DEPTH`.
    empty_roots: Vec<Hash>,
    /// Global tree position of `leaves[0]`. Zero for full trees, non-zero
    /// when only storing leaves within the PIR window.
    leaf_offset: u64,
    /// Shard roots for shards before the window, obtained from lightwalletd's
    /// `GetSubtreeRoots`. Indexed 0..N where shard `i` in the global tree
    /// maps to `prefetched_shard_roots[i]`.
    prefetched_shard_roots: Vec<Hash>,
    /// Cached sub-shard roots indexed by local sub-shard position within the
    /// window. `ss_root_cache[i]` = `Some(root)` if clean, `None` if dirty.
    /// Length is always [`L0_DB_ROWS`] (8,192).
    ss_root_cache: Vec<Option<Hash>>,
}

impl CommitmentTreeDb {
    pub fn is_canonical_hash(hash: &Hash) -> bool {
        bool::from(MerkleHashOrchard::from_bytes(hash).is_some())
    }

    pub fn new() -> Self {
        Self {
            leaves: Vec::new(),
            blocks: Vec::new(),
            empty_roots: precompute_empty_roots(),
            leaf_offset: 0,
            prefetched_shard_roots: Vec::new(),
            ss_root_cache: vec![None; L0_DB_ROWS],
        }
    }

    /// Create a tree that only stores leaves starting at `leaf_offset`.
    ///
    /// `prefetched_shard_roots` contains shard roots for all shards before the
    /// window (shards `0..window_start_shard`), typically from `GetSubtreeRoots`.
    /// `leaf_offset` must be shard-aligned (`leaf_offset % SHARD_LEAVES == 0`).
    pub fn with_offset(leaf_offset: u64, prefetched_shard_roots: Vec<Hash>) -> Self {
        debug_assert_eq!(
            leaf_offset as usize % SHARD_LEAVES,
            0,
            "leaf_offset must be shard-aligned"
        );
        Self {
            leaves: Vec::new(),
            blocks: Vec::new(),
            empty_roots: precompute_empty_roots(),
            leaf_offset,
            prefetched_shard_roots,
            ss_root_cache: vec![None; L0_DB_ROWS],
        }
    }

    /// Total number of leaves in the global tree (offset + local leaves).
    pub fn tree_size(&self) -> u64 {
        self.leaf_offset + self.leaves.len() as u64
    }

    /// Height of the most recently ingested block, if any.
    pub fn latest_height(&self) -> Option<u64> {
        self.blocks.last().map(|b| b.height)
    }

    /// Hash of the most recently ingested block, if any.
    pub fn latest_block_hash(&self) -> Option<[u8; 32]> {
        self.blocks.last().map(|b| b.hash)
    }

    /// Number of populated shards in the global tree (completed + frontier).
    pub fn populated_shards(&self) -> u32 {
        let total = self.tree_size();
        if total == 0 {
            0
        } else {
            ((total as usize - 1) / SHARD_LEAVES + 1) as u32
        }
    }

    /// First shard index in the PIR window.
    pub fn window_start_shard(&self) -> u32 {
        (self.leaf_offset as usize / SHARD_LEAVES) as u32
    }

    /// Number of shards with local leaf data (window shards).
    fn local_shard_count(&self) -> u32 {
        if self.leaves.is_empty() {
            return 0;
        }
        let first_local_shard = self.window_start_shard();
        let last_local_shard = ((self.tree_size() as usize - 1) / SHARD_LEAVES) as u32;
        last_local_shard - first_local_shard + 1
    }

    /// Number of shards in the PIR window (capped at [`L0_MAX_SHARDS`]).
    pub fn window_shard_count(&self) -> u32 {
        self.local_shard_count().min(L0_MAX_SHARDS as u32)
    }

    /// Global leaf offset.
    pub fn leaf_offset(&self) -> u64 {
        self.leaf_offset
    }

    /// Pre-fetched shard roots for shards before the window.
    pub fn prefetched_shard_roots(&self) -> &[Hash] {
        &self.prefetched_shard_roots
    }

    /// Block records (for snapshot serialization).
    pub fn blocks(&self) -> &[BlockRecord] {
        &self.blocks
    }

    /// Raw leaf data (for snapshot serialization).
    pub fn leaves(&self) -> &[Hash] {
        &self.leaves
    }

    /// Reference to precomputed empty roots.
    pub fn empty_roots(&self) -> &[Hash] {
        &self.empty_roots
    }

    /// Sub-shard root cache (for snapshot serialization).
    pub fn ss_root_cache(&self) -> &[Option<Hash>] {
        &self.ss_root_cache
    }

    // ── Mutation ──────────────────────────────────────────────────────

    /// Append note commitments from a newly ingested block.
    /// Invalidates only the sub-shard cache slots touched by the new leaves.
    pub fn append_commitments(&mut self, height: u64, hash: [u8; 32], commitments: &[Hash]) {
        if !commitments.is_empty() {
            let old_local_len = self.leaves.len();
            self.leaves.extend_from_slice(commitments);
            let new_local_len = self.leaves.len();

            let first_dirty = old_local_len / SUBSHARD_LEAVES;
            let last_dirty = (new_local_len - 1) / SUBSHARD_LEAVES;
            for slot in first_dirty..=last_dirty.min(L0_DB_ROWS - 1) {
                self.ss_root_cache[slot] = None;
            }
        }
        self.blocks.push(BlockRecord {
            height,
            hash,
            num_commitments: commitments.len() as u32,
        });
    }

    /// Roll back all blocks with height strictly greater than `target_height`.
    pub fn rollback_to(&mut self, target_height: u64) {
        let mut to_remove: usize = 0;
        while let Some(last) = self.blocks.last() {
            if last.height > target_height {
                to_remove += last.num_commitments as usize;
                self.blocks.pop();
            } else {
                break;
            }
        }
        let new_len = self.leaves.len().saturating_sub(to_remove);
        self.leaves.truncate(new_len);

        let frontier_slot = if new_len == 0 {
            0
        } else {
            (new_len - 1) / SUBSHARD_LEAVES
        };
        for slot in frontier_slot..L0_DB_ROWS {
            self.ss_root_cache[slot] = None;
        }
    }

    // ── Leaf / root queries ──────────────────────────────────────────

    /// Retrieve the 256 leaf commitments for a sub-shard.
    ///
    /// The shard must be within the local window (`>= window_start_shard`).
    /// Positions beyond the tree's current frontier are filled with
    /// `MerkleHashOrchard::empty_root(Level::from(0))` (the empty leaf
    /// sentinel), **not** zero bytes.
    pub fn subshard_leaves(&self, shard_idx: u32, subshard_idx: u8) -> Vec<Hash> {
        let global_start =
            (shard_idx as usize) * SHARD_LEAVES + (subshard_idx as usize) * SUBSHARD_LEAVES;
        let empty_leaf = self.empty_roots[0];
        (0..SUBSHARD_LEAVES)
            .map(|i| {
                let global_pos = global_start + i;
                let local_pos = global_pos as u64 - self.leaf_offset;
                if global_pos >= self.leaf_offset as usize
                    && (local_pos as usize) < self.leaves.len()
                {
                    self.leaves[local_pos as usize]
                } else {
                    empty_leaf
                }
            })
            .collect()
    }

    /// Compute the 256 sub-shard roots for a given shard.
    ///
    /// The shard must be within the local window.
    /// Sub-shards entirely beyond the tree frontier use the precomputed
    /// `empty_root(SUBSHARD_HEIGHT)` without recomputing Sinsemilla hashes.
    pub fn subshard_roots(&self, shard_idx: u32) -> Vec<Hash> {
        let shard_global_start = (shard_idx as usize) * SHARD_LEAVES;
        let empty_ss_root = self.empty_roots[SUBSHARD_HEIGHT];
        let total = self.tree_size() as usize;

        (0..SUBSHARDS_PER_SHARD)
            .map(|ss| {
                let ss_global_start = shard_global_start + ss * SUBSHARD_LEAVES;
                if ss_global_start >= total {
                    return empty_ss_root;
                }
                let leaves = self.subshard_leaves(shard_idx, ss as u8);
                self.complete_subtree_root(&leaves, 0)
            })
            .collect()
    }

    /// Compute shard roots for all populated shards.
    ///
    /// For shards before the window, returns pre-fetched roots. For window
    /// shards, computes from leaf data.
    pub fn shard_roots(&self) -> Vec<(u32, Hash)> {
        let n = self.populated_shards();
        let window_start = self.window_start_shard();

        (0..n)
            .map(|i| {
                if i < window_start {
                    let root = if (i as usize) < self.prefetched_shard_roots.len() {
                        self.prefetched_shard_roots[i as usize]
                    } else {
                        self.empty_roots[SHARD_HEIGHT]
                    };
                    (i, root)
                } else {
                    let ss_roots = self.subshard_roots(i);
                    let root = self.complete_subtree_root(&ss_roots, SUBSHARD_HEIGHT as u8);
                    (i, root)
                }
            })
            .collect()
    }

    /// Compute the full depth-32 tree root at the current state.
    ///
    /// For an empty tree, returns `empty_root(TREE_DEPTH)`.
    pub fn tree_root(&self) -> Hash {
        if self.tree_size() == 0 {
            return self.empty_roots[TREE_DEPTH];
        }
        let shard_roots: Vec<Hash> = self.shard_roots().into_iter().map(|(_, h)| h).collect();
        self.sparse_subtree_root(&shard_roots, 1 << SHARD_HEIGHT, SHARD_HEIGHT as u8)
    }

    // ── Broadcast / PIR database ─────────────────────────────────────

    /// Build PIR database bytes and broadcast payload in a single pass.
    ///
    /// Iterates each sub-shard's leaves once, writing them into the PIR
    /// database and simultaneously computing sub-shard roots + shard roots.
    /// This avoids the O(8192 × 256) redundant Sinsemilla hashing that
    /// separate `build_pir_db()` + `broadcast_data()` calls would incur.
    pub fn build_pir_db_and_broadcast(&mut self, anchor_height: u64) -> (Vec<u8>, BroadcastData) {
        let window_start = self.window_start_shard();
        let window_count = self.window_shard_count();
        let n_shards = self.populated_shards();
        let total = self.tree_size() as usize;
        let empty_ss_root = self.empty_roots[SUBSHARD_HEIGHT];

        let mut cap_roots = Vec::with_capacity(n_shards as usize);
        let mut broadcast_ss = Vec::with_capacity(window_count as usize);
        let mut db = Vec::with_capacity(L0_DB_BYTES);
        let mut cache_hits: u32 = 0;
        let mut cache_misses: u32 = 0;

        // Prefetched shard roots (before the window)
        for i in 0..window_start {
            let root = if (i as usize) < self.prefetched_shard_roots.len() {
                self.prefetched_shard_roots[i as usize]
            } else {
                self.empty_roots[SHARD_HEIGHT]
            };
            cap_roots.push(root);
        }

        // Window shards: iterate leaves once, build db + roots simultaneously
        for i in 0..window_count {
            let shard_idx = window_start + i;
            let shard_global_start = (shard_idx as usize) * SHARD_LEAVES;
            let mut ss_roots = Vec::with_capacity(SUBSHARDS_PER_SHARD);

            for ss in 0..SUBSHARDS_PER_SHARD {
                let cache_slot = (i as usize) * SUBSHARDS_PER_SHARD + ss;
                let leaves = self.subshard_leaves(shard_idx, ss as u8);
                for leaf in &leaves {
                    db.extend_from_slice(leaf);
                }
                let ss_global_start = shard_global_start + ss * SUBSHARD_LEAVES;
                if ss_global_start >= total {
                    ss_roots.push(empty_ss_root);
                } else if let Some(cached) = self.ss_root_cache[cache_slot] {
                    ss_roots.push(cached);
                    cache_hits += 1;
                } else {
                    let root = self.complete_subtree_root(&leaves, 0);
                    self.ss_root_cache[cache_slot] = Some(root);
                    ss_roots.push(root);
                    cache_misses += 1;
                }
            }

            let shard_root = self.complete_subtree_root(&ss_roots, SUBSHARD_HEIGHT as u8);
            cap_roots.push(shard_root);
            broadcast_ss.push(ShardSubRoots { roots: ss_roots });
        }

        db.resize(L0_DB_BYTES, 0u8);

        tracing::info!(cache_hits, cache_misses, "subshard root cache stats");

        let broadcast = BroadcastData {
            cap: CapData {
                shard_roots: cap_roots,
            },
            subshard_roots: broadcast_ss,
            window_start_shard: window_start,
            window_shard_count: window_count,
            anchor_height,
        };

        (db, broadcast)
    }

    /// Build the full broadcast payload at the given anchor height.
    pub fn broadcast_data(&mut self, anchor_height: u64) -> BroadcastData {
        self.build_pir_db_and_broadcast(anchor_height).1
    }

    /// Build the PIR database as row-major bytes.
    ///
    /// Each row is one sub-shard: 256 leaves × 32 bytes = 8,192 bytes.
    /// Padding rows beyond the window are zero-filled.
    pub fn build_pir_db(&mut self) -> Vec<u8> {
        self.build_pir_db_and_broadcast(0).0
    }

    // ── Internal hashing ─────────────────────────────────────────────

    /// Root of a complete binary subtree from exactly `2^k` leaves.
    fn complete_subtree_root(&self, leaves: &[Hash], base_level: u8) -> Hash {
        debug_assert!(leaves.len().is_power_of_two());
        if leaves.len() == 1 {
            return leaves[0];
        }
        let mut current: Vec<Hash> = leaves.to_vec();
        let mut level = base_level;
        while current.len() > 1 {
            let mut next = Vec::with_capacity(current.len() / 2);
            for pair in current.chunks_exact(2) {
                next.push(hash_combine(level, &pair[0], &pair[1]));
            }
            current = next;
            level += 1;
        }
        current[0]
    }

    /// Root of a sparse subtree where `populated` leaves are left-packed and
    /// positions beyond `populated.len()` are empty.
    fn sparse_subtree_root(&self, populated: &[Hash], capacity: usize, base_level: u8) -> Hash {
        debug_assert!(capacity.is_power_of_two());
        self.sparse_root_rec(populated, 0, capacity, base_level)
    }

    fn sparse_root_rec(&self, leaves: &[Hash], offset: usize, size: usize, base_level: u8) -> Hash {
        if size == 1 {
            return if offset < leaves.len() {
                leaves[offset]
            } else {
                self.empty_roots[base_level as usize]
            };
        }

        let half = size / 2;
        let child_height = half.trailing_zeros() as u8;
        let combine_level = base_level + child_height;

        let left = self.sparse_root_rec(leaves, offset, half, base_level);
        let right = if offset + half >= leaves.len() {
            self.empty_roots[combine_level as usize]
        } else {
            self.sparse_root_rec(leaves, offset + half, half, base_level)
        };

        hash_combine(combine_level, &left, &right)
    }
}

impl Default for CommitmentTreeDb {
    fn default() -> Self {
        Self::new()
    }
}

// ── Free functions ───────────────────────────────────────────────────

/// Precompute empty subtree roots for levels `0..=TREE_DEPTH`.
fn precompute_empty_roots() -> Vec<Hash> {
    let mut roots = Vec::with_capacity(TREE_DEPTH + 1);
    let empty_leaf = MerkleHashOrchard::empty_leaf();
    roots.push(empty_leaf.to_bytes());

    let mut current = empty_leaf;
    for level in 0..TREE_DEPTH {
        current =
            <MerkleHashOrchard as Hashable>::combine(Level::from(level as u8), &current, &current);
        roots.push(current.to_bytes());
    }
    roots
}

/// Combine two hashes at a given level using Sinsemilla (MerkleHashOrchard).
fn hash_combine(level: u8, left: &Hash, right: &Hash) -> Hash {
    let l = bytes_to_mho(left);
    let r = bytes_to_mho(right);
    <MerkleHashOrchard as Hashable>::combine(Level::from(level), &l, &r).to_bytes()
}

fn bytes_to_mho(hash: &Hash) -> MerkleHashOrchard {
    Option::from(MerkleHashOrchard::from_bytes(hash)).expect("invalid MerkleHashOrchard bytes")
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_leaf(byte: u8) -> Hash {
        let mut h = [0u8; 32];
        h[0] = byte;
        h
    }

    #[test]
    fn empty_tree_root_matches_protocol() {
        let tree = CommitmentTreeDb::new();
        let root = tree.tree_root();
        assert_eq!(root, tree.empty_roots[TREE_DEPTH]);

        let expected = <MerkleHashOrchard as Hashable>::empty_root(Level::from(TREE_DEPTH as u8));
        assert_eq!(root, expected.to_bytes());
    }

    #[test]
    fn tree_size_tracking() {
        let mut tree = CommitmentTreeDb::new();
        assert_eq!(tree.tree_size(), 0);
        assert_eq!(tree.populated_shards(), 0);

        tree.append_commitments(100, [1u8; 32], &[make_leaf(1), make_leaf(2)]);
        assert_eq!(tree.tree_size(), 2);
        assert_eq!(tree.populated_shards(), 1);
        assert_eq!(tree.latest_height(), Some(100));

        tree.append_commitments(101, [2u8; 32], &[make_leaf(3)]);
        assert_eq!(tree.tree_size(), 3);
        assert_eq!(tree.latest_height(), Some(101));
    }

    #[test]
    fn rollback_removes_blocks_and_leaves() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1), make_leaf(2)]);
        tree.append_commitments(101, [2u8; 32], &[make_leaf(3)]);
        tree.append_commitments(102, [3u8; 32], &[make_leaf(4), make_leaf(5)]);

        assert_eq!(tree.tree_size(), 5);

        tree.rollback_to(100);
        assert_eq!(tree.tree_size(), 2);
        assert_eq!(tree.latest_height(), Some(100));
        assert_eq!(tree.blocks.len(), 1);
    }

    #[test]
    fn rollback_all() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1)]);
        tree.append_commitments(101, [2u8; 32], &[make_leaf(2)]);

        tree.rollback_to(0);
        assert_eq!(tree.tree_size(), 0);
        assert!(tree.blocks.is_empty());
    }

    #[test]
    fn subshard_leaves_padding() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1), make_leaf(2), make_leaf(3)]);

        let leaves = tree.subshard_leaves(0, 0);
        assert_eq!(leaves.len(), SUBSHARD_LEAVES);
        assert_eq!(leaves[0], make_leaf(1));
        assert_eq!(leaves[1], make_leaf(2));
        assert_eq!(leaves[2], make_leaf(3));

        let empty_leaf = tree.empty_roots[0];
        for leaf in &leaves[3..] {
            assert_eq!(
                *leaf, empty_leaf,
                "positions beyond frontier must be empty_leaf sentinel"
            );
        }
    }

    #[test]
    fn subshard_leaves_entirely_empty() {
        let tree = CommitmentTreeDb::new();
        let leaves = tree.subshard_leaves(0, 0);
        assert_eq!(leaves.len(), SUBSHARD_LEAVES);
        let empty_leaf = tree.empty_roots[0];
        for leaf in &leaves {
            assert_eq!(*leaf, empty_leaf);
        }
    }

    #[test]
    fn subshard_root_of_empty_subshard() {
        let tree = CommitmentTreeDb::new();
        let leaves = tree.subshard_leaves(0, 0);
        let root = tree.complete_subtree_root(&leaves, 0);
        assert_eq!(
            root, tree.empty_roots[SUBSHARD_HEIGHT],
            "root of 256 empty leaves at level 0 should equal empty_root(8)"
        );
    }

    #[test]
    fn shard_root_of_empty_shard() {
        let tree = CommitmentTreeDb::new();
        let ss_roots: Vec<Hash> = (0..SUBSHARDS_PER_SHARD)
            .map(|_| tree.empty_roots[SUBSHARD_HEIGHT])
            .collect();
        let root = tree.complete_subtree_root(&ss_roots, SUBSHARD_HEIGHT as u8);
        assert_eq!(
            root, tree.empty_roots[SHARD_HEIGHT],
            "root of 256 empty sub-shard roots should equal empty_root(16)"
        );
    }

    #[test]
    fn single_leaf_tree_root_is_deterministic() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(42)]);
        let root1 = tree.tree_root();

        let mut tree2 = CommitmentTreeDb::new();
        tree2.append_commitments(100, [1u8; 32], &[make_leaf(42)]);
        let root2 = tree2.tree_root();

        assert_eq!(root1, root2, "same input must produce same root");
    }

    #[test]
    fn single_leaf_tree_root_differs_from_empty() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1)]);
        let root = tree.tree_root();
        let empty = CommitmentTreeDb::new().tree_root();
        assert_ne!(
            root, empty,
            "non-empty tree root must differ from empty tree root"
        );
    }

    #[test]
    fn two_leaves_root_manual_verification() {
        let mut tree = CommitmentTreeDb::new();
        let leaf_a = make_leaf(10);
        let leaf_b = make_leaf(20);
        tree.append_commitments(100, [1u8; 32], &[leaf_a, leaf_b]);

        let pair_hash = hash_combine(0, &leaf_a, &leaf_b);
        let empty_leaf = tree.empty_roots[0];
        let empty_pair = hash_combine(0, &empty_leaf, &empty_leaf);
        assert_eq!(empty_pair, tree.empty_roots[1]);

        let level1 = hash_combine(1, &pair_hash, &tree.empty_roots[1]);

        let mut current = level1;
        for k in 2..SUBSHARD_HEIGHT {
            current = hash_combine(k as u8, &current, &tree.empty_roots[k]);
        }

        let ss_roots = tree.subshard_roots(0);
        assert_eq!(
            ss_roots[0], current,
            "sub-shard 0 root must match manual computation"
        );

        for ss in &ss_roots[1..] {
            assert_eq!(*ss, tree.empty_roots[SUBSHARD_HEIGHT]);
        }
    }

    #[test]
    fn window_counts() {
        let mut tree = CommitmentTreeDb::new();
        assert_eq!(tree.window_start_shard(), 0);
        assert_eq!(tree.window_shard_count(), 0);

        tree.append_commitments(100, [1u8; 32], &[make_leaf(1)]);
        assert_eq!(tree.window_shard_count(), 1);
    }

    #[test]
    fn pir_db_size_is_correct() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1)]);
        let db = tree.build_pir_db();
        assert_eq!(db.len(), L0_DB_BYTES, "PIR db must be exactly L0_DB_BYTES");
    }

    #[test]
    fn pir_db_contains_leaf_data() {
        let mut tree = CommitmentTreeDb::new();
        let leaf = make_leaf(0xAB);
        tree.append_commitments(100, [1u8; 32], &[leaf]);
        let db = tree.build_pir_db();

        assert_eq!(&db[..32], &leaf[..]);
    }

    #[test]
    fn pir_db_padding_is_zero() {
        let mut tree = CommitmentTreeDb::new();
        let db = tree.build_pir_db();
        assert_eq!(db.len(), L0_DB_BYTES);
        assert!(
            db.iter().all(|&b| b == 0),
            "empty tree PIR db must be all zeros"
        );
    }

    #[test]
    fn broadcast_data_structure() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1), make_leaf(2)]);

        let bd = tree.broadcast_data(100);
        assert_eq!(bd.anchor_height, 100);
        assert_eq!(bd.window_start_shard, 0);
        assert_eq!(bd.window_shard_count, 1);
        assert_eq!(bd.cap.shard_roots.len(), 1);
        assert_eq!(bd.subshard_roots.len(), 1);
        assert_eq!(bd.subshard_roots[0].roots.len(), SUBSHARDS_PER_SHARD);
    }

    #[test]
    fn shard_root_matches_cap() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1)]);

        let shard_roots = tree.shard_roots();
        let bd = tree.broadcast_data(100);

        assert_eq!(shard_roots.len(), bd.cap.shard_roots.len());
        for (i, (idx, root)) in shard_roots.iter().enumerate() {
            assert_eq!(*idx, i as u32);
            assert_eq!(*root, bd.cap.shard_roots[i]);
        }
    }

    #[test]
    fn rollback_then_reappend_produces_same_state() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1), make_leaf(2)]);
        let root_after_100 = tree.tree_root();

        tree.append_commitments(101, [2u8; 32], &[make_leaf(3)]);
        tree.rollback_to(100);
        assert_eq!(tree.tree_root(), root_after_100);

        tree.append_commitments(101, [2u8; 32], &[make_leaf(3)]);
        let root_after_101 = tree.tree_root();

        let mut tree2 = CommitmentTreeDb::new();
        tree2.append_commitments(100, [1u8; 32], &[make_leaf(1), make_leaf(2)]);
        tree2.append_commitments(101, [2u8; 32], &[make_leaf(3)]);
        assert_eq!(tree2.tree_root(), root_after_101);
    }

    #[test]
    fn empty_roots_are_consistent() {
        let tree = CommitmentTreeDb::new();
        for level in 0..=TREE_DEPTH {
            let expected =
                <MerkleHashOrchard as Hashable>::empty_root(Level::from(level as u8)).to_bytes();
            assert_eq!(
                tree.empty_roots[level], expected,
                "empty_roots[{level}] mismatch"
            );
        }
    }

    #[test]
    fn append_empty_block_preserves_state() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1)]);
        let root_before = tree.tree_root();
        let size_before = tree.tree_size();

        tree.append_commitments(101, [2u8; 32], &[]);
        assert_eq!(tree.tree_size(), size_before);
        assert_eq!(tree.tree_root(), root_before);
        assert_eq!(tree.latest_height(), Some(101));
    }

    // ── Windowed (offset) tests ──────────────────────────────────────

    #[test]
    fn with_offset_basic() {
        let prefetched = vec![make_leaf(0xAA); 5];
        let offset = 5 * SHARD_LEAVES as u64;
        let tree = CommitmentTreeDb::with_offset(offset, prefetched.clone());

        assert_eq!(tree.tree_size(), offset);
        assert_eq!(tree.leaf_offset(), offset);
        assert_eq!(tree.window_start_shard(), 5);
        assert_eq!(tree.populated_shards(), 5);
        assert_eq!(tree.prefetched_shard_roots().len(), 5);
    }

    #[test]
    fn with_offset_append_and_broadcast() {
        let prefetched = vec![make_leaf(0xAA); 2];
        let offset = 2 * SHARD_LEAVES as u64;
        let mut tree = CommitmentTreeDb::with_offset(offset, prefetched.clone());

        tree.append_commitments(100, [1u8; 32], &[make_leaf(1), make_leaf(2)]);
        assert_eq!(tree.tree_size(), offset + 2);
        assert_eq!(tree.populated_shards(), 3);
        assert_eq!(tree.window_start_shard(), 2);
        assert_eq!(tree.window_shard_count(), 1);

        let bd = tree.broadcast_data(100);
        assert_eq!(bd.cap.shard_roots.len(), 3);
        assert_eq!(bd.cap.shard_roots[0], make_leaf(0xAA));
        assert_eq!(bd.cap.shard_roots[1], make_leaf(0xAA));
        assert_eq!(bd.window_start_shard, 2);
        assert_eq!(bd.window_shard_count, 1);
        assert_eq!(bd.subshard_roots.len(), 1);
    }

    #[test]
    fn with_offset_shard_roots_combines_prefetched_and_computed() {
        let prefetched = vec![make_leaf(0xBB); 3];
        let offset = 3 * SHARD_LEAVES as u64;
        let mut tree = CommitmentTreeDb::with_offset(offset, prefetched);

        tree.append_commitments(100, [1u8; 32], &[make_leaf(1)]);

        let roots = tree.shard_roots();
        assert_eq!(roots.len(), 4);
        assert_eq!(roots[0], (0, make_leaf(0xBB)));
        assert_eq!(roots[1], (1, make_leaf(0xBB)));
        assert_eq!(roots[2], (2, make_leaf(0xBB)));
        assert_ne!(roots[3].1, make_leaf(0xBB));
    }

    #[test]
    fn with_offset_pir_db_starts_at_window() {
        let prefetched = vec![make_leaf(0xCC); 2];
        let offset = 2 * SHARD_LEAVES as u64;
        let mut tree = CommitmentTreeDb::with_offset(offset, prefetched);

        let leaf = make_leaf(0xDD);
        tree.append_commitments(100, [1u8; 32], &[leaf]);

        let db = tree.build_pir_db();
        assert_eq!(db.len(), L0_DB_BYTES);
        assert_eq!(&db[..32], &leaf[..]);
    }

    // ── Cache tests ─────────────────────────────────────────────────

    #[test]
    fn cache_warm_hit_produces_same_result() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1), make_leaf(2)]);
        let (db1, bd1) = tree.build_pir_db_and_broadcast(100);
        let (db2, bd2) = tree.build_pir_db_and_broadcast(100);
        assert_eq!(db1, db2, "PIR db must be identical on warm cache hit");
        assert_eq!(bd1.cap.shard_roots, bd2.cap.shard_roots);
        for (a, b) in bd1.subshard_roots.iter().zip(bd2.subshard_roots.iter()) {
            assert_eq!(a.roots, b.roots);
        }
        assert!(tree.ss_root_cache[0].is_some());
    }

    #[test]
    fn cache_invalidated_on_append() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1)]);
        let (_, bd1) = tree.build_pir_db_and_broadcast(100);
        assert!(tree.ss_root_cache[0].is_some());
        tree.append_commitments(101, [2u8; 32], &[make_leaf(3)]);
        assert!(
            tree.ss_root_cache[0].is_none(),
            "slot 0 must be invalidated"
        );
        let (_, bd2) = tree.build_pir_db_and_broadcast(101);
        assert_ne!(bd1.cap.shard_roots[0], bd2.cap.shard_roots[0]);
    }

    #[test]
    fn cache_invalidated_on_rollback() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1), make_leaf(2)]);
        tree.append_commitments(101, [2u8; 32], &[make_leaf(3)]);
        tree.build_pir_db_and_broadcast(101);
        assert!(tree.ss_root_cache[0].is_some());
        tree.rollback_to(100);
        assert!(
            tree.ss_root_cache[0].is_none(),
            "cache must be invalidated on rollback"
        );
        let (_, bd) = tree.build_pir_db_and_broadcast(100);
        assert_eq!(bd.cap.shard_roots.len(), 1);
    }

    #[test]
    fn cache_only_invalidates_dirty_subshards() {
        let mut tree = CommitmentTreeDb::new();
        let leaves: Vec<Hash> = (0..SUBSHARD_LEAVES).map(|i| make_leaf(i as u8)).collect();
        tree.append_commitments(100, [1u8; 32], &leaves);
        tree.append_commitments(101, [2u8; 32], &leaves);
        tree.build_pir_db_and_broadcast(101);
        assert!(tree.ss_root_cache[0].is_some(), "slot 0 cached");
        assert!(tree.ss_root_cache[1].is_some(), "slot 1 cached");
        // Append one leaf -- only touches sub-shard 2
        tree.append_commitments(102, [3u8; 32], &[make_leaf(0xFF)]);
        assert!(tree.ss_root_cache[0].is_some(), "slot 0 must stay cached");
        assert!(tree.ss_root_cache[1].is_some(), "slot 1 must stay cached");
        assert!(
            tree.ss_root_cache[2].is_none(),
            "slot 2 must be invalidated"
        );
    }
}
