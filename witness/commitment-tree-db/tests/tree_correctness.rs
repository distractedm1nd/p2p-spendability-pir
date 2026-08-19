//! Tree correctness tests against mainnet lightwalletd.
//!
//! Ingests real Orchard note commitments from lightwalletd, builds the
//! commitment tree using [`CommitmentTreeDb`], and verifies against
//! canonical values from `GetSubtreeRoots`.
//!
//! **`verify_shard_root_against_mainnet`** — Verifies a single completed
//! shard root against `GetSubtreeRoots`. Validates Sinsemilla hash chain
//! correctness (levels 0–15), empty-subtree sentinels, and commitment
//! extraction ordering.
//!
//! **`verify_witness_reconstruction`** — Ingests a completed shard plus
//! frontier blocks, then:
//! - Verifies the completed shard root against `GetSubtreeRoots`
//! - Picks a note in the frontier shard (partial sub-shards with
//!   empty-leaf sentinel padding)
//! - Reconstructs the full 32-sibling authentication path via the
//!   broadcast + sub-shard decomposition
//! - Hashes leaf-to-root and verifies against `tree_root()`
//!
//! This validates the full witness reconstruction pipeline including the
//! cap tree (levels 16–31), frontier sub-shard handling, and broadcast
//! data consistency.
//!
//! Both tests require network access to lightwalletd.

use chain_ingest::LwdClient;
use commitment_ingest::extract_commitments;
use commitment_tree_db::CommitmentTreeDb;
use incrementalmerkletree::{Hashable, Level};
use orchard::tree::MerkleHashOrchard;
use witness_types::*;

const LWD_ENDPOINT: &str = "https://us.zec.stardust.rest:443";
const IRONWOOD_PROTOCOL: i32 = 2;
const BATCH_SIZE: u64 = 10_000;
const FRONTIER_BLOCKS: u64 = 200;

// ── Independent Sinsemilla helpers ──────────────────────────────────────
//
// These reimplement the hash operations WITHOUT using commitment_tree_db
// internals, providing an independent cross-check of the tree's hashing.

fn sinsemilla_combine(level: u8, left: &Hash, right: &Hash) -> Hash {
    let l = Option::from(MerkleHashOrchard::from_bytes(left)).expect("invalid left hash");
    let r = Option::from(MerkleHashOrchard::from_bytes(right)).expect("invalid right hash");
    <MerkleHashOrchard as Hashable>::combine(Level::from(level), &l, &r).to_bytes()
}

fn compute_empty_roots() -> Vec<Hash> {
    let mut roots = Vec::with_capacity(TREE_DEPTH + 1);
    let mut current = MerkleHashOrchard::empty_leaf();
    roots.push(current.to_bytes());
    for level in 0..TREE_DEPTH {
        current =
            <MerkleHashOrchard as Hashable>::combine(Level::from(level as u8), &current, &current);
        roots.push(current.to_bytes());
    }
    roots
}

/// Extract authentication-path siblings from a complete binary subtree.
///
/// `leaves` must have a power-of-two length. Returns siblings ordered
/// leaf-to-root (ascending level). `base_level` is the Merkle combine
/// level of the leaf nodes.
fn extract_siblings(leaves: &[Hash], target: usize, base_level: u8) -> Vec<Hash> {
    let depth = leaves.len().trailing_zeros() as usize;
    let mut siblings = Vec::with_capacity(depth);
    let mut current = leaves.to_vec();
    let mut idx = target;

    for level_offset in 0..depth {
        siblings.push(current[idx ^ 1]);

        let level = base_level + level_offset as u8;
        let mut next = Vec::with_capacity(current.len() / 2);
        for pair in current.chunks_exact(2) {
            next.push(sinsemilla_combine(level, &pair[0], &pair[1]));
        }
        current = next;
        idx >>= 1;
    }
    siblings
}

/// Extract cap-tree siblings (levels 16–31) using recursive sparse-root
/// computation. Avoids materializing the full 65,536-element padded array.
fn extract_cap_siblings(
    shard_roots: &[Hash],
    target_shard: usize,
    empty_roots: &[Hash],
) -> Vec<Hash> {
    let cap_capacity = 1usize << SHARD_HEIGHT;
    let mut siblings = Vec::with_capacity(SHARD_HEIGHT);
    extract_cap_rec(
        shard_roots,
        target_shard,
        0,
        cap_capacity,
        SHARD_HEIGHT as u8,
        empty_roots,
        &mut siblings,
    );
    // Recursion pushes siblings from highest level first; reverse to get
    // ascending-level order matching the PirWitness convention.
    siblings.reverse();
    siblings
}

fn extract_cap_rec(
    populated: &[Hash],
    target: usize,
    offset: usize,
    size: usize,
    base_level: u8,
    empty_roots: &[Hash],
    siblings: &mut Vec<Hash>,
) {
    if size == 1 {
        return;
    }
    let half = size / 2;
    let target_in_left = target < offset + half;

    let (target_offset, sibling_offset) = if target_in_left {
        (offset, offset + half)
    } else {
        (offset + half, offset)
    };

    let sibling_root =
        sparse_subtree_root(populated, sibling_offset, half, base_level, empty_roots);
    siblings.push(sibling_root);

    extract_cap_rec(
        populated,
        target,
        target_offset,
        half,
        base_level,
        empty_roots,
        siblings,
    );
}

/// Root of a sparse left-packed subtree. Positions beyond `populated.len()`
/// use the precomputed empty root at the appropriate level.
fn sparse_subtree_root(
    populated: &[Hash],
    offset: usize,
    size: usize,
    base_level: u8,
    empty_roots: &[Hash],
) -> Hash {
    if size == 1 {
        return if offset < populated.len() {
            populated[offset]
        } else {
            empty_roots[base_level as usize]
        };
    }

    let half = size / 2;
    let child_height = half.trailing_zeros() as u8;
    let combine_level = base_level + child_height;

    let left = sparse_subtree_root(populated, offset, half, base_level, empty_roots);
    let right = if offset + half >= populated.len() {
        empty_roots[combine_level as usize]
    } else {
        sparse_subtree_root(populated, offset + half, half, base_level, empty_roots)
    };

    sinsemilla_combine(combine_level, &left, &right)
}

/// Hash a leaf through its 32-sibling authentication path to recompute
/// the tree root. Uses position bits for left/right ordering at each level.
fn hash_to_root(leaf: &Hash, siblings: &[Hash; TREE_DEPTH], position: u64) -> Hash {
    let mut current = *leaf;
    for (level, sibling) in siblings.iter().enumerate() {
        let (left, right) = if (position >> level) & 1 == 0 {
            (&current, sibling)
        } else {
            (sibling, &current)
        };
        current = sinsemilla_combine(level as u8, left, right);
    }
    current
}

// ── Shared ingestion helpers ───────────────────────────────────────────

async fn connect_and_get_subtree_roots() -> (LwdClient, Vec<chain_ingest::proto::SubtreeRoot>) {
    let mut client = LwdClient::connect(&[LWD_ENDPOINT.to_string()])
        .await
        .expect("failed to connect to lightwalletd");

    let roots = client
        .get_subtree_roots(IRONWOOD_PROTOCOL, 0, 256)
        .await
        .expect("failed to get subtree roots");

    assert!(
        roots.len() >= 2,
        "need at least 2 completed shards, got {}",
        roots.len()
    );

    (client, roots)
}

async fn ingest_block_range(
    client: &mut LwdClient,
    from: u64,
    to: u64,
) -> (Vec<Hash>, Option<u64>) {
    let mut all_commitments = Vec::new();
    let mut tree_size_at_start: Option<u64> = None;

    let mut current = from;
    while current <= to {
        let batch_end = (current + BATCH_SIZE - 1).min(to);
        tracing::info!(from = current, to = batch_end, "fetching block range");

        let blocks = client
            .get_block_range(current, batch_end)
            .await
            .expect("failed to fetch blocks");

        for block in &blocks {
            if block.height == from && tree_size_at_start.is_none() {
                if let Some(meta) = &block.chain_metadata {
                    let size_after = meta.ironwood_commitment_tree_size as u64;
                    let cmx_count = extract_commitments(block).unwrap().len() as u64;
                    tree_size_at_start = Some(size_after.saturating_sub(cmx_count));
                }
            }
            all_commitments.extend(extract_commitments(block).unwrap());
        }

        current = batch_end + 1;
    }

    (all_commitments, tree_size_at_start)
}

// ── Test 1: shard root verification ────────────────────────────────────

#[tokio::test]
#[ignore = "requires an Ironwood-capable mainnet lightwalletd"]
async fn verify_shard_root_against_mainnet() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init()
        .ok();

    let (mut client, subtree_roots) = connect_and_get_subtree_roots().await;
    let num_completed = subtree_roots.len();
    let target_shard = num_completed - 1;

    let mut canonical_root = [0u8; 32];
    canonical_root.copy_from_slice(&subtree_roots[target_shard].root_hash);

    let shard_end_height = subtree_roots[target_shard].completing_block_height;
    let shard_start_height = subtree_roots[target_shard - 1].completing_block_height;

    tracing::info!(
        shard = target_shard,
        start_height = shard_start_height,
        end_height = shard_end_height,
        block_span = shard_end_height - shard_start_height,
        "ingesting blocks for shard root verification"
    );

    let (all_commitments, tree_size_at_start) =
        ingest_block_range(&mut client, shard_start_height, shard_end_height).await;

    let tree_size_before =
        tree_size_at_start.unwrap_or((target_shard as u64) * (SHARD_LEAVES as u64));

    tracing::info!(
        total_commitments = all_commitments.len(),
        tree_size_before,
        "block ingestion complete"
    );

    let shard_leaf_start = (target_shard as u64) * (SHARD_LEAVES as u64);
    let skip = shard_leaf_start.saturating_sub(tree_size_before) as usize;

    let shard_commits: Vec<_> = all_commitments
        .iter()
        .skip(skip)
        .take(SHARD_LEAVES)
        .copied()
        .collect();

    assert_eq!(
        shard_commits.len(),
        SHARD_LEAVES,
        "expected exactly {SHARD_LEAVES} leaves for shard {target_shard}, got {}",
        shard_commits.len()
    );

    let mut tree = CommitmentTreeDb::new();
    tree.append_commitments(shard_end_height, [0u8; 32], &shard_commits);

    let shard_roots = tree.shard_roots();
    assert_eq!(shard_roots.len(), 1);
    let (idx, computed_root) = shard_roots[0];
    assert_eq!(idx, 0);

    tracing::info!(
        shard = target_shard,
        computed = hex::encode(computed_root),
        canonical = hex::encode(canonical_root),
        "comparing shard roots"
    );

    assert_eq!(
        computed_root,
        canonical_root,
        "\nshard {target_shard} root MISMATCH — Sinsemilla hash chain is incorrect!\n  \
         computed:  {}\n  canonical: {}\n\n\
         This means the commitment tree does NOT match the canonical Orchard tree.\n\
         Check: MerkleHashOrchard::combine levels, empty_root sentinels, leaf ordering.",
        hex::encode(computed_root),
        hex::encode(canonical_root)
    );

    tracing::info!(
        shard = target_shard,
        "PASS: shard root verified against mainnet — Sinsemilla hash chain is correct"
    );
}

// ── Test 2: witness reconstruction with frontier ───────────────────────

#[tokio::test]
#[ignore = "requires an Ironwood-capable mainnet lightwalletd"]
async fn verify_witness_reconstruction() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init()
        .ok();

    let (mut client, subtree_roots) = connect_and_get_subtree_roots().await;
    let num_completed = subtree_roots.len();
    let target_shard = num_completed - 1;

    let mut canonical_root = [0u8; 32];
    canonical_root.copy_from_slice(&subtree_roots[target_shard].root_hash);

    let shard_start_height = subtree_roots[target_shard - 1].completing_block_height;
    let shard_end_height = subtree_roots[target_shard].completing_block_height;
    let fetch_end = shard_end_height + FRONTIER_BLOCKS;

    tracing::info!(
        shard = target_shard,
        shard_start = shard_start_height,
        shard_end = shard_end_height,
        frontier_end = fetch_end,
        "ingesting blocks for witness reconstruction test"
    );

    // ── 1. Ingest completed shard + frontier ──

    let (all_commitments, tree_size_at_start) =
        ingest_block_range(&mut client, shard_start_height, fetch_end).await;

    let tree_size_before =
        tree_size_at_start.unwrap_or((target_shard as u64) * (SHARD_LEAVES as u64));

    let shard_leaf_start = (target_shard as u64) * (SHARD_LEAVES as u64);
    let skip = shard_leaf_start.saturating_sub(tree_size_before) as usize;

    let shard_commits: Vec<_> = all_commitments
        .iter()
        .skip(skip)
        .take(SHARD_LEAVES)
        .copied()
        .collect();
    assert_eq!(shard_commits.len(), SHARD_LEAVES);

    let frontier_commits: Vec<_> = all_commitments
        .iter()
        .skip(skip + SHARD_LEAVES)
        .copied()
        .collect();
    assert!(
        !frontier_commits.is_empty(),
        "no frontier commitments found — increase FRONTIER_BLOCKS"
    );

    tracing::info!(
        shard_leaves = shard_commits.len(),
        frontier_leaves = frontier_commits.len(),
        "split into completed shard + frontier"
    );

    // ── 2. Build tree (2 shards: completed + frontier) ──

    let mut tree = CommitmentTreeDb::new();
    tree.append_commitments(shard_end_height, [0u8; 32], &shard_commits);
    tree.append_commitments(fetch_end, [1u8; 32], &frontier_commits);

    assert_eq!(tree.populated_shards(), 2);
    assert_eq!(
        tree.tree_size(),
        (SHARD_LEAVES + frontier_commits.len()) as u64
    );

    // ── 3. Verify completed shard root against GetSubtreeRoots ──

    let shard_roots_vec = tree.shard_roots();
    let (_, computed_shard_root) = shard_roots_vec[0];
    assert_eq!(
        computed_shard_root, canonical_root,
        "completed shard root mismatch against GetSubtreeRoots"
    );
    tracing::info!("completed shard root verified against GetSubtreeRoots");

    // ── 4. Pick a note in the frontier shard ──
    //
    // Choose the midpoint of the frontier's populated range so the note's
    // sub-shard is likely partially filled (some real leaves, some empty
    // sentinels) — the exact scenario the witness system must handle.

    let frontier_note_offset = frontier_commits.len() / 2;
    let note_position = (SHARD_LEAVES + frontier_note_offset) as u64;
    let note_leaf = tree.leaves()[note_position as usize];

    let (shard_idx, subshard_idx, leaf_idx) = decompose_position(note_position);
    assert_eq!(shard_idx, 1, "target note should be in shard 1 (frontier)");

    tracing::info!(
        position = note_position,
        shard_idx,
        subshard_idx,
        leaf_idx,
        "selected frontier note for witness reconstruction"
    );

    // ── 5. Build broadcast data ──

    let broadcast = tree.broadcast_data(fetch_end);
    assert_eq!(broadcast.window_shard_count, 2);

    // ── 6. Reconstruct 32-sibling authentication path ──

    let empty_roots = compute_empty_roots();

    // Lower 8 siblings (levels 0–7): from sub-shard leaves
    let ss_leaves = tree.subshard_leaves(shard_idx, subshard_idx);
    assert_eq!(ss_leaves.len(), SUBSHARD_LEAVES);
    assert_eq!(
        ss_leaves[leaf_idx as usize], note_leaf,
        "sub-shard leaf at target index doesn't match expected note"
    );

    // Verify frontier padding: at least some leaves beyond the populated
    // range should be the empty-leaf sentinel, not zero bytes.
    if frontier_note_offset < SUBSHARD_LEAVES {
        let last_leaf = ss_leaves[SUBSHARD_LEAVES - 1];
        let empty_leaf = empty_roots[0];
        let zero_leaf = [0u8; 32];
        if last_leaf != zero_leaf {
            assert_eq!(
                last_leaf, empty_leaf,
                "unpopulated positions must use empty_leaf sentinel, not arbitrary values"
            );
        }
    }

    let lower = extract_siblings(&ss_leaves, leaf_idx as usize, 0);
    assert_eq!(lower.len(), SUBSHARD_HEIGHT);

    // Middle 8 siblings (levels 8–15): from broadcast sub-shard roots
    let shard_window_offset = (shard_idx - broadcast.window_start_shard) as usize;
    let shard_ss_roots = &broadcast.subshard_roots[shard_window_offset].roots;
    assert_eq!(shard_ss_roots.len(), SUBSHARDS_PER_SHARD);

    let middle = extract_siblings(shard_ss_roots, subshard_idx as usize, SUBSHARD_HEIGHT as u8);
    assert_eq!(middle.len(), SUBSHARD_HEIGHT);

    // Upper 16 siblings (levels 16–31): from cap tree shard roots
    let cap_roots = &broadcast.cap.shard_roots;
    let upper = extract_cap_siblings(cap_roots, shard_idx as usize, &empty_roots);
    assert_eq!(upper.len(), SHARD_HEIGHT);

    // ── 7. Assemble full authentication path ──

    let mut siblings = [[0u8; 32]; TREE_DEPTH];
    for (i, s) in lower
        .iter()
        .chain(middle.iter())
        .chain(upper.iter())
        .enumerate()
    {
        siblings[i] = *s;
    }

    // ── 8. Hash leaf-to-root and verify ──

    let computed_root = hash_to_root(&note_leaf, &siblings, note_position);
    let tree_root = tree.tree_root();

    tracing::info!(
        computed = hex::encode(computed_root),
        tree_root = hex::encode(tree_root),
        "comparing witness hash-to-root against tree_root()"
    );

    assert_eq!(
        computed_root,
        tree_root,
        "\nwitness reconstruction FAILED!\n  \
         hash-to-root: {}\n  tree_root:    {}\n\n\
         The decomposed authentication path (broadcast + sub-shard leaves) \
         is inconsistent with the full tree root.\n\
         Check: sub-shard decomposition, cap tree sibling extraction, \
         empty-root sentinels for frontier sub-shards.",
        hex::encode(computed_root),
        hex::encode(tree_root)
    );

    tracing::info!(
        position = note_position,
        shard = shard_idx,
        subshard = subshard_idx,
        leaf = leaf_idx,
        frontier_leaves = frontier_commits.len(),
        "PASS: witness reconstruction verified — \
         decomposed auth path produces correct tree root"
    );
}
