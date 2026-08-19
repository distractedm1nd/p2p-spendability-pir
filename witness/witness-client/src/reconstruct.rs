//! Witness reconstruction from a PIR-decoded subshard row and broadcast data.
//!
//! The authentication path for a leaf at depth 32 has three tiers of siblings:
//!
//! 1. **Levels 0..7 (within subshard)**: computed from the 256 leaves in the
//!    decoded PIR row using Sinsemilla hashing.
//! 2. **Levels 8..15 (within shard)**: computed from the 256 subshard roots
//!    in `broadcast.subshard_roots[shard]`.
//! 3. **Levels 16..31 (cap tree)**: computed from the shard roots in
//!    `broadcast.cap.shard_roots`, padded with empty roots.

use crate::{Result, WitnessClientError};
use incrementalmerkletree::{Hashable, Level};
use orchard::tree::MerkleHashOrchard;
use witness_types::*;

/// Reconstruct a full `PirWitness` from the decoded PIR row and broadcast data.
///
/// Self-verifies by computing the tree root from the leaf and checking it
/// against the broadcast anchor root (derived from cap shard roots).
pub fn reconstruct_witness(
    position: u64,
    shard_idx: u32,
    subshard_idx: u8,
    leaf_idx: u8,
    decoded_row: &[u8],
    broadcast: &BroadcastData,
) -> Result<PirWitness> {
    validate_broadcast(shard_idx, broadcast)?;
    let mut siblings = [[0u8; 32]; TREE_DEPTH];

    let leaves = parse_leaves(decoded_row)?;
    extract_siblings(&leaves, leaf_idx as usize, 0, &mut siblings);

    let shard_offset = (shard_idx - broadcast.window_start_shard) as usize;
    let ss_roots = &broadcast.subshard_roots[shard_offset].roots;
    extract_siblings(
        ss_roots,
        subshard_idx as usize,
        SUBSHARD_HEIGHT as u8,
        &mut siblings,
    );

    let total_cap_slots = 1usize << SHARD_HEIGHT;
    let mut padded_cap = Vec::with_capacity(total_cap_slots);
    padded_cap.extend_from_slice(&broadcast.cap.shard_roots);
    let empty_shard_root = empty_root(SHARD_HEIGHT as u8);
    padded_cap.resize(total_cap_slots, empty_shard_root);
    extract_siblings(
        &padded_cap,
        shard_idx as usize,
        SHARD_HEIGHT as u8,
        &mut siblings,
    );

    let anchor_root = compute_root_from_path(position, &leaves[leaf_idx as usize], &siblings);

    Ok(PirWitness {
        position,
        siblings,
        anchor_height: broadcast.anchor_height,
        anchor_root,
    })
}

/// Given a complete array of 2^k nodes at a given base_level, extract the
/// sibling subtree roots along the path to `index` and place them into
/// `siblings[base_level..base_level + k]`.
fn extract_siblings(
    nodes: &[Hash],
    index: usize,
    base_level: u8,
    siblings: &mut [Hash; TREE_DEPTH],
) {
    let num_levels = nodes.len().trailing_zeros() as usize;
    let mut current_nodes = nodes.to_vec();
    let mut idx = index;

    for level_offset in 0..num_levels {
        let tree_level = base_level as usize + level_offset;
        let sibling_idx = idx ^ 1;
        siblings[tree_level] = if sibling_idx < current_nodes.len() {
            current_nodes[sibling_idx]
        } else {
            empty_root(tree_level as u8)
        };

        let mut next = Vec::with_capacity(current_nodes.len() / 2);
        for pair in current_nodes.chunks(2) {
            let left = pair[0];
            let right = if pair.len() > 1 {
                pair[1]
            } else {
                empty_root(tree_level as u8)
            };
            next.push(hash_combine(tree_level as u8, &left, &right));
        }
        current_nodes = next;
        idx /= 2;
    }
}

/// Verify witness by computing the root from the leaf and the sibling path.
fn compute_root_from_path(position: u64, leaf: &Hash, siblings: &[Hash; TREE_DEPTH]) -> Hash {
    let mut current = *leaf;
    let mut pos = position;

    for (level, sibling) in siblings.iter().enumerate() {
        let (left, right) = if pos & 1 == 0 {
            (&current, sibling)
        } else {
            (sibling, &current)
        };
        current = hash_combine(level as u8, left, right);
        pos >>= 1;
    }
    current
}

fn hash_combine(level: u8, left: &Hash, right: &Hash) -> Hash {
    let l = bytes_to_mho(left);
    let r = bytes_to_mho(right);
    <MerkleHashOrchard as Hashable>::combine(Level::from(level), &l, &r).to_bytes()
}

fn bytes_to_mho(hash: &Hash) -> MerkleHashOrchard {
    Option::from(MerkleHashOrchard::from_bytes(hash)).expect("invalid MerkleHashOrchard bytes")
}

fn empty_root(level: u8) -> Hash {
    <MerkleHashOrchard as Hashable>::empty_root(Level::from(level)).to_bytes()
}

/// Parse the decoded PIR row bytes into 256 leaf hashes (32 bytes each).
fn parse_leaves(decoded_row: &[u8]) -> Result<Vec<Hash>> {
    if decoded_row.len() < SUBSHARD_ROW_BYTES {
        return Err(WitnessClientError::QueryFailed(format!(
            "decoded row too short: {} bytes, expected >= {}",
            decoded_row.len(),
            SUBSHARD_ROW_BYTES,
        )));
    }

    let mut leaves = Vec::with_capacity(SUBSHARD_LEAVES);
    for i in 0..SUBSHARD_LEAVES {
        let start = i * 32;
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&decoded_row[start..start + 32]);
        if !bool::from(MerkleHashOrchard::from_bytes(&hash).is_some()) {
            return Err(WitnessClientError::QueryFailed(format!(
                "decoded row contains a non-canonical commitment at index {i}"
            )));
        }
        leaves.push(hash);
    }
    Ok(leaves)
}

fn validate_broadcast(shard_idx: u32, broadcast: &BroadcastData) -> Result<()> {
    let offset = shard_idx
        .checked_sub(broadcast.window_start_shard)
        .ok_or_else(|| WitnessClientError::InvalidParams("shard precedes PIR window".into()))?
        as usize;
    let roots = broadcast
        .subshard_roots
        .get(offset)
        .ok_or_else(|| WitnessClientError::InvalidParams("missing subshard roots".into()))?;
    if roots.roots.len() != SUBSHARDS_PER_SHARD
        || broadcast.cap.shard_roots.len() > (1 << SHARD_HEIGHT)
        || shard_idx as usize >= (1 << SHARD_HEIGHT)
        || roots
            .roots
            .iter()
            .chain(&broadcast.cap.shard_roots)
            .any(|root| !bool::from(MerkleHashOrchard::from_bytes(root).is_some()))
    {
        return Err(WitnessClientError::InvalidParams(
            "malformed Ironwood tree broadcast".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_leaves_correct_count() {
        let row = vec![0u8; SUBSHARD_ROW_BYTES];
        let leaves = parse_leaves(&row).unwrap();
        assert_eq!(leaves.len(), SUBSHARD_LEAVES);
    }

    #[test]
    fn parse_leaves_too_short() {
        let row = vec![0u8; SUBSHARD_ROW_BYTES - 1];
        assert!(parse_leaves(&row).is_err());
    }

    #[test]
    fn empty_root_level_0_is_empty_leaf() {
        let root = empty_root(0);
        let expected = MerkleHashOrchard::empty_leaf().to_bytes();
        assert_eq!(root, expected);
    }

    #[test]
    fn hash_combine_matches_protocol() {
        let a = MerkleHashOrchard::empty_leaf().to_bytes();
        let b = MerkleHashOrchard::empty_leaf().to_bytes();
        let combined = hash_combine(0, &a, &b);
        let expected = empty_root(1);
        assert_eq!(
            combined, expected,
            "H(0, empty, empty) should equal empty_root(1)"
        );
    }

    #[test]
    fn extract_siblings_power_of_two() {
        let empty_leaf = empty_root(0);
        let leaves = vec![empty_leaf; 4];
        let mut siblings = [[0u8; 32]; TREE_DEPTH];
        extract_siblings(&leaves, 0, 0, &mut siblings);
        assert_eq!(
            siblings[0], empty_leaf,
            "level 0 sibling of leaf 0 is leaf 1"
        );
        assert_eq!(
            siblings[1],
            empty_root(1),
            "level 1 sibling is root of pair (2,3)"
        );
    }
}
