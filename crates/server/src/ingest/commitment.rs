use crate::ingest::proto::{CompactBlock, CompactOrchardAction};
use orchard::tree::MerkleHashOrchard;
use thiserror::Error;
use witness_pir::Hash;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("Ironwood {field} at height {height} has {actual} bytes; expected {expected}")]
    InvalidField {
        height: u64,
        field: &'static str,
        actual: usize,
        expected: usize,
    },
    #[error("Ironwood commitment at height {height} is not canonically encoded")]
    NonCanonicalCommitment { height: u64 },
    #[error("compact block at height {height} omitted Ironwood tree-size metadata")]
    MissingTreeSize { height: u64 },
    #[error("Ironwood tree size {tree_size} at height {height} is smaller than its {commitments} commitments")]
    InvalidTreeSize {
        height: u64,
        tree_size: u32,
        commitments: usize,
    },
}

pub fn extract_commitments(block: &CompactBlock) -> Result<Vec<Hash>, ParseError> {
    block
        .vtx
        .iter()
        .flat_map(|tx| &tx.ironwood_actions)
        .map(|action| commitment(block.height, action))
        .collect()
}

pub fn ironwood_prior_tree_size(
    block: &CompactBlock,
    commitments: usize,
) -> Result<u32, ParseError> {
    let tree_size = block
        .chain_metadata
        .as_ref()
        .ok_or(ParseError::MissingTreeSize {
            height: block.height,
        })?
        .ironwood_commitment_tree_size;
    tree_size
        .checked_sub(commitments.try_into().unwrap_or(u32::MAX))
        .ok_or(ParseError::InvalidTreeSize {
            height: block.height,
            tree_size,
            commitments,
        })
}

fn commitment(height: u64, action: &CompactOrchardAction) -> Result<Hash, ParseError> {
    let cmx = field(height, "commitment", &action.cmx)?;
    if bool::from(MerkleHashOrchard::from_bytes(&cmx).is_some()) {
        Ok(cmx)
    } else {
        Err(ParseError::NonCanonicalCommitment { height })
    }
}

fn field<const N: usize>(
    height: u64,
    name: &'static str,
    bytes: &[u8],
) -> Result<[u8; N], ParseError> {
    bytes.try_into().map_err(|_| ParseError::InvalidField {
        height,
        field: name,
        actual: bytes.len(),
        expected: N,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::proto::{ChainMetadata, CompactTx};

    fn action(byte: u8) -> CompactOrchardAction {
        CompactOrchardAction {
            nullifier: vec![byte; 32],
            cmx: vec![byte; 32],
            ephemeral_key: vec![byte; 32],
            ciphertext: vec![byte; 52],
        }
    }

    #[test]
    fn extracts_only_strict_ironwood_actions() {
        let block = CompactBlock {
            height: 10,
            vtx: vec![CompactTx {
                actions: vec![action(1)],
                ironwood_actions: vec![action(2)],
                ..Default::default()
            }],
            ..Default::default()
        };
        let commitments = extract_commitments(&block).unwrap();
        assert_eq!(commitments, vec![[2; 32]]);
    }

    #[test]
    fn rejects_malformed_action() {
        let mut malformed = action(1);
        malformed.cmx.pop();
        let block = CompactBlock {
            height: 10,
            vtx: vec![CompactTx {
                ironwood_actions: vec![malformed],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(matches!(
            extract_commitments(&block),
            Err(ParseError::InvalidField {
                field: "commitment",
                actual: 31,
                ..
            })
        ));

        let noncanonical = CompactBlock {
            height: 10,
            vtx: vec![CompactTx {
                ironwood_actions: vec![action(0xff)],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(matches!(
            extract_commitments(&noncanonical),
            Err(ParseError::NonCanonicalCommitment { .. })
        ));
    }

    #[test]
    fn reads_ironwood_tree_size() {
        let block = CompactBlock {
            chain_metadata: Some(ChainMetadata {
                ironwood_commitment_tree_size: 42,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(ironwood_prior_tree_size(&block, 2).unwrap(), 40);
    }
}
