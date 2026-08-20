use super::proto::{CompactBlock, TreeState};
use super::{ClientError, LwdClient};
use pir_protocol::ZcashNetwork;
use thiserror::Error;

pub const NU6_3_MAINNET_ACTIVATION_HEIGHT: u64 = 3_428_143;
pub const NU6_3_TESTNET_ACTIVATION_HEIGHT: u64 = 4_134_000;

pub const fn nu6_3_activation_height(network: ZcashNetwork) -> u64 {
    match network {
        ZcashNetwork::Main => NU6_3_MAINNET_ACTIVATION_HEIGHT,
        ZcashNetwork::Test => NU6_3_TESTNET_ACTIVATION_HEIGHT,
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IronwoodError {
    #[error("Ironwood nullifier at height {height} has {actual} bytes; expected 32")]
    InvalidNullifier { height: u64, actual: usize },
    #[error("lightwalletd returned network {actual:?}; expected {expected}")]
    WrongNetwork {
        actual: String,
        expected: ZcashNetwork,
    },
    #[error("lightwalletd returned tree state height {actual}; expected {expected}")]
    WrongHeight { actual: u64, expected: u64 },
    #[error("lightwalletd omitted the Ironwood tree at height {height}")]
    MissingTree { height: u64 },
}

#[derive(Debug, Error)]
pub enum EndpointError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Invalid(#[from] IronwoodError),
}

pub fn extract_ironwood_nullifiers(block: &CompactBlock) -> Result<Vec<[u8; 32]>, IronwoodError> {
    block
        .vtx
        .iter()
        .flat_map(|tx| &tx.ironwood_actions)
        .map(|action| {
            action
                .nullifier
                .as_slice()
                .try_into()
                .map_err(|_| IronwoodError::InvalidNullifier {
                    height: block.height,
                    actual: action.nullifier.len(),
                })
        })
        .collect()
}

pub fn validate_ironwood_tree_state(
    expected_network: ZcashNetwork,
    expected_height: u64,
    state: &TreeState,
) -> Result<(), IronwoodError> {
    if state.network != expected_network.as_str() {
        return Err(IronwoodError::WrongNetwork {
            actual: state.network.clone(),
            expected: expected_network,
        });
    }
    if state.height != expected_height {
        return Err(IronwoodError::WrongHeight {
            actual: state.height,
            expected: expected_height,
        });
    }
    if state.ironwood_tree.is_empty() {
        return Err(IronwoodError::MissingTree {
            height: expected_height,
        });
    }
    Ok(())
}

pub async fn require_ironwood_tree_state(
    client: &mut LwdClient,
    expected_network: ZcashNetwork,
    expected_height: u64,
) -> Result<(), EndpointError> {
    validate_ironwood_tree_state(
        expected_network,
        expected_height,
        &client.get_tree_state(expected_height).await?,
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::proto::{CompactOrchardAction, CompactTx};
    use prost::Message;

    #[test]
    fn decodes_and_extracts_tag_nine_only() {
        let mut encoded = vec![0x4a, 0x22, 0x0a, 0x20];
        encoded.extend([7u8; 32]);
        let tx = CompactTx::decode(encoded.as_slice()).unwrap();
        assert_eq!(tx.ironwood_actions[0].nullifier, vec![7; 32]);

        let block = CompactBlock {
            height: NU6_3_MAINNET_ACTIVATION_HEIGHT,
            vtx: vec![CompactTx {
                actions: vec![CompactOrchardAction {
                    nullifier: vec![1; 32],
                    ..Default::default()
                }],
                ironwood_actions: vec![CompactOrchardAction {
                    nullifier: vec![2; 32],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(extract_ironwood_nullifiers(&block).unwrap(), vec![[2; 32]]);
    }

    #[test]
    fn validates_tree_state() {
        let state = TreeState {
            network: "main".into(),
            height: NU6_3_MAINNET_ACTIVATION_HEIGHT,
            ironwood_tree: "00".into(),
            ..Default::default()
        };
        validate_ironwood_tree_state(ZcashNetwork::Main, state.height, &state).unwrap();
    }
}
