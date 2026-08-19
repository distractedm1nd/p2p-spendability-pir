//! Validated, protocol-neutral compact block sync and follow pipeline.

use crate::proto::CompactBlock;
use crate::{ClientError, LwdClient};
use std::collections::VecDeque;
use std::future::Future;
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;

const BATCH_SIZE: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct ValidatedBlock {
    pub block: CompactBlock,
    pub hash: [u8; 32],
    pub prev_hash: [u8; 32],
}

impl ValidatedBlock {
    pub fn height(&self) -> u64 {
        self.block.height
    }
}

#[derive(Debug, Clone)]
pub enum BlockEvent {
    NewBlock(ValidatedBlock),
    Reorg { rollback_to: u64 },
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("block {height} has a {actual}-byte {field}; expected 32")]
    InvalidHash {
        height: u64,
        field: &'static str,
        actual: usize,
    },
    #[error("block range returned height {actual}; expected {expected}")]
    UnexpectedHeight { expected: u64, actual: u64 },
    #[error("block range ended at height {actual}; expected {expected}")]
    IncompleteRange { expected: u64, actual: u64 },
    #[error("block {height} does not link to the preceding block")]
    BrokenLink { height: u64 },
    #[error("local block {height} no longer matches lightwalletd and the fork exceeds the {depth}-block tracking window")]
    ReorgBeyondWindow { height: u64, depth: usize },
    #[error("block consumer dropped")]
    ConsumerDropped,
    #[error("invalid block {height}: {reason}")]
    InvalidBlock { height: u64, reason: String },
}

/// Sync an inclusive range in validated batches.
///
/// A batch is fully validated before any block from it reaches `consume`, so a
/// missing or malformed range cannot advance a consumer checkpoint.
pub async fn sync_blocks<F, Fut, E>(
    client: &mut LwdClient,
    from: u64,
    to: u64,
    mut consume: F,
) -> Result<(), E>
where
    F: FnMut(ValidatedBlock) -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: From<PipelineError>,
{
    let mut current = from;
    let mut expected_prev = None;
    while current <= to {
        let end = (current + BATCH_SIZE - 1).min(to);
        let blocks = fetch_validated(client, current, end, expected_prev)
            .await
            .map_err(E::from)?;
        expected_prev = blocks.last().map(|block| block.hash);
        for block in blocks {
            consume(block).await?;
        }
        current = end + 1;
    }
    Ok(())
}

/// Follow the confirmed chain and emit generic block/reorg events.
pub async fn follow_blocks<F, Fut>(
    client: &mut LwdClient,
    start_height: u64,
    start_hash: [u8; 32],
    confirmation_depth: u64,
    reorg_window: usize,
    poll_interval: Duration,
    mut consume: F,
) -> Result<(), PipelineError>
where
    F: FnMut(BlockEvent) -> Fut,
    Fut: Future<Output = Result<(), PipelineError>>,
{
    let history_start = if start_height == 0 {
        0
    } else {
        start_height
            .saturating_sub(reorg_window.saturating_sub(1) as u64)
            .max(1)
    };
    let recent_blocks = fetch_validated(client, history_start, start_height, None).await?;
    if recent_blocks.last().map(|block| block.hash) != Some(start_hash) {
        return Err(PipelineError::ReorgBeyondWindow {
            height: start_height,
            depth: reorg_window,
        });
    }

    let mut history = VecDeque::with_capacity(reorg_window + 1);
    for block in recent_blocks {
        push_history(&mut history, reorg_window, block.height(), block.hash);
    }
    let mut current_height = start_height;

    loop {
        let (tip_height, _) = client.get_latest_block().await?;
        let target = tip_height.saturating_sub(confirmation_depth);
        let overlap_end = current_height.min(target);
        let oldest = history
            .front()
            .map_or(current_height, |(height, _)| *height);
        if overlap_end < oldest {
            return Err(PipelineError::ReorgBeyondWindow {
                height: current_height,
                depth: reorg_window,
            });
        }

        let overlap = fetch_validated(client, oldest, overlap_end, None).await?;
        let fork = overlap
            .iter()
            .rev()
            .find(|block| hash_at(&history, block.height()) == Some(block.hash))
            .map(ValidatedBlock::height)
            .ok_or(PipelineError::ReorgBeyondWindow {
                height: current_height,
                depth: reorg_window,
            })?;

        if fork < current_height {
            consume(BlockEvent::Reorg { rollback_to: fork }).await?;
            while history.back().is_some_and(|(height, _)| *height > fork) {
                history.pop_back();
            }
        }

        if fork < target {
            let mut next = fork + 1;
            let mut expected_prev = hash_at(&history, fork);
            while next <= target {
                let end = (next + BATCH_SIZE - 1).min(target);
                let blocks = fetch_validated(client, next, end, expected_prev).await?;
                expected_prev = blocks.last().map(|block| block.hash);
                for block in blocks {
                    push_history(&mut history, reorg_window, block.height(), block.hash);
                    consume(BlockEvent::NewBlock(block)).await?;
                }
                next = end + 1;
            }
        }

        current_height = target;
        sleep(poll_interval).await;
    }
}

fn hash_at(history: &VecDeque<(u64, [u8; 32])>, height: u64) -> Option<[u8; 32]> {
    history
        .iter()
        .find_map(|(candidate, hash)| (*candidate == height).then_some(*hash))
}

fn push_history(
    history: &mut VecDeque<(u64, [u8; 32])>,
    max_depth: usize,
    height: u64,
    hash: [u8; 32],
) {
    history.push_back((height, hash));
    if history.len() > max_depth {
        history.pop_front();
    }
}

async fn fetch_validated(
    client: &mut LwdClient,
    start: u64,
    end: u64,
    expected_prev: Option<[u8; 32]>,
) -> Result<Vec<ValidatedBlock>, PipelineError> {
    let blocks = client.get_block_range(start, end).await?;
    validate_range(blocks, start, end, expected_prev)
}

fn validate_range(
    blocks: Vec<CompactBlock>,
    start: u64,
    end: u64,
    expected_prev: Option<[u8; 32]>,
) -> Result<Vec<ValidatedBlock>, PipelineError> {
    let mut validated = Vec::with_capacity(blocks.len());
    let mut expected_height = start;
    let mut prior_hash = expected_prev;

    for block in blocks {
        if block.height != expected_height {
            return Err(PipelineError::UnexpectedHeight {
                expected: expected_height,
                actual: block.height,
            });
        }
        let hash = exact_hash(block.height, "hash", &block.hash)?;
        let prev_hash = exact_hash(block.height, "previous hash", &block.prev_hash)?;
        if prior_hash.is_some_and(|prior| prior != prev_hash) {
            return Err(PipelineError::BrokenLink {
                height: block.height,
            });
        }
        prior_hash = Some(hash);
        expected_height += 1;
        validated.push(ValidatedBlock {
            block,
            hash,
            prev_hash,
        });
    }

    if expected_height != end + 1 {
        return Err(PipelineError::IncompleteRange {
            expected: end,
            actual: expected_height.saturating_sub(1),
        });
    }
    Ok(validated)
}

fn exact_hash(height: u64, field: &'static str, bytes: &[u8]) -> Result<[u8; 32], PipelineError> {
    bytes.try_into().map_err(|_| PipelineError::InvalidHash {
        height,
        field,
        actual: bytes.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(height: u64, hash: u8, prev: u8) -> CompactBlock {
        CompactBlock {
            height,
            hash: vec![hash; 32],
            prev_hash: vec![prev; 32],
            ..Default::default()
        }
    }

    #[test]
    fn validates_complete_linked_range() {
        let blocks =
            validate_range(vec![block(10, 10, 9), block(11, 11, 10)], 10, 11, None).unwrap();
        assert_eq!(blocks.len(), 2);
    }

    #[test]
    fn rejects_missing_block_before_emission() {
        let error = validate_range(vec![block(10, 10, 9)], 10, 11, None).unwrap_err();
        assert!(matches!(error, PipelineError::IncompleteRange { .. }));
    }

    #[test]
    fn rejects_broken_link_and_malformed_hash() {
        assert!(matches!(
            validate_range(vec![block(10, 10, 9), block(11, 11, 8)], 10, 11, None),
            Err(PipelineError::BrokenLink { height: 11 })
        ));
        let mut malformed = block(10, 10, 9);
        malformed.hash.pop();
        assert!(matches!(
            validate_range(vec![malformed], 10, 10, None),
            Err(PipelineError::InvalidHash { actual: 31, .. })
        ));
    }
}
