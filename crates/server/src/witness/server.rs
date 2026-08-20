use super::routes;
use super::snapshot_io;
use super::state::{AppState, PirState, ServerConfig, WitnessMetadata};
use crate::ingest::{BlockEvent, PipelineError};
use axum::routing::{get, post};
use axum::Router;
use pir_protocol::{
    PirEngine, ServerPhase, ZcashNetwork, CONFIRMATION_DEPTH, DATASET_VERSION, IRONWOOD_POOL,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use witness_pir::CommitmentTreeDb;
use witness_pir::{L0_MAX_SHARDS, SHARD_LEAVES};

const IRONWOOD_PROTOCOL: i32 = 2;
const FOLLOW_POLL_INTERVAL: Duration = Duration::from_secs(2);
const SNAPSHOT_ARTIFACTS: &[&str] = &["witness_snapshot.bin", "witness_snapshot.bin.tmp"];

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
    #[error("snapshot io error: {0}")]
    SnapshotIo(#[from] snapshot_io::SnapshotIoError),
    #[error("pir setup failed: {0}")]
    PirSetup(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Client(#[from] crate::ingest::ClientError),
    #[error("ingest task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Endpoint(#[from] crate::ingest::EndpointError),
    #[error(transparent)]
    Parse(#[from] crate::ingest::ParseError),
    #[error(transparent)]
    Dataset(#[from] crate::ingest::DatasetError),
    #[error("lightwalletd returned no block at height {height} while bootstrapping windowed sync")]
    MissingCompletingBlock { height: u64 },
    #[error("malformed Ironwood subtree root at index {index}")]
    InvalidSubtreeRoot { index: usize },
    #[error("malformed block hash at height {height}")]
    InvalidBlockHash { height: u64 },
    #[error("lightwalletd returned height {actual} while requesting completing block {expected}")]
    WrongCompletingBlockHeight { actual: u64, expected: u64 },
    #[error(
        "{context} tree size mismatch before appending block {height}: expected {expected}, got {actual}"
    )]
    TreeSizeMismatch {
        context: &'static str,
        height: u64,
        expected: u64,
        actual: u64,
    },
}

pub type Result<T> = std::result::Result<T, ServerError>;

/// Build the Axum router for the given AppState.
pub fn build_router<P: PirEngine + 'static>(state: Arc<AppState<P>>) -> Router {
    Router::new()
        .route("/health", get(routes::health::<P>))
        .route("/metadata", get(routes::metadata::<P>))
        .route("/broadcast", get(routes::broadcast::<P>))
        .route("/params", get(routes::params::<P>))
        .route("/query", post(routes::query::<P>))
        .with_state(state)
}

/// Build PIR server state from the current commitment tree and store it.
pub fn rebuild_pir<P: PirEngine>(
    engine: &P,
    tree: &mut CommitmentTreeDb,
    scenario: &pir_protocol::YpirScenario,
    anchor_height: u64,
    zcash_network: ZcashNetwork,
) -> std::result::Result<PirState<P>, ServerError> {
    let total_start = std::time::Instant::now();

    let build_start = std::time::Instant::now();
    let (db_bytes, broadcast) = tree.build_pir_db_and_broadcast(anchor_height);
    let build_ms = build_start.elapsed().as_millis();

    let setup_start = std::time::Instant::now();
    let engine_state = engine
        .setup(&db_bytes, scenario)
        .map_err(|e| ServerError::PirSetup(e.to_string()))?;
    let setup_ms = setup_start.elapsed().as_millis();

    let metadata = WitnessMetadata {
        zcash_network,
        commitment_pool: IRONWOOD_POOL.into(),
        dataset_version: DATASET_VERSION,
        anchor_height,
        tree_size: tree.tree_size(),
        window_start_shard: tree.window_start_shard(),
        window_shard_count: tree.window_shard_count(),
        populated_shards: tree.populated_shards(),
        phase: ServerPhase::Serving,
    };

    tracing::info!(
        total_ms = total_start.elapsed().as_millis() as u64,
        build_ms = build_ms as u64,
        setup_ms = setup_ms as u64,
        db_bytes = db_bytes.len(),
        tree_size = metadata.tree_size,
        shards = metadata.populated_shards,
        window = format_args!(
            "{}..+{}",
            metadata.window_start_shard, metadata.window_shard_count
        ),
        anchor_height,
        "pir rebuild complete",
    );

    Ok(PirState {
        engine_state,
        broadcast,
        metadata,
    })
}

/// Determine the sync start point using GetSubtreeRoots.
///
/// Returns `(CommitmentTreeDb, sync_from_height)`.
/// If there are enough completed shards, creates a windowed tree with
/// prefetched roots and syncs only the window. Otherwise syncs from NU6.3.
pub async fn prepare_tree(
    client: &mut crate::ingest::LwdClient,
    target_height: u64,
    window_shard_limit: usize,
    zcash_network: ZcashNetwork,
) -> Result<(CommitmentTreeDb, u64)> {
    let window_shard_limit = window_shard_limit.clamp(1, L0_MAX_SHARDS);
    let mut subtree_roots = client
        .get_subtree_roots(IRONWOOD_PROTOCOL, 0, 65535)
        .await?;
    subtree_roots.retain(|root| root.completing_block_height <= target_height);
    let num_completed = subtree_roots.len();

    tracing::info!(
        completed_shards = num_completed,
        "fetched subtree roots from lightwalletd"
    );

    if num_completed >= window_shard_limit {
        // Window: keep the last (`window_shard_limit` - 1) completed shards + frontier
        let window_start = num_completed - (window_shard_limit - 1);
        let leaf_offset = (window_start as u64) * (SHARD_LEAVES as u64);

        let prefetched: Vec<[u8; 32]> = subtree_roots[..window_start]
            .iter()
            .enumerate()
            .map(|(index, sr)| {
                if sr.root_hash.len() != 32 {
                    return Err(ServerError::InvalidSubtreeRoot { index });
                }
                let mut root = [0u8; 32];
                root.copy_from_slice(&sr.root_hash);
                if !CommitmentTreeDb::is_canonical_hash(&root) {
                    return Err(ServerError::InvalidSubtreeRoot { index });
                }
                Ok(root)
            })
            .collect::<Result<_>>()?;

        let completing_block_height = subtree_roots[window_start - 1].completing_block_height;
        let sync_from = completing_block_height + 1;

        // Seed the window with all fully completed shard roots first.
        let mut tree = CommitmentTreeDb::with_offset(leaf_offset, prefetched);
        let completing_blocks = client
            .get_block_range(completing_block_height, completing_block_height)
            .await?;

        // The shard-completing block can also contain the first leaves of the
        // window we are about to sync. If we skip those leaves, every later
        // position in the window is shifted.
        let block = completing_blocks
            .first()
            .ok_or(ServerError::MissingCompletingBlock {
                height: completing_block_height,
            })?;
        if block.height != completing_block_height {
            return Err(ServerError::WrongCompletingBlockHeight {
                actual: block.height,
                expected: completing_block_height,
            });
        }
        let spillover = completing_block_spillover(block, leaf_offset)?;
        let hash = block
            .hash
            .as_slice()
            .try_into()
            .map_err(|_| ServerError::InvalidBlockHash {
                height: completing_block_height,
            })?;
        tree.append_commitments(completing_block_height, hash, &spillover);

        tracing::info!(
            window_start_shard = window_start,
            window_shard_limit,
            prefetched_roots = subtree_roots[..window_start].len(),
            sync_from,
            leaf_offset,
            "using windowed sync (skipping {} shards)",
            window_start,
        );

        Ok((tree, sync_from))
    } else {
        let floor = min_sync_height(zcash_network);
        tracing::info!(
            completed_shards = num_completed,
            window_shard_limit,
            sync_from = floor,
            "full sync from NU6.3 (fewer than {} completed shards)",
            window_shard_limit,
        );
        Ok((CommitmentTreeDb::new(), floor))
    }
}

fn completing_block_spillover(
    block: &crate::ingest::proto::CompactBlock,
    leaf_offset: u64,
) -> Result<Vec<[u8; 32]>> {
    // `ironwood_commitment_tree_size` is the cumulative size after this block,
    // so it tells us how many of this block's commitments landed inside the
    // current window beyond `leaf_offset`.
    let all = crate::ingest::extract_commitments(block)?;
    let end_tree_size = block
        .chain_metadata
        .as_ref()
        .map_or(0u64, |m| m.ironwood_commitment_tree_size as u64);
    Ok(spillover_from_commitments(&all, end_tree_size, leaf_offset))
}

fn spillover_from_commitments(
    commitments: &[[u8; 32]],
    end_tree_size: u64,
    leaf_offset: u64,
) -> Vec<[u8; 32]> {
    if end_tree_size <= leaf_offset {
        return vec![];
    }

    // Keep only the suffix whose absolute positions are inside the window.
    let spillover_count = (end_tree_size - leaf_offset) as usize;
    let skip = commitments.len().saturating_sub(spillover_count);
    commitments[skip..].to_vec()
}

pub fn validate_prior_tree_size(
    tree: &CommitmentTreeDb,
    height: u64,
    prior_tree_size: Option<u32>,
    context: &'static str,
) -> Result<()> {
    let Some(expected) = prior_tree_size else {
        return Ok(());
    };

    let actual = tree.tree_size();
    let expected = u64::from(expected);
    if actual == expected {
        return Ok(());
    }

    tracing::error!(
        context,
        height,
        expected_tree_size = expected,
        actual_tree_size = actual,
        leaf_offset = tree.leaf_offset(),
        latest_height = tree.latest_height(),
        latest_hash = ?tree.latest_block_hash(),
        "tree size mismatch before appending commitments"
    );

    Err(ServerError::TreeSizeMismatch {
        context,
        height,
        expected,
        actual,
    })
}

/// Lowest block height we'll ever sync.
fn min_sync_height(network: ZcashNetwork) -> u64 {
    crate::ingest::nu6_3_activation_height(network)
}

/// Sync a block range into the tree, reporting progress via `phase`.
pub async fn sync_range(
    lwd_urls: &[String],
    zcash_network: ZcashNetwork,
    from: u64,
    to: u64,
    tree: &mut CommitmentTreeDb,
    phase: &arc_swap::ArcSwap<ServerPhase>,
) -> Result<()> {
    if from > to {
        return Ok(());
    }

    let mut client = crate::ingest::LwdClient::connect(lwd_urls).await?;
    crate::ingest::require_ironwood_tree_state(&mut client, zcash_network, to).await?;
    crate::ingest::sync_blocks(&mut client, from, to, |block| {
        let result = (|| {
            let commitments = crate::ingest::extract_commitments(&block.block)?;
            let prior_tree_size =
                crate::ingest::ironwood_prior_tree_size(&block.block, commitments.len())?;
            validate_prior_tree_size(tree, block.height(), Some(prior_tree_size), "initial sync")?;
            tree.append_commitments(block.height(), block.hash, &commitments);
            if block.height() % 1000 == 0 {
                phase.store(Arc::new(ServerPhase::Syncing {
                    current_height: block.height(),
                    target_height: to,
                }));
                tracing::info!(
                    height = block.height(),
                    tree_size = tree.tree_size(),
                    "sync progress"
                );
            }
            Ok(())
        })();
        std::future::ready(result)
    })
    .await
}

/// Main server entry point. Runs sync mode, transitions to follow mode, serves HTTP.
pub async fn run<P: PirEngine + 'static>(config: ServerConfig, engine: Arc<P>) -> Result<()> {
    let app_state = Arc::new(AppState::new(config.clone(), engine.clone()));

    // Try to bind early so health checks are available during sync.
    // Non-fatal: if the port is busy we'll sync + save snapshot and retry.
    let early_http = match tokio::net::TcpListener::bind(&config.listen_addr).await {
        Ok(listener) => {
            tracing::info!(listen = %config.listen_addr, "http server started (sync in progress)");
            let router = build_router(app_state.clone());
            Some(tokio::spawn(async move {
                axum::serve(listener, router).await.ok();
            }))
        }
        Err(e) => {
            tracing::warn!(addr = %config.listen_addr, error = %e, "port busy, will retry after sync");
            None
        }
    };

    let mut tree = sync_into(app_state.clone()).await?;
    let anchor_height = tree.latest_height().unwrap_or(0);
    tracing::info!(anchor_height, tree_size = tree.tree_size(), "serving");

    let http_handle = match early_http {
        Some(h) => h,
        None => {
            let router = build_router(app_state.clone());
            let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
            tracing::info!(listen = %config.listen_addr, "http server started");
            tokio::spawn(async move {
                axum::serve(listener, router).await.ok();
            })
        }
    };

    // Follow mode
    let latest_height = tree.latest_height().unwrap_or(0);
    let latest_hash = tree.latest_block_hash().unwrap_or([0u8; 32]);
    let (tx, mut rx) = mpsc::channel::<BlockEvent>(100);
    let follow_handle = {
        let mut follow_client = crate::ingest::LwdClient::connect(&config.lwd_urls).await?;
        crate::ingest::require_ironwood_tree_state(
            &mut follow_client,
            config.zcash_network,
            latest_height,
        )
        .await?;
        tokio::spawn(async move {
            crate::ingest::follow_blocks(
                &mut follow_client,
                latest_height,
                latest_hash,
                CONFIRMATION_DEPTH,
                CONFIRMATION_DEPTH as usize * 2,
                FOLLOW_POLL_INTERVAL,
                |event| {
                    let tx = tx.clone();
                    async move {
                        tx.send(event)
                            .await
                            .map_err(|_| PipelineError::ConsumerDropped)
                    }
                },
            )
            .await
        })
    };

    let mut blocks_since_snapshot: u64 = 0;

    while let Some(event) = rx.recv().await {
        match event {
            BlockEvent::NewBlock(block) => {
                let commitments = crate::ingest::extract_commitments(&block.block)?;
                let prior_tree_size =
                    crate::ingest::ironwood_prior_tree_size(&block.block, commitments.len())?;
                validate_prior_tree_size(
                    &tree,
                    block.height(),
                    Some(prior_tree_size),
                    "follow mode",
                )?;
                tree.append_commitments(block.height(), block.hash, &commitments);
                blocks_since_snapshot += 1;
                tracing::info!(
                    height = block.height(),
                    cmx = commitments.len(),
                    tree_size = tree.tree_size(),
                    "new block"
                );
            }
            BlockEvent::Reorg { rollback_to } => {
                tree.rollback_to(rollback_to);
                tracing::info!(rollback_to, tree_size = tree.tree_size(), "reorg handled");
            }
        }

        let anchor_height = tree.latest_height().unwrap_or(0);
        let pir_state = rebuild_pir(
            &*engine,
            &mut tree,
            &app_state.scenario,
            anchor_height,
            config.zcash_network,
        )?;
        app_state.live_pir.store(Arc::new(Some(pir_state)));

        if blocks_since_snapshot >= config.snapshot_interval {
            snapshot_io::save_snapshot(&tree, &config.data_dir).await?;
            blocks_since_snapshot = 0;
            tracing::info!("periodic snapshot saved");
        }
    }

    follow_handle.await??;
    http_handle.abort();
    Ok(())
}

/// Simplified runner for testing: runs sync, builds PIR, returns the app state.
pub async fn run_sync_only<P: PirEngine + 'static>(
    config: ServerConfig,
    engine: Arc<P>,
) -> Result<(Arc<AppState<P>>, CommitmentTreeDb)> {
    let app_state = Arc::new(AppState::new(config, engine));
    let tree = sync_into(app_state.clone()).await?;
    Ok((app_state, tree))
}

pub async fn sync_into<P: PirEngine + 'static>(
    app_state: Arc<AppState<P>>,
) -> Result<CommitmentTreeDb> {
    let config = app_state.config.clone();
    crate::ingest::ensure_ironwood_dataset(
        &config.data_dir,
        config.zcash_network,
        SNAPSHOT_ARTIFACTS,
    )?;
    let engine = app_state.engine.clone();

    let mut client = crate::ingest::LwdClient::connect(&config.lwd_urls).await?;

    let (tip_height, _) = client.get_latest_block().await?;
    let target_height = tip_height.saturating_sub(CONFIRMATION_DEPTH);
    crate::ingest::require_ironwood_tree_state(&mut client, config.zcash_network, target_height)
        .await?;

    let (mut tree, forward_start) = match snapshot_io::load_snapshot(&config.data_dir).await {
        Ok(t) => {
            let resume = t
                .latest_height()
                .map(|height| height + 1)
                .unwrap_or_else(|| min_sync_height(config.zcash_network));
            (t, resume)
        }
        Err(snapshot_io::SnapshotIoError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            prepare_tree(
                &mut client,
                target_height,
                config.effective_window_shard_limit(),
                config.zcash_network,
            )
            .await?
        }
        Err(error) => return Err(error.into()),
    };

    if forward_start <= target_height {
        app_state.phase.store(Arc::new(ServerPhase::Syncing {
            current_height: forward_start,
            target_height,
        }));
        sync_range(
            &config.lwd_urls,
            config.zcash_network,
            forward_start,
            target_height,
            &mut tree,
            &app_state.phase,
        )
        .await?;
    }

    let anchor_height = tree.latest_height().unwrap_or(0);
    let pir_state = rebuild_pir(
        &*engine,
        &mut tree,
        &app_state.scenario,
        anchor_height,
        config.zcash_network,
    )?;
    app_state.live_pir.store(Arc::new(Some(pir_state)));
    app_state.phase.store(Arc::new(ServerPhase::Serving));
    snapshot_io::save_snapshot(&tree, &config.data_dir).await?;

    Ok(tree)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_leaf(tag: u64) -> [u8; 32] {
        let mut hash = [0u8; 32];
        hash[..8].copy_from_slice(&tag.to_le_bytes());
        hash
    }

    #[test]
    fn spillover_slice_keeps_only_commitments_past_offset() {
        let commitments = vec![make_leaf(1), make_leaf(2), make_leaf(3), make_leaf(4)];

        let spillover = spillover_from_commitments(&commitments, 6, 4);

        assert_eq!(spillover, vec![make_leaf(3), make_leaf(4)]);
    }

    #[test]
    fn validate_prior_tree_size_rejects_drift() {
        let mut tree = CommitmentTreeDb::new();
        tree.append_commitments(100, [1u8; 32], &[make_leaf(1), make_leaf(2)]);

        let err = validate_prior_tree_size(&tree, 101, Some(1), "test").unwrap_err();

        match err {
            ServerError::TreeSizeMismatch {
                context,
                height,
                expected,
                actual,
            } => {
                assert_eq!(context, "test");
                assert_eq!(height, 101);
                assert_eq!(expected, 1);
                assert_eq!(actual, 2);
            }
            other => panic!("expected TreeSizeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn effective_window_shard_limit_is_clamped() {
        assert_eq!(0usize.clamp(1, L0_MAX_SHARDS), 1);
        assert_eq!(2usize.clamp(1, L0_MAX_SHARDS), 2);
        assert_eq!(64usize.clamp(1, L0_MAX_SHARDS), L0_MAX_SHARDS);
    }
}
