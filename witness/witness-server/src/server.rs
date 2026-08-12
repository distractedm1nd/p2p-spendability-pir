use crate::routes;
use crate::snapshot_io;
use crate::state::{AppState, PirState, ServerConfig, WitnessMetadata};
use axum::routing::{get, post};
use axum::Router;
use commitment_tree_db::CommitmentTreeDb;
use pir_types::{PirEngine, ServerPhase, NU5_MAINNET_ACTIVATION};
use std::sync::Arc;
use tokio::sync::mpsc;
use witness_types::{WitnessChainEvent, L0_MAX_SHARDS, SHARD_LEAVES};

const ORCHARD_PROTOCOL: i32 = 1;

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[error("ingest error: {0}")]
    Ingest(Box<commitment_ingest::ingest::IngestError>),
    #[error("snapshot io error: {0}")]
    SnapshotIo(#[from] snapshot_io::SnapshotIoError),
    #[error("pir setup failed: {0}")]
    PirSetup(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("chain client error: {0}")]
    Client(Box<chain_ingest::ClientError>),
    #[error("lightwalletd returned no block at height {height} while bootstrapping windowed sync")]
    MissingCompletingBlock { height: u64 },
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

impl From<chain_ingest::ClientError> for ServerError {
    fn from(e: chain_ingest::ClientError) -> Self {
        ServerError::Client(Box::new(e))
    }
}

impl From<commitment_ingest::ingest::IngestError> for ServerError {
    fn from(e: commitment_ingest::ingest::IngestError) -> Self {
        ServerError::Ingest(Box::new(e))
    }
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
    scenario: &pir_types::YpirScenario,
    anchor_height: u64,
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
/// Returns `(CommitmentTreeDb, sync_from_height, initial_tree_size)`.
/// If there are enough completed shards, creates a windowed tree with
/// prefetched roots and syncs only the window. Otherwise syncs from NU5.
async fn prepare_tree(
    client: &mut chain_ingest::LwdClient,
    tip_height: u64,
    window_shard_limit: usize,
) -> Result<(CommitmentTreeDb, u64, Option<u32>)> {
    let window_shard_limit = window_shard_limit.clamp(1, L0_MAX_SHARDS);
    let subtree_roots = client.get_subtree_roots(ORCHARD_PROTOCOL, 0, 65535).await?;
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
            .map(|sr| {
                let mut root = [0u8; 32];
                let len = sr.root_hash.len().min(32);
                root[..len].copy_from_slice(&sr.root_hash[..len]);
                root
            })
            .collect();

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
        let spillover = completing_block_spillover(block, leaf_offset);
        if !spillover.is_empty() {
            let mut hash = [0u8; 32];
            let len = block.hash.len().min(32);
            hash[..len].copy_from_slice(&block.hash[..len]);
            tree.append_commitments(completing_block_height, hash, &spillover);
        }

        let initial_tree_size = Some(tree.tree_size() as u32);

        tracing::info!(
            window_start_shard = window_start,
            window_shard_limit,
            prefetched_roots = subtree_roots[..window_start].len(),
            sync_from,
            leaf_offset,
            initial_tree_size,
            "using windowed sync (skipping {} shards)",
            window_start,
        );

        Ok((tree, sync_from, initial_tree_size))
    } else {
        let floor = min_sync_height(tip_height);
        tracing::info!(
            completed_shards = num_completed,
            window_shard_limit,
            sync_from = floor,
            "full sync from NU5 (fewer than {} completed shards)",
            window_shard_limit,
        );
        Ok((CommitmentTreeDb::new(), floor, None))
    }
}

fn completing_block_spillover(
    block: &chain_ingest::proto::CompactBlock,
    leaf_offset: u64,
) -> Vec<[u8; 32]> {
    // `orchard_commitment_tree_size` is the cumulative size after this block,
    // so it tells us how many of this block's commitments landed inside the
    // current window beyond `leaf_offset`.
    let all = commitment_ingest::parser::extract_commitments(block);
    let end_tree_size = block
        .chain_metadata
        .as_ref()
        .map_or(0u64, |m| m.orchard_commitment_tree_size as u64);
    spillover_from_commitments(&all, end_tree_size, leaf_offset)
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

fn validate_prior_tree_size(
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
fn min_sync_height(tip_height: u64) -> u64 {
    if tip_height >= NU5_MAINNET_ACTIVATION {
        NU5_MAINNET_ACTIVATION
    } else {
        1
    }
}

/// Sync a block range into the tree, reporting progress via `phase`.
pub async fn sync_range(
    lwd_urls: &[String],
    from: u64,
    to: u64,
    tree: &mut CommitmentTreeDb,
    initial_tree_size: Option<u32>,
    phase: &arc_swap::ArcSwap<ServerPhase>,
) -> Result<()> {
    if from > to {
        return Ok(());
    }

    let (tx, mut rx) = mpsc::channel::<WitnessChainEvent>(1000);
    let sync_handle = {
        let mut client = chain_ingest::LwdClient::connect(lwd_urls)
            .await
            .map_err(commitment_ingest::ingest::IngestError::from)?;
        let ts = initial_tree_size;
        tokio::spawn(async move {
            commitment_ingest::ingest::sync(&mut client, from, to, ts, &tx).await
        })
    };

    while let Some(event) = rx.recv().await {
        if let WitnessChainEvent::NewBlock {
            height,
            hash,
            commitments,
            prior_tree_size,
            ..
        } = event
        {
            validate_prior_tree_size(tree, height, prior_tree_size, "initial sync")?;
            tree.append_commitments(height, hash, &commitments);

            if height % 1000 == 0 {
                phase.store(Arc::new(ServerPhase::Syncing {
                    current_height: height,
                    target_height: to,
                }));
                tracing::info!(height, tree_size = tree.tree_size(), "sync progress");
            }
        }
    }

    sync_handle.await.ok();
    Ok(())
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

    let mut client = chain_ingest::LwdClient::connect(&config.lwd_urls)
        .await
        .map_err(commitment_ingest::ingest::IngestError::from)?;

    let (tip_height, _) = client
        .get_latest_block()
        .await
        .map_err(commitment_ingest::ingest::IngestError::from)?;

    // Try loading from snapshot first
    let (mut tree, forward_start, initial_tree_size) =
        match snapshot_io::load_snapshot(&config.data_dir).await {
            Ok(t) => {
                let resume = t.latest_height().map(|h| h + 1).unwrap_or(0);
                let ts = if t.tree_size() > 0 {
                    Some(t.tree_size() as u32)
                } else {
                    None
                };
                tracing::info!(
                    resume_height = resume,
                    tree_size = t.tree_size(),
                    leaf_offset = t.leaf_offset(),
                    "loaded snapshot"
                );
                (t, resume, ts)
            }
            Err(_) => {
                // No snapshot — use GetSubtreeRoots for smart sync
                prepare_tree(
                    &mut client,
                    tip_height,
                    config.effective_window_shard_limit(),
                )
                .await?
            }
        };

    if forward_start <= tip_height {
        app_state.phase.store(Arc::new(ServerPhase::Syncing {
            current_height: forward_start,
            target_height: tip_height,
        }));
        tracing::info!(from = forward_start, to = tip_height, "entering sync mode");
        sync_range(
            &config.lwd_urls,
            forward_start,
            tip_height,
            &mut tree,
            initial_tree_size,
            &app_state.phase,
        )
        .await?;
    }

    tracing::info!(
        tree_size = tree.tree_size(),
        shards = tree.populated_shards(),
        window_start = tree.window_start_shard(),
        latest_height = tree.latest_height(),
        "sync complete",
    );

    snapshot_io::save_snapshot(&tree, &config.data_dir).await?;
    tracing::info!("snapshot saved after sync");

    let anchor_height = tree.latest_height().unwrap_or(0);
    let pir_state = rebuild_pir(&*engine, &mut tree, &app_state.scenario, anchor_height)?;
    app_state.live_pir.store(Arc::new(Some(pir_state)));
    app_state.phase.store(Arc::new(ServerPhase::Serving));
    tracing::info!(anchor_height, tree_size = tree.tree_size(), "serving");

    // Save snapshot again after rebuild so warm cache is persisted
    snapshot_io::save_snapshot(&tree, &config.data_dir).await?;
    tracing::info!("snapshot saved with warm cache");

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
    let follow_tree_size = if tree.tree_size() > 0 {
        Some(tree.tree_size() as u32)
    } else {
        None
    };

    let (tx, mut rx) = mpsc::channel::<WitnessChainEvent>(100);
    let follow_handle = {
        let mut follow_client = chain_ingest::LwdClient::connect(&config.lwd_urls)
            .await
            .map_err(commitment_ingest::ingest::IngestError::from)?;
        let ts = follow_tree_size;
        tokio::spawn(async move {
            commitment_ingest::ingest::follow(
                &mut follow_client,
                latest_height,
                latest_hash,
                ts,
                &tx,
            )
            .await
        })
    };

    let mut blocks_since_snapshot: u64 = 0;

    while let Some(event) = rx.recv().await {
        match event {
            WitnessChainEvent::NewBlock {
                height,
                hash,
                commitments,
                prior_tree_size,
                ..
            } => {
                validate_prior_tree_size(&tree, height, prior_tree_size, "follow mode")?;
                tree.append_commitments(height, hash, &commitments);
                blocks_since_snapshot += 1;
                tracing::info!(
                    height,
                    cmx = commitments.len(),
                    tree_size = tree.tree_size(),
                    "new block"
                );
            }
            WitnessChainEvent::Reorg { rollback_to } => {
                tree.rollback_to(rollback_to);
                tracing::info!(rollback_to, tree_size = tree.tree_size(), "reorg handled");
            }
        }

        let anchor_height = tree.latest_height().unwrap_or(0);
        let pir_state = rebuild_pir(&*engine, &mut tree, &app_state.scenario, anchor_height)?;
        app_state.live_pir.store(Arc::new(Some(pir_state)));

        if blocks_since_snapshot >= config.snapshot_interval {
            snapshot_io::save_snapshot(&tree, &config.data_dir).await?;
            blocks_since_snapshot = 0;
            tracing::info!("periodic snapshot saved");
        }
    }

    follow_handle.abort();
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
    let engine = app_state.engine.clone();

    let mut client = chain_ingest::LwdClient::connect(&config.lwd_urls)
        .await
        .map_err(commitment_ingest::ingest::IngestError::from)?;

    let (tip_height, _) = client
        .get_latest_block()
        .await
        .map_err(commitment_ingest::ingest::IngestError::from)?;

    let (mut tree, forward_start, initial_tree_size) =
        match snapshot_io::load_snapshot(&config.data_dir).await {
            Ok(t) => {
                let resume = t.latest_height().map(|h| h + 1).unwrap_or(0);
                let ts = if t.tree_size() > 0 {
                    Some(t.tree_size() as u32)
                } else {
                    None
                };
                (t, resume, ts)
            }
            Err(_) => {
                prepare_tree(
                    &mut client,
                    tip_height,
                    config.effective_window_shard_limit(),
                )
                .await?
            }
        };

    if forward_start <= tip_height {
        app_state.phase.store(Arc::new(ServerPhase::Syncing {
            current_height: forward_start,
            target_height: tip_height,
        }));
        sync_range(
            &config.lwd_urls,
            forward_start,
            tip_height,
            &mut tree,
            initial_tree_size,
            &app_state.phase,
        )
        .await?;
    }

    snapshot_io::save_snapshot(&tree, &config.data_dir).await?;

    let anchor_height = tree.latest_height().unwrap_or(0);
    let pir_state = rebuild_pir(&*engine, &mut tree, &app_state.scenario, anchor_height)?;
    app_state.live_pir.store(Arc::new(Some(pir_state)));
    app_state.phase.store(Arc::new(ServerPhase::Serving));

    Ok(tree)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chain_ingest::proto::{ChainMetadata, CompactBlock, CompactOrchardAction, CompactTx};
    use witness_types::SHARD_LEAVES;

    fn make_leaf(tag: u64) -> [u8; 32] {
        let mut hash = [0u8; 32];
        hash[..8].copy_from_slice(&tag.to_le_bytes());
        hash
    }

    fn make_action(tag: u64) -> CompactOrchardAction {
        CompactOrchardAction {
            nullifier: vec![0; 32],
            cmx: make_leaf(tag).to_vec(),
            ephemeral_key: vec![0; 32],
            ciphertext: vec![0; 52],
        }
    }

    fn make_block(height: u64, tags: &[u64], tree_size: u64) -> CompactBlock {
        CompactBlock {
            height,
            hash: vec![height as u8; 32],
            prev_hash: vec![height.saturating_sub(1) as u8; 32],
            vtx: vec![CompactTx {
                actions: tags.iter().copied().map(make_action).collect(),
                ..Default::default()
            }],
            chain_metadata: Some(ChainMetadata {
                sapling_commitment_tree_size: 0,
                orchard_commitment_tree_size: tree_size as u32,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn spillover_slice_keeps_only_commitments_past_offset() {
        let commitments = vec![make_leaf(1), make_leaf(2), make_leaf(3), make_leaf(4)];

        let spillover = spillover_from_commitments(&commitments, 6, 4);

        assert_eq!(spillover, vec![make_leaf(3), make_leaf(4)]);
    }

    #[test]
    fn completing_block_spillover_matches_window_bootstrap_reference_tree() {
        let spillover_count = 3usize;
        let next_block_count = 2usize;
        let first_block_tags: Vec<u64> = (0..(SHARD_LEAVES + spillover_count))
            .map(|i| i as u64 + 1)
            .collect();
        let second_block_tags: Vec<u64> = (0..next_block_count)
            .map(|i| SHARD_LEAVES as u64 + spillover_count as u64 + i as u64 + 1)
            .collect();

        let completing_block = make_block(
            100,
            &first_block_tags,
            SHARD_LEAVES as u64 + spillover_count as u64,
        );
        let next_block = make_block(
            101,
            &second_block_tags,
            SHARD_LEAVES as u64 + spillover_count as u64 + next_block_count as u64,
        );

        let spillover = completing_block_spillover(&completing_block, SHARD_LEAVES as u64);
        let expected_spillover: Vec<_> = first_block_tags
            .iter()
            .skip(SHARD_LEAVES)
            .copied()
            .map(make_leaf)
            .collect();
        assert_eq!(spillover, expected_spillover);

        let completed_shard: Vec<_> = first_block_tags
            .iter()
            .take(SHARD_LEAVES)
            .copied()
            .map(make_leaf)
            .collect();
        let second_block_commitments = commitment_ingest::parser::extract_commitments(&next_block);

        let mut prefetched_tree = CommitmentTreeDb::new();
        prefetched_tree.append_commitments(99, [0xAA; 32], &completed_shard);
        let prefetched_root = prefetched_tree.shard_roots()[0].1;

        let mut reference_tree = CommitmentTreeDb::new();
        reference_tree.append_commitments(
            completing_block.height,
            [0x11; 32],
            &commitment_ingest::parser::extract_commitments(&completing_block),
        );
        reference_tree.append_commitments(next_block.height, [0x22; 32], &second_block_commitments);

        let mut windowed_tree =
            CommitmentTreeDb::with_offset(SHARD_LEAVES as u64, vec![prefetched_root]);
        windowed_tree.append_commitments(completing_block.height, [0x11; 32], &spillover);
        windowed_tree.append_commitments(next_block.height, [0x22; 32], &second_block_commitments);

        assert_eq!(
            windowed_tree.leaves(),
            &reference_tree.leaves()[SHARD_LEAVES..],
            "window bootstrap should preserve leaf ordering across the shard boundary"
        );
        assert_eq!(
            windowed_tree.tree_root(),
            reference_tree.tree_root(),
            "window bootstrap should match the full reference tree root"
        );
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
