use super::routes;
use super::snapshot_io;
use super::state::{AppState, PirState, ServerConfig};
use crate::ingest::{BlockEvent, PipelineError};
use axum::routing::{get, post};
use axum::Router;
use nullifier_pir::HashTableDb;
use nullifier_pir::{
    PirEngine, ServerPhase, SpendabilityMetadata, ZcashNetwork, DATASET_VERSION, IRONWOOD_POOL,
    NUM_BUCKETS,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const BACKFILL_BATCH: u64 = 50_000;
const FOLLOW_POLL_INTERVAL: Duration = Duration::from_secs(2);
const SNAPSHOT_ARTIFACTS: &[&str] = &["snapshot.bin", "snapshot.bin.tmp"];

fn min_sync_height(tip_height: u64, network: ZcashNetwork) -> u64 {
    let activation = crate::ingest::nu6_3_activation_height(network);
    if tip_height >= activation {
        activation
    } else {
        1
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[error(transparent)]
    Client(#[from] crate::ingest::ClientError),
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
    #[error(transparent)]
    Ironwood(#[from] crate::ingest::IronwoodError),
    #[error("hashtable error: {0}")]
    HashTable(#[from] nullifier_pir::HashTableError),
    #[error("snapshot io error: {0}")]
    SnapshotIo(#[from] snapshot_io::SnapshotIoError),
    #[error("pir setup failed: {0}")]
    PirSetup(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("ingest task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Dataset(#[from] crate::ingest::DatasetError),
    #[error(transparent)]
    Endpoint(#[from] crate::ingest::EndpointError),
}

pub type Result<T> = std::result::Result<T, ServerError>;

/// Build the Axum router for the given AppState.
pub fn build_router<P: PirEngine + 'static>(state: Arc<AppState<P>>) -> Router {
    Router::new()
        .route("/health", get(routes::health::<P>))
        .route("/metadata", get(routes::metadata::<P>))
        .route("/params", get(routes::params::<P>))
        .route("/query", post(routes::query::<P>))
        .with_state(state)
}

/// Build PIR server state from the current hash table and store it in the ArcSwap.
pub fn rebuild_pir<P: PirEngine>(
    engine: &P,
    hashtable: &HashTableDb,
    scenario: &nullifier_pir::YpirScenario,
    zcash_network: ZcashNetwork,
) -> std::result::Result<PirState<P>, ServerError> {
    let total_start = std::time::Instant::now();

    let serialize_start = std::time::Instant::now();
    let db_bytes = hashtable.to_pir_bytes();
    let serialize_ms = serialize_start.elapsed().as_millis();

    let setup_start = std::time::Instant::now();
    let engine_state = engine
        .setup(&db_bytes, scenario)
        .map_err(|e| ServerError::PirSetup(e.to_string()))?;
    let setup_ms = setup_start.elapsed().as_millis();

    let metadata = SpendabilityMetadata {
        zcash_network,
        nullifier_pool: IRONWOOD_POOL.into(),
        dataset_version: DATASET_VERSION,
        earliest_height: hashtable.earliest_height().unwrap_or(0),
        latest_height: hashtable.latest_height().unwrap_or(0),
        num_nullifiers: hashtable.len() as u64,
        num_buckets: NUM_BUCKETS as u64,
        phase: ServerPhase::Serving,
    };

    tracing::info!(
        total_ms = total_start.elapsed().as_millis() as u64,
        serialize_ms = serialize_ms as u64,
        setup_ms = setup_ms as u64,
        db_bytes = db_bytes.len(),
        nullifiers = metadata.num_nullifiers,
        height_range = format_args!("{}..{}", metadata.earliest_height, metadata.latest_height),
        "pir rebuild complete",
    );

    Ok(PirState {
        engine_state,
        metadata,
    })
}

/// Sync a block range into the hashtable, reporting progress via `phase`.
pub async fn sync_range(
    lwd_urls: &[String],
    zcash_network: ZcashNetwork,
    from: u64,
    to: u64,
    hashtable: &mut HashTableDb,
    phase: &arc_swap::ArcSwap<ServerPhase>,
) -> Result<()> {
    if from > to {
        return Ok(());
    }

    let mut client = crate::ingest::LwdClient::connect(lwd_urls).await?;
    crate::ingest::require_ironwood_tree_state(&mut client, zcash_network, to).await?;
    crate::ingest::sync_blocks(&mut client, from, to, |block| {
        let result = (|| {
            let nullifiers = crate::ingest::extract_ironwood_nullifiers(&block.block)?;
            hashtable.insert_block(block.height(), block.hash, &nullifiers)?;
            if block.height() % 1000 == 0 {
                phase.store(Arc::new(ServerPhase::Syncing {
                    current_height: block.height(),
                    target_height: to,
                }));
                tracing::info!(
                    height = block.height(),
                    nullifiers = hashtable.len(),
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

    let mut hashtable = sync_into(app_state.clone()).await?;
    tracing::info!(
        height = hashtable.latest_height(),
        nullifiers = hashtable.len(),
        "serving"
    );

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
    let latest_height = hashtable.latest_height().unwrap_or(0);
    let latest_hash = hashtable.latest_block_hash().unwrap_or([0u8; 32]);

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
                config.confirmation_depth,
                nullifier_pir::CONFIRMATION_DEPTH as usize * 2,
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
                let nullifiers = crate::ingest::extract_ironwood_nullifiers(&block.block)?;
                hashtable.insert_block(block.height(), block.hash, &nullifiers)?;
                hashtable.evict_to_target();
                blocks_since_snapshot += 1;
                tracing::info!(height = block.height(), nfs = nullifiers.len(), "new block");
            }
            BlockEvent::Reorg { rollback_to } => {
                while hashtable
                    .latest_height()
                    .is_some_and(|height| height > rollback_to)
                {
                    if let Some(hash) = hashtable.latest_block_hash() {
                        hashtable.rollback_block(&hash)?;
                    }
                }
                hashtable.evict_to_target();
                blocks_since_snapshot += 1;
                tracing::info!(rollback_to, "reorg handled");
            }
        }

        // Rebuild PIR and atomic swap
        let pir_state = rebuild_pir(
            &*engine,
            &hashtable,
            &app_state.scenario,
            config.zcash_network,
        )?;
        app_state.live_pir.store(Arc::new(Some(pir_state)));

        // Periodic snapshot
        if blocks_since_snapshot >= config.snapshot_interval {
            snapshot_io::save_snapshot(&hashtable, &config.data_dir).await?;
            blocks_since_snapshot = 0;
            tracing::info!("periodic snapshot saved");
        }
    }

    follow_handle.await??;
    http_handle.abort();
    Ok(())
}

/// Simplified runner for testing: runs sync, builds PIR, returns the app state
/// without entering the follow loop. Caller can then hit HTTP routes directly.
pub async fn run_sync_only<P: PirEngine + 'static>(
    config: ServerConfig,
    engine: Arc<P>,
) -> Result<(Arc<AppState<P>>, HashTableDb)> {
    let app_state = Arc::new(AppState::new(config, engine));
    let hashtable = sync_into(app_state.clone()).await?;
    Ok((app_state, hashtable))
}

pub async fn sync_into<P: PirEngine + 'static>(app_state: Arc<AppState<P>>) -> Result<HashTableDb> {
    let config = app_state.config.clone();
    let engine = app_state.engine.clone();
    crate::ingest::ensure_ironwood_dataset(
        &config.data_dir,
        config.zcash_network,
        SNAPSHOT_ARTIFACTS,
    )?;

    let mut client = crate::ingest::LwdClient::connect(&config.lwd_urls).await?;

    let (tip_height, _) = client.get_latest_block().await?;
    let target_height = tip_height.saturating_sub(config.confirmation_depth);
    crate::ingest::require_ironwood_tree_state(&mut client, config.zcash_network, target_height)
        .await?;

    let (mut hashtable, from_snapshot) = match snapshot_io::load_snapshot(&config.data_dir).await {
        Ok(ht) => (ht, true),
        Err(snapshot_io::SnapshotIoError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            (HashTableDb::new(), false)
        }
        Err(error) => return Err(error.into()),
    };

    let floor = min_sync_height(target_height, config.zcash_network);
    let forward_start = if from_snapshot {
        hashtable
            .latest_height()
            .map(|h| h + 1)
            .unwrap_or(target_height)
    } else {
        let initial = target_height.saturating_sub(BACKFILL_BATCH);
        initial.max(floor)
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
            &mut hashtable,
            &app_state.phase,
        )
        .await?;
    }

    if !from_snapshot {
        let mut backfill_end = forward_start.saturating_sub(1);
        while hashtable.len() < config.target_size && backfill_end >= floor {
            let backfill_start = backfill_end.saturating_sub(BACKFILL_BATCH - 1).max(floor);
            sync_range(
                &config.lwd_urls,
                config.zcash_network,
                backfill_start,
                backfill_end,
                &mut hashtable,
                &app_state.phase,
            )
            .await?;

            if backfill_start == floor {
                break;
            }
            backfill_end = backfill_start.saturating_sub(1);
        }
    }

    hashtable.evict_to_target();
    let pir_state = rebuild_pir(
        &*engine,
        &hashtable,
        &app_state.scenario,
        config.zcash_network,
    )?;
    app_state.live_pir.store(Arc::new(Some(pir_state)));
    app_state.phase.store(Arc::new(ServerPhase::Serving));
    snapshot_io::save_snapshot(&hashtable, &config.data_dir).await?;

    Ok(hashtable)
}
