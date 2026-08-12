use crate::routes;
use crate::snapshot_io;
use crate::state::{AppState, PirState, ServerConfig};
use axum::routing::{get, post};
use axum::Router;
use hashtable_pir::HashTableDb;
use spend_types::{
    ChainEvent, PirEngine, ServerPhase, SpendabilityMetadata, NU5_MAINNET_ACTIVATION, NUM_BUCKETS,
};
use std::sync::Arc;
use tokio::sync::mpsc;

const BACKFILL_BATCH: u64 = 50_000;

/// Lowest block height we'll ever sync. On mainnet this is the NU5 activation
/// height; on test chains whose tip is below that, fall back to height 1.
fn min_sync_height(tip_height: u64) -> u64 {
    if tip_height >= NU5_MAINNET_ACTIVATION {
        NU5_MAINNET_ACTIVATION
    } else {
        1
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[error("ingest error: {0}")]
    Ingest(Box<nf_ingest::ingest::IngestError>),
    #[error("hashtable error: {0}")]
    HashTable(#[from] hashtable_pir::HashTableError),
    #[error("snapshot io error: {0}")]
    SnapshotIo(#[from] snapshot_io::SnapshotIoError),
    #[error("pir setup failed: {0}")]
    PirSetup(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<nf_ingest::ingest::IngestError> for ServerError {
    fn from(e: nf_ingest::ingest::IngestError) -> Self {
        ServerError::Ingest(Box::new(e))
    }
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
    scenario: &spend_types::YpirScenario,
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
    from: u64,
    to: u64,
    hashtable: &mut HashTableDb,
    initial_tree_size: Option<u32>,
    phase: &arc_swap::ArcSwap<ServerPhase>,
) -> Result<()> {
    if from > to {
        return Ok(());
    }

    let (tx, mut rx) = mpsc::channel::<ChainEvent>(1000);
    let sync_handle = {
        let mut client = nf_ingest::LwdClient::connect(lwd_urls)
            .await
            .map_err(nf_ingest::ingest::IngestError::from)?;
        tokio::spawn(async move {
            nf_ingest::ingest::sync(&mut client, from, to, initial_tree_size, &tx).await
        })
    };

    while let Some(event) = rx.recv().await {
        if let ChainEvent::NewBlock {
            height,
            hash,
            nullifiers,
            ..
        } = event
        {
            hashtable.insert_block(height, hash, &nullifiers)?;

            if height % 1000 == 0 {
                phase.store(Arc::new(ServerPhase::Syncing {
                    current_height: height,
                    target_height: to,
                }));
                tracing::info!(height, nullifiers = hashtable.len(), "sync progress");
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

    // Connect to lightwalletd
    let mut client = nf_ingest::LwdClient::connect(&config.lwd_urls)
        .await
        .map_err(nf_ingest::ingest::IngestError::from)?;

    let (tip_height, _) = client
        .get_latest_block()
        .await
        .map_err(nf_ingest::ingest::IngestError::from)?;

    // Load snapshot or fresh start
    let (mut hashtable, from_snapshot) = match snapshot_io::load_snapshot(&config.data_dir).await {
        Ok(ht) => {
            let resume = ht.latest_height().map(|h| h + 1).unwrap_or(0);
            tracing::info!(resume_height = resume, "loaded snapshot");
            (ht, true)
        }
        Err(_) => (HashTableDb::new(), false),
    };

    // Sync mode: catch up to tip, then backfill if we need more nullifiers
    let floor = min_sync_height(tip_height);
    let forward_start = if from_snapshot {
        hashtable
            .latest_height()
            .map(|h| h + 1)
            .unwrap_or(tip_height)
    } else {
        let initial = tip_height.saturating_sub(BACKFILL_BATCH);
        initial.max(floor)
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
            &mut hashtable,
            None,
            &app_state.phase,
        )
        .await?;
    }

    // Backfill earlier blocks until we reach target_size or the floor height
    if !from_snapshot {
        let mut backfill_end = forward_start.saturating_sub(1);
        while hashtable.len() < config.target_size && backfill_end >= floor {
            let backfill_start = backfill_end.saturating_sub(BACKFILL_BATCH - 1).max(floor);
            tracing::info!(
                from = backfill_start,
                to = backfill_end,
                nullifiers = hashtable.len(),
                target = config.target_size,
                "backfilling earlier blocks",
            );
            sync_range(
                &config.lwd_urls,
                backfill_start,
                backfill_end,
                &mut hashtable,
                None,
                &app_state.phase,
            )
            .await?;

            if backfill_start == floor {
                break;
            }
            backfill_end = backfill_start.saturating_sub(1);
        }
        tracing::info!(
            nullifiers = hashtable.len(),
            blocks = hashtable.num_blocks(),
            earliest = hashtable.earliest_height(),
            latest = hashtable.latest_height(),
            "sync complete",
        );
    }

    // Save snapshot after sync
    snapshot_io::save_snapshot(&hashtable, &config.data_dir).await?;
    tracing::info!("snapshot saved after sync");

    // Evict down to target now that we've filled up
    hashtable.evict_to_target();

    // Build PIR once and start serving
    let pir_state = rebuild_pir(&*engine, &hashtable, &app_state.scenario)?;
    app_state.live_pir.store(Arc::new(Some(pir_state)));
    app_state.phase.store(Arc::new(ServerPhase::Serving));
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

    let (tx, mut rx) = mpsc::channel::<ChainEvent>(100);
    let follow_handle = {
        let mut follow_client = nf_ingest::LwdClient::connect(&config.lwd_urls)
            .await
            .map_err(nf_ingest::ingest::IngestError::from)?;
        tokio::spawn(async move {
            nf_ingest::ingest::follow(&mut follow_client, latest_height, latest_hash, None, &tx)
                .await
        })
    };

    let mut blocks_since_snapshot: u64 = 0;

    while let Some(event) = rx.recv().await {
        match event {
            ChainEvent::NewBlock {
                height,
                hash,
                nullifiers,
                ..
            } => {
                hashtable.insert_block(height, hash, &nullifiers)?;
                hashtable.evict_to_target();
                blocks_since_snapshot += 1;
                tracing::info!(height, nfs = nullifiers.len(), "new block");
            }
            ChainEvent::Reorg {
                orphaned,
                new_blocks,
            } => {
                for block in orphaned.iter().rev() {
                    if let Err(e) = hashtable.rollback_block(&block.hash) {
                        tracing::warn!(height = block.height, error = %e, "rollback failed, skipping");
                    }
                }
                for block in &new_blocks {
                    hashtable.insert_block(block.height, block.hash, &block.nullifiers)?;
                }
                hashtable.evict_to_target();
                blocks_since_snapshot += 1;
                tracing::info!(
                    orphaned = orphaned.len(),
                    new = new_blocks.len(),
                    "reorg handled"
                );
            }
        }

        // Rebuild PIR and atomic swap
        let pir_state = rebuild_pir(&*engine, &hashtable, &app_state.scenario)?;
        app_state.live_pir.store(Arc::new(Some(pir_state)));

        // Periodic snapshot
        if blocks_since_snapshot >= config.snapshot_interval {
            snapshot_io::save_snapshot(&hashtable, &config.data_dir).await?;
            blocks_since_snapshot = 0;
            tracing::info!("periodic snapshot saved");
        }
    }

    follow_handle.abort();
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

    let mut client = nf_ingest::LwdClient::connect(&config.lwd_urls)
        .await
        .map_err(nf_ingest::ingest::IngestError::from)?;

    let (tip_height, _) = client
        .get_latest_block()
        .await
        .map_err(nf_ingest::ingest::IngestError::from)?;

    let (mut hashtable, from_snapshot) = match snapshot_io::load_snapshot(&config.data_dir).await {
        Ok(ht) => (ht, true),
        Err(_) => (HashTableDb::new(), false),
    };

    let floor = min_sync_height(tip_height);
    let forward_start = if from_snapshot {
        hashtable
            .latest_height()
            .map(|h| h + 1)
            .unwrap_or(tip_height)
    } else {
        let initial = tip_height.saturating_sub(BACKFILL_BATCH);
        initial.max(floor)
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
            &mut hashtable,
            None,
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
                backfill_start,
                backfill_end,
                &mut hashtable,
                None,
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
    snapshot_io::save_snapshot(&hashtable, &config.data_dir).await?;

    let pir_state = rebuild_pir(&*engine, &hashtable, &app_state.scenario)?;
    app_state.live_pir.store(Arc::new(Some(pir_state)));
    app_state.phase.store(Arc::new(ServerPhase::Serving));

    Ok(hashtable)
}
