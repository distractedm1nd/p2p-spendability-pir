use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use chain_ingest::{ChainAction, ChainTracker, LwdClient};
use pir_types::{PirEngine, ServerPhase, CONFIRMATION_DEPTH};
use serde::Serialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

#[cfg(feature = "nullifier")]
use hashtable_pir::HashTableDb;
#[cfg(feature = "nullifier")]
use spend_types::{BUCKET_BYTES, NUM_BUCKETS};

#[cfg(feature = "witness")]
use commitment_tree_db::CommitmentTreeDb;
#[cfg(feature = "witness")]
use witness_types::{L0_DB_ROWS, SUBSHARD_ROW_BYTES};

const FOLLOW_POLL_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct CombinedConfig {
    #[cfg(feature = "nullifier")]
    pub target_size: usize,
    pub snapshot_interval: u64,
    pub data_dir: PathBuf,
    pub lwd_urls: Vec<String>,
    pub listen_addr: SocketAddr,
}

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[cfg(feature = "nullifier")]
    #[error("nullifier server error: {0}")]
    Nullifier(#[from] spend_server::server::ServerError),
    #[cfg(feature = "witness")]
    #[error("witness server error: {0}")]
    Witness(#[from] witness_server::server::ServerError),
    #[error("chain client error: {0}")]
    Client(#[from] chain_ingest::ClientError),
    #[cfg(feature = "nullifier")]
    #[error("hashtable error: {0}")]
    HashTable(#[from] hashtable_pir::HashTableError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ServerError>;

struct CombinedHealthState {
    #[cfg(feature = "nullifier")]
    nf_phase: Arc<arc_swap::ArcSwap<ServerPhase>>,
    #[cfg(feature = "witness")]
    wit_phase: Arc<arc_swap::ArcSwap<ServerPhase>>,
}

#[derive(Serialize)]
struct CombinedHealthResponse {
    #[cfg(feature = "nullifier")]
    nullifier: SubsystemHealth,
    #[cfg(feature = "witness")]
    witness: SubsystemHealth,
}

#[derive(Serialize)]
struct SubsystemHealth {
    phase: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_height: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_height: Option<u64>,
}

fn phase_to_health(phase: &ServerPhase) -> SubsystemHealth {
    match phase {
        ServerPhase::Serving => SubsystemHealth {
            phase: "serving".into(),
            current_height: None,
            target_height: None,
        },
        ServerPhase::Syncing {
            current_height,
            target_height,
        } => SubsystemHealth {
            phase: "syncing".into(),
            current_height: Some(*current_height),
            target_height: Some(*target_height),
        },
    }
}

async fn combined_health(State(state): State<Arc<CombinedHealthState>>) -> impl IntoResponse {
    #[allow(unused_mut)]
    let mut all_serving = true;

    #[cfg(feature = "nullifier")]
    let nf_health = {
        let nf_phase = state.nf_phase.load();
        if !matches!(nf_phase.as_ref(), ServerPhase::Serving) {
            all_serving = false;
        }
        phase_to_health(&nf_phase)
    };

    #[cfg(feature = "witness")]
    let wit_health = {
        let wit_phase = state.wit_phase.load();
        if !matches!(wit_phase.as_ref(), ServerPhase::Serving) {
            all_serving = false;
        }
        phase_to_health(&wit_phase)
    };

    let body = CombinedHealthResponse {
        #[cfg(feature = "nullifier")]
        nullifier: nf_health,
        #[cfg(feature = "witness")]
        witness: wit_health,
    };

    let status = if all_serving {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status, axum::Json(body))
}

/// Create both application states before starting sync or any frontend.
#[cfg(all(feature = "nullifier", feature = "witness"))]
pub fn create_app_states<NfP: PirEngine, WitP: PirEngine>(
    config: &CombinedConfig,
    nf_engine: Arc<NfP>,
    wit_engine: Arc<WitP>,
) -> (
    Arc<spend_server::state::AppState<NfP>>,
    Arc<witness_server::state::AppState<WitP>>,
) {
    let nullifier = Arc::new(spend_server::state::AppState::new(
        spend_server::state::ServerConfig {
            target_size: config.target_size,
            confirmation_depth: CONFIRMATION_DEPTH,
            snapshot_interval: config.snapshot_interval,
            data_dir: config.data_dir.join("nullifier"),
            lwd_urls: config.lwd_urls.clone(),
            listen_addr: config.listen_addr,
        },
        nf_engine,
    ));
    let witness = Arc::new(witness_server::state::AppState::new(
        witness_server::state::ServerConfig {
            snapshot_interval: config.snapshot_interval,
            data_dir: config.data_dir.join("witness"),
            lwd_urls: config.lwd_urls.clone(),
            listen_addr: config.listen_addr,
            window_shard_limit: witness_server::state::DEFAULT_WINDOW_SHARD_LIMIT,
        },
        wit_engine,
    ));

    (nullifier, witness)
}

/// Main entry point for the combined PIR server.
pub async fn run<
    #[cfg(feature = "nullifier")] NfP: PirEngine + 'static,
    #[cfg(feature = "witness")] WitP: PirEngine + 'static,
>(
    config: CombinedConfig,
    #[cfg(feature = "nullifier")] nf_engine: Arc<NfP>,
    #[cfg(feature = "witness")] wit_engine: Arc<WitP>,
) -> Result<()> {
    run_inner(
        config,
        #[cfg(feature = "nullifier")]
        nf_engine,
        #[cfg(feature = "witness")]
        wit_engine,
        #[cfg(all(feature = "nullifier", feature = "witness"))]
        None,
        CancellationToken::new(),
    )
    .await
}

#[cfg(all(feature = "nullifier", feature = "witness"))]
pub async fn run_with_states<NfP: PirEngine + 'static, WitP: PirEngine + 'static>(
    config: CombinedConfig,
    nf_state: Arc<spend_server::state::AppState<NfP>>,
    wit_state: Arc<witness_server::state::AppState<WitP>>,
) -> Result<()> {
    run_with_states_until(config, nf_state, wit_state, CancellationToken::new()).await
}

/// Run the complete combined lifecycle using caller-created states until
/// `shutdown` is cancelled.
#[cfg(all(feature = "nullifier", feature = "witness"))]
pub async fn run_with_states_until<NfP: PirEngine + 'static, WitP: PirEngine + 'static>(
    config: CombinedConfig,
    nf_state: Arc<spend_server::state::AppState<NfP>>,
    wit_state: Arc<witness_server::state::AppState<WitP>>,
    shutdown: CancellationToken,
) -> Result<()> {
    let nf_engine = nf_state.engine.clone();
    let wit_engine = wit_state.engine.clone();
    run_inner(
        config,
        nf_engine,
        wit_engine,
        Some((nf_state, wit_state)),
        shutdown,
    )
    .await
}

async fn run_inner<
    #[cfg(feature = "nullifier")] NfP: PirEngine + 'static,
    #[cfg(feature = "witness")] WitP: PirEngine + 'static,
>(
    config: CombinedConfig,
    #[cfg(feature = "nullifier")] nf_engine: Arc<NfP>,
    #[cfg(feature = "witness")] wit_engine: Arc<WitP>,
    #[cfg(all(feature = "nullifier", feature = "witness"))] provided_states: Option<(
        Arc<spend_server::state::AppState<NfP>>,
        Arc<witness_server::state::AppState<WitP>>,
    )>,
    shutdown: CancellationToken,
) -> Result<()> {
    #[cfg(feature = "nullifier")]
    let nf_config = spend_server::state::ServerConfig {
        target_size: config.target_size,
        confirmation_depth: CONFIRMATION_DEPTH,
        snapshot_interval: config.snapshot_interval,
        data_dir: config.data_dir.join("nullifier"),
        lwd_urls: config.lwd_urls.clone(),
        listen_addr: config.listen_addr,
    };
    #[cfg(feature = "witness")]
    let wit_config = witness_server::state::ServerConfig {
        snapshot_interval: config.snapshot_interval,
        data_dir: config.data_dir.join("witness"),
        lwd_urls: config.lwd_urls.clone(),
        listen_addr: config.listen_addr,
        window_shard_limit: witness_server::state::DEFAULT_WINDOW_SHARD_LIMIT,
    };

    tracing::info!("starting sync for enabled subsystems");

    if shutdown.is_cancelled() {
        return Ok(());
    }

    #[cfg(all(feature = "nullifier", feature = "witness"))]
    let ((nf_state, mut hashtable), (wit_state, mut tree)) =
        if let Some((nf_state, wit_state)) = provided_states {
            let (nf_result, wit_result) = tokio::join!(
                spend_server::server::sync_into(nf_state.clone()),
                witness_server::server::sync_into(wit_state.clone()),
            );
            (
                (nf_state, nf_result.map_err(ServerError::Nullifier)?),
                (wit_state, wit_result.map_err(ServerError::Witness)?),
            )
        } else {
            let (nf_result, wit_result) = tokio::join!(
                spend_server::server::run_sync_only(nf_config, nf_engine.clone()),
                witness_server::server::run_sync_only(wit_config, wit_engine.clone()),
            );
            (
                nf_result.map_err(ServerError::Nullifier)?,
                wit_result.map_err(ServerError::Witness)?,
            )
        };

    #[cfg(all(feature = "nullifier", not(feature = "witness")))]
    let (nf_state, mut hashtable) =
        spend_server::server::run_sync_only(nf_config, nf_engine.clone())
            .await
            .map_err(ServerError::Nullifier)?;

    #[cfg(all(feature = "witness", not(feature = "nullifier")))]
    let (wit_state, mut tree) =
        witness_server::server::run_sync_only(wit_config, wit_engine.clone())
            .await
            .map_err(ServerError::Witness)?;

    tracing::info!("sync complete, building router");

    // --- Build router ---

    let health_state = Arc::new(CombinedHealthState {
        #[cfg(feature = "nullifier")]
        nf_phase: Arc::new(arc_swap::ArcSwap::from_pointee(ServerPhase::Serving)),
        #[cfg(feature = "witness")]
        wit_phase: Arc::new(arc_swap::ArcSwap::from_pointee(ServerPhase::Serving)),
    });

    #[allow(unused_mut)]
    let mut router = Router::new()
        .route("/health", get(combined_health))
        .with_state(health_state);

    #[cfg(feature = "nullifier")]
    {
        router = router.nest(
            "/nullifier",
            spend_server::server::build_router(nf_state.clone()),
        );
    }

    #[cfg(feature = "witness")]
    {
        router = router.nest(
            "/witness",
            witness_server::server::build_router(wit_state.clone()),
        );
    }

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    tracing::info!(listen = %config.listen_addr, "http server started");
    let http_handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    // --- Follow loop ---

    // Determine the starting height from whichever subsystem(s) are enabled.
    // Determine the starting height/hash from whichever subsystem(s) are enabled.
    // When both are enabled, catch up the one that's behind first.
    #[cfg(all(feature = "nullifier", feature = "witness"))]
    let (follow_height, follow_hash) = {
        let nf_latest = hashtable.latest_height().unwrap_or(0);
        let wit_latest = tree.latest_height().unwrap_or(0);
        match nf_latest.cmp(&wit_latest) {
            std::cmp::Ordering::Less => {
                tracing::info!(
                    from = nf_latest + 1,
                    to = wit_latest,
                    "catching up nullifier subsystem"
                );
                catch_up_nullifier(&config.lwd_urls, nf_latest + 1, wit_latest, &mut hashtable)
                    .await?;
            }
            std::cmp::Ordering::Greater => {
                tracing::info!(from = wit_latest + 1, to = nf_latest, "catching up witness");
                let ts = if tree.tree_size() > 0 {
                    Some(tree.tree_size() as u32)
                } else {
                    None
                };
                catch_up_witness(&config.lwd_urls, wit_latest + 1, nf_latest, &mut tree, ts)
                    .await?;
            }
            std::cmp::Ordering::Equal => {}
        }
        let h = hashtable.latest_height().unwrap_or(0);
        let hash = hashtable.latest_block_hash().unwrap_or([0u8; 32]);
        (h, hash)
    };

    #[cfg(all(feature = "nullifier", not(feature = "witness")))]
    let (follow_height, follow_hash) = {
        let h = hashtable.latest_height().unwrap_or(0);
        let hash = hashtable.latest_block_hash().unwrap_or([0u8; 32]);
        (h, hash)
    };

    #[cfg(all(feature = "witness", not(feature = "nullifier")))]
    let (follow_height, follow_hash) = {
        let h = tree.latest_height().unwrap_or(0);
        (h, [0u8; 32])
    };

    tracing::info!(height = follow_height, "entering follow mode");

    #[cfg(feature = "nullifier")]
    let nf_scenario = pir_types::YpirScenario {
        num_items: NUM_BUCKETS as u64,
        item_size_bits: (BUCKET_BYTES * 8) as u64,
    };

    #[cfg(feature = "witness")]
    let wit_scenario = pir_types::YpirScenario {
        num_items: L0_DB_ROWS as u64,
        item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
    };

    let mut client = LwdClient::connect(&config.lwd_urls).await?;
    let mut tracker =
        ChainTracker::with_tip(follow_height, follow_hash, CONFIRMATION_DEPTH as usize * 2);
    let mut current_height = follow_height;
    let mut blocks_since_snapshot: u64 = 0;
    #[cfg(feature = "nullifier")]
    let mut nf_prev_tree_size: Option<u32> = None;

    loop {
        let latest_block = tokio::select! {
            _ = shutdown.cancelled() => break,
            latest_block = client.get_latest_block() => latest_block?,
        };
        let (tip_height, _) = latest_block;

        if tip_height <= current_height {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = sleep(FOLLOW_POLL_INTERVAL) => {}
            }
            continue;
        }

        let blocks = client
            .get_block_range(current_height + 1, tip_height)
            .await?;

        for block in &blocks {
            let height = block.height;
            let hash = to_hash_array(&block.hash);
            let prev_hash = to_hash_array(&block.prev_hash);

            #[cfg(feature = "nullifier")]
            let (nullifiers, nf_this_tree_size) =
                nf_ingest::extract_nullifiers_with_meta(block, nf_prev_tree_size);
            #[cfg(feature = "witness")]
            let commitments = commitment_ingest::extract_commitments(block);

            match tracker.push_block(height, hash, prev_hash) {
                ChainAction::Extend => {
                    #[cfg(feature = "nullifier")]
                    {
                        if let Err(e) = hashtable.insert_block(height, hash, &nullifiers) {
                            tracing::warn!(height, error = %e, "nullifier insert failed");
                        }
                        hashtable.evict_to_target();
                        nf_prev_tree_size = nf_this_tree_size;
                    }

                    #[cfg(feature = "witness")]
                    {
                        tree.append_commitments(height, hash, &commitments);
                    }

                    current_height = height;
                    blocks_since_snapshot += 1;

                    #[cfg(all(feature = "nullifier", feature = "witness"))]
                    tracing::info!(
                        height,
                        nfs = nullifiers.len(),
                        cmx = commitments.len(),
                        tree_size = tree.tree_size(),
                        "new block"
                    );
                    #[cfg(all(feature = "nullifier", not(feature = "witness")))]
                    tracing::info!(height, nfs = nullifiers.len(), "new block");
                    #[cfg(all(feature = "witness", not(feature = "nullifier")))]
                    tracing::info!(
                        height,
                        cmx = commitments.len(),
                        tree_size = tree.tree_size(),
                        "new block"
                    );
                }
                ChainAction::Reorg { rollback_to } => {
                    #[cfg(feature = "nullifier")]
                    {
                        while hashtable.latest_height().is_some_and(|h| h > rollback_to) {
                            if let Some(bh) = hashtable.latest_block_hash() {
                                if let Err(e) = hashtable.rollback_block(&bh) {
                                    tracing::warn!(error = %e, "nullifier rollback failed");
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                        if let Err(e) = hashtable.insert_block(height, hash, &nullifiers) {
                            tracing::warn!(height, error = %e, "nullifier insert after reorg failed");
                        }
                        nf_prev_tree_size = nf_this_tree_size;
                    }

                    #[cfg(feature = "witness")]
                    {
                        tree.rollback_to(rollback_to);
                        tree.append_commitments(height, hash, &commitments);
                    }

                    current_height = height;
                    blocks_since_snapshot += 1;
                    #[cfg(feature = "witness")]
                    tracing::info!(
                        rollback_to,
                        new_height = height,
                        tree_size = tree.tree_size(),
                        "reorg handled"
                    );
                    #[cfg(not(feature = "witness"))]
                    tracing::info!(rollback_to, new_height = height, "reorg handled");
                }
            }
        }

        // Rebuild PIR databases
        #[cfg(feature = "nullifier")]
        {
            let nf_pir = spend_server::server::rebuild_pir(&*nf_engine, &hashtable, &nf_scenario)
                .map_err(ServerError::Nullifier)?;
            nf_state.live_pir.store(Arc::new(Some(nf_pir)));
        }

        #[cfg(feature = "witness")]
        {
            let anchor_height = tree.latest_height().unwrap_or(0);
            let wit_pir = witness_server::server::rebuild_pir(
                &*wit_engine,
                &mut tree,
                &wit_scenario,
                anchor_height,
            )
            .map_err(ServerError::Witness)?;
            wit_state.live_pir.store(Arc::new(Some(wit_pir)));
        }

        // Periodic snapshots
        if blocks_since_snapshot >= config.snapshot_interval {
            #[cfg(feature = "nullifier")]
            {
                let nf_data_dir = config.data_dir.join("nullifier");
                spend_server::snapshot_io::save_snapshot(&hashtable, &nf_data_dir)
                    .await
                    .map_err(spend_server::server::ServerError::from)
                    .map_err(ServerError::Nullifier)?;
            }
            #[cfg(feature = "witness")]
            {
                let wit_data_dir = config.data_dir.join("witness");
                witness_server::snapshot_io::save_snapshot(&tree, &wit_data_dir)
                    .await
                    .map_err(witness_server::server::ServerError::from)
                    .map_err(ServerError::Witness)?;
            }
            blocks_since_snapshot = 0;
            tracing::info!("periodic snapshots saved");
        }

        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = sleep(FOLLOW_POLL_INTERVAL) => {}
        }
    }

    tracing::info!("stopping combined PIR server");
    http_handle.abort();
    Ok(())
}

#[cfg(feature = "nullifier")]
async fn catch_up_nullifier(
    lwd_urls: &[String],
    from: u64,
    to: u64,
    hashtable: &mut HashTableDb,
) -> Result<()> {
    let phase = arc_swap::ArcSwap::from_pointee(ServerPhase::Serving);
    spend_server::server::sync_range(lwd_urls, from, to, hashtable, None, &phase)
        .await
        .map_err(ServerError::Nullifier)?;
    hashtable.evict_to_target();
    Ok(())
}

#[cfg(feature = "witness")]
async fn catch_up_witness(
    lwd_urls: &[String],
    from: u64,
    to: u64,
    tree: &mut CommitmentTreeDb,
    initial_tree_size: Option<u32>,
) -> Result<()> {
    let phase = arc_swap::ArcSwap::from_pointee(ServerPhase::Serving);
    witness_server::server::sync_range(lwd_urls, from, to, tree, initial_tree_size, &phase)
        .await
        .map_err(ServerError::Witness)?;
    Ok(())
}

fn to_hash_array(bytes: &[u8]) -> [u8; 32] {
    let mut arr = [0u8; 32];
    let len = bytes.len().min(32);
    arr[..len].copy_from_slice(&bytes[..len]);
    arr
}
