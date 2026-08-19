use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use chain_ingest::{BlockEvent, LwdClient, PipelineError, ValidatedBlock};
use commitment_tree_db::CommitmentTreeDb;
use hashtable_pir::HashTableDb;
use pir_types::{PirEngine, ServerPhase, ZcashNetwork, CONFIRMATION_DEPTH};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const FOLLOW_POLL_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct CombinedConfig {
    pub zcash_network: ZcashNetwork,
    pub target_size: usize,
    pub snapshot_interval: u64,
    pub data_dir: PathBuf,
    pub lwd_urls: Vec<String>,
    pub listen_addr: SocketAddr,
}

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[error("nullifier server error: {0}")]
    Nullifier(#[from] spend_server::server::ServerError),
    #[error("witness server error: {0}")]
    Witness(#[from] witness_server::server::ServerError),
    #[error("chain client error: {0}")]
    Client(#[from] chain_ingest::ClientError),
    #[error("chain pipeline error: {0}")]
    Pipeline(#[from] chain_ingest::PipelineError),
    #[error("chain task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Ironwood(#[from] chain_ingest::IronwoodError),
    #[error(transparent)]
    IronwoodEndpoint(#[from] chain_ingest::EndpointError),
    #[error(transparent)]
    Dataset(#[from] chain_ingest::DatasetError),
    #[error("hashtable error: {0}")]
    HashTable(#[from] hashtable_pir::HashTableError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("nullifier and witness stores ended shared sync at different chain tips")]
    SubsystemTipMismatch,
}

pub type Result<T> = std::result::Result<T, ServerError>;

async fn combined_health() -> StatusCode {
    StatusCode::OK
}

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
            zcash_network: config.zcash_network,
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
            zcash_network: config.zcash_network,
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

pub async fn run_with_states_until<NfP: PirEngine + 'static, WitP: PirEngine + 'static>(
    config: CombinedConfig,
    nf_state: Arc<spend_server::state::AppState<NfP>>,
    wit_state: Arc<witness_server::state::AppState<WitP>>,
    shutdown: CancellationToken,
) -> Result<()> {
    if shutdown.is_cancelled() {
        return Ok(());
    }

    let (mut hashtable, mut tree) = tokio::select! {
        _ = shutdown.cancelled() => return Ok(()),
        result = sync_both(&config, &nf_state, &wit_state) => result?,
    };

    let follow_height = hashtable.latest_height().unwrap_or(0);
    if tree.latest_height() != Some(follow_height)
        || tree.latest_block_hash() != hashtable.latest_block_hash()
    {
        return Err(ServerError::SubsystemTipMismatch);
    }
    let follow_hash = hashtable.latest_block_hash().unwrap_or([0; 32]);

    let router = Router::new()
        .route("/health", get(combined_health))
        .nest(
            "/nullifier",
            spend_server::server::build_router(nf_state.clone()),
        )
        .nest(
            "/witness",
            witness_server::server::build_router(wit_state.clone()),
        );
    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    tracing::info!(listen = %config.listen_addr, "http server started");
    let http_handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    tracing::info!(height = follow_height, "entering shared follow mode");

    let mut client = LwdClient::connect(&config.lwd_urls).await?;
    chain_ingest::require_ironwood_tree_state(&mut client, config.zcash_network, follow_height)
        .await?;
    let (block_tx, mut block_rx) = tokio::sync::mpsc::channel(100);
    let follow_handle = tokio::spawn(async move {
        chain_ingest::follow_blocks(
            &mut client,
            follow_height,
            follow_hash,
            CONFIRMATION_DEPTH,
            CONFIRMATION_DEPTH as usize * 2,
            FOLLOW_POLL_INTERVAL,
            |event| {
                let tx = block_tx.clone();
                async move {
                    tx.send(event)
                        .await
                        .map_err(|_| PipelineError::ConsumerDropped)
                }
            },
        )
        .await
    });

    let mut blocks_since_snapshot = 0;
    loop {
        let event = tokio::select! {
            _ = shutdown.cancelled() => break,
            event = block_rx.recv() => match event {
                Some(event) => event,
                None => {
                    follow_handle.await??;
                    return Ok(());
                }
            },
        };

        match event {
            BlockEvent::Reorg { rollback_to } => {
                while hashtable
                    .latest_height()
                    .is_some_and(|height| height > rollback_to)
                {
                    hashtable.rollback_block(&hashtable.latest_block_hash().unwrap())?;
                }
                tree.rollback_to(rollback_to);
                tracing::info!(rollback_to, "reorg rolled back");
            }
            BlockEvent::NewBlock(block) => {
                let (nullifiers, commitments) = apply_block(
                    &block,
                    Some(&mut hashtable),
                    Some(&mut tree),
                    "combined follow",
                )?;
                evict_to_size(&mut hashtable, config.target_size);
                blocks_since_snapshot += 1;
                tracing::info!(
                    height = block.height(),
                    nfs = nullifiers,
                    cmx = commitments,
                    tree_size = tree.tree_size(),
                    "new confirmed block"
                );
            }
        }

        rebuild_both(&config, &nf_state, &wit_state, &hashtable, &mut tree)?;
        if blocks_since_snapshot >= config.snapshot_interval {
            save_both(&config, &hashtable, &tree).await?;
            blocks_since_snapshot = 0;
            tracing::info!("periodic snapshots saved");
        }
    }

    follow_handle.abort();
    http_handle.abort();
    Ok(())
}

async fn sync_both<NfP: PirEngine, WitP: PirEngine>(
    config: &CombinedConfig,
    nf_state: &Arc<spend_server::state::AppState<NfP>>,
    wit_state: &Arc<witness_server::state::AppState<WitP>>,
) -> Result<(HashTableDb, CommitmentTreeDb)> {
    let nf_dir = config.data_dir.join("nullifier");
    let wit_dir = config.data_dir.join("witness");
    chain_ingest::ensure_ironwood_dataset(
        &nf_dir,
        config.zcash_network,
        &["snapshot.bin", "snapshot.bin.tmp"],
    )?;
    chain_ingest::ensure_ironwood_dataset(
        &wit_dir,
        config.zcash_network,
        &["witness_snapshot.bin", "witness_snapshot.bin.tmp"],
    )?;

    let mut client = LwdClient::connect(&config.lwd_urls).await?;
    let (tip_height, _) = client.get_latest_block().await?;
    let target_height = tip_height.saturating_sub(CONFIRMATION_DEPTH);
    chain_ingest::require_ironwood_tree_state(&mut client, config.zcash_network, target_height)
        .await?;

    let mut hashtable = match spend_server::snapshot_io::load_snapshot(&nf_dir).await {
        Ok(db) => db,
        Err(spend_server::snapshot_io::SnapshotIoError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            HashTableDb::new()
        }
        Err(error) => return Err(spend_server::server::ServerError::from(error).into()),
    };
    let nf_from = hashtable
        .latest_height()
        .map(|height| height + 1)
        .unwrap_or_else(|| chain_ingest::nu6_3_activation_height(config.zcash_network));

    let (mut tree, wit_from) = match witness_server::snapshot_io::load_snapshot(&wit_dir).await {
        Ok(tree) => {
            let from = tree
                .latest_height()
                .map(|height| height + 1)
                .unwrap_or_else(|| chain_ingest::nu6_3_activation_height(config.zcash_network));
            (tree, from)
        }
        Err(witness_server::snapshot_io::SnapshotIoError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            witness_server::server::prepare_tree(
                &mut client,
                target_height,
                wit_state.config.effective_window_shard_limit(),
                config.zcash_network,
            )
            .await?
        }
        Err(error) => return Err(witness_server::server::ServerError::from(error).into()),
    };

    let sync_from = nf_from.min(wit_from);
    if sync_from <= target_height {
        nf_state.phase.store(Arc::new(ServerPhase::Syncing {
            current_height: sync_from,
            target_height,
        }));
        wit_state.phase.store(Arc::new(ServerPhase::Syncing {
            current_height: sync_from,
            target_height,
        }));
        tracing::info!(
            from = sync_from,
            to = target_height,
            nullifier_from = nf_from,
            witness_from = wit_from,
            "starting shared historical sync"
        );

        chain_ingest::sync_blocks(&mut client, sync_from, target_height, |block| {
            let result: Result<()> = (|| {
                apply_block(
                    &block,
                    (block.height() >= nf_from).then_some(&mut hashtable),
                    (block.height() >= wit_from).then_some(&mut tree),
                    "combined historical sync",
                )?;
                evict_to_size(&mut hashtable, config.target_size);
                if block.height() % 1000 == 0 {
                    let phase = Arc::new(ServerPhase::Syncing {
                        current_height: block.height(),
                        target_height,
                    });
                    nf_state.phase.store(phase.clone());
                    wit_state.phase.store(phase);
                    tracing::info!(
                        height = block.height(),
                        nullifiers = hashtable.len(),
                        tree_size = tree.tree_size(),
                        "shared sync progress"
                    );
                }
                Ok(())
            })();
            std::future::ready(result)
        })
        .await?;
    }

    evict_to_size(&mut hashtable, config.target_size);
    rebuild_both(config, nf_state, wit_state, &hashtable, &mut tree)?;
    save_both(config, &hashtable, &tree).await?;
    nf_state.phase.store(Arc::new(ServerPhase::Serving));
    wit_state.phase.store(Arc::new(ServerPhase::Serving));
    tracing::info!(height = tree.latest_height(), "shared sync complete");
    Ok((hashtable, tree))
}

fn apply_block(
    block: &ValidatedBlock,
    mut hashtable: Option<&mut HashTableDb>,
    mut tree: Option<&mut CommitmentTreeDb>,
    context: &'static str,
) -> Result<(usize, usize)> {
    let nullifiers = hashtable
        .as_ref()
        .map(|_| chain_ingest::extract_ironwood_nullifiers(&block.block))
        .transpose()?;
    let parsed_commitments = tree
        .as_ref()
        .map(|_| {
            let commitments = commitment_ingest::extract_commitments(&block.block)?;
            let prior_tree_size =
                commitment_ingest::ironwood_prior_tree_size(&block.block, commitments.len())?;
            Ok::<_, commitment_ingest::ParseError>((commitments, prior_tree_size))
        })
        .transpose()
        .map_err(witness_server::server::ServerError::from)?;

    if let (Some(tree), Some((_, prior_tree_size))) = (tree.as_deref(), &parsed_commitments) {
        witness_server::server::validate_prior_tree_size(
            tree,
            block.height(),
            Some(*prior_tree_size),
            context,
        )?;
    }
    if let (Some(db), Some(nullifiers)) = (hashtable.as_deref_mut(), &nullifiers) {
        db.insert_block(block.height(), block.hash, nullifiers)?;
    }
    if let (Some(tree), Some((commitments, _))) = (tree.as_deref_mut(), &parsed_commitments) {
        tree.append_commitments(block.height(), block.hash, commitments);
    }

    Ok((
        nullifiers.as_ref().map_or(0, Vec::len),
        parsed_commitments
            .as_ref()
            .map_or(0, |(commitments, _)| commitments.len()),
    ))
}

fn evict_to_size(hashtable: &mut HashTableDb, target_size: usize) {
    while hashtable.len() > target_size && hashtable.evict_oldest_block().is_some() {}
}

fn rebuild_both<NfP: PirEngine, WitP: PirEngine>(
    config: &CombinedConfig,
    nf_state: &Arc<spend_server::state::AppState<NfP>>,
    wit_state: &Arc<witness_server::state::AppState<WitP>>,
    hashtable: &HashTableDb,
    tree: &mut CommitmentTreeDb,
) -> Result<()> {
    let nf_pir = spend_server::server::rebuild_pir(
        &*nf_state.engine,
        hashtable,
        &nf_state.scenario,
        config.zcash_network,
    )?;
    nf_state.live_pir.store(Arc::new(Some(nf_pir)));

    let anchor_height = tree.latest_height().unwrap_or(0);
    let wit_pir = witness_server::server::rebuild_pir(
        &*wit_state.engine,
        tree,
        &wit_state.scenario,
        anchor_height,
        config.zcash_network,
    )?;
    wit_state.live_pir.store(Arc::new(Some(wit_pir)));
    Ok(())
}

async fn save_both(
    config: &CombinedConfig,
    hashtable: &HashTableDb,
    tree: &CommitmentTreeDb,
) -> Result<()> {
    spend_server::snapshot_io::save_snapshot(hashtable, &config.data_dir.join("nullifier"))
        .await
        .map_err(spend_server::server::ServerError::from)?;
    witness_server::snapshot_io::save_snapshot(tree, &config.data_dir.join("witness"))
        .await
        .map_err(witness_server::server::ServerError::from)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chain_ingest::proto::{ChainMetadata, CompactBlock, CompactOrchardAction, CompactTx};

    #[test]
    fn one_validated_block_feeds_both_stores() {
        let nullifier = [7; 32];
        let commitment = [2; 32];
        let compact = CompactBlock {
            height: 10,
            hash: vec![10; 32],
            prev_hash: vec![9; 32],
            vtx: vec![CompactTx {
                ironwood_actions: vec![CompactOrchardAction {
                    nullifier: nullifier.to_vec(),
                    cmx: commitment.to_vec(),
                    ephemeral_key: vec![3; 32],
                    ciphertext: vec![4; 52],
                }],
                ..Default::default()
            }],
            chain_metadata: Some(ChainMetadata {
                ironwood_commitment_tree_size: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        let block = ValidatedBlock {
            block: compact,
            hash: [10; 32],
            prev_hash: [9; 32],
        };
        let mut hashtable = HashTableDb::new();
        let mut tree = CommitmentTreeDb::new();

        apply_block(&block, Some(&mut hashtable), Some(&mut tree), "test").unwrap();

        assert!(hashtable.contains(&nullifier));
        assert_eq!(tree.leaves(), &[commitment]);
        assert_eq!(hashtable.latest_height(), tree.latest_height());
    }
}
