use nf_ingest::proto::compact_tx_streamer_server::{CompactTxStreamer, CompactTxStreamerServer};
use nf_ingest::proto::*;
use spend_server::pir_stub::StubPirEngine;
use spend_server::server::{build_router, run_sync_only};
use spend_server::state::ServerConfig;
use spend_types::{hash_to_bucket, PirEngine, BUCKET_BYTES, NUM_BUCKETS};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

// ── Mock lightwalletd ──────────────────────────────────────────────────────

#[derive(Clone)]
struct MockState {
    blocks: Arc<Mutex<Vec<CompactBlock>>>,
}

impl MockState {
    fn new() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn set_blocks(&self, blocks: Vec<CompactBlock>) {
        *self.blocks.lock().unwrap() = blocks;
    }

    #[allow(dead_code)]
    fn push_block(&self, block: CompactBlock) {
        self.blocks.lock().unwrap().push(block);
    }
}

struct MockStreamer {
    state: MockState,
}

#[tonic::async_trait]
impl CompactTxStreamer for MockStreamer {
    async fn get_latest_block(
        &self,
        _req: Request<ChainSpec>,
    ) -> Result<Response<BlockId>, Status> {
        let blocks = self.state.blocks.lock().unwrap();
        let tip = blocks
            .last()
            .ok_or_else(|| Status::not_found("no blocks"))?;
        Ok(Response::new(BlockId {
            height: tip.height,
            hash: tip.hash.clone(),
        }))
    }

    async fn get_block(&self, req: Request<BlockId>) -> Result<Response<CompactBlock>, Status> {
        let id = req.into_inner();
        let blocks = self.state.blocks.lock().unwrap();
        blocks
            .iter()
            .find(|b| b.height == id.height)
            .cloned()
            .map(Response::new)
            .ok_or_else(|| Status::not_found("block not found"))
    }

    async fn get_block_nullifiers(
        &self,
        req: Request<BlockId>,
    ) -> Result<Response<CompactBlock>, Status> {
        self.get_block(req).await
    }

    type GetBlockRangeStream = tokio_stream::Iter<std::vec::IntoIter<Result<CompactBlock, Status>>>;

    async fn get_block_range(
        &self,
        req: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        let range = req.into_inner();
        let start = range.start.as_ref().map(|b| b.height).unwrap_or(0);
        let end = range.end.as_ref().map(|b| b.height).unwrap_or(0);
        let blocks = self.state.blocks.lock().unwrap();
        let mut result: Vec<Result<CompactBlock, Status>> = blocks
            .iter()
            .filter(|b| b.height >= start && b.height <= end)
            .cloned()
            .map(Ok)
            .collect();
        result.sort_by_key(|r: &Result<CompactBlock, Status>| r.as_ref().unwrap().height);
        Ok(Response::new(tokio_stream::iter(result)))
    }

    type GetBlockRangeNullifiersStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<CompactBlock, Status>>>;
    async fn get_block_range_nullifiers(
        &self,
        req: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        self.get_block_range(req).await
    }

    async fn get_transaction(
        &self,
        _: Request<TxFilter>,
    ) -> Result<Response<RawTransaction>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn send_transaction(
        &self,
        _: Request<RawTransaction>,
    ) -> Result<Response<SendResponse>, Status> {
        Err(Status::unimplemented(""))
    }
    type GetTaddressTxidsStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<RawTransaction, Status>>>;
    async fn get_taddress_txids(
        &self,
        _: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        Err(Status::unimplemented(""))
    }
    type GetTaddressTransactionsStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<RawTransaction, Status>>>;
    async fn get_taddress_transactions(
        &self,
        _: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn get_taddress_balance(
        &self,
        _: Request<AddressList>,
    ) -> Result<Response<Balance>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn get_taddress_balance_stream(
        &self,
        _: Request<tonic::Streaming<Address>>,
    ) -> Result<Response<Balance>, Status> {
        Err(Status::unimplemented(""))
    }
    type GetMempoolTxStream = tokio_stream::Iter<std::vec::IntoIter<Result<CompactTx, Status>>>;
    async fn get_mempool_tx(
        &self,
        _: Request<GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        Err(Status::unimplemented(""))
    }
    type GetMempoolStreamStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<RawTransaction, Status>>>;
    async fn get_mempool_stream(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn get_tree_state(&self, req: Request<BlockId>) -> Result<Response<TreeState>, Status> {
        Ok(Response::new(TreeState {
            network: "main".into(),
            height: req.into_inner().height,
            ironwood_tree: "00".into(),
            ..Default::default()
        }))
    }
    async fn get_latest_tree_state(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<TreeState>, Status> {
        Err(Status::unimplemented(""))
    }
    type GetSubtreeRootsStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<SubtreeRoot, Status>>>;
    async fn get_subtree_roots(
        &self,
        _: Request<GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn get_address_utxos(
        &self,
        _: Request<GetAddressUtxosArg>,
    ) -> Result<Response<GetAddressUtxosReplyList>, Status> {
        Err(Status::unimplemented(""))
    }
    type GetAddressUtxosStreamStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<GetAddressUtxosReply, Status>>>;
    async fn get_address_utxos_stream(
        &self,
        _: Request<GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn get_lightd_info(&self, _: Request<Empty>) -> Result<Response<LightdInfo>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn ping(&self, _: Request<Duration>) -> Result<Response<PingResponse>, Status> {
        Err(Status::unimplemented(""))
    }
}

async fn spawn_mock_lwd(state: MockState) -> (SocketAddr, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CompactTxStreamerServer::new(MockStreamer { state }))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    rx.await.ok();
                },
            )
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (addr, tx)
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn make_nf(seed: u32) -> [u8; 32] {
    let mut nf = [0u8; 32];
    nf[0..4].copy_from_slice(&seed.to_le_bytes());
    for (i, byte) in nf.iter_mut().enumerate().skip(4) {
        *byte = ((seed >> ((i % 4) * 8)) as u8).wrapping_add(i as u8);
    }
    nf
}

fn hash_for(n: u16) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0..2].copy_from_slice(&n.to_le_bytes());
    h
}

fn make_compact_block(
    height: u64,
    hash: [u8; 32],
    prev_hash: [u8; 32],
    nullifiers: &[[u8; 32]],
) -> CompactBlock {
    let actions: Vec<CompactOrchardAction> = nullifiers
        .iter()
        .map(|nf| CompactOrchardAction {
            nullifier: nf.to_vec(),
            cmx: vec![0; 32],
            ephemeral_key: vec![0; 32],
            ciphertext: vec![0; 52],
        })
        .collect();
    CompactBlock {
        height,
        hash: hash.to_vec(),
        prev_hash: prev_hash.to_vec(),
        vtx: if actions.is_empty() {
            vec![]
        } else {
            vec![CompactTx {
                ironwood_actions: actions,
                ..Default::default()
            }]
        },
        ..Default::default()
    }
}

fn build_chain(count: u16, nfs_per_block: u32) -> (Vec<CompactBlock>, Vec<Vec<[u8; 32]>>) {
    let mut blocks = Vec::new();
    let mut all_nfs = Vec::new();
    for i in 1..=count {
        let nfs: Vec<[u8; 32]> = (0..nfs_per_block)
            .map(|j| make_nf(i as u32 * 1000 + j))
            .collect();
        blocks.push(make_compact_block(
            i as u64,
            hash_for(i),
            hash_for(i - 1),
            &nfs,
        ));
        all_nfs.push(nfs);
    }
    (blocks, all_nfs)
}

fn make_config(lwd_addr: SocketAddr, data_dir: &std::path::Path) -> ServerConfig {
    ServerConfig {
        zcash_network: spend_types::ZcashNetwork::Main,
        target_size: spend_types::TARGET_SIZE,
        confirmation_depth: 0,
        snapshot_interval: 100,
        data_dir: data_dir.to_path_buf(),
        lwd_urls: vec![format!("http://{lwd_addr}")],
        listen_addr: "127.0.0.1:0".parse().unwrap(),
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_server_startup_sync_mode() {
    let mock = MockState::new();
    let (blocks, _) = build_chain(10, 5);
    mock.set_blocks(blocks);

    let (lwd_addr, _lwd_shutdown) = spawn_mock_lwd(mock).await;
    let tmp = tempfile::tempdir().unwrap();
    let config = make_config(lwd_addr, tmp.path());

    let engine = Arc::new(StubPirEngine);
    let app_state = Arc::new(spend_server::state::AppState::new(config, engine));

    // Before sync: live_pir is None
    assert!(app_state.live_pir.load().is_none());

    let router = build_router(app_state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await });

    let client = reqwest::Client::new();

    // /health always works
    let resp = client
        .get(format!("http://{http_addr}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // /metadata returns 503 (no PIR state)
    let resp = client
        .get(format!("http://{http_addr}/metadata"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);

    // /query returns 503
    let resp = client
        .post(format!("http://{http_addr}/query"))
        .body(vec![0u8; 4])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);

    // /params always works
    let resp = client
        .get(format!("http://{http_addr}/params"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let scenario: spend_types::YpirScenario = resp.json().await.unwrap();
    assert_eq!(scenario.num_items, NUM_BUCKETS as u64);
}

#[tokio::test]
async fn test_server_sync_to_serving() {
    let mock = MockState::new();
    let (blocks, all_nfs) = build_chain(50, 5);
    let total_nfs: usize = all_nfs.iter().map(|v| v.len()).sum();
    mock.set_blocks(blocks);

    let (lwd_addr, _lwd_shutdown) = spawn_mock_lwd(mock).await;
    let tmp = tempfile::tempdir().unwrap();
    let config = make_config(lwd_addr, tmp.path());

    let engine = Arc::new(StubPirEngine);
    let (app_state, hashtable) = run_sync_only(config, engine).await.unwrap();

    let phase = (**app_state.phase.load()).clone();
    assert!(matches!(phase, spend_types::ServerPhase::Serving));

    let guard = app_state.live_pir.load();
    assert!(guard.is_some());
    match guard.as_ref() {
        Some(pir_state) => {
            assert_eq!(pir_state.metadata.latest_height, 50);
            assert_eq!(pir_state.metadata.num_nullifiers, total_nfs as u64);
        }
        None => panic!("expected PIR state"),
    }

    assert_eq!(hashtable.len(), total_nfs);
    assert_eq!(hashtable.earliest_height(), Some(1));
    assert_eq!(hashtable.latest_height(), Some(50));
}

#[tokio::test]
async fn test_server_query_after_sync() {
    let mock = MockState::new();
    let (blocks, all_nfs) = build_chain(20, 5);
    mock.set_blocks(blocks);

    let (lwd_addr, _lwd_shutdown) = spawn_mock_lwd(mock).await;
    let tmp = tempfile::tempdir().unwrap();
    let config = make_config(lwd_addr, tmp.path());

    let engine = Arc::new(StubPirEngine);
    let (app_state, _hashtable) = run_sync_only(config, engine).await.unwrap();

    let router = build_router(app_state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await });

    let client = reqwest::Client::new();
    let nf = &all_nfs[5][0];
    let bucket_idx = hash_to_bucket(nf);

    let resp = client
        .post(format!("http://{http_addr}/query"))
        .body(bucket_idx.to_le_bytes().to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let bucket_data = resp.bytes().await.unwrap();
    assert_eq!(bucket_data.len(), BUCKET_BYTES);

    let found = bucket_data
        .chunks_exact(spend_types::ENTRY_BYTES)
        .any(|chunk| chunk[..32] == nf[..]);
    assert!(found, "queried nullifier not found in bucket response");
}

#[tokio::test]
async fn test_server_metadata_after_sync() {
    let mock = MockState::new();
    let (blocks, _) = build_chain(30, 3);
    mock.set_blocks(blocks);

    let (lwd_addr, _lwd_shutdown) = spawn_mock_lwd(mock).await;
    let tmp = tempfile::tempdir().unwrap();
    let config = make_config(lwd_addr, tmp.path());

    let engine = Arc::new(StubPirEngine);
    let (app_state, _) = run_sync_only(config, engine).await.unwrap();

    let router = build_router(app_state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await });

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{http_addr}/metadata"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let meta: spend_types::SpendabilityMetadata = resp.json().await.unwrap();
    assert_eq!(meta.latest_height, 30);
    assert_eq!(meta.earliest_height, 1);
    assert_eq!(meta.num_nullifiers, 90);
    assert!(matches!(meta.phase, spend_types::ServerPhase::Serving));
}

#[tokio::test]
async fn test_server_snapshot_save_load() {
    let mock = MockState::new();
    let (blocks, all_nfs) = build_chain(40, 4);
    mock.set_blocks(blocks);

    let (lwd_addr, _lwd_shutdown) = spawn_mock_lwd(mock).await;
    let tmp = tempfile::tempdir().unwrap();
    let config = make_config(lwd_addr, tmp.path());

    let engine = Arc::new(StubPirEngine);
    let (_, hashtable) = run_sync_only(config, engine).await.unwrap();

    let snapshot_path = tmp.path().join("snapshot.bin");
    assert!(snapshot_path.exists(), "snapshot file should exist");

    let restored = spend_server::snapshot_io::load_snapshot(tmp.path())
        .await
        .unwrap();
    assert_eq!(restored.len(), hashtable.len());
    assert_eq!(restored.latest_height(), hashtable.latest_height());

    for nf in &all_nfs[0] {
        assert!(restored.contains(nf));
    }
    for nf in &all_nfs[39] {
        assert!(restored.contains(nf));
    }
}

#[tokio::test]
async fn test_server_follow_new_block() {
    let mock = MockState::new();
    let (blocks, _) = build_chain(10, 5);
    mock.set_blocks(blocks);

    let (lwd_addr, lwd_shutdown) = spawn_mock_lwd(mock).await;
    let tmp = tempfile::tempdir().unwrap();
    let config = make_config(lwd_addr, tmp.path());

    let engine = Arc::new(StubPirEngine);
    let (app_state, mut hashtable) = run_sync_only(config, engine.clone()).await.unwrap();

    assert_eq!(hashtable.latest_height(), Some(10));

    // Simulate follow: insert a new block
    let new_nfs = [make_nf(99_000), make_nf(99_001)];
    hashtable.insert_block(11, hash_for(11), &new_nfs).unwrap();
    hashtable.evict_to_target();

    // Rebuild PIR and swap (as the follow loop does)
    let db_bytes = hashtable.to_pir_bytes();
    let engine_state = engine.setup(&db_bytes, &app_state.scenario).unwrap();
    let metadata = spend_types::SpendabilityMetadata {
        zcash_network: spend_types::ZcashNetwork::Main,
        nullifier_pool: spend_types::IRONWOOD_POOL.into(),
        dataset_version: spend_types::DATASET_VERSION,
        earliest_height: hashtable.earliest_height().unwrap_or(0),
        latest_height: hashtable.latest_height().unwrap_or(0),
        num_nullifiers: hashtable.len() as u64,
        num_buckets: NUM_BUCKETS as u64,
        phase: spend_types::ServerPhase::Serving,
    };
    app_state
        .live_pir
        .store(Arc::new(Some(spend_server::state::PirState {
            engine_state,
            metadata,
        })));

    let guard = app_state.live_pir.load();
    match guard.as_ref() {
        Some(pir_state) => {
            assert_eq!(pir_state.metadata.latest_height, 11);
            assert_eq!(pir_state.metadata.num_nullifiers, 52);

            let bucket_idx = hash_to_bucket(&new_nfs[0]);
            let result = engine
                .answer_query(&pir_state.engine_state, &bucket_idx.to_le_bytes())
                .unwrap();
            let found = result
                .chunks_exact(spend_types::ENTRY_BYTES)
                .any(|chunk| chunk[..32] == new_nfs[0][..]);
            assert!(found, "new nullifier should be queryable after follow");
        }
        None => panic!("expected PIR state"),
    }

    lwd_shutdown.send(()).ok();
}

#[tokio::test]
async fn test_server_reorg_handling() {
    let mock = MockState::new();
    let (blocks, all_nfs) = build_chain(10, 5);
    mock.set_blocks(blocks);

    let (lwd_addr, lwd_shutdown) = spawn_mock_lwd(mock).await;
    let tmp = tempfile::tempdir().unwrap();
    let config = make_config(lwd_addr, tmp.path());

    let engine = Arc::new(StubPirEngine);
    let (app_state, mut hashtable) = run_sync_only(config, engine.clone()).await.unwrap();

    for nf in &all_nfs[9] {
        assert!(hashtable.contains(nf));
    }

    // Simulate reorg: rollback block 10, insert replacement
    hashtable.rollback_block(&hash_for(10)).unwrap();
    let replacement_nfs = vec![make_nf(88_000), make_nf(88_001), make_nf(88_002)];
    let mut new_hash_10 = [0u8; 32];
    new_hash_10[0] = 0xAA;
    hashtable
        .insert_block(10, new_hash_10, &replacement_nfs)
        .unwrap();
    hashtable.evict_to_target();

    // Rebuild PIR
    let db_bytes = hashtable.to_pir_bytes();
    let engine_state = engine.setup(&db_bytes, &app_state.scenario).unwrap();
    let metadata = spend_types::SpendabilityMetadata {
        zcash_network: spend_types::ZcashNetwork::Main,
        nullifier_pool: spend_types::IRONWOOD_POOL.into(),
        dataset_version: spend_types::DATASET_VERSION,
        earliest_height: hashtable.earliest_height().unwrap_or(0),
        latest_height: hashtable.latest_height().unwrap_or(0),
        num_nullifiers: hashtable.len() as u64,
        num_buckets: NUM_BUCKETS as u64,
        phase: spend_types::ServerPhase::Serving,
    };
    app_state
        .live_pir
        .store(Arc::new(Some(spend_server::state::PirState {
            engine_state,
            metadata,
        })));

    // Old block 10 nullifiers should be gone
    for nf in &all_nfs[9] {
        assert!(!hashtable.contains(nf), "orphaned nf still present");
    }

    // Replacement nullifiers should be present and queryable
    let guard = app_state.live_pir.load();
    match guard.as_ref() {
        Some(pir_state) => {
            for nf in &replacement_nfs {
                assert!(hashtable.contains(nf), "replacement nf missing");

                let bucket_idx = hash_to_bucket(nf);
                let result = engine
                    .answer_query(&pir_state.engine_state, &bucket_idx.to_le_bytes())
                    .unwrap();
                let found = result
                    .chunks_exact(spend_types::ENTRY_BYTES)
                    .any(|chunk| chunk[..32] == nf[..]);
                assert!(found, "replacement nf not in PIR query result");
            }
        }
        None => panic!("expected PIR state"),
    }

    assert_eq!(hashtable.len(), 48); // 9*5 + 3

    lwd_shutdown.send(()).ok();
}
