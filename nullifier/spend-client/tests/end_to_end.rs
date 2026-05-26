#![cfg(any(feature = "ypir", feature = "ipir"))]

use nf_ingest::proto::compact_tx_streamer_server::{CompactTxStreamer, CompactTxStreamerServer};
use nf_ingest::proto::*;
use spend_client::SpendClient;
#[cfg(feature = "ipir")]
use spend_server::pir_ipir::IpirPirEngine;
#[cfg(all(feature = "ypir", not(feature = "ipir")))]
use spend_server::pir_ypir::YpirPirEngine;
use spend_server::server::{build_router, run_sync_only};
use spend_server::state::ServerConfig;
use spend_types::{PirEngine, YpirScenario, NUM_BUCKETS};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

// ── Mock lightwalletd (same pattern as nf-ingest and spend-server tests) ──

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
}

struct MockStreamer {
    state: MockState,
}

#[tonic::async_trait]
impl CompactTxStreamer for MockStreamer {
    async fn get_latest_block(&self, _: Request<ChainSpec>) -> Result<Response<BlockId>, Status> {
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
    async fn get_tree_state(&self, _: Request<BlockId>) -> Result<Response<TreeState>, Status> {
        Err(Status::unimplemented(""))
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

// ── Helpers ──

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
                actions,
                ..Default::default()
            }]
        },
        ..Default::default()
    }
}

// ── End-to-end test ──

#[tokio::test]
#[cfg(all(feature = "ypir", not(feature = "ipir")))]
async fn test_end_to_end_is_spent() {
    let scenario = scenario();
    run_end_to_end_is_spent(Arc::new(YpirPirEngine::new(&scenario))).await;
}

#[tokio::test]
#[cfg(feature = "ipir")]
async fn test_ipir_end_to_end_is_spent() {
    let scenario = scenario();
    let engine = Arc::new(IpirPirEngine::new(&scenario).unwrap());
    run_end_to_end_is_spent(engine).await;
}

fn scenario() -> YpirScenario {
    YpirScenario {
        num_items: NUM_BUCKETS as u64,
        item_size_bits: (spend_types::BUCKET_BYTES * 8) as u64,
    }
}

async fn run_end_to_end_is_spent<P>(engine: Arc<P>)
where
    P: PirEngine + 'static,
{
    // Build test chain: 20 blocks, 5 nullifiers each
    let mock = MockState::new();
    let mut all_nfs: Vec<Vec<[u8; 32]>> = Vec::new();
    let mut blocks = Vec::new();
    for i in 1u16..=20 {
        let nfs: Vec<[u8; 32]> = (0..5).map(|j| make_nf(i as u32 * 1000 + j)).collect();
        blocks.push(make_compact_block(
            i as u64,
            hash_for(i),
            hash_for(i - 1),
            &nfs,
        ));
        all_nfs.push(nfs);
    }
    mock.set_blocks(blocks);

    // Start mock lightwalletd
    let (lwd_addr, _lwd_shutdown) = spawn_mock_lwd(mock).await;
    let tmp = tempfile::tempdir().unwrap();

    let config = ServerConfig {
        target_size: spend_types::TARGET_SIZE,
        confirmation_depth: spend_types::CONFIRMATION_DEPTH,
        snapshot_interval: 100,
        data_dir: tmp.path().to_path_buf(),
        lwd_urls: vec![format!("http://{lwd_addr}")],
        listen_addr: "127.0.0.1:0".parse().unwrap(),
    };

    let (app_state, _hashtable) = run_sync_only(config, engine).await.unwrap();

    // Start HTTP server
    let router = build_router(app_state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await });

    // Connect SpendClient
    let client = SpendClient::connect(&format!("http://{http_addr}"))
        .await
        .unwrap();

    assert_eq!(client.earliest_height(), 1);
    assert_eq!(client.latest_height(), 20);
    assert_eq!(client.metadata().num_nullifiers, 100);

    // Test: known nullifier IS spent
    let known_nf = &all_nfs[9][2]; // block 10, third nullifier
    let meta = client.is_spent(known_nf).await.unwrap();
    assert!(meta.is_some(), "known nullifier should be spent");

    // Test: random nullifier is NOT spent
    let absent_nf = make_nf(999_999);
    let meta = client.is_spent(&absent_nf).await.unwrap();
    assert!(meta.is_none(), "absent nullifier should not be spent");
}
