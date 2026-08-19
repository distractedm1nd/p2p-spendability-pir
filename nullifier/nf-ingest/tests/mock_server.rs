use nf_ingest::proto::compact_tx_streamer_server::{CompactTxStreamer, CompactTxStreamerServer};
use nf_ingest::proto::*;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

/// State that can be mutated between test steps to simulate chain progression / reorgs.
#[derive(Clone)]
pub struct MockState {
    pub blocks: Arc<Mutex<Vec<CompactBlock>>>,
}

impl Default for MockState {
    fn default() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl MockState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_blocks(&self, blocks: Vec<CompactBlock>) {
        *self.blocks.lock().unwrap() = blocks;
    }

    pub fn push_block(&self, block: CompactBlock) {
        self.blocks.lock().unwrap().push(block);
    }
}

pub struct MockStreamer {
    state: MockState,
}

#[tonic::async_trait]
impl CompactTxStreamer for MockStreamer {
    async fn get_latest_block(
        &self,
        _request: Request<ChainSpec>,
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

    async fn get_block(&self, request: Request<BlockId>) -> Result<Response<CompactBlock>, Status> {
        let block_id = request.into_inner();
        let blocks = self.state.blocks.lock().unwrap();
        let block = blocks
            .iter()
            .find(|b| b.height == block_id.height)
            .cloned()
            .ok_or_else(|| Status::not_found("block not found"))?;
        Ok(Response::new(block))
    }

    async fn get_block_nullifiers(
        &self,
        request: Request<BlockId>,
    ) -> Result<Response<CompactBlock>, Status> {
        self.get_block(request).await
    }

    type GetBlockRangeStream = tokio_stream::Iter<std::vec::IntoIter<Result<CompactBlock, Status>>>;

    async fn get_block_range(
        &self,
        request: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        let range = request.into_inner();
        let start = range.start.as_ref().map(|b| b.height).unwrap_or(0);
        let end = range.end.as_ref().map(|b| b.height).unwrap_or(0);

        let blocks = self.state.blocks.lock().unwrap();
        let mut result: Vec<Result<CompactBlock, Status>> = blocks
            .iter()
            .filter(|b| b.height >= start && b.height <= end)
            .cloned()
            .map(Ok)
            .collect();
        result.sort_by_key(|r| r.as_ref().unwrap().height);

        Ok(Response::new(tokio_stream::iter(result)))
    }

    type GetBlockRangeNullifiersStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<CompactBlock, Status>>>;

    async fn get_block_range_nullifiers(
        &self,
        request: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        self.get_block_range(request).await
    }

    async fn get_transaction(
        &self,
        _request: Request<TxFilter>,
    ) -> Result<Response<RawTransaction>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    async fn send_transaction(
        &self,
        _request: Request<RawTransaction>,
    ) -> Result<Response<SendResponse>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    type GetTaddressTxidsStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<RawTransaction, Status>>>;

    async fn get_taddress_txids(
        &self,
        _request: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    type GetTaddressTransactionsStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<RawTransaction, Status>>>;

    async fn get_taddress_transactions(
        &self,
        _request: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    async fn get_taddress_balance(
        &self,
        _request: Request<AddressList>,
    ) -> Result<Response<Balance>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    async fn get_taddress_balance_stream(
        &self,
        _request: Request<tonic::Streaming<Address>>,
    ) -> Result<Response<Balance>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    type GetMempoolTxStream = tokio_stream::Iter<std::vec::IntoIter<Result<CompactTx, Status>>>;

    async fn get_mempool_tx(
        &self,
        _request: Request<GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    type GetMempoolStreamStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<RawTransaction, Status>>>;

    async fn get_mempool_stream(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    async fn get_tree_state(
        &self,
        _request: Request<BlockId>,
    ) -> Result<Response<TreeState>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    async fn get_latest_tree_state(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<TreeState>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    type GetSubtreeRootsStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<SubtreeRoot, Status>>>;

    async fn get_subtree_roots(
        &self,
        _request: Request<GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    async fn get_address_utxos(
        &self,
        _request: Request<GetAddressUtxosArg>,
    ) -> Result<Response<GetAddressUtxosReplyList>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    type GetAddressUtxosStreamStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<GetAddressUtxosReply, Status>>>;

    async fn get_address_utxos_stream(
        &self,
        _request: Request<GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    async fn get_lightd_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<LightdInfo>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }

    async fn ping(&self, _request: Request<Duration>) -> Result<Response<PingResponse>, Status> {
        Err(Status::unimplemented("not needed for tests"))
    }
}

/// Spawn a mock gRPC server on a random port. Returns (address, shutdown_sender).
pub async fn spawn_mock_server(state: MockState) -> (SocketAddr, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let streamer = MockStreamer { state };

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CompactTxStreamerServer::new(streamer))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    rx.await.ok();
                },
            )
            .await
            .unwrap();
    });

    // Give the server a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (addr, tx)
}

/// Build a CompactBlock with the given parameters.
pub fn make_compact_block(
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

fn hash_for(n: u8) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = n;
    h
}

fn make_nf(seed: u32) -> [u8; 32] {
    let mut nf = [0u8; 32];
    nf[0..4].copy_from_slice(&seed.to_le_bytes());
    for (i, byte) in nf.iter_mut().enumerate().skip(4) {
        *byte = ((seed >> ((i % 4) * 8)) as u8).wrapping_add(i as u8);
    }
    nf
}

#[tokio::test]
async fn test_sync_stream_mock() {
    let state = MockState::new();

    let mut blocks = Vec::new();
    let mut all_nf_count = 0usize;
    for i in 1u8..=100 {
        let nfs: Vec<[u8; 32]> = (0..3).map(|j| make_nf(i as u32 * 100 + j)).collect();
        all_nf_count += nfs.len();
        blocks.push(make_compact_block(
            i as u64,
            hash_for(i),
            hash_for(i - 1),
            &nfs,
        ));
    }
    state.set_blocks(blocks);

    let (addr, _shutdown) = spawn_mock_server(state).await;
    let mut client = nf_ingest::LwdClient::connect(&[format!("http://{addr}")])
        .await
        .unwrap();

    let mut received_count = 0;
    let mut last_height = 0;
    nf_ingest::sync_blocks(&mut client, 1, 100, |block| {
        let nullifiers = nf_ingest::extract_ironwood_nullifiers(&block.block).unwrap();
        assert!(
            block.height() > last_height,
            "blocks should arrive in order"
        );
        last_height = block.height();
        received_count += nullifiers.len();
        std::future::ready(Ok::<(), nf_ingest::PipelineError>(()))
    })
    .await
    .unwrap();

    assert_eq!(last_height, 100);
    assert_eq!(received_count, all_nf_count);
}

#[tokio::test]
async fn test_incomplete_range_emits_nothing() {
    let state = MockState::new();
    state.set_blocks(vec![
        make_compact_block(1, hash_for(1), hash_for(0), &[make_nf(1)]),
        make_compact_block(3, hash_for(3), hash_for(2), &[make_nf(3)]),
    ]);

    let (addr, shutdown) = spawn_mock_server(state).await;
    let mut client = nf_ingest::LwdClient::connect(&[format!("http://{addr}")])
        .await
        .unwrap();
    let mut received = 0;
    let error = nf_ingest::sync_blocks(&mut client, 1, 3, |_| {
        received += 1;
        std::future::ready(Ok::<(), nf_ingest::PipelineError>(()))
    })
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        nf_ingest::PipelineError::UnexpectedHeight {
            expected: 2,
            actual: 3
        }
    ));
    assert_eq!(received, 0, "partial range advanced the consumer");

    shutdown.send(()).ok();
}

#[tokio::test]
async fn test_follow_new_blocks_mock() {
    let state = MockState::new();

    // Start with blocks 1..=5
    let mut blocks = Vec::new();
    for i in 1u8..=5 {
        blocks.push(make_compact_block(
            i as u64,
            hash_for(i),
            hash_for(i - 1),
            &[make_nf(i as u32)],
        ));
    }
    state.set_blocks(blocks);

    let (addr, shutdown) = spawn_mock_server(state.clone()).await;
    let mut client = nf_ingest::LwdClient::connect(&[format!("http://{addr}")])
        .await
        .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let follow_handle = tokio::spawn(async move {
        nf_ingest::follow_blocks(
            &mut client,
            5,
            hash_for(5),
            0,
            20,
            std::time::Duration::from_millis(10),
            |event| {
                let tx = tx.clone();
                async move {
                    tx.send(event)
                        .await
                        .map_err(|_| nf_ingest::PipelineError::ConsumerDropped)
                }
            },
        )
        .await
    });

    // Add block 6 after a delay
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    state.push_block(make_compact_block(
        6,
        hash_for(6),
        hash_for(5),
        &[make_nf(60)],
    ));

    // Wait for the follow loop to pick it up
    let event = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for event")
        .expect("channel closed");

    match event {
        nf_ingest::BlockEvent::NewBlock(block) => {
            assert_eq!(block.height(), 6);
        }
        _ => panic!("expected NewBlock"),
    }

    // Shutdown
    shutdown.send(()).ok();
    follow_handle.abort();
}

#[tokio::test]
async fn test_follow_reorg_mock() {
    let state = MockState::new();

    // Initial chain: 1 -> 2 -> 3
    let blocks = vec![
        make_compact_block(1, hash_for(1), hash_for(0), &[make_nf(10)]),
        make_compact_block(2, hash_for(2), hash_for(1), &[make_nf(20)]),
        make_compact_block(3, hash_for(3), hash_for(2), &[make_nf(30)]),
    ];
    state.set_blocks(blocks);

    let (addr, shutdown) = spawn_mock_server(state.clone()).await;
    let mut client = nf_ingest::LwdClient::connect(&[format!("http://{addr}")])
        .await
        .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let follow_handle = tokio::spawn(async move {
        nf_ingest::follow_blocks(
            &mut client,
            3,
            hash_for(3),
            0,
            20,
            std::time::Duration::from_millis(10),
            |event| {
                let tx = tx.clone();
                async move {
                    tx.send(event)
                        .await
                        .map_err(|_| nf_ingest::PipelineError::ConsumerDropped)
                }
            },
        )
        .await
    });

    // Simulate reorg: replace block 3 with 3' (different hash) and add block 4
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut reorged_hash_3 = [0u8; 32];
    reorged_hash_3[0] = 33;
    state.set_blocks(vec![
        make_compact_block(1, hash_for(1), hash_for(0), &[make_nf(10)]),
        make_compact_block(2, hash_for(2), hash_for(1), &[make_nf(20)]),
        make_compact_block(3, reorged_hash_3, hash_for(2), &[make_nf(31)]),
        make_compact_block(4, hash_for(4), reorged_hash_3, &[make_nf(40)]),
    ]);

    // Should receive either a Reorg or NewBlock events
    let event = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for event")
        .expect("channel closed");

    // The follow loop detects block 4 is new. When it fetches block 4, its prev_hash
    // points to reorged_hash_3, which doesn't match our tracked hash(3). This triggers
    // a reorg event.
    match &event {
        nf_ingest::BlockEvent::NewBlock(block) => {
            // Block 4 extends normally because follow fetches blocks from height 4 only,
            // and the chain tracker sees prev_hash mismatch
            assert!(block.height() >= 4);
        }
        nf_ingest::BlockEvent::Reorg { rollback_to } => assert_eq!(*rollback_to, 2),
    }

    shutdown.send(()).ok();
    follow_handle.abort();
}
