use std::{net::SocketAddr, sync::Arc};

use nullifier_pir::HashTableDb;
use pir_protocol::{p2p::*, ServerPhase, YpirScenario, ZcashNetwork};
use spendability_pir_client::{P2pPirNode, SpendClient, WitnessClient};
use spendability_pir_server::{
    nullifier::{self, state::AppState as SpendState},
    p2p::{P2pPirService, SpendPirEngine, WitnessPirEngine},
    witness::{self, state::AppState as WitnessState},
};
use witness_pir::CommitmentTreeDb;
use zakura_network::{
    zakura::{
        spawn_zakura_endpoint_with_services, CustomService, Peer, Service, Stream, ZakuraConnId,
        ZakuraPeerId, ZakuraServiceId,
    },
    Config, P2pStack,
};

#[derive(Debug)]
struct NoopService;

impl Service for NoopService {
    fn name(&self) -> &'static str {
        "noop"
    }

    fn streams(&self) -> &[Stream] {
        &[]
    }

    fn add_peer(&self, _peer: Peer) {}

    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
}

#[tokio::test]
#[ignore = "full nullifier and witness YPIR setup"]
async fn http_and_zakura_clients_match() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let nf_scenario = YpirScenario {
        num_items: nullifier_pir::NUM_BUCKETS as u64,
        item_size_bits: (nullifier_pir::BUCKET_BYTES * 8) as u64,
        poly_len: nullifier_pir::YPIR_POLY_LEN,
    };
    let nf_engine = Arc::new(SpendPirEngine::new(&nf_scenario));
    let nf_state = Arc::new(SpendState::new(
        nullifier::state::ServerConfig {
            zcash_network: ZcashNetwork::Main,
            target_size: 1,
            confirmation_depth: 1,
            snapshot_interval: 1,
            data_dir: temp.path().join("nullifier"),
            lwd_urls: vec![],
            listen_addr: addr,
        },
        nf_engine.clone(),
    ));
    let nullifier = [42; 32];
    let mut table = HashTableDb::new();
    table.insert_block(1, [1; 32], &[nullifier]).unwrap();
    nf_state.live_pir.store(Arc::new(Some(
        nullifier::server::rebuild_pir(&*nf_engine, &table, &nf_state.scenario, ZcashNetwork::Main)
            .unwrap(),
    )));
    nf_state.phase.store(Arc::new(ServerPhase::Serving));

    let witness_scenario = YpirScenario {
        num_items: witness_pir::L0_DB_ROWS as u64,
        item_size_bits: (witness_pir::SUBSHARD_ROW_BYTES * 8) as u64,
        poly_len: witness_pir::YPIR_POLY_LEN,
    };
    let witness_engine = Arc::new(WitnessPirEngine::new(&witness_scenario));
    let witness_state = Arc::new(WitnessState::new(
        witness::state::ServerConfig {
            zcash_network: ZcashNetwork::Main,
            snapshot_interval: 1,
            data_dir: temp.path().join("witness"),
            lwd_urls: vec![],
            listen_addr: addr,
            window_shard_limit: 16,
        },
        witness_engine.clone(),
    ));
    let mut tree = CommitmentTreeDb::new();
    tree.append_commitments(1, [1; 32], &[[0; 32]]);
    witness_state.live_pir.store(Arc::new(Some(
        witness::server::rebuild_pir(
            &*witness_engine,
            &mut tree,
            &witness_state.scenario,
            1,
            ZcashNetwork::Main,
        )
        .unwrap(),
    )));
    witness_state.phase.store(Arc::new(ServerPhase::Serving));

    let router = axum::Router::new()
        .nest(
            "/nullifier",
            nullifier::server::build_router(nf_state.clone()),
        )
        .nest(
            "/witness",
            witness::server::build_router(witness_state.clone()),
        );
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let http_addr = listener.local_addr().unwrap();
    let http_task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let server_identity = tempfile::tempdir().unwrap();
    let client_identity = tempfile::tempdir().unwrap();
    let mut config = Config {
        p2p_stack: P2pStack::Zakura,
        identity_dir: server_identity.path().to_owned(),
        ..Config::default()
    };
    config.zakura.listen_addr = Some(addr);
    config.zakura.bootstrap_peers.clear();
    let endpoint = spawn_zakura_endpoint_with_services(
        &config,
        |_supervisor, _trace| Arc::new(NoopService),
        None,
        vec![CustomService {
            service: Arc::new(P2pPirService::new(witness_state, nf_state)),
            provides: vec![ZakuraServiceId::new(PIR_SERVICE_ID).unwrap()],
            seeks: vec![],
        }],
    )
    .await
    .unwrap()
    .unwrap();
    let server_addr = endpoint.node_addr().await;
    let direct = server_addr
        .direct_addresses()
        .find(|address| address.ip().is_loopback())
        .unwrap();
    let (node, client) = P2pPirNode::spawn(
        client_identity.path().to_owned(),
        vec![format!("{}@{direct}", server_addr.node_id)],
        ZcashNetwork::Main,
    )
    .await
    .unwrap();
    let session = tokio::time::timeout(std::time::Duration::from_secs(10), client.session())
        .await
        .unwrap()
        .unwrap();
    let health: CombinedHealthResponse =
        serde_json::from_slice(&session.request(Message::HealthReq, vec![]).await.unwrap())
            .unwrap();
    assert_eq!(health.nullifier.phase, "serving");
    assert_eq!(health.witness.phase, "serving");

    let http_spend =
        SpendClient::connect(&format!("http://{http_addr}/nullifier"), ZcashNetwork::Main)
            .await
            .unwrap();
    let p2p_spend = SpendClient::connect_p2p(session.clone(), ZcashNetwork::Main)
        .await
        .unwrap();
    assert_eq!(
        http_spend.metadata().latest_height,
        p2p_spend.metadata().latest_height
    );
    assert_eq!(
        http_spend.is_spent(&nullifier).await.unwrap(),
        p2p_spend.is_spent(&nullifier).await.unwrap()
    );

    let http_witness =
        WitnessClient::connect(&format!("http://{http_addr}/witness"), ZcashNetwork::Main)
            .await
            .unwrap();
    let p2p_witness = WitnessClient::connect_p2p(session, ZcashNetwork::Main)
        .await
        .unwrap();
    assert_eq!(
        http_witness.broadcast().anchor_height,
        p2p_witness.broadcast().anchor_height
    );
    assert_eq!(
        http_witness.get_witness(0).await.unwrap(),
        p2p_witness.get_witness(0).await.unwrap()
    );

    node.shutdown().await;
    endpoint.shutdown().await;
    http_task.abort();
}
