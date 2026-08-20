//! End-to-end witness tests against mainnet lightwalletd.
//!
//! **Test A** (`e2e_witness_reconstruction_stub`): Ingests real Orchard note
//! commitments, builds the commitment tree, produces PIR database + broadcast,
//! extracts a sub-shard row directly (stub — no PIR encryption), reconstructs
//! a witness, and verifies:
//! - The witness anchor root matches `tree.tree_root()`
//! - Shard roots from broadcast match canonical `GetSubtreeRoots`
//! - The leaf in the PIR row matches the tree's stored leaf
//!
//! **Test B** (`e2e_witness_roundtrip_server`): Spins up an in-process
//! `witness-server` with `StubPirEngine`, queries it over HTTP with a raw
//! row-index request, reconstructs the witness client-side, and verifies
//! the same invariants as Test A.
//!
//! Both tests require network access to lightwalletd and are tagged `#[ignore]`
//! so they don't run in CI by default.

use pir_protocol::PirEngine;
use spendability_pir_client::witness::reconstruct::reconstruct_witness;
use spendability_pir_server::ingest::extract_commitments;
use spendability_pir_server::ingest::LwdClient;
use witness_pir::CommitmentTreeDb;
use witness_pir::*;

const LWD_ENDPOINT: &str = "https://us.zec.stardust.rest:443";
const IRONWOOD_PROTOCOL: i32 = 2;
const BATCH_SIZE: u64 = 10_000;

/// Ingest blocks in batches and append commitments to the tree.
async fn ingest_blocks(client: &mut LwdClient, tree: &mut CommitmentTreeDb, from: u64, to: u64) {
    let mut current = from;
    while current <= to {
        let batch_end = (current + BATCH_SIZE - 1).min(to);
        tracing::info!(from = current, to = batch_end, "fetching block range");

        let blocks = client
            .get_block_range(current, batch_end)
            .await
            .expect("failed to fetch blocks");

        for block in &blocks {
            let cmx = extract_commitments(block).unwrap();
            let mut hash = [0u8; 32];
            let len = block.hash.len().min(32);
            hash[..len].copy_from_slice(&block.hash[..len]);
            tree.append_commitments(block.height, hash, &cmx);
        }

        current = batch_end + 1;
    }
}

/// Build a windowed tree covering the last 2 completed shards from mainnet.
///
/// Returns `(tree, subtree_roots, num_completed)` with the tree synced through
/// the end of the last completed shard.
async fn build_windowed_tree(
    client: &mut LwdClient,
) -> (
    CommitmentTreeDb,
    Vec<spendability_pir_server::ingest::proto::SubtreeRoot>,
    usize,
) {
    let subtree_roots = client
        .get_subtree_roots(IRONWOOD_PROTOCOL, 0, 65535)
        .await
        .expect("failed to get subtree roots");

    let num_completed = subtree_roots.len();
    assert!(
        num_completed >= 3,
        "need at least 3 completed shards for this test, got {num_completed}"
    );

    tracing::info!(num_completed, "fetched completed Ironwood shard roots");

    // Prefetch all but the last 2 completed shards
    let prefetch_count = num_completed - 2;
    let prefetched: Vec<Hash> = subtree_roots[..prefetch_count]
        .iter()
        .map(|sr| {
            let mut root = [0u8; 32];
            root.copy_from_slice(&sr.root_hash);
            root
        })
        .collect();

    let leaf_offset = (prefetch_count as u64) * (SHARD_LEAVES as u64);

    let sync_start = subtree_roots[prefetch_count - 1].completing_block_height + 1;
    let sync_end = subtree_roots[num_completed - 1].completing_block_height;

    tracing::info!(
        prefetch_count,
        leaf_offset,
        sync_start,
        sync_end,
        block_span = sync_end - sync_start,
        "building windowed tree"
    );

    let mut tree = CommitmentTreeDb::with_offset(leaf_offset, prefetched);
    ingest_blocks(client, &mut tree, sync_start, sync_end).await;

    tracing::info!(
        tree_size = tree.tree_size(),
        populated_shards = tree.populated_shards(),
        window_start = tree.window_start_shard(),
        window_count = tree.window_shard_count(),
        "tree built"
    );

    (tree, subtree_roots, num_completed)
}

// ── Test A: Stub PIR ─────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn e2e_witness_reconstruction_stub() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init()
        .ok();

    let mut client = LwdClient::connect(&[LWD_ENDPOINT.to_string()])
        .await
        .expect("failed to connect to lightwalletd");

    let (mut tree, subtree_roots, num_completed) = build_windowed_tree(&mut client).await;

    let anchor_height = tree.latest_height().unwrap();
    let (pir_db, broadcast) = tree.build_pir_db_and_broadcast(anchor_height);

    tracing::info!(
        pir_db_len = pir_db.len(),
        cap_shards = broadcast.cap.shard_roots.len(),
        subshard_groups = broadcast.subshard_roots.len(),
        anchor_height,
        window_start = broadcast.window_start_shard,
        window_count = broadcast.window_shard_count,
        "PIR DB + broadcast built"
    );

    // ── Verify broadcast shard roots match canonical ──────────────────

    for (i, sr) in subtree_roots[..num_completed].iter().enumerate() {
        let mut canonical = [0u8; 32];
        canonical.copy_from_slice(&sr.root_hash);
        assert_eq!(
            broadcast.cap.shard_roots[i], canonical,
            "shard {i} root mismatch between broadcast and GetSubtreeRoots"
        );
    }
    tracing::info!(
        verified = num_completed,
        "all broadcast shard roots match canonical GetSubtreeRoots"
    );

    // ── Pick a test position in the last completed shard ─────────────

    let target_shard = (num_completed - 1) as u32;
    let test_subshard: u8 = 100;
    let test_leaf: u8 = 42;
    let position = (target_shard as u64) * (SHARD_LEAVES as u64)
        + (test_subshard as u64) * (SUBSHARD_LEAVES as u64)
        + (test_leaf as u64);

    let (shard_idx, subshard_idx, leaf_idx) = decompose_position(position);
    assert_eq!(shard_idx, target_shard);
    assert_eq!(subshard_idx, test_subshard);
    assert_eq!(leaf_idx, test_leaf);

    tracing::info!(
        position,
        shard_idx,
        subshard_idx,
        leaf_idx,
        "test position chosen"
    );

    // ── Extract sub-shard row from PIR DB (stub — direct access) ─────

    let row_idx = physical_row_index(shard_idx, subshard_idx, broadcast.window_start_shard);
    let row_start = row_idx * SUBSHARD_ROW_BYTES;
    let row_end = row_start + SUBSHARD_ROW_BYTES;
    let decoded_row = &pir_db[row_start..row_end];

    // ── Reconstruct witness ──────────────────────────────────────────

    let witness = reconstruct_witness(
        position,
        shard_idx,
        subshard_idx,
        leaf_idx,
        decoded_row,
        &broadcast,
    )
    .expect("witness reconstruction failed");

    tracing::info!(
        position = witness.position,
        anchor_height = witness.anchor_height,
        anchor_root = hex::encode(witness.anchor_root),
        "witness reconstructed"
    );

    // ── Verify anchor root matches tree root ─────────────────────────

    let tree_root = tree.tree_root();
    assert_eq!(
        witness.anchor_root,
        tree_root,
        "\nwitness anchor root does NOT match tree root!\n  \
         witness: {}\n  tree:    {}\n\n\
         The witness reconstruction or tree root computation is incorrect.",
        hex::encode(witness.anchor_root),
        hex::encode(tree_root),
    );
    tracing::info!("PASS: witness anchor root matches tree root");

    // ── Verify leaf from PIR row matches tree's stored leaf ──────────

    let leaf_in_row: &[u8] = &decoded_row[test_leaf as usize * 32..(test_leaf as usize + 1) * 32];
    let local_leaf_idx = (position - tree.leaf_offset()) as usize;
    let leaf_in_tree = &tree.leaves()[local_leaf_idx];
    assert_eq!(
        leaf_in_row, leaf_in_tree,
        "leaf at position {position} in PIR row doesn't match tree's stored leaf"
    );
    tracing::info!("PASS: PIR row leaf matches tree leaf at position {position}");

    // ── Verify witness has correct structure ──────────────────────────

    assert_eq!(witness.position, position);
    assert_eq!(witness.anchor_height, anchor_height);
    assert_eq!(witness.siblings.len(), TREE_DEPTH);

    tracing::info!(
        position,
        shard_idx,
        subshard_idx,
        leaf_idx,
        "PASS: end-to-end witness reconstruction verified against mainnet"
    );
}

// ── Test B: Full server round-trip with StubPirEngine ────────────────

#[tokio::test]
#[ignore]
async fn e2e_witness_roundtrip_server() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init()
        .ok();

    let mut client = LwdClient::connect(&[LWD_ENDPOINT.to_string()])
        .await
        .expect("failed to connect to lightwalletd");

    let (mut tree, subtree_roots, num_completed) = build_windowed_tree(&mut client).await;

    let anchor_height = tree.latest_height().unwrap();
    let (pir_db, broadcast) = tree.build_pir_db_and_broadcast(anchor_height);

    // ── Set up in-process witness-server with StubPirEngine ──────────

    use pir_protocol::YpirScenario;
    use spendability_pir_server::pir_stub::StubPirEngine;
    use spendability_pir_server::witness::state::{AppState, PirState, WitnessMetadata};

    let engine = std::sync::Arc::new(StubPirEngine);
    let scenario = YpirScenario {
        num_items: L0_DB_ROWS as u64,
        item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
    };

    let engine_state = engine.setup(&pir_db, &scenario).unwrap();
    let metadata = WitnessMetadata {
        zcash_network: pir_protocol::ZcashNetwork::Main,
        commitment_pool: "ironwood".into(),
        dataset_version: pir_protocol::DATASET_VERSION,
        anchor_height,
        tree_size: tree.tree_size(),
        window_start_shard: tree.window_start_shard(),
        window_shard_count: tree.window_shard_count(),
        populated_shards: tree.populated_shards(),
        phase: pir_protocol::ServerPhase::Serving,
    };

    let config = spendability_pir_server::witness::state::ServerConfig {
        zcash_network: pir_protocol::ZcashNetwork::Main,
        snapshot_interval: 100,
        data_dir: std::path::PathBuf::from("/tmp/witness-e2e-test"),
        lwd_urls: vec![LWD_ENDPOINT.to_string()],
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        window_shard_limit: spendability_pir_server::witness::state::DEFAULT_WINDOW_SHARD_LIMIT,
    };

    let app_state = std::sync::Arc::new(AppState::new(config, engine));
    app_state.live_pir.store(std::sync::Arc::new(Some(PirState {
        engine_state,
        broadcast: broadcast.clone(),
        metadata,
    })));
    app_state
        .phase
        .store(std::sync::Arc::new(pir_protocol::ServerPhase::Serving));

    let router = spendability_pir_server::witness::server::build_router(app_state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    let base_url = format!("http://{addr}");
    let http = reqwest::Client::new();

    tracing::info!(base_url, "in-process witness-server started");

    // ── Verify /health ───────────────────────────────────────────────

    let health: serde_json::Value = http
        .get(format!("{base_url}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["phase"], "Serving");
    tracing::info!(?health, "health check passed");

    // ── Fetch broadcast via HTTP ─────────────────────────────────────

    let http_broadcast: BroadcastData = http
        .get(format!("{base_url}/broadcast"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(http_broadcast.anchor_height, anchor_height);
    assert_eq!(
        http_broadcast.cap.shard_roots.len(),
        broadcast.cap.shard_roots.len()
    );

    // ── Pick a test position and query via HTTP ──────────────────────

    let target_shard = (num_completed - 1) as u32;
    let test_subshard: u8 = 100;
    let test_leaf: u8 = 42;
    let position = (target_shard as u64) * (SHARD_LEAVES as u64)
        + (test_subshard as u64) * (SUBSHARD_LEAVES as u64)
        + (test_leaf as u64);

    let (shard_idx, subshard_idx, leaf_idx) = decompose_position(position);
    let row_idx =
        physical_row_index(shard_idx, subshard_idx, http_broadcast.window_start_shard) as u32;

    // StubPirEngine expects a 4-byte LE row index as the query
    let query_bytes = row_idx.to_le_bytes();
    let resp = http
        .post(format!("{base_url}/query"))
        .body(query_bytes.to_vec())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "query returned {}",
        resp.status()
    );

    let decoded_row = resp.bytes().await.unwrap();
    assert_eq!(
        decoded_row.len(),
        SUBSHARD_ROW_BYTES,
        "response row size mismatch"
    );

    // ── Reconstruct and verify ───────────────────────────────────────

    let witness = reconstruct_witness(
        position,
        shard_idx,
        subshard_idx,
        leaf_idx,
        &decoded_row,
        &http_broadcast,
    )
    .expect("witness reconstruction failed");

    let tree_root = tree.tree_root();
    assert_eq!(
        witness.anchor_root, tree_root,
        "witness anchor root does NOT match tree root after server round-trip"
    );

    // Verify shard roots
    for (i, sr) in subtree_roots[..num_completed].iter().enumerate() {
        let mut canonical = [0u8; 32];
        canonical.copy_from_slice(&sr.root_hash);
        assert_eq!(
            http_broadcast.cap.shard_roots[i], canonical,
            "shard {i} root mismatch in HTTP broadcast"
        );
    }

    tracing::info!(
        position,
        anchor_height,
        "PASS: end-to-end server round-trip verified against mainnet"
    );
}
