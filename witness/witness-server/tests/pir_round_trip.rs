//! End-to-end PIR round-trip test against mainnet.
//!
//! This test exercises the full witness-serving stack: it syncs from
//! lightwalletd, builds the YPIR database, starts the witness server in
//! process, and issues real encrypted `/query` requests over HTTP.
//!
//! Key invariants validated here:
//! - the server's advertised broadcast metadata matches an independently built
//!   canonical reference derived from lightwalletd;
//! - the first locally served leaf (`window-start`) matches the spillover leaf
//!   from the shard-completing block, which is the boundary that regressed in
//!   the skipped-spillover bug;
//! - both the `window-start` and `frontier` positions return the canonical
//!   leaf, anchor root, and sibling path when reconstructed from the PIR row;
//! - the test derives its oracle from raw lightwalletd data instead of
//!   trusting the server's own in-memory tree.
//!
//! Requires network access to lightwalletd.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use chain_ingest::proto::CompactBlock;
use chain_ingest::LwdClient;
use commitment_ingest::extract_commitments;
use commitment_tree_db::CommitmentTreeDb;
use pir_types::YpirScenario;
use tokio::net::TcpListener;
use witness_client::reconstruct::reconstruct_witness;
use witness_client::WitnessClient;
use witness_server::pir_ypir::YpirPirEngine;
use witness_server::server::{build_router, run_sync_only};
use witness_server::state::ServerConfig;
use witness_types::*;
use ypir::client::YPIRClient;
use ypir::params::params_for_scenario_simplepir;
use ypir::serialize::ToBytes;

const LWD_ENDPOINT: &str = "https://us.zec.stardust.rest:443";
const ORCHARD_PROTOCOL: i32 = 1;
const TEST_WINDOW_SHARD_LIMIT: usize = 2;

/// Canonical oracle data built directly from lightwalletd.
///
/// The test uses this to compare the server's PIR responses against a second,
/// independently constructed view of the same Orchard commitment tree window.
struct CanonicalReference {
    tree: CommitmentTreeDb,
    broadcast: BroadcastData,
    pir_db: Vec<u8>,
    window_spillover: Vec<Hash>,
}

/// Returns a persistent snapshot directory when CI provides one, otherwise a
/// temporary directory for local runs.
fn data_dir() -> (PathBuf, Option<tempfile::TempDir>) {
    if let Ok(dir) = std::env::var("PIR_TEST_DATA_DIR") {
        let path = PathBuf::from(&dir);
        std::fs::create_dir_all(&path).expect("failed to create PIR_TEST_DATA_DIR");
        (path, None)
    } else {
        let tmp = tempfile::tempdir().expect("failed to create tempdir");
        let path = tmp.path().to_path_buf();
        (path, Some(tmp))
    }
}

/// Normalizes variable-length byte slices into the fixed-size hash type used by
/// the witness data structures.
fn copy_hash(bytes: &[u8]) -> Hash {
    let mut hash = [0u8; 32];
    let len = bytes.len().min(32);
    hash[..len].copy_from_slice(&bytes[..len]);
    hash
}

/// Chooses the minimal set of positions that still protects the key windowing
/// invariants.
///
/// `window-start` covers the historical skipped-spillover regression boundary,
/// while `frontier` proves the newest locally tracked note still reconstructs
/// correctly.
fn candidate_positions(tree: &CommitmentTreeDb) -> Vec<(&'static str, u64)> {
    let first_local = tree.leaf_offset();
    let frontier = tree.leaf_offset() + tree.leaves().len() as u64 - 1;
    if first_local == frontier {
        vec![("window-start", first_local)]
    } else {
        vec![("window-start", first_local), ("frontier", frontier)]
    }
}

/// Maps a logical note position to the PIR row that contains it.
fn row_bounds(position: u64, window_start_shard: u32) -> (u32, u8, u8, usize, usize) {
    let (shard_idx, subshard_idx, leaf_idx) = decompose_position(position);
    let row_idx = physical_row_index(shard_idx, subshard_idx, window_start_shard);
    let row_start = row_idx * SUBSHARD_ROW_BYTES;
    let row_end = row_start + SUBSHARD_ROW_BYTES;
    (shard_idx, subshard_idx, leaf_idx, row_start, row_end)
}

/// Extracts a single 32-byte note commitment from a decoded PIR row.
fn leaf_from_decoded_row(decoded_row: &[u8], leaf_idx: u8) -> Hash {
    let start = leaf_idx as usize * 32;
    let end = start + 32;
    let mut leaf = [0u8; 32];
    leaf.copy_from_slice(&decoded_row[start..end]);
    leaf
}

/// Reads the canonical leaf directly from the independently reconstructed tree.
fn reference_leaf_at_position(tree: &CommitmentTreeDb, position: u64) -> Hash {
    let local_idx = (position - tree.leaf_offset()) as usize;
    tree.leaves()[local_idx]
}

/// Returns the commitments from a shard-completing block that spill into the
/// first locally served shard.
///
/// This mirrors the production bootstrap behavior that must include those
/// leaves before continuing the forward sync.
fn canonical_window_spillover(block: &CompactBlock, leaf_offset: u64) -> Vec<Hash> {
    let commitments = extract_commitments(block);
    let end_tree_size = block
        .chain_metadata
        .as_ref()
        .map(|meta| meta.orchard_commitment_tree_size as u64)
        .unwrap_or(0);

    if end_tree_size <= leaf_offset {
        return vec![];
    }

    let spillover_count = (end_tree_size - leaf_offset) as usize;
    let skip = commitments.len().saturating_sub(spillover_count);
    commitments[skip..].to_vec()
}

/// Appends all Orchard commitments from a compact block into the reference tree.
fn append_block(tree: &mut CommitmentTreeDb, block: &CompactBlock) {
    let commitments = extract_commitments(block);
    tree.append_commitments(block.height, copy_hash(&block.hash), &commitments);
}

/// Builds a canonical reference window by replaying lightwalletd data without
/// relying on the server's synced state.
///
/// The logic intentionally mirrors the production window bootstrap, including
/// shard-root prefetching and shard-completing-block spillover handling, so the
/// resulting tree can act as an external oracle for the test.
async fn build_canonical_reference(
    anchor_height: u64,
    window_start_shard: u32,
) -> CanonicalReference {
    let mut client = LwdClient::connect(&[LWD_ENDPOINT.to_string()])
        .await
        .expect("failed to connect to lightwalletd for canonical reference");

    let subtree_roots = client
        .get_subtree_roots(ORCHARD_PROTOCOL, 0, 65535)
        .await
        .expect("failed to fetch subtree roots for canonical reference");

    assert!(
        subtree_roots.len() >= window_start_shard as usize,
        "expected subtree roots through shard {}, got {}",
        window_start_shard.saturating_sub(1),
        subtree_roots.len()
    );

    let prefetched: Vec<Hash> = subtree_roots[..window_start_shard as usize]
        .iter()
        .map(|sr| copy_hash(&sr.root_hash))
        .collect();

    let leaf_offset = (window_start_shard as u64) * (SHARD_LEAVES as u64);
    let mut tree = CommitmentTreeDb::with_offset(leaf_offset, prefetched);

    let (sync_from, window_spillover) = if window_start_shard == 0 {
        (1, vec![])
    } else {
        let completing_block_height =
            subtree_roots[window_start_shard as usize - 1].completing_block_height;
        let blocks = client
            .get_block_range(completing_block_height, completing_block_height)
            .await
            .expect("failed to fetch shard-completing block");
        let block = blocks
            .first()
            .expect("shard-completing block should be present");
        let spillover = canonical_window_spillover(block, leaf_offset);
        if !spillover.is_empty() {
            tree.append_commitments(completing_block_height, copy_hash(&block.hash), &spillover);
        }
        (completing_block_height + 1, spillover)
    };

    if sync_from <= anchor_height {
        let blocks = client
            .get_block_range(sync_from, anchor_height)
            .await
            .expect("failed to fetch canonical block range");
        for block in &blocks {
            append_block(&mut tree, block);
        }
    }

    assert_eq!(
        tree.latest_height(),
        Some(anchor_height),
        "canonical reference should be synced through the served anchor"
    );

    let (pir_db, broadcast) = tree.build_pir_db_and_broadcast(anchor_height);

    CanonicalReference {
        tree,
        broadcast,
        pir_db,
        window_spillover,
    }
}

/// Issues a raw YPIR query against the running server and returns the decoded
/// subshard row bytes.
async fn query_server_row(
    base_url: &str,
    scenario: &YpirScenario,
    broadcast: &BroadcastData,
    position: u64,
) -> Vec<u8> {
    let (shard_idx, subshard_idx, _leaf_idx, _row_start, _row_end) =
        row_bounds(position, broadcast.window_start_shard);
    let row_idx = physical_row_index(shard_idx, subshard_idx, broadcast.window_start_shard);

    let params = params_for_scenario_simplepir(scenario.num_items, scenario.item_size_bits);
    let ypir_client = YPIRClient::new(&params);
    let (query, seed) = ypir_client.generate_query_simplepir(row_idx);
    let query_bytes = query.to_bytes();

    let response = reqwest::Client::new()
        .post(format!("{base_url}/query"))
        .body(query_bytes)
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST /query failed for position {position}: {e}"));

    let response_bytes = response
        .error_for_status()
        .unwrap_or_else(|e| panic!("/query returned error for position {position}: {e}"))
        .bytes()
        .await
        .unwrap_or_else(|e| panic!("failed to read /query response for position {position}: {e}"));

    ypir_client.decode_response_simplepir(seed, &response_bytes)
}

/// Queries the server once for a position, then reconstructs the witness from
/// the returned PIR row.
///
/// Keeping this as a single helper ensures each checked position incurs only
/// one encrypted query in the test.
async fn query_server_witness(
    base_url: &str,
    scenario: &YpirScenario,
    broadcast: &BroadcastData,
    position: u64,
) -> (PirWitness, Hash) {
    let decoded_row = query_server_row(base_url, scenario, broadcast, position).await;
    let (shard_idx, subshard_idx, leaf_idx, _, _) =
        row_bounds(position, broadcast.window_start_shard);
    let leaf = leaf_from_decoded_row(&decoded_row, leaf_idx);
    let witness = reconstruct_witness(
        position,
        shard_idx,
        subshard_idx,
        leaf_idx,
        &decoded_row,
        broadcast,
    )
    .unwrap_or_else(|e| {
        panic!("server witness reconstruction failed for position {position}: {e}")
    });

    (witness, leaf)
}

/// Returns the canonical PIR row slice and row coordinates for a position.
fn reference_decoded_row(canonical: &CanonicalReference, position: u64) -> (&[u8], u32, u8, u8) {
    let (shard_idx, subshard_idx, leaf_idx, row_start, row_end) =
        row_bounds(position, canonical.broadcast.window_start_shard);
    (
        &canonical.pir_db[row_start..row_end],
        shard_idx,
        subshard_idx,
        leaf_idx,
    )
}

#[tokio::test]
async fn full_pir_round_trip() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init()
        .ok();

    let (dir, _tmp_guard) = data_dir();

    let config = ServerConfig {
        snapshot_interval: 0,
        data_dir: dir,
        lwd_urls: vec![LWD_ENDPOINT.to_string()],
        listen_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        window_shard_limit: TEST_WINDOW_SHARD_LIMIT,
    };

    let scenario = YpirScenario {
        num_items: L0_DB_ROWS as u64,
        item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
    };
    let engine = Arc::new(YpirPirEngine::new(&scenario));

    tracing::info!("starting sync and YPIR setup");

    let (app_state, tree) = run_sync_only(config, engine)
        .await
        .expect("run_sync_only failed");

    let tree_root = tree.tree_root();
    let tree_size = tree.tree_size();
    let anchor_height = tree.latest_height().expect("tree has no blocks");

    tracing::info!(
        tree_size,
        anchor_height,
        window_start = tree.window_start_shard(),
        tree_root = hex::encode(tree_root),
        "sync complete, YPIR database built"
    );

    let canonical = build_canonical_reference(anchor_height, tree.window_start_shard()).await;
    assert_eq!(
        canonical.broadcast.anchor_height, anchor_height,
        "canonical reference anchor height should match the served anchor"
    );

    let router = build_router(app_state);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind listener");
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let base_url = format!("http://{addr}");
    tracing::info!(url = %base_url, "server listening");

    let client = WitnessClient::connect(&base_url)
        .await
        .expect("WitnessClient::connect failed");

    assert_eq!(client.anchor_height(), anchor_height);
    let server_broadcast = client.broadcast().clone();
    assert_eq!(
        server_broadcast.anchor_height, canonical.broadcast.anchor_height,
        "server broadcast anchor height should match canonical reference"
    );
    assert_eq!(
        server_broadcast.window_start_shard, canonical.broadcast.window_start_shard,
        "server broadcast window start should match canonical reference"
    );
    assert_eq!(
        server_broadcast.window_shard_count, canonical.broadcast.window_shard_count,
        "server broadcast window size should match canonical reference"
    );
    assert_eq!(
        server_broadcast.cap.shard_roots, canonical.broadcast.cap.shard_roots,
        "server broadcast shard roots should match canonical reference"
    );

    // Precompute the server responses once so the test can validate multiple
    // invariants for a position without paying for duplicate encrypted queries.
    let candidate_positions = candidate_positions(&tree);
    let mut server_results = HashMap::new();
    for &(_, position) in &candidate_positions {
        let result = query_server_witness(&base_url, &scenario, &server_broadcast, position).await;
        server_results.insert(position, result);
    }

    let window_start_position = tree.leaf_offset();
    // Assert the specific spillover boundary that previously regressed when the
    // shard-completing block's first local leaves were skipped during bootstrap.
    if let Some(expected_spillover_leaf) = canonical.window_spillover.first() {
        let (_, server_leaf) = server_results
            .get(&window_start_position)
            .expect("window-start result should be precomputed");
        assert_eq!(
            *server_leaf, *expected_spillover_leaf,
            "window-start leaf should match the shard-completing block spillover from lightwalletd"
        );
    }

    for (label, position) in candidate_positions {
        let (server_witness, server_leaf) = server_results
            .remove(&position)
            .expect("candidate position result should be precomputed");
        let (canonical_row, shard_idx, subshard_idx, leaf_idx) =
            reference_decoded_row(&canonical, position);

        let canonical_leaf = leaf_from_decoded_row(canonical_row, leaf_idx);
        let canonical_tree_leaf = reference_leaf_at_position(&canonical.tree, position);
        // Reconstruct the canonical witness from the oracle PIR row so the test
        // compares full witness semantics, not just the leaf bytes.
        let canonical_witness = reconstruct_witness(
            position,
            shard_idx,
            subshard_idx,
            leaf_idx,
            canonical_row,
            &canonical.broadcast,
        )
        .unwrap_or_else(|e| panic!("canonical witness reconstruction failed for {label}: {e}"));

        assert_eq!(
            canonical_leaf, canonical_tree_leaf,
            "canonical row leaf mismatch for {label}"
        );
        assert_eq!(
            server_witness.position, position,
            "witness position mismatch for {label}"
        );
        assert_eq!(
            server_witness.anchor_height, canonical.broadcast.anchor_height,
            "anchor height mismatch for {label}"
        );
        assert_eq!(
            server_leaf, canonical_leaf,
            "\nserver leaf mismatch for {label}!\n  \
             server:    {}\n  canonical: {}\n\n\
             The PIR-served leaf at this position does not match lightwalletd-derived canonical data.",
            hex::encode(server_leaf),
            hex::encode(canonical_leaf)
        );
        assert_eq!(
            server_witness.anchor_root,
            canonical_witness.anchor_root,
            "\nserver root mismatch for {label}!\n  \
             server:    {}\n  canonical: {}\n\n\
             The PIR-served authentication path does not reconstruct the canonical root.",
            hex::encode(server_witness.anchor_root),
            hex::encode(canonical_witness.anchor_root)
        );
        assert_eq!(
            server_witness.siblings, canonical_witness.siblings,
            "authentication path mismatch for {label}"
        );

        tracing::info!(
            label,
            position,
            leaf = hex::encode(server_leaf),
            root = hex::encode(server_witness.anchor_root),
            "PASS: PIR witness matches canonical lightwalletd reference"
        );
    }

    tracing::info!("PASS: full PIR round-trip verified against canonical lightwalletd data");
}
