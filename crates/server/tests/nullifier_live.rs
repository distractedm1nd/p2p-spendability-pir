//! Live integration test against a running spend-server + real lightwalletd.
//!
//! Prerequisites:
//!   1. Start the server: cargo run -p spendability-pir-server --release -- \
//!      --zcash-network main --lwd-url https://us.zec.stardust.rest:443 --data-dir ./data
//!   2. Run this test: cargo test -p spendability-pir-server --features live --test nullifier_live -- \
//!      --nocapture --ignored
//!
//! This test is ignored by default so it doesn't run in CI.

#![cfg(feature = "live")]

use nullifier_pir::ZcashNetwork;
use spendability_pir_client::nullifier::SpendClient;
use spendability_pir_server::ingest::extract_ironwood_nullifiers;
use spendability_pir_server::ingest::LwdClient;
use std::time::Instant;

const SERVER_URL: &str = "http://127.0.0.1:8080";
const LWD_URL: &str = "https://us.zec.stardust.rest:443";

#[tokio::test]
#[ignore]
async fn test_live_random_nullifier_not_spent() {
    let client = SpendClient::connect(SERVER_URL, ZcashNetwork::Main)
        .await
        .unwrap();
    println!("Connected to spend-server");
    println!("  earliest_height: {}", client.earliest_height());
    println!("  latest_height:   {}", client.latest_height());
    println!("  nullifiers:      {}", client.metadata().num_nullifiers);

    let random_nf = [0xAB; 32];
    let start = Instant::now();
    let spent = client.is_spent(&random_nf).await.unwrap();
    println!(
        "\nRandom nullifier: is_spent={} (round-trip {:?})",
        spent,
        start.elapsed()
    );
    assert!(!spent, "random nullifier should not be spent");
}

#[tokio::test]
#[ignore]
async fn test_live_real_nullifier_is_spent() {
    // Connect to lightwalletd directly to fetch a recent block with real nullifiers
    let mut lwd = LwdClient::connect(&[LWD_URL.to_string()]).await.unwrap();
    let (tip_height, _) = lwd.get_latest_block().await.unwrap();
    println!("Chain tip: {tip_height}");

    // Scan backwards from the tip to find a block with Ironwood nullifiers
    let mut real_nf: Option<[u8; 32]> = None;
    let mut found_height = 0u64;
    let search_start = tip_height.saturating_sub(500);

    println!("Searching for Ironwood nullifiers in blocks {search_start}..{tip_height}");
    let blocks = lwd.get_block_range(search_start, tip_height).await.unwrap();

    for block in &blocks {
        let nfs = extract_ironwood_nullifiers(block).unwrap();
        if !nfs.is_empty() {
            real_nf = Some(nfs[0]);
            found_height = block.height;
            println!(
                "  Found {} nullifiers at height {} (using first one: {:02x}{:02x}{:02x}{:02x}...)",
                nfs.len(),
                block.height,
                nfs[0][0],
                nfs[0][1],
                nfs[0][2],
                nfs[0][3],
            );
            break;
        }
    }

    let real_nf = real_nf.expect("no Ironwood nullifiers found in recent 500 blocks");

    // Connect to our spend-server
    let client = SpendClient::connect(SERVER_URL, ZcashNetwork::Main)
        .await
        .unwrap();
    println!(
        "\nSpend-server covers heights {}..{}",
        client.earliest_height(),
        client.latest_height()
    );

    if found_height < client.earliest_height() || found_height > client.latest_height() {
        println!(
            "SKIP: nullifier at height {} is outside server range {}..{}, cannot test",
            found_height,
            client.earliest_height(),
            client.latest_height(),
        );
        return;
    }

    // Query the real nullifier
    let start = Instant::now();
    let spent = client.is_spent(&real_nf).await.unwrap();
    let elapsed = start.elapsed();
    println!(
        "Real nullifier (height {}): spent={} (round-trip {:?})",
        found_height, spent, elapsed
    );
    assert!(spent, "real nullifier should be found");
}

#[tokio::test]
#[ignore]
async fn test_live_server_tracks_new_blocks() {
    let mut client = SpendClient::connect(SERVER_URL, ZcashNetwork::Main)
        .await
        .unwrap();
    let initial_height = client.latest_height();
    let initial_nfs = client.metadata().num_nullifiers;
    println!("Initial state: height={initial_height}, nullifiers={initial_nfs}");

    // Wait for a new block (polling every 10s, timeout after 5 min)
    println!("Waiting for a new block (this may take up to ~75 seconds)...");
    let deadline = Instant::now() + std::time::Duration::from_secs(300);
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        client.refresh_metadata().await.unwrap();
        let current_height = client.latest_height();

        if current_height > initial_height {
            let new_nfs = client.metadata().num_nullifiers;
            println!(
                "New block detected! height: {} -> {}, nullifiers: {} -> {} (+{})",
                initial_height,
                current_height,
                initial_nfs,
                new_nfs,
                new_nfs.saturating_sub(initial_nfs),
            );
            assert!(current_height > initial_height);
            return;
        }

        if Instant::now() > deadline {
            println!("TIMEOUT: no new block after 5 minutes. Chain may be stalled.");
            return;
        }

        print!(".");
    }
}
