#![cfg(all(feature = "nullifier", feature = "witness"))]

//! Throughput tests proving the system handles high TPS within its
//! per-block rebuild budget.
//!
//! - `rebuild_under_20s_at_3tps`: single-block test at 3 TPS / 75 s blocks
//! - `sustained_5tps_15s_blocks`: 2-minute sustained run at 5 TPS / 15 s blocks
//!
//! Run in release mode for meaningful YPIR timing:
//!   cargo test --release -p combined-server --test throughput_3tps -- --nocapture

use arc_swap::ArcSwap;
use commitment_tree_db::CommitmentTreeDb;
use hashtable_pir::HashTableDb;
use pir_types::{PirEngine, YpirScenario};
use spend_types::{NullifierWithMeta, BUCKET_BYTES, NUM_BUCKETS};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use witness_types::{L0_DB_ROWS, SUBSHARD_ROW_BYTES};

fn make_nf(seed: u32) -> NullifierWithMeta {
    let mut nf = [0u8; 32];
    nf[0..4].copy_from_slice(&seed.to_le_bytes());
    for (i, byte) in nf.iter_mut().enumerate().skip(4) {
        *byte = ((seed >> ((i % 4) * 8)) as u8).wrapping_add(i as u8);
    }
    NullifierWithMeta {
        nullifier: nf,
        first_output_position: 0,
        action_count: 1,
    }
}

/// Produce a valid Pallas base field element (valid `MerkleHashOrchard` bytes).
/// The top byte is zeroed so the value is well below the field modulus.
fn make_cmx(seed: u32) -> [u8; 32] {
    let mut cmx = [0u8; 32];
    cmx[0..4].copy_from_slice(&seed.wrapping_mul(7).to_le_bytes());
    for (i, byte) in cmx.iter_mut().enumerate().skip(4).take(27) {
        *byte = ((seed >> ((i % 4) * 8)) as u8).wrapping_add(i as u8 ^ 0xAB);
    }
    cmx[31] = 0;
    cmx
}

fn block_hash_for(height: u64) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0..8].copy_from_slice(&height.to_le_bytes());
    h
}

fn nf_scenario() -> YpirScenario {
    YpirScenario {
        num_items: NUM_BUCKETS as u64,
        item_size_bits: (BUCKET_BYTES * 8) as u64,
    }
}

fn wit_scenario() -> YpirScenario {
    YpirScenario {
        num_items: L0_DB_ROWS as u64,
        item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
    }
}

const REBUILD_BUDGET_3TPS: f64 = 20.0;

#[test]
fn rebuild_under_20s_at_3tps() {
    let actions_per_block: u32 = 450; // 3 TPS × 2 actions/tx × 75 s
    let warmup_blocks: u64 = 10;

    println!("\n=== 3 TPS Throughput Test ===");
    println!(
        "Block profile: {} nullifiers + {} commitments per block",
        actions_per_block, actions_per_block,
    );
    println!(
        "Nullifier DB: {} buckets × {} B = {} MB",
        NUM_BUCKETS,
        BUCKET_BYTES,
        NUM_BUCKETS * BUCKET_BYTES / (1024 * 1024),
    );
    println!(
        "Witness DB:   {} rows × {} B = {} MB",
        L0_DB_ROWS,
        SUBSHARD_ROW_BYTES,
        L0_DB_ROWS * SUBSHARD_ROW_BYTES / (1024 * 1024),
    );

    let nf_sc = nf_scenario();
    let wit_sc = wit_scenario();

    let t0 = Instant::now();
    let nf_engine = spend_server::pir_ypir::YpirPirEngine::new(&nf_sc);
    println!("Nullifier engine params: {:?}", t0.elapsed());

    let t0 = Instant::now();
    let wit_engine = witness_server::pir_ypir::YpirPirEngine::new(&wit_sc);
    println!("Witness engine params:   {:?}", t0.elapsed());

    let mut hashtable = HashTableDb::new();
    let mut tree = CommitmentTreeDb::new();

    for blk in 1..=warmup_blocks {
        let nfs: Vec<NullifierWithMeta> = (0..actions_per_block)
            .map(|j| make_nf(blk as u32 * 10_000 + j))
            .collect();
        let cmxs: Vec<[u8; 32]> = (0..actions_per_block)
            .map(|j| make_cmx(blk as u32 * 10_000 + j))
            .collect();

        hashtable
            .insert_block(blk, block_hash_for(blk), &nfs)
            .unwrap();
        tree.append_commitments(blk, block_hash_for(blk), &cmxs);
    }
    hashtable.evict_to_target();

    println!(
        "Warm-up: {} blocks, {} nullifiers, {} tree leaves",
        warmup_blocks,
        hashtable.len(),
        tree.tree_size(),
    );

    // Initial PIR build (untimed — warms subshard cache)
    let t0 = Instant::now();
    let _nf_state = spend_server::server::rebuild_pir(&nf_engine, &hashtable, &nf_sc)
        .expect("initial nullifier rebuild");
    println!("Initial nullifier rebuild: {:?}", t0.elapsed());

    let anchor = tree.latest_height().unwrap_or(0);
    let t0 = Instant::now();
    let _wit_state = witness_server::server::rebuild_pir(&wit_engine, &mut tree, &wit_sc, anchor)
        .expect("initial witness rebuild");
    println!("Initial witness rebuild:   {:?}", t0.elapsed());

    // Simulate one new block
    let new_height = warmup_blocks + 1;
    let new_nfs: Vec<NullifierWithMeta> = (0..actions_per_block)
        .map(|j| make_nf(99_000 + j))
        .collect();
    let new_cmxs: Vec<[u8; 32]> = (0..actions_per_block)
        .map(|j| make_cmx(99_000 + j))
        .collect();

    // ── TIMED SECTION ────────────────────────────────────────────────
    let total_start = Instant::now();

    let ingest_start = Instant::now();
    hashtable
        .insert_block(new_height, block_hash_for(new_height), &new_nfs)
        .unwrap();
    hashtable.evict_to_target();
    tree.append_commitments(new_height, block_hash_for(new_height), &new_cmxs);
    let ingest_ms = ingest_start.elapsed().as_millis();

    let nf_start = Instant::now();
    let _nf_state = spend_server::server::rebuild_pir(&nf_engine, &hashtable, &nf_sc)
        .expect("nullifier rebuild");
    let nf_ms = nf_start.elapsed().as_millis();

    let anchor = tree.latest_height().unwrap_or(0);
    let wit_start = Instant::now();
    let _wit_state = witness_server::server::rebuild_pir(&wit_engine, &mut tree, &wit_sc, anchor)
        .expect("witness rebuild");
    let wit_ms = wit_start.elapsed().as_millis();

    let total = total_start.elapsed();
    let total_s = total.as_secs_f64();

    println!("\n=== Per-block rebuild breakdown (3 TPS) ===");
    println!("  Ingest:            {} ms", ingest_ms);
    println!("  Nullifier rebuild: {} ms", nf_ms);
    println!("  Witness rebuild:   {} ms", wit_ms);
    println!(
        "  TOTAL:             {:.1} ms",
        total.as_secs_f64() * 1000.0
    );
    println!(
        "\n  Budget: {:.1}s / {:.1}s  {}",
        total_s,
        REBUILD_BUDGET_3TPS,
        if total_s < REBUILD_BUDGET_3TPS {
            "OK"
        } else {
            "EXCEEDED"
        },
    );

    assert!(
        total_s < REBUILD_BUDGET_3TPS,
        "per-block rebuild ({:.1}s) exceeds {:.0}s budget at 3 TPS",
        total_s,
        REBUILD_BUDGET_3TPS,
    );
}

/// Sustained 2-minute test at 5 TPS with 15 s blocks, plus a
/// background thread issuing 1 nullifier PIR query per second.
///
/// 5 TPS × 2 actions/tx × 15 s = 150 actions per block.
/// 120 s / 15 s = 8 blocks.
///
/// Each block cycle (ingest + rebuild) must complete within the 15 s
/// block interval. The query thread measures answer_query latency
/// under concurrent rebuild load.
#[test]
fn sustained_5tps_15s_blocks() {
    const BLOCK_INTERVAL_SECS: f64 = 15.0;
    const DURATION_SECS: f64 = 120.0;
    const TPS: u32 = 5;
    const ACTIONS_PER_TX: u32 = 2;
    const ACTIONS_PER_BLOCK: u32 = TPS * ACTIONS_PER_TX * BLOCK_INTERVAL_SECS as u32; // 150
    const NUM_BLOCKS: u64 = (DURATION_SECS / BLOCK_INTERVAL_SECS) as u64; // 8
    const WARMUP_BLOCKS: u64 = 10;

    println!("\n=== Sustained 5 TPS / 15 s blocks — 2 min + 1 RPS queries ===");
    println!(
        "  {} actions/block, {} blocks, budget {:.0}s/block",
        ACTIONS_PER_BLOCK, NUM_BLOCKS, BLOCK_INTERVAL_SECS,
    );
    println!(
        "  Nullifier DB: {} MB   Witness DB: {} MB",
        NUM_BUCKETS * BUCKET_BYTES / (1024 * 1024),
        L0_DB_ROWS * SUBSHARD_ROW_BYTES / (1024 * 1024),
    );

    let nf_sc = nf_scenario();
    let wit_sc = wit_scenario();

    let nf_engine = Arc::new(spend_server::pir_ypir::YpirPirEngine::new(&nf_sc));
    let wit_engine = witness_server::pir_ypir::YpirPirEngine::new(&wit_sc);

    // ── Warm-up ──────────────────────────────────────────────────────
    let mut hashtable = HashTableDb::new();
    let mut tree = CommitmentTreeDb::new();

    for blk in 1..=WARMUP_BLOCKS {
        let nfs: Vec<NullifierWithMeta> = (0..ACTIONS_PER_BLOCK)
            .map(|j| make_nf(blk as u32 * 10_000 + j))
            .collect();
        let cmxs: Vec<[u8; 32]> = (0..ACTIONS_PER_BLOCK)
            .map(|j| make_cmx(blk as u32 * 10_000 + j))
            .collect();

        hashtable
            .insert_block(blk, block_hash_for(blk), &nfs)
            .unwrap();
        tree.append_commitments(blk, block_hash_for(blk), &cmxs);
    }
    hashtable.evict_to_target();

    // Initial PIR build (warms subshard cache)
    let nf_state = spend_server::server::rebuild_pir(&*nf_engine, &hashtable, &nf_sc)
        .expect("initial nullifier rebuild");
    let anchor = tree.latest_height().unwrap_or(0);
    let _wit_state = witness_server::server::rebuild_pir(&wit_engine, &mut tree, &wit_sc, anchor)
        .expect("initial witness rebuild");

    println!(
        "  Warm-up done: {} nullifiers, {} tree leaves",
        hashtable.len(),
        tree.tree_size(),
    );

    // ── Set up shared PIR state for query thread ─────────────────────
    let live_nf: Arc<
        ArcSwap<spend_server::state::PirState<spend_server::pir_ypir::YpirPirEngine>>,
    > = Arc::new(ArcSwap::from_pointee(nf_state));

    // Pre-generate a valid YPIR query (reusable across rebuilds since
    // params are static).
    let params = nf_engine.params();
    let client = ypir::client::YPIRClient::new(params);
    let (query, _seed) = client.generate_query_simplepir(0);
    let query_bytes: Vec<u8> = ypir::serialize::ToBytes::to_bytes(&query);
    println!("  Query size: {} bytes", query_bytes.len());

    // ── Spawn query thread (1 RPS) ───────────────────────────────────
    let stop_flag = Arc::new(AtomicBool::new(false));
    let query_handle = {
        let engine = nf_engine.clone();
        let live = live_nf.clone();
        let stop = stop_flag.clone();
        let qb = query_bytes;

        std::thread::spawn(move || {
            let mut latencies: Vec<Duration> = Vec::new();
            let mut errors: u32 = 0;

            while !stop.load(Ordering::Relaxed) {
                let guard = live.load();
                let t0 = Instant::now();
                match engine.answer_query(&guard.engine_state, &qb) {
                    Ok(_) => latencies.push(t0.elapsed()),
                    Err(_) => errors += 1,
                }
                std::thread::sleep(Duration::from_secs(1));
            }

            (latencies, errors)
        })
    };

    // ── Sustained run ────────────────────────────────────────────────
    println!(
        "\n  {:>5}  {:>8}  {:>8}  {:>8}  {:>8}  {}",
        "Block", "Ingest", "NF PIR", "Wit PIR", "Total", "Status",
    );
    println!("  {}", "-".repeat(62));

    let run_start = Instant::now();
    let mut worst_s: f64 = 0.0;
    let mut total_rebuild_ms: u128 = 0;

    for i in 0..NUM_BLOCKS {
        let height = WARMUP_BLOCKS + 1 + i;
        let seed_base = (height as u32) * 10_000;

        let nfs: Vec<NullifierWithMeta> = (0..ACTIONS_PER_BLOCK)
            .map(|j| make_nf(seed_base + j))
            .collect();
        let cmxs: Vec<[u8; 32]> = (0..ACTIONS_PER_BLOCK)
            .map(|j| make_cmx(seed_base + j))
            .collect();

        let cycle_start = Instant::now();

        // Ingest
        let ingest_start = Instant::now();
        hashtable
            .insert_block(height, block_hash_for(height), &nfs)
            .unwrap();
        hashtable.evict_to_target();
        tree.append_commitments(height, block_hash_for(height), &cmxs);
        let ingest_ms = ingest_start.elapsed().as_millis();

        // Nullifier rebuild
        let nf_start = Instant::now();
        let nf_state = spend_server::server::rebuild_pir(&*nf_engine, &hashtable, &nf_sc)
            .expect("nullifier rebuild");
        let nf_ms = nf_start.elapsed().as_millis();

        // Atomic swap — query thread immediately sees the new state
        live_nf.store(Arc::new(nf_state));

        // Witness rebuild
        let anchor = tree.latest_height().unwrap_or(0);
        let wit_start = Instant::now();
        let _wit_state =
            witness_server::server::rebuild_pir(&wit_engine, &mut tree, &wit_sc, anchor)
                .expect("witness rebuild");
        let wit_ms = wit_start.elapsed().as_millis();

        let cycle = cycle_start.elapsed();
        let cycle_s = cycle.as_secs_f64();
        total_rebuild_ms += cycle.as_millis();
        if cycle_s > worst_s {
            worst_s = cycle_s;
        }

        let status = if cycle_s < BLOCK_INTERVAL_SECS {
            "OK"
        } else {
            "EXCEEDED"
        };

        println!(
            "  {:>5}  {:>5} ms  {:>5} ms  {:>5} ms  {:>5} ms  {}",
            height,
            ingest_ms,
            nf_ms,
            wit_ms,
            cycle.as_millis(),
            status,
        );

        assert!(
            cycle_s < BLOCK_INTERVAL_SECS,
            "block {} rebuild ({:.1}s) exceeds {:.0}s block interval",
            height,
            cycle_s,
            BLOCK_INTERVAL_SECS,
        );
    }

    // ── Stop query thread and collect results ────────────────────────
    stop_flag.store(true, Ordering::Relaxed);
    let (query_latencies, query_errors) = query_handle.join().expect("query thread panicked");

    let run_elapsed = run_start.elapsed();
    let avg_rebuild_ms = total_rebuild_ms as f64 / NUM_BLOCKS as f64;

    // ── Report ───────────────────────────────────────────────────────
    println!("\n=== Rebuild Summary ===");
    println!("  Blocks processed:  {}", NUM_BLOCKS);
    println!(
        "  Simulated time:    {:.0} s ({:.0} min)",
        DURATION_SECS,
        DURATION_SECS / 60.0
    );
    println!("  Actual wall time:  {:.1} s", run_elapsed.as_secs_f64());
    println!("  Avg cycle:         {:.0} ms", avg_rebuild_ms);
    println!("  Worst cycle:       {:.0} ms", worst_s * 1000.0);
    println!(
        "  Headroom:          {:.0}% of {:.0}s interval",
        (1.0 - worst_s / BLOCK_INTERVAL_SECS) * 100.0,
        BLOCK_INTERVAL_SECS,
    );
    println!(
        "  Nullifiers:        {} (across {} blocks)",
        hashtable.len(),
        hashtable.num_blocks(),
    );
    println!("  Tree leaves:       {}", tree.tree_size());

    println!("\n=== Query Thread (1 RPS) ===");
    println!(
        "  Queries completed: {}   Errors: {}",
        query_latencies.len(),
        query_errors,
    );
    if !query_latencies.is_empty() {
        let min = query_latencies.iter().min().unwrap();
        let max = query_latencies.iter().max().unwrap();
        let sum_ms: u128 = query_latencies.iter().map(|d| d.as_millis()).sum();
        let avg_ms = sum_ms as f64 / query_latencies.len() as f64;

        println!("  Min latency:       {:?}", min);
        println!("  Avg latency:       {:.0} ms", avg_ms);
        println!("  Max latency:       {:?}", max);

        let mut sorted: Vec<u128> = query_latencies.iter().map(|d| d.as_millis()).collect();
        sorted.sort();
        let p50 = sorted[sorted.len() / 2];
        let p99 = sorted[(sorted.len() as f64 * 0.99) as usize];
        println!("  p50 latency:       {} ms", p50);
        println!("  p99 latency:       {} ms", p99);
    }

    assert_eq!(query_errors, 0, "PIR queries failed during sustained run");
}

/// Scaling benchmark: measures YPIR setup cost at database sizes
/// corresponding to various coverage targets at 3 TPS.
///
/// Bypasses data structures entirely — measures `engine.setup()` on
/// zero-filled byte vectors at each target size. YPIR cost depends on
/// database geometry (rows × row_bytes), not content.
///
/// 3 TPS × 2 actions/tx × 25 s blocks = 150 actions/block = 518,400/day.
#[test]
fn bench_scaling_ceiling() {
    const BLOCK_INTERVAL: f64 = 25.0;
    const MAX_DB_BYTES: usize = 2 * 1024 * 1024 * 1024; // 2 GB cap per subsystem

    struct Scenario {
        label: &'static str,
        nf_buckets: u64,
        wit_subshards: u64,
    }

    // 3 TPS × 2 actions/tx × 25 s blocks = 150 actions/block = 518,400/day
    // (daily throughput unchanged; only block interval & actions-per-block change)
    //
    // NF bucket count: next_pow2(actions_in_period / BUCKET_CAPACITY)
    // WIT subshards:   ceil(actions_in_period / SHARD_LEAVES) * SUBSHARDS_PER_SHARD
    //                  = ceil(actions / 65536) * 256
    let scenarios = [
        Scenario {
            label: "2 days",
            nf_buckets: 16_384,   // 2^14, DB = 56 MB
            wit_subshards: 8_192, // 32 shards, DB = 64 MB
        },
        Scenario {
            label: "1 week",
            nf_buckets: 32_768,    // 2^15, DB = 112 MB
            wit_subshards: 14_336, // 56 shards, DB = 112 MB
        },
        Scenario {
            label: "2 weeks",
            nf_buckets: 65_536,    // 2^16, DB = 224 MB
            wit_subshards: 28_416, // 111 shards, DB = 222 MB
        },
        Scenario {
            label: "1 month",
            nf_buckets: 262_144,   // 2^18, DB = 896 MB
            wit_subshards: 60_928, // 238 shards, DB = 476 MB
        },
        Scenario {
            label: "2 months",
            nf_buckets: 524_288,    // 2^19, DB = 1,792 MB
            wit_subshards: 121_600, // 475 shards, DB = 950 MB
        },
        Scenario {
            label: "3 months",
            nf_buckets: 524_288,    // 2^19 (same — capacity covers ~113 days)
            wit_subshards: 182_272, // 712 shards, DB = 1,424 MB
        },
    ];

    println!("\n=== PIR Scaling Benchmark (3 TPS / 25 s blocks) ===");
    println!(
        "  Row sizes: NF = {} B, WIT = {} B",
        BUCKET_BYTES, SUBSHARD_ROW_BYTES
    );
    println!(
        "  Memory cap: {} GB per subsystem",
        MAX_DB_BYTES / (1024 * 1024 * 1024)
    );
    println!();
    println!(
        "  {:>10}  {:>8}  {:>10}  {:>8}  {:>10}  {:>10}  {:>6}  {}",
        "Coverage", "NF DB", "NF setup", "WIT DB", "WIT setup", "Combined", "Block", "Status",
    );
    println!("  {}", "-".repeat(90));

    for sc in &scenarios {
        let nf_db_bytes = sc.nf_buckets as usize * BUCKET_BYTES;
        let wit_db_bytes = sc.wit_subshards as usize * SUBSHARD_ROW_BYTES;

        if nf_db_bytes > MAX_DB_BYTES || wit_db_bytes > MAX_DB_BYTES {
            println!(
                "  {:>10}  {:>5} MB  {:>10}  {:>5} MB  {:>10}  {:>10}  {:>5}s  SKIPPED (exceeds memory cap)",
                sc.label,
                nf_db_bytes / (1024 * 1024),
                "",
                wit_db_bytes / (1024 * 1024),
                "",
                "",
                BLOCK_INTERVAL as u32,
            );
            continue;
        }

        // Nullifier
        let nf_sc = YpirScenario {
            num_items: sc.nf_buckets,
            item_size_bits: (BUCKET_BYTES * 8) as u64,
        };
        let nf_engine = spend_server::pir_ypir::YpirPirEngine::new(&nf_sc);
        let nf_db = vec![0u8; nf_db_bytes];

        let nf_t0 = Instant::now();
        let _nf_state = nf_engine.setup(&nf_db, &nf_sc).expect("NF setup failed");
        let nf_elapsed = nf_t0.elapsed();

        drop(_nf_state);
        drop(nf_db);

        // Witness
        let wit_sc = YpirScenario {
            num_items: sc.wit_subshards,
            item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
        };
        let wit_engine = witness_server::pir_ypir::YpirPirEngine::new(&wit_sc);
        let wit_db = vec![0u8; wit_db_bytes];

        let wit_t0 = Instant::now();
        let _wit_state = wit_engine
            .setup(&wit_db, &wit_sc)
            .expect("WIT setup failed");
        let wit_elapsed = wit_t0.elapsed();

        drop(_wit_state);
        drop(wit_db);

        let combined_s = nf_elapsed.as_secs_f64() + wit_elapsed.as_secs_f64();
        let status = if combined_s < BLOCK_INTERVAL {
            "OK"
        } else {
            "EXCEEDED"
        };

        println!(
            "  {:>10}  {:>5} MB  {:>7.1} s  {:>5} MB  {:>7.1} s  {:>7.1} s  {:>5}s  {}",
            sc.label,
            nf_db_bytes / (1024 * 1024),
            nf_elapsed.as_secs_f64(),
            wit_db_bytes / (1024 * 1024),
            wit_elapsed.as_secs_f64(),
            combined_s,
            BLOCK_INTERVAL as u32,
            status,
        );
    }

    println!();
}
