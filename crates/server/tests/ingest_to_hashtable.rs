mod mock_server;

use mock_server::{make_compact_block, spawn_mock_server, MockState};
use nullifier_pir::HashTableDb;

async fn sync_into(
    client: &mut spendability_pir_server::ingest::LwdClient,
    from: u64,
    to: u64,
    db: &mut HashTableDb,
) {
    spendability_pir_server::ingest::sync_blocks(client, from, to, |block| {
        let nullifiers =
            spendability_pir_server::ingest::extract_ironwood_nullifiers(&block.block).unwrap();
        db.insert_block(block.height(), block.hash, &nullifiers)
            .unwrap();
        std::future::ready(Ok::<(), spendability_pir_server::ingest::PipelineError>(()))
    })
    .await
    .unwrap();
}

fn hash_for(n: u16) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0..2].copy_from_slice(&n.to_le_bytes());
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

/// Build a chain of compact blocks, each with `nfs_per_block` random nullifiers.
fn build_chain(
    count: u16,
    nfs_per_block: u32,
) -> (
    Vec<spendability_pir_server::ingest::proto::CompactBlock>,
    Vec<Vec<[u8; 32]>>,
) {
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

#[tokio::test]
async fn test_sync_into_hashtable() {
    let state = MockState::new();
    let (blocks, all_nfs) = build_chain(500, 5);
    let total_nfs: usize = all_nfs.iter().map(|v| v.len()).sum();
    state.set_blocks(blocks);

    let (addr, _shutdown) = spawn_mock_server(state).await;
    let mut client =
        spendability_pir_server::ingest::LwdClient::connect(&[format!("http://{addr}")])
            .await
            .unwrap();

    let mut db = HashTableDb::new();
    sync_into(&mut client, 1, 500, &mut db).await;

    assert_eq!(db.num_blocks(), 500);
    assert_eq!(
        db.len(),
        total_nfs,
        "hashtable should contain all nullifiers"
    );
    assert_eq!(db.earliest_height(), Some(1));
    assert_eq!(db.latest_height(), Some(500));

    for block_nfs in all_nfs.iter().step_by(50) {
        for nf in block_nfs {
            assert!(db.contains(nf), "known nullifier not found in hashtable");
        }
    }
}

#[tokio::test]
async fn test_sync_reorg_rollback_and_snapshot() {
    let state = MockState::new();

    let (blocks, all_nfs) = build_chain(100, 5);
    state.set_blocks(blocks);

    let (addr, shutdown) = spawn_mock_server(state.clone()).await;
    let mut client =
        spendability_pir_server::ingest::LwdClient::connect(&[format!("http://{addr}")])
            .await
            .unwrap();

    let mut db = HashTableDb::new();
    sync_into(&mut client, 1, 100, &mut db).await;
    assert_eq!(db.len(), 500);
    assert_eq!(db.latest_height(), Some(100));

    let hash_99 = hash_for(99);
    let hash_100 = hash_for(100);
    db.rollback_block(&hash_100).unwrap();
    db.rollback_block(&hash_99).unwrap();
    assert_eq!(db.len(), 490);
    assert_eq!(db.latest_height(), Some(98));

    for nf in &all_nfs[98] {
        assert!(!db.contains(nf), "orphaned nf from block 99 should be gone");
    }
    for nf in &all_nfs[99] {
        assert!(
            !db.contains(nf),
            "orphaned nf from block 100 should be gone"
        );
    }

    let replacement_nfs_99: Vec<[u8; 32]> = (0..5).map(|j| make_nf(99_000 + j)).collect();
    let replacement_nfs_100: Vec<[u8; 32]> = (0..5).map(|j| make_nf(100_000 + j)).collect();
    let mut new_hash_99 = [0u8; 32];
    new_hash_99[0] = 0xAA;
    let mut new_hash_100 = [0u8; 32];
    new_hash_100[0] = 0xBB;

    db.insert_block(99, new_hash_99, &replacement_nfs_99)
        .unwrap();
    db.insert_block(100, new_hash_100, &replacement_nfs_100)
        .unwrap();
    assert_eq!(db.len(), 500);

    for nf in &replacement_nfs_99 {
        assert!(db.contains(nf), "new block 99 nf should be present");
    }
    for nf in &replacement_nfs_100 {
        assert!(db.contains(nf), "new block 100 nf should be present");
    }

    let snap = db.to_snapshot();
    let restored = HashTableDb::from_snapshot(&snap).unwrap();

    assert_eq!(restored.len(), db.len());
    assert_eq!(restored.earliest_height(), db.earliest_height());
    assert_eq!(restored.latest_height(), db.latest_height());
    assert_eq!(restored.num_blocks(), db.num_blocks());

    for (i, block_nfs) in all_nfs.iter().enumerate().take(98) {
        for nf in block_nfs {
            assert!(
                restored.contains(nf),
                "block {} nf missing after snapshot restore",
                i + 1,
            );
        }
    }
    for nf in &replacement_nfs_99 {
        assert!(restored.contains(nf));
    }
    for nf in &replacement_nfs_100 {
        assert!(restored.contains(nf));
    }

    shutdown.send(()).ok();
}

#[tokio::test]
async fn test_eviction_during_sync() {
    let state = MockState::new();

    let (blocks, _all_nfs) = build_chain(200, 10);
    state.set_blocks(blocks);

    let (addr, _shutdown) = spawn_mock_server(state).await;
    let mut client =
        spendability_pir_server::ingest::LwdClient::connect(&[format!("http://{addr}")])
            .await
            .unwrap();

    let mut db = HashTableDb::new();
    sync_into(&mut client, 1, 200, &mut db).await;

    assert_eq!(db.len(), 2000);

    for _ in 0..50 {
        db.evict_oldest_block();
    }

    assert_eq!(db.len(), 1500);
    assert_eq!(db.earliest_height(), Some(51));
    assert_eq!(db.latest_height(), Some(200));
    assert_eq!(db.num_blocks(), 150);
}
