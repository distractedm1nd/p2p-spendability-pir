use nullifier_pir::{
    hash_to_bucket, PirEngine, YpirScenario, BUCKET_BYTES, ENTRY_BYTES, NUM_BUCKETS,
};
use spendability_pir_server::pir::YpirPirEngine;
use ypir::client::YPIRClient;
use ypir::serialize::ToBytes;

fn scenario() -> YpirScenario {
    YpirScenario {
        num_items: NUM_BUCKETS as u64,
        item_size_bits: (BUCKET_BYTES * 8) as u64,
    }
}

fn make_nf(seed: u32) -> [u8; 32] {
    let mut nf = [0u8; 32];
    nf[0..4].copy_from_slice(&seed.to_le_bytes());
    for (i, byte) in nf.iter_mut().enumerate().skip(4) {
        *byte = ((seed >> ((i % 4) * 8)) as u8).wrapping_add(i as u8);
    }
    nf
}

/// Build a flat DB byte array with a single nullifier placed in the correct bucket.
fn build_db_with_nf(nf: &[u8; 32]) -> Vec<u8> {
    let mut db = vec![0u8; NUM_BUCKETS * BUCKET_BYTES];
    let bucket_idx = hash_to_bucket(nf) as usize;
    let offset = bucket_idx * BUCKET_BYTES;
    db[offset..offset + ENTRY_BYTES].copy_from_slice(nf);
    db
}

#[test]
fn test_ypir_roundtrip_found() {
    let sc = scenario();
    let engine = YpirPirEngine::new(&sc);
    let params = engine.params();

    let nf = make_nf(12345);
    let db_bytes = build_db_with_nf(&nf);
    let bucket_idx = hash_to_bucket(&nf) as usize;

    let state = engine.setup(&db_bytes, &sc).unwrap();

    let client = YPIRClient::new(params);
    let (query, seed) = client.generate_query_simplepir(bucket_idx);
    let query_bytes = query.to_bytes();

    let response = engine.answer_query(&state, &query_bytes).unwrap();

    let decoded = client.decode_response_simplepir(seed, &response);
    assert!(
        decoded.len() >= BUCKET_BYTES,
        "decoded response too short: {} < {}",
        decoded.len(),
        BUCKET_BYTES,
    );

    let bucket_data = &decoded[..BUCKET_BYTES];
    let found = bucket_data
        .chunks_exact(ENTRY_BYTES)
        .any(|chunk| chunk[..32] == nf[..]);
    assert!(found, "nullifier not found in decoded bucket");
}

#[test]
fn test_ypir_roundtrip_not_found() {
    let sc = scenario();
    let engine = YpirPirEngine::new(&sc);
    let params = engine.params();

    let present_nf = make_nf(12345);
    let absent_nf = make_nf(99999);
    let db_bytes = build_db_with_nf(&present_nf);

    let absent_bucket = hash_to_bucket(&absent_nf) as usize;

    let state = engine.setup(&db_bytes, &sc).unwrap();

    let client = YPIRClient::new(params);
    let (query, seed) = client.generate_query_simplepir(absent_bucket);
    let query_bytes = query.to_bytes();

    let response = engine.answer_query(&state, &query_bytes).unwrap();
    let decoded = client.decode_response_simplepir(seed, &response);
    let bucket_data = &decoded[..BUCKET_BYTES];

    let found = bucket_data
        .chunks_exact(ENTRY_BYTES)
        .any(|chunk| chunk[..32] == absent_nf[..]);
    assert!(!found, "absent nullifier should not appear in bucket");
}

#[test]
fn test_ypir_with_hashtable() {
    let sc = scenario();
    let engine = YpirPirEngine::new(&sc);
    let params = engine.params();

    let mut db = nullifier_pir::HashTableDb::new();
    let nfs: Vec<[u8; 32]> = (100..110).map(make_nf).collect();
    db.insert_block(1, [1u8; 32], &nfs).unwrap();

    let pir_bytes = db.to_pir_bytes();
    let state = engine.setup(&pir_bytes, &sc).unwrap();

    let client = YPIRClient::new(params);

    for nf in &nfs {
        let bucket_idx = hash_to_bucket(nf) as usize;
        let (query, seed) = client.generate_query_simplepir(bucket_idx);
        let query_bytes = query.to_bytes();

        let response = engine.answer_query(&state, &query_bytes).unwrap();
        let decoded = client.decode_response_simplepir(seed, &response);
        let bucket_data = &decoded[..BUCKET_BYTES];

        let found = bucket_data
            .chunks_exact(ENTRY_BYTES)
            .any(|chunk| chunk[..32] == nf[..]);
        assert!(
            found,
            "nullifier {:?} not found in decoded bucket {}",
            &nf[..4],
            bucket_idx,
        );
    }
}
