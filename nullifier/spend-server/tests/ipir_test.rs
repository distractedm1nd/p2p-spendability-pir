#![cfg(feature = "ipir")]

use ipir_sp::serialize::serialize_packing_keys;
use spend_server::pir_ipir::IpirPirEngine;
use spend_types::{
    hash_to_bucket, PirEngine, YpirScenario, BUCKET_BYTES, ENTRY_BYTES, IPIR_SETUP_SEED,
    NUM_BUCKETS,
};

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

fn build_db_with_nf(nf: &[u8; 32]) -> Vec<u8> {
    let mut db = vec![0u8; NUM_BUCKETS * BUCKET_BYTES];
    let bucket_idx = hash_to_bucket(nf) as usize;
    let offset = bucket_idx * BUCKET_BYTES;
    let entry = spend_types::NullifierEntry {
        nullifier: *nf,
        spend_height: 1,
        first_output_position: 0,
        action_count: 1,
    };
    db[offset..offset + ENTRY_BYTES].copy_from_slice(&entry.to_bytes());
    db
}

fn query_for_bucket(engine: &IpirPirEngine, bucket_idx: usize) -> (Vec<u8>, ipir_sp::IPIRSeed) {
    let client = ipir_sp::IPIRClient::new(engine.rlwe_params(), engine.ypir_params());
    let offline_query_polys =
        client.generate_public_query_setup_simplepir_from_seed(IPIR_SETUP_SEED);
    assert_eq!(offline_query_polys, engine.offline_query_polys());

    let (query, packing_keys, seed) =
        client.generate_fresh_query_simplepir(&offline_query_polys, bucket_idx);
    let mut query_bytes =
        serialize_packing_keys(client.rlwe_params(), &packing_keys).expect("serialize keys");
    query_bytes.extend(query.to_packed_bytes(client.rlwe_params().q));
    (query_bytes, seed)
}

#[test]
fn test_ipir_roundtrip_found() {
    let sc = scenario();
    let engine = IpirPirEngine::new(&sc).unwrap();

    let nf = make_nf(12345);
    let db_bytes = build_db_with_nf(&nf);
    let bucket_idx = hash_to_bucket(&nf) as usize;

    let state = engine.setup(&db_bytes, &sc).unwrap();
    let (query_bytes, seed) = query_for_bucket(&engine, bucket_idx);
    let response = engine.answer_query(&state, &query_bytes).unwrap();

    let client = ipir_sp::IPIRClient::new(engine.rlwe_params(), engine.ypir_params());
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
fn test_ipir_roundtrip_not_found() {
    let sc = scenario();
    let engine = IpirPirEngine::new(&sc).unwrap();

    let present_nf = make_nf(12345);
    let absent_nf = make_nf(99999);
    let db_bytes = build_db_with_nf(&present_nf);
    let absent_bucket = hash_to_bucket(&absent_nf) as usize;

    let state = engine.setup(&db_bytes, &sc).unwrap();
    let (query_bytes, seed) = query_for_bucket(&engine, absent_bucket);
    let response = engine.answer_query(&state, &query_bytes).unwrap();

    let client = ipir_sp::IPIRClient::new(engine.rlwe_params(), engine.ypir_params());
    let decoded = client.decode_response_simplepir(seed, &response);
    let bucket_data = &decoded[..BUCKET_BYTES];

    let found = bucket_data
        .chunks_exact(ENTRY_BYTES)
        .any(|chunk| chunk[..32] == absent_nf[..]);
    assert!(!found, "absent nullifier should not appear in bucket");
}
