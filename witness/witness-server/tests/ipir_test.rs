#![cfg(feature = "ipir")]

use ipir_sp::serialize::serialize_packing_keys;
use pir_types::{PirEngine, YpirScenario, IPIR_SETUP_SEED};
use witness_server::pir_ipir::IpirPirEngine;
use witness_types::{L0_DB_ROWS, SUBSHARD_ROW_BYTES};

fn scenario() -> YpirScenario {
    YpirScenario {
        num_items: L0_DB_ROWS as u64,
        item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
    }
}

fn query_for_row(engine: &IpirPirEngine, row_idx: usize) -> (Vec<u8>, ipir_sp::IPIRSeed) {
    let client = ipir_sp::IPIRClient::new(engine.rlwe_params(), engine.ypir_params());
    let offline_query_polys =
        client.generate_public_query_setup_simplepir_from_seed(IPIR_SETUP_SEED);
    assert_eq!(offline_query_polys, engine.offline_query_polys());

    let (query, packing_keys, seed) =
        client.generate_fresh_query_simplepir(&offline_query_polys, row_idx);
    let mut query_bytes =
        serialize_packing_keys(client.rlwe_params(), &packing_keys).expect("serialize keys");
    query_bytes.extend(query.to_packed_bytes(client.rlwe_params().q));
    (query_bytes, seed)
}

#[test]
fn test_ipir_subshard_row_roundtrip() {
    let sc = scenario();
    let engine = IpirPirEngine::new(&sc).unwrap();
    let row_idx = 17usize;
    let mut db = vec![0u8; L0_DB_ROWS * SUBSHARD_ROW_BYTES];
    let row_start = row_idx * SUBSHARD_ROW_BYTES;
    for (i, byte) in db[row_start..row_start + 128].iter_mut().enumerate() {
        *byte = (i as u8).wrapping_mul(3).wrapping_add(1);
    }

    let state = engine.setup(&db, &sc).unwrap();
    let (query_bytes, seed) = query_for_row(&engine, row_idx);
    let response = engine.answer_query(&state, &query_bytes).unwrap();

    let client = ipir_sp::IPIRClient::new(engine.rlwe_params(), engine.ypir_params());
    let decoded = client.decode_response_simplepir(seed, &response);
    assert!(
        decoded.len() >= SUBSHARD_ROW_BYTES,
        "decoded response too short: {} < {}",
        decoded.len(),
        SUBSHARD_ROW_BYTES,
    );
    assert_eq!(&decoded[..128], &db[row_start..row_start + 128]);
}
