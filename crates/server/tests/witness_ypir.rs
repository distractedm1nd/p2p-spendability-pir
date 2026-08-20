use pir_protocol::{PirEngine, YpirScenario};
use spendability_pir_server::pir::YpirPirEngine;
use witness_pir::{L0_DB_BYTES, L0_DB_ROWS, SUBSHARD_ROW_BYTES, YPIR_POLY_LEN};
use ypir::client::YPIRClient;
use ypir::params::{params_for_scenario_simplepir_with_config, YPIRSPConfig};
use ypir::serialize::ToBytes;

#[test]
fn witness_4096_degree_roundtrip() {
    let scenario = YpirScenario {
        num_items: L0_DB_ROWS as u64,
        item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
        poly_len: YPIR_POLY_LEN,
    };
    let engine = YpirPirEngine::new(&scenario);
    let mut db = vec![0; L0_DB_BYTES];
    let row_idx = 17;
    let row_start = row_idx * SUBSHARD_ROW_BYTES;
    let row_end = row_start + SUBSHARD_ROW_BYTES;
    for (i, byte) in db[row_start..row_end].iter_mut().enumerate() {
        *byte = i as u8;
    }

    let state = engine.setup(&db, &scenario).unwrap();
    let params = params_for_scenario_simplepir_with_config(
        scenario.num_items,
        scenario.item_size_bits,
        YPIRSPConfig::for_poly_len(scenario.poly_len),
    );
    let client = YPIRClient::new(&params);
    let (query, seed) = client.generate_query_simplepir(row_idx);
    let response = engine.answer_query(&state, &query.to_bytes()).unwrap();
    let decoded = client.decode_response_simplepir(seed, &response);

    assert_eq!(&decoded[..SUBSHARD_ROW_BYTES], &db[row_start..row_end]);
}
