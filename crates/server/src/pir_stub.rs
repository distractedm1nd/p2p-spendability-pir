use pir_protocol::{PirEngine, YpirScenario};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StubPirError {
    #[error("invalid query: expected 4 bytes, got {0}")]
    InvalidQuery(usize),
    #[error("row index {0} out of range (max {1})")]
    OutOfRange(u32, u64),
}

pub struct StubPirEngine;

pub struct StubServerState {
    db: Vec<u8>,
    num_items: u64,
    row_bytes: usize,
}

impl PirEngine for StubPirEngine {
    type ServerState = StubServerState;
    type Error = StubPirError;

    fn setup(
        &self,
        db_bytes: &[u8],
        scenario: &YpirScenario,
    ) -> Result<StubServerState, StubPirError> {
        Ok(StubServerState {
            db: db_bytes.to_vec(),
            num_items: scenario.num_items,
            row_bytes: scenario.item_size_bits.div_ceil(8) as usize,
        })
    }

    fn answer_query(
        &self,
        state: &StubServerState,
        query_bytes: &[u8],
    ) -> Result<Vec<u8>, StubPirError> {
        if query_bytes.len() < 4 {
            return Err(StubPirError::InvalidQuery(query_bytes.len()));
        }
        let idx = u32::from_le_bytes(query_bytes[..4].try_into().unwrap());
        if idx as u64 >= state.num_items {
            return Err(StubPirError::OutOfRange(idx, state.num_items));
        }
        let start = idx as usize * state.row_bytes;
        let end = start + state.row_bytes;
        if end > state.db.len() {
            return Err(StubPirError::OutOfRange(idx, state.num_items));
        }
        Ok(state.db[start..end].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use witness_pir::{L0_DB_BYTES, L0_DB_ROWS, SUBSHARD_ROW_BYTES, YPIR_POLY_LEN};

    #[test]
    fn stub_setup_and_query() {
        let engine = StubPirEngine;
        let scenario = YpirScenario {
            num_items: L0_DB_ROWS as u64,
            item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
            poly_len: YPIR_POLY_LEN,
        };

        let mut db = vec![0u8; L0_DB_BYTES];
        // Write a known pattern into row 0
        db[..32].fill(0xAB);

        let state = engine.setup(&db, &scenario).unwrap();

        let query = 0u32.to_le_bytes();
        let result = engine.answer_query(&state, &query).unwrap();
        assert_eq!(result.len(), SUBSHARD_ROW_BYTES);
        assert_eq!(&result[..32], &[0xAB; 32]);
    }

    #[test]
    fn stub_invalid_query() {
        let engine = StubPirEngine;
        let scenario = YpirScenario {
            num_items: L0_DB_ROWS as u64,
            item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
            poly_len: YPIR_POLY_LEN,
        };
        let db = vec![0u8; L0_DB_BYTES];
        let state = engine.setup(&db, &scenario).unwrap();
        assert!(engine.answer_query(&state, &[0, 1]).is_err());
    }

    #[test]
    fn stub_out_of_range() {
        let engine = StubPirEngine;
        let scenario = YpirScenario {
            num_items: L0_DB_ROWS as u64,
            item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
            poly_len: YPIR_POLY_LEN,
        };
        let db = vec![0u8; L0_DB_BYTES];
        let state = engine.setup(&db, &scenario).unwrap();
        let idx = (L0_DB_ROWS as u32).to_le_bytes();
        assert!(engine.answer_query(&state, &idx).is_err());
    }
}
