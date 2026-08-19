use spend_types::{PirEngine, YpirScenario, BUCKET_BYTES};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StubPirError {
    #[error("invalid query: expected 4 bytes, got {0}")]
    InvalidQuery(usize),
    #[error("bucket index {0} out of range")]
    OutOfRange(u32),
}

pub struct StubPirEngine;

pub struct StubServerState {
    buckets: Vec<u8>,
    num_items: u64,
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
            buckets: db_bytes.to_vec(),
            num_items: scenario.num_items,
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
            return Err(StubPirError::OutOfRange(idx));
        }
        let start = idx as usize * BUCKET_BYTES;
        let end = start + BUCKET_BYTES;
        Ok(state.buckets[start..end].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spend_types::{hash_to_bucket, BUCKET_BYTES, ENTRY_BYTES, NUM_BUCKETS};

    #[test]
    fn test_stub_pir_setup_and_query() {
        let engine = StubPirEngine;
        let scenario = YpirScenario {
            num_items: NUM_BUCKETS as u64,
            item_size_bits: (BUCKET_BYTES * 8) as u64,
        };

        let mut db = hashtable_pir::HashTableDb::new();
        let mut nf = [0u8; 32];
        nf[0..4].copy_from_slice(&42u32.to_le_bytes());
        for (i, byte) in nf.iter_mut().enumerate().skip(4) {
            *byte = i as u8;
        }
        db.insert_block(1, [1u8; 32], &[nf]).unwrap();

        let pir_bytes = db.to_pir_bytes();
        let state = engine.setup(&pir_bytes, &scenario).unwrap();

        let bucket_idx = hash_to_bucket(&nf);
        let query = bucket_idx.to_le_bytes();
        let result = engine.answer_query(&state, &query).unwrap();

        assert_eq!(result.len(), BUCKET_BYTES);
        let found = result
            .chunks_exact(ENTRY_BYTES)
            .any(|chunk| chunk[..32] == nf[..]);
        assert!(found, "nullifier not found in returned bucket");
    }

    #[test]
    fn test_stub_pir_invalid_query() {
        let engine = StubPirEngine;
        let scenario = YpirScenario {
            num_items: NUM_BUCKETS as u64,
            item_size_bits: (BUCKET_BYTES * 8) as u64,
        };
        let db_bytes = vec![0u8; spend_types::DB_BYTES];
        let state = engine.setup(&db_bytes, &scenario).unwrap();

        assert!(engine.answer_query(&state, &[0, 1]).is_err());
    }

    #[test]
    fn test_stub_pir_out_of_range() {
        let engine = StubPirEngine;
        let scenario = YpirScenario {
            num_items: NUM_BUCKETS as u64,
            item_size_bits: (BUCKET_BYTES * 8) as u64,
        };
        let db_bytes = vec![0u8; spend_types::DB_BYTES];
        let state = engine.setup(&db_bytes, &scenario).unwrap();

        let idx = (NUM_BUCKETS as u32).to_le_bytes();
        assert!(engine.answer_query(&state, &idx).is_err());
    }
}
