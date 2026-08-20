use pir_protocol::{PirEngine, YpirScenario};
use spiral_rs::params::Params;
use std::io::Cursor;
use thiserror::Error;
use ypir::params::{
    params_for_scenario_simplepir_with_config, DbRowsCols, PtModulusBits, YPIRSPConfig,
};
use ypir::serialize::{FilePtIter, OfflinePrecomputedValues};
use ypir::server::YServer;

#[derive(Error, Debug)]
pub enum YpirError {
    #[error("YPIR setup failed: {0}")]
    Setup(String),
    #[error("YPIR query failed: {0}")]
    Query(String),
}

pub struct YpirServerState {
    server: YServer<'static, u16>,
    offline_vals: OfflinePrecomputedValues<'static>,
}

// Safety: YServer and OfflinePrecomputedValues own their memory buffers exclusively.
// AlignedMemory64 is a heap allocation with no shared aliasing. The 'static lifetime
// references a leaked Params that lives for the process duration.
unsafe impl Send for YpirServerState {}
unsafe impl Sync for YpirServerState {}

pub struct YpirPirEngine {
    params: &'static Params,
    row_bytes: usize,
}

impl YpirPirEngine {
    pub fn new(scenario: &YpirScenario) -> Self {
        let params = Box::leak(Box::new(params_for_scenario_simplepir_with_config(
            scenario.num_items,
            scenario.item_size_bits,
            YPIRSPConfig::for_poly_len(scenario.poly_len),
        )));
        debug_assert_eq!(scenario.item_size_bits % 8, 0);
        Self {
            params,
            row_bytes: scenario.item_size_bits as usize / 8,
        }
    }

    pub fn params(&self) -> &'static Params {
        self.params
    }
}

impl PirEngine for YpirPirEngine {
    type ServerState = YpirServerState;
    type Error = YpirError;

    fn setup(
        &self,
        db_bytes: &[u8],
        _scenario: &YpirScenario,
    ) -> Result<YpirServerState, YpirError> {
        let db_cols = self.params.db_cols_simplepir();
        let pt_bits = self.params.pt_modulus_bits();

        let cursor = Cursor::new(db_bytes);
        let pt_iter = FilePtIter::new(cursor, self.row_bytes, db_cols, pt_bits);

        let server = YServer::<u16>::new(self.params, pt_iter, true, false, true);
        let offline_vals = server.perform_offline_precomputation_simplepir(None, None, None);

        Ok(YpirServerState {
            server,
            offline_vals,
        })
    }

    fn answer_query(
        &self,
        state: &YpirServerState,
        query_bytes: &[u8],
    ) -> Result<Vec<u8>, YpirError> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state
                .server
                .perform_full_online_computation_simplepir(&state.offline_vals, query_bytes)
        }))
        .map_err(|e| {
            let msg = e
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| e.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            YpirError::Query(msg.to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use witness_pir::{L0_DB_ROWS, SUBSHARD_ROW_BYTES, YPIR_POLY_LEN};

    #[test]
    fn witness_uses_4096_degree_layout() {
        let engine = YpirPirEngine::new(&YpirScenario {
            num_items: L0_DB_ROWS as u64,
            item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
            poly_len: YPIR_POLY_LEN,
        });

        assert_eq!(engine.params().poly_len, 4_096);
        assert_eq!(engine.params().db_rows(), 4_096);
        assert_eq!(engine.params().instances, 2);
    }
}
