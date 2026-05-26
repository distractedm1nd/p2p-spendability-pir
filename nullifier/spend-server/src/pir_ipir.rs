use ipir_sp::params_for_simplepir;
use ipir_sp::serialize::{deserialize_packing_keys, serialized_packing_keys_len};
use ipir_sp::server::{build_pack_preprocessed_blocks, IPIRServer};
use ipir_sp::YpirSchemeParams;
use spend_types::{PirEngine, YpirScenario, BUCKET_BYTES, IPIR_SETUP_SEED};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IpirError {
    #[error("IPIR setup failed: {0}")]
    Setup(String),
    #[error("IPIR query failed: {0}")]
    Query(String),
}

pub struct IpirServerState {
    rlwe: &'static inspiring::RlweParams,
    server: IPIRServer<u16>,
    pack_preprocessed: Vec<inspiring::QueryPackPreprocessed<'static>>,
    top_key_images: inspiring::TopKeyImages<'static>,
}

pub struct IpirPirEngine {
    rlwe: &'static inspiring::RlweParams,
    ypir: YpirSchemeParams,
    offline_query_polys: Vec<Vec<u64>>,
}

impl IpirPirEngine {
    pub fn new(scenario: &YpirScenario) -> Result<Self, IpirError> {
        let (rlwe, ypir) = params_for_simplepir(scenario.num_items, scenario.item_size_bits)
            .map_err(|e| IpirError::Setup(e.to_string()))?;
        let rlwe = Box::leak(Box::new(rlwe));
        let client = ipir_sp::IPIRClient::new(rlwe, &ypir);
        let offline_query_polys =
            client.generate_public_query_setup_simplepir_from_seed(IPIR_SETUP_SEED);

        Ok(Self {
            rlwe,
            ypir,
            offline_query_polys,
        })
    }

    pub fn rlwe_params(&self) -> &'static inspiring::RlweParams {
        self.rlwe
    }

    pub fn ypir_params(&self) -> &YpirSchemeParams {
        &self.ypir
    }

    pub fn offline_query_polys(&self) -> &[Vec<u64>] {
        &self.offline_query_polys
    }
}

impl PirEngine for IpirPirEngine {
    type ServerState = IpirServerState;
    type Error = IpirError;

    fn setup(
        &self,
        db_bytes: &[u8],
        _scenario: &YpirScenario,
    ) -> Result<IpirServerState, IpirError> {
        let coeffs = RowPtIter::new(
            db_bytes,
            BUCKET_BYTES,
            self.ypir.db_rows,
            self.ypir.db_cols,
            self.ypir.p.trailing_zeros() as usize,
        );
        let server = IPIRServer::<u16>::new_auto_kernel(self.ypir.clone(), coeffs, false, true);
        let offline =
            server.perform_offline_precomputation_simplepir(self.rlwe, &self.offline_query_polys);
        let pack_preprocessed = build_pack_preprocessed_blocks(self.rlwe, &offline.crs_blocks)
            .map_err(|e| IpirError::Setup(e.to_string()))?;
        let top_key_images = inspiring::TopKeyImages::build(self.rlwe);

        Ok(IpirServerState {
            rlwe: self.rlwe,
            server,
            pack_preprocessed,
            top_key_images,
        })
    }

    fn answer_query(
        &self,
        state: &IpirServerState,
        query_bytes: &[u8],
    ) -> Result<Vec<u8>, IpirError> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let packing_keys_len = serialized_packing_keys_len(state.rlwe);
            if query_bytes.len() < packing_keys_len {
                return Err(IpirError::Query(format!(
                    "query must include {packing_keys_len} bytes of packing keys, got {}",
                    query_bytes.len()
                )));
            }

            let packing_keys =
                deserialize_packing_keys(state.rlwe, &query_bytes[..packing_keys_len])
                    .map_err(|e| IpirError::Query(e.to_string()))?;
            let online_query = &query_bytes[packing_keys_len..];
            let (response, _timing) = state
                .server
                .perform_full_online_computation_simplepir_measured(
                    state.rlwe,
                    online_query,
                    &packing_keys,
                    &state.top_key_images,
                    &state.pack_preprocessed,
                )
                .map_err(|e| IpirError::Query(e.to_string()))?;
            Ok(response)
        }))
        .map_err(|e| {
            let msg = e
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| e.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            IpirError::Query(msg.to_string())
        })?
    }
}

struct RowPtIter<'a> {
    data: &'a [u8],
    row_bytes: usize,
    db_cols: usize,
    pt_bits: usize,
    pos: usize,
    total: usize,
}

impl<'a> RowPtIter<'a> {
    fn new(
        data: &'a [u8],
        row_bytes: usize,
        db_rows: usize,
        db_cols: usize,
        pt_bits: usize,
    ) -> Self {
        Self {
            data,
            row_bytes,
            db_cols,
            pt_bits,
            pos: 0,
            total: db_rows * db_cols,
        }
    }
}

impl Iterator for RowPtIter<'_> {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.total {
            return None;
        }

        let row = self.pos / self.db_cols;
        let col = self.pos % self.db_cols;
        self.pos += 1;

        // IPIR-SP consumes row-major plaintext coefficients; source snapshots
        // store row bytes, so bits beyond each row become zero padding.
        let row_start = row * self.row_bytes;
        let row_end = row_start
            .saturating_add(self.row_bytes)
            .min(self.data.len());
        let row_bytes = self.data.get(row_start..row_end).unwrap_or(&[]);
        Some(ipir_sp::bits::read_bits(row_bytes, col * self.pt_bits, self.pt_bits) as u16)
    }
}
