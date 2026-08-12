use spend_types::{
    hash_to_bucket, SpendMetadata, SpendabilityMetadata, YpirScenario, BUCKET_BYTES, ENTRY_BYTES,
};
use thiserror::Error;
use ypir::client::YPIRClient;
use ypir::params::params_for_scenario_simplepir;
use ypir::serialize::ToBytes;

#[derive(Error, Debug)]
pub enum SpendClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("server unavailable (503)")]
    ServerUnavailable,
    #[error("invalid params from server: {0}")]
    InvalidParams(String),
    #[error("query failed: {0}")]
    QueryFailed(String),
}

pub type Result<T> = std::result::Result<T, SpendClientError>;

pub struct SpendClient {
    http: reqwest::Client,
    base_url: String,
    scenario: YpirScenario,
    metadata: SpendabilityMetadata,
    ypir_client: YPIRClient,
}

impl SpendClient {
    /// Connect to a spend-server, fetch params and metadata, initialize the PIR client.
    pub async fn connect(url: &str) -> Result<Self> {
        let base_url = url.trim_end_matches('/').to_string();
        let http = reqwest::Client::new();

        let scenario: YpirScenario = http
            .get(format!("{base_url}/params"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let metadata_resp = http.get(format!("{base_url}/metadata")).send().await?;

        if metadata_resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(SpendClientError::ServerUnavailable);
        }

        let metadata: SpendabilityMetadata = metadata_resp.error_for_status()?.json().await?;

        if scenario.item_size_bits < 2048 * 14 {
            return Err(SpendClientError::InvalidParams(format!(
                "item_size_bits {} below SimplePIR minimum 28672",
                scenario.item_size_bits,
            )));
        }

        let ypir_client = {
            let params = params_for_scenario_simplepir(scenario.num_items, scenario.item_size_bits);
            YPIRClient::new(&params)
        };

        tracing::info!(
            base_url,
            earliest = metadata.earliest_height,
            latest = metadata.latest_height,
            nullifiers = metadata.num_nullifiers,
            "connected to spend-server",
        );

        Ok(Self {
            http,
            base_url,
            scenario,
            metadata,
            ypir_client,
        })
    }

    /// Check if a nullifier has been spent, returning spend metadata on match.
    pub async fn is_spent(&self, nf: &[u8; 32]) -> Result<Option<SpendMetadata>> {
        let bucket_idx = hash_to_bucket(nf) as usize;

        let (query_bytes, seed) = {
            let (query, seed) = self.ypir_client.generate_query_simplepir(bucket_idx);
            (query.to_bytes(), seed)
        };

        let resp = self
            .http
            .post(format!("{}/query", self.base_url))
            .body(query_bytes)
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(SpendClientError::ServerUnavailable);
        }

        let response_bytes = resp
            .error_for_status()
            .map_err(|e| SpendClientError::QueryFailed(e.to_string()))?
            .bytes()
            .await?;

        let decoded = self
            .ypir_client
            .decode_response_simplepir(seed, &response_bytes);

        Ok(scan_bucket_for_nf(&decoded, nf))
    }

    /// Re-fetch metadata from the server to get updated heights.
    pub async fn refresh_metadata(&mut self) -> Result<()> {
        let resp = self
            .http
            .get(format!("{}/metadata", self.base_url))
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(SpendClientError::ServerUnavailable);
        }

        self.metadata = resp.error_for_status()?.json().await?;
        Ok(())
    }

    pub fn earliest_height(&self) -> u64 {
        self.metadata.earliest_height
    }

    pub fn latest_height(&self) -> u64 {
        self.metadata.latest_height
    }

    pub fn metadata(&self) -> &SpendabilityMetadata {
        &self.metadata
    }

    pub fn scenario(&self) -> &YpirScenario {
        &self.scenario
    }
}

/// Blocking wrapper around `SpendClient` for use from synchronous FFI contexts.
/// Owns a single-threaded tokio runtime internally.
pub struct SpendClientBlocking {
    rt: tokio::runtime::Runtime,
    client: SpendClient,
}

impl SpendClientBlocking {
    pub fn connect(url: &str) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| SpendClientError::QueryFailed(e.to_string()))?;
        let client = rt.block_on(SpendClient::connect(url))?;
        Ok(Self { rt, client })
    }

    /// Check a batch of nullifiers against the PIR database.
    /// Parallel to the input: `Some(meta)` = spent, `None` = not spent.
    /// Calls `progress` after each query with fraction complete (0.0..=1.0).
    pub fn check_nullifiers(
        &self,
        nullifiers: &[[u8; 32]],
        progress: impl Fn(f64),
    ) -> Result<Vec<Option<SpendMetadata>>> {
        let total = nullifiers.len();
        let mut results = Vec::with_capacity(total);
        for (i, nf) in nullifiers.iter().enumerate() {
            let meta = self.rt.block_on(self.client.is_spent(nf))?;
            results.push(meta);
            progress((i + 1) as f64 / total as f64);
        }
        Ok(results)
    }

    pub fn metadata(&self) -> &SpendabilityMetadata {
        self.client.metadata()
    }

    pub fn earliest_height(&self) -> u64 {
        self.client.earliest_height()
    }

    pub fn latest_height(&self) -> u64 {
        self.client.latest_height()
    }
}

/// Scan the decoded bucket bytes for a nullifier match, returning spend metadata.
pub fn scan_bucket_for_nf(decoded_row: &[u8], nf: &[u8; 32]) -> Option<SpendMetadata> {
    let bucket_data = if decoded_row.len() >= BUCKET_BYTES {
        &decoded_row[..BUCKET_BYTES]
    } else {
        decoded_row
    };

    bucket_data
        .chunks_exact(ENTRY_BYTES)
        .find(|entry| entry[..32] == nf[..])
        .map(|entry| SpendMetadata::from_entry_tail(entry[32..41].try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use spend_types::NullifierEntry;

    fn make_nf(seed: u32) -> [u8; 32] {
        let mut nf = [0u8; 32];
        nf[0..4].copy_from_slice(&seed.to_le_bytes());
        for (i, byte) in nf.iter_mut().enumerate().skip(4) {
            *byte = ((seed >> ((i % 4) * 8)) as u8).wrapping_add(i as u8);
        }
        nf
    }

    fn make_entry(nf: [u8; 32], height: u32, pos: u32, count: u8) -> NullifierEntry {
        NullifierEntry {
            nullifier: nf,
            spend_height: height,
            first_output_position: pos,
            action_count: count,
        }
    }

    fn place_entry(bucket: &mut [u8], slot: usize, entry: &NullifierEntry) {
        let offset = slot * ENTRY_BYTES;
        bucket[offset..offset + ENTRY_BYTES].copy_from_slice(&entry.to_bytes());
    }

    #[test]
    fn test_bucket_scan_found() {
        let nf = make_nf(42);
        let entry = make_entry(nf, 100, 5000, 3);
        let mut bucket = vec![0u8; BUCKET_BYTES];
        place_entry(&mut bucket, 3, &entry);

        let meta = scan_bucket_for_nf(&bucket, &nf).unwrap();
        assert_eq!(meta.spend_height, 100);
        assert_eq!(meta.first_output_position, 5000);
        assert_eq!(meta.action_count, 3);
    }

    #[test]
    fn test_bucket_scan_not_found() {
        let nf = make_nf(42);
        let absent = make_nf(99);
        let entry = make_entry(nf, 100, 5000, 3);
        let mut bucket = vec![0u8; BUCKET_BYTES];
        place_entry(&mut bucket, 3, &entry);

        assert!(scan_bucket_for_nf(&bucket, &absent).is_none());
    }

    #[test]
    fn test_bucket_scan_empty() {
        let nf = make_nf(42);
        let bucket = vec![0u8; BUCKET_BYTES];
        assert!(scan_bucket_for_nf(&bucket, &nf).is_none());
    }

    #[test]
    fn test_bucket_scan_last_slot() {
        let nf = make_nf(42);
        let entry = make_entry(nf, 200, 8000, 1);
        let mut bucket = vec![0u8; BUCKET_BYTES];
        let last_slot = (BUCKET_BYTES / ENTRY_BYTES) - 1;
        place_entry(&mut bucket, last_slot, &entry);

        let meta = scan_bucket_for_nf(&bucket, &nf).unwrap();
        assert_eq!(meta.spend_height, 200);
    }

    #[test]
    fn test_bucket_scan_oversized_row() {
        let nf = make_nf(42);
        let entry = make_entry(nf, 300, 9000, 5);
        let mut row = vec![0u8; BUCKET_BYTES + 1024];
        place_entry(&mut row, 5, &entry);

        assert!(scan_bucket_for_nf(&row, &nf).is_some());
    }
}
