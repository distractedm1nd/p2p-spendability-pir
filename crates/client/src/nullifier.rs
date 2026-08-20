use nullifier_pir::{
    hash_to_bucket, Nullifier, SpendabilityMetadata, YpirScenario, ZcashNetwork, BUCKET_BYTES,
    ENTRY_BYTES, NUM_BUCKETS, YPIR_POLY_LEN,
};
use thiserror::Error;
use ypir::client::YPIRClient;
use ypir::params::{params_for_scenario_simplepir_with_config, YPIRSPConfig};
use ypir::serialize::ToBytes;

#[derive(Error, Debug)]
pub enum SpendClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("server unavailable")]
    ServerUnavailable,
    #[error("invalid params: {0}")]
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
    pub async fn connect(url: &str, zcash_network: ZcashNetwork) -> Result<Self> {
        let base_url = url.trim_end_matches('/').to_string();
        let http = reqwest::Client::new();
        let scenario: YpirScenario = http
            .get(format!("{base_url}/params"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let metadata: SpendabilityMetadata = http
            .get(format!("{base_url}/metadata"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        validate_metadata(&metadata, zcash_network)?;
        if scenario.num_items != NUM_BUCKETS as u64
            || scenario.item_size_bits != (BUCKET_BYTES * 8) as u64
            || scenario.poly_len != YPIR_POLY_LEN
        {
            return Err(SpendClientError::InvalidParams(format!(
                "unexpected Ironwood nullifier PIR scenario: {} rows, {} item bits, degree {}",
                scenario.num_items, scenario.item_size_bits, scenario.poly_len
            )));
        }

        let params = params_for_scenario_simplepir_with_config(
            scenario.num_items,
            scenario.item_size_bits,
            YPIRSPConfig::for_poly_len(scenario.poly_len),
        );
        let ypir_client = YPIRClient::new(&params);
        Ok(Self {
            http,
            base_url,
            scenario,
            metadata,
            ypir_client,
        })
    }

    pub async fn is_spent(&self, nf: &Nullifier) -> Result<bool> {
        let (query, seed) = self
            .ypir_client
            .generate_query_simplepir(hash_to_bucket(nf) as usize);
        let response = self
            .http
            .post(format!("{}/query", self.base_url))
            .body(query.to_bytes())
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(SpendClientError::ServerUnavailable);
        }
        let bytes = response
            .error_for_status()
            .map_err(|error| SpendClientError::QueryFailed(error.to_string()))?
            .bytes()
            .await?;
        let row = self.ypir_client.decode_response_simplepir(seed, &bytes);
        Ok(bucket_contains(&row, nf))
    }

    pub async fn refresh_metadata(&mut self) -> Result<()> {
        let metadata = self
            .http
            .get(format!("{}/metadata", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        validate_metadata(&metadata, self.metadata.zcash_network)?;
        self.metadata = metadata;
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

pub struct SpendClientBlocking {
    rt: tokio::runtime::Runtime,
    client: SpendClient,
}

impl SpendClientBlocking {
    pub fn connect(url: &str, zcash_network: ZcashNetwork) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| SpendClientError::QueryFailed(error.to_string()))?;
        let client = rt.block_on(SpendClient::connect(url, zcash_network))?;
        Ok(Self { rt, client })
    }

    pub fn check_nullifiers(
        &self,
        nullifiers: &[Nullifier],
        progress: impl Fn(f64),
    ) -> Result<Vec<bool>> {
        let total = nullifiers.len();
        nullifiers
            .iter()
            .enumerate()
            .map(|(index, nf)| {
                let result = self.rt.block_on(self.client.is_spent(nf))?;
                progress((index + 1) as f64 / total as f64);
                Ok(result)
            })
            .collect()
    }

    pub fn metadata(&self) -> &SpendabilityMetadata {
        self.client.metadata()
    }
}

pub fn bucket_contains(decoded_row: &[u8], nf: &Nullifier) -> bool {
    decoded_row[..decoded_row.len().min(BUCKET_BYTES)]
        .chunks_exact(ENTRY_BYTES)
        .any(|entry| entry == nf)
}

fn validate_metadata(metadata: &SpendabilityMetadata, zcash_network: ZcashNetwork) -> Result<()> {
    if metadata.nullifier_pool == nullifier_pir::IRONWOOD_POOL
        && metadata.dataset_version == nullifier_pir::DATASET_VERSION
        && metadata.zcash_network == zcash_network
    {
        Ok(())
    } else {
        Err(SpendClientError::InvalidParams(
            "server is not the current Ironwood dataset".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_raw_ironwood_bucket() {
        let nf = [42; 32];
        let mut bucket = vec![0; BUCKET_BYTES];
        bucket[3 * ENTRY_BYTES..4 * ENTRY_BYTES].copy_from_slice(&nf);
        assert!(bucket_contains(&bucket, &nf));
        assert!(!bucket_contains(&bucket, &[99; 32]));
    }
}
