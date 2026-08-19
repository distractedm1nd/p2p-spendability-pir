use pir_types::{ZcashNetwork, DATASET_VERSION, IRONWOOD_POOL};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use thiserror::Error;

pub const DATASET_MARKER_FILENAME: &str = "dataset.json";

#[derive(Debug, Error)]
pub enum DatasetError {
    #[error("dataset I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid dataset marker: {0}")]
    Json(#[from] serde_json::Error),
    #[error("dataset is {actual}; expected {expected}")]
    WrongNetwork {
        actual: ZcashNetwork,
        expected: ZcashNetwork,
    },
    #[error("dataset identifies pool {pool:?} version {version}; expected Ironwood version {DATASET_VERSION}")]
    WrongDataset { pool: String, version: u32 },
    #[error("{artifact} exists without {DATASET_MARKER_FILENAME}; rebuild it as Ironwood")]
    UnlabelledArtifact { artifact: String },
}

#[derive(Deserialize, Serialize)]
struct DatasetMarker {
    zcash_network: ZcashNetwork,
    pool: String,
    dataset_version: u32,
}

pub fn ensure_ironwood_dataset(
    dir: &Path,
    expected_network: ZcashNetwork,
    artifacts: &[&str],
) -> Result<(), DatasetError> {
    fs::create_dir_all(dir)?;
    let marker_path = dir.join(DATASET_MARKER_FILENAME);
    if marker_path.exists() {
        let marker: DatasetMarker = serde_json::from_slice(&fs::read(&marker_path)?)?;
        if marker.pool != IRONWOOD_POOL || marker.dataset_version != DATASET_VERSION {
            return Err(DatasetError::WrongDataset {
                pool: marker.pool,
                version: marker.dataset_version,
            });
        }
        if marker.zcash_network != expected_network {
            return Err(DatasetError::WrongNetwork {
                actual: marker.zcash_network,
                expected: expected_network,
            });
        }
        return Ok(());
    }

    if let Some(artifact) = artifacts.iter().find(|name| dir.join(name).exists()) {
        return Err(DatasetError::UnlabelledArtifact {
            artifact: (*artifact).into(),
        });
    }

    let mut bytes = serde_json::to_vec_pretty(&DatasetMarker {
        zcash_network: expected_network,
        pool: IRONWOOD_POOL.into(),
        dataset_version: DATASET_VERSION,
    })?;
    bytes.push(b'\n');
    let tmp = dir.join("dataset.json.tmp");
    let mut file = File::create(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(tmp, marker_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "spendability-pir-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn marker_binds_artifacts_to_network() {
        let dir = temp_dir("dataset");
        ensure_ironwood_dataset(&dir, ZcashNetwork::Main, &["snapshot.bin"]).unwrap();
        ensure_ironwood_dataset(&dir, ZcashNetwork::Main, &["snapshot.bin"]).unwrap();
        assert!(matches!(
            ensure_ironwood_dataset(&dir, ZcashNetwork::Test, &["snapshot.bin"]),
            Err(DatasetError::WrongNetwork { .. })
        ));
        fs::write(
            dir.join(DATASET_MARKER_FILENAME),
            r#"{"zcash_network":"main","pool":"orchard","dataset_version":2}"#,
        )
        .unwrap();
        assert!(matches!(
            ensure_ironwood_dataset(&dir, ZcashNetwork::Main, &["snapshot.bin"]),
            Err(DatasetError::WrongDataset { .. })
        ));
        fs::remove_dir_all(dir).unwrap();

        let dir = temp_dir("unlabelled");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("snapshot.bin"), []).unwrap();
        assert!(matches!(
            ensure_ironwood_dataset(&dir, ZcashNetwork::Main, &["snapshot.bin"]),
            Err(DatasetError::UnlabelledArtifact { .. })
        ));
        fs::remove_dir_all(dir).unwrap();
    }
}
