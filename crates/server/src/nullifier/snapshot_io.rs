use nullifier_pir::HashTableDb;
use std::path::Path;
use thiserror::Error;
use tokio::task;

#[derive(Error, Debug)]
pub enum SnapshotIoError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot decode: {0}")]
    Decode(#[from] nullifier_pir::HashTableError),
}

const SNAPSHOT_FILENAME: &str = "snapshot.bin";
const SNAPSHOT_TMP_FILENAME: &str = "snapshot.bin.tmp";

/// Atomically write a snapshot to disk: serialize -> temp file -> fsync -> rename.
pub async fn save_snapshot(db: &HashTableDb, dir: &Path) -> Result<(), SnapshotIoError> {
    let data = db.to_snapshot();
    let tmp_path = dir.join(SNAPSHOT_TMP_FILENAME);
    let final_path = dir.join(SNAPSHOT_FILENAME);

    let tmp_clone = tmp_path.clone();
    let final_clone = final_path.clone();

    task::spawn_blocking(move || -> Result<(), SnapshotIoError> {
        use std::fs;
        use std::io::Write;

        let mut f = fs::File::create(&tmp_clone)?;
        f.write_all(&data)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp_clone, &final_clone)?;
        Ok(())
    })
    .await
    .expect("spawn_blocking panicked")?;

    Ok(())
}

/// Load a snapshot from disk if it exists.
pub async fn load_snapshot(dir: &Path) -> Result<HashTableDb, SnapshotIoError> {
    let path = dir.join(SNAPSHOT_FILENAME);
    let data = tokio::fs::read(&path).await?;
    let db = HashTableDb::from_snapshot(&data)?;
    Ok(db)
}
