mod snapshot;

use spend_types::{
    hash_to_bucket, NullifierEntry, NullifierWithMeta, BUCKET_CAPACITY, NUM_BUCKETS,
};
use std::collections::{BTreeMap, HashMap};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HashTableError {
    #[error(
        "bucket {bucket_idx} overflow: already at capacity {}",
        BUCKET_CAPACITY
    )]
    BucketOverflow { bucket_idx: u32 },
    #[error("block hash not found for rollback")]
    BlockNotFound,
    #[error("snapshot error: {0}")]
    Snapshot(String),
}

pub type Result<T> = std::result::Result<T, HashTableError>;

#[derive(Clone)]
struct Bucket {
    entries: [NullifierEntry; BUCKET_CAPACITY],
    count: u8,
}

impl Bucket {
    fn new() -> Self {
        Self {
            entries: [NullifierEntry::ZERO; BUCKET_CAPACITY],
            count: 0,
        }
    }

    fn remove(&mut self, slot: u8) {
        self.entries[slot as usize] = NullifierEntry::ZERO;
        // Zeroing the slot is sufficient. On insert, scan for the first
        // empty slot (O(BUCKET_CAPACITY) = O(112), fine).
    }

    fn find_free_slot(&self) -> Option<u8> {
        for i in 0..BUCKET_CAPACITY {
            if self.entries[i].is_empty() {
                return Some(i as u8);
            }
        }
        None
    }

    fn contains(&self, nf: &[u8; 32]) -> bool {
        for i in 0..BUCKET_CAPACITY {
            if &self.entries[i].nullifier == nf {
                return true;
            }
        }
        false
    }
}

#[derive(Clone)]
struct BlockRecord {
    block_hash: [u8; 32],
    slots: Vec<(u32, u8)>, // (bucket_idx, slot_idx)
}

/// Bucketed hash table for nullifier storage with per-block tracking,
/// LRU eviction by height, and crash-safe snapshots.
pub struct HashTableDb {
    buckets: Vec<Bucket>,
    block_index: BTreeMap<u64, BlockRecord>,
    block_hash_to_height: HashMap<[u8; 32], u64>,
    num_entries: usize,
}

impl HashTableDb {
    pub fn new() -> Self {
        Self {
            buckets: (0..NUM_BUCKETS).map(|_| Bucket::new()).collect(),
            block_index: BTreeMap::new(),
            block_hash_to_height: HashMap::new(),
            num_entries: 0,
        }
    }

    /// Insert all nullifiers from a single block.
    pub fn insert_block(
        &mut self,
        height: u64,
        block_hash: [u8; 32],
        nullifiers: &[NullifierWithMeta],
    ) -> Result<()> {
        let mut slots = Vec::with_capacity(nullifiers.len());
        let spend_height = height as u32;

        for nwm in nullifiers {
            let bucket_idx = hash_to_bucket(&nwm.nullifier);
            let bucket = &mut self.buckets[bucket_idx as usize];

            let slot = bucket
                .find_free_slot()
                .ok_or(HashTableError::BucketOverflow { bucket_idx })?;
            bucket.entries[slot as usize] = NullifierEntry {
                nullifier: nwm.nullifier,
                spend_height,
                first_output_position: nwm.first_output_position,
                action_count: nwm.action_count,
            };
            if slot >= bucket.count {
                bucket.count = slot + 1;
            }
            slots.push((bucket_idx, slot));
            self.num_entries += 1;
        }

        let record = BlockRecord { block_hash, slots };
        self.block_index.insert(height, record);
        self.block_hash_to_height.insert(block_hash, height);

        Ok(())
    }

    /// Remove all nullifiers inserted by the block with the given hash.
    pub fn rollback_block(&mut self, block_hash: &[u8; 32]) -> Result<()> {
        let height = self
            .block_hash_to_height
            .remove(block_hash)
            .ok_or(HashTableError::BlockNotFound)?;

        let record = self
            .block_index
            .remove(&height)
            .ok_or(HashTableError::BlockNotFound)?;

        for (bucket_idx, slot_idx) in &record.slots {
            self.buckets[*bucket_idx as usize].remove(*slot_idx);
        }
        self.num_entries -= record.slots.len();

        Ok(())
    }

    /// Evict the oldest (lowest-height) block. Returns the evicted height.
    pub fn evict_oldest_block(&mut self) -> Option<u64> {
        let oldest_height = *self.block_index.keys().next()?;
        let record = self.block_index.remove(&oldest_height)?;
        self.block_hash_to_height.remove(&record.block_hash);

        for (bucket_idx, slot_idx) in &record.slots {
            self.buckets[*bucket_idx as usize].remove(*slot_idx);
        }
        self.num_entries -= record.slots.len();

        Some(oldest_height)
    }

    /// Evict oldest blocks until `len() <= TARGET_SIZE`.
    pub fn evict_to_target(&mut self) {
        while self.num_entries > spend_types::TARGET_SIZE {
            if self.evict_oldest_block().is_none() {
                break;
            }
        }
    }

    /// Non-private lookup for testing and server-side use.
    pub fn contains(&self, nf: &[u8; 32]) -> bool {
        let bucket_idx = hash_to_bucket(nf);
        self.buckets[bucket_idx as usize].contains(nf)
    }

    /// Serialize buckets as row-major `NUM_BUCKETS x BUCKET_BYTES` for PIR.
    /// Each entry is serialized as `ENTRY_BYTES` (41) bytes.
    pub fn to_pir_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(spend_types::DB_BYTES);
        for bucket in &self.buckets {
            for entry in &bucket.entries {
                out.extend_from_slice(&entry.to_bytes());
            }
        }
        out
    }

    pub fn len(&self) -> usize {
        self.num_entries
    }

    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
    }

    pub fn earliest_height(&self) -> Option<u64> {
        self.block_index.keys().next().copied()
    }

    pub fn latest_height(&self) -> Option<u64> {
        self.block_index.keys().next_back().copied()
    }

    pub fn latest_block_hash(&self) -> Option<[u8; 32]> {
        self.block_index.values().next_back().map(|r| r.block_hash)
    }

    pub fn num_blocks(&self) -> usize {
        self.block_index.len()
    }
}

impl Default for HashTableDb {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spend_types::{BUCKET_BYTES, DB_BYTES, ENTRY_BYTES, NUM_BUCKETS};

    fn make_nf(seed: u32) -> [u8; 32] {
        let mut nf = [0u8; 32];
        nf[0..4].copy_from_slice(&seed.to_le_bytes());
        for (i, byte) in nf.iter_mut().enumerate().skip(4) {
            *byte = ((seed >> ((i % 4) * 8)) as u8).wrapping_add(i as u8);
        }
        nf
    }

    fn make_hash(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    fn make_nwms(start: u32, count: u32) -> Vec<NullifierWithMeta> {
        (start..start + count)
            .map(|i| NullifierWithMeta {
                nullifier: make_nf(i),
                first_output_position: 1000 + i,
                action_count: 2,
            })
            .collect()
    }

    #[test]
    fn test_insert_and_contains() {
        let mut db = HashTableDb::new();
        let nwms = make_nwms(0, 100);
        db.insert_block(1, make_hash(1), &nwms).unwrap();

        assert_eq!(db.len(), 100);
        for nwm in &nwms {
            assert!(db.contains(&nwm.nullifier), "inserted nf not found");
        }
    }

    #[test]
    fn test_insert_no_false_positive() {
        let mut db = HashTableDb::new();
        let nwms = make_nwms(0, 100);
        db.insert_block(1, make_hash(1), &nwms).unwrap();

        let missing = make_nwms(1000, 100);
        for nwm in &missing {
            assert!(
                !db.contains(&nwm.nullifier),
                "false positive for non-inserted nf"
            );
        }
    }

    #[test]
    fn test_rollback() {
        let mut db = HashTableDb::new();
        let nwms = make_nwms(0, 50);
        let hash = make_hash(1);
        db.insert_block(1, hash, &nwms).unwrap();
        assert_eq!(db.len(), 50);

        db.rollback_block(&hash).unwrap();
        assert_eq!(db.len(), 0);
        for nwm in &nwms {
            assert!(
                !db.contains(&nwm.nullifier),
                "nf still present after rollback"
            );
        }
    }

    #[test]
    fn test_evict_oldest() {
        let mut db = HashTableDb::new();
        let nwms_100 = make_nwms(0, 10);
        let nwms_101 = make_nwms(100, 10);
        let nwms_102 = make_nwms(200, 10);

        db.insert_block(100, make_hash(1), &nwms_100).unwrap();
        db.insert_block(101, make_hash(2), &nwms_101).unwrap();
        db.insert_block(102, make_hash(3), &nwms_102).unwrap();

        assert_eq!(db.len(), 30);
        let evicted = db.evict_oldest_block();
        assert_eq!(evicted, Some(100));
        assert_eq!(db.len(), 20);

        for nwm in &nwms_100 {
            assert!(!db.contains(&nwm.nullifier), "evicted nf still present");
        }
        for nwm in &nwms_101 {
            assert!(db.contains(&nwm.nullifier), "non-evicted nf missing");
        }
        for nwm in &nwms_102 {
            assert!(db.contains(&nwm.nullifier), "non-evicted nf missing");
        }
    }

    #[test]
    fn test_evict_to_target() {
        let mut db = HashTableDb::new();
        let nwms = make_nwms(0, 100);
        db.insert_block(1, make_hash(1), &nwms).unwrap();
        assert_eq!(db.len(), 100);

        db.evict_to_target();
        assert_eq!(db.len(), 100);
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let mut db = HashTableDb::new();
        let nwms_1 = make_nwms(0, 50);
        let nwms_2 = make_nwms(1000, 30);
        db.insert_block(100, make_hash(1), &nwms_1).unwrap();
        db.insert_block(101, make_hash(2), &nwms_2).unwrap();

        let snap = db.to_snapshot();
        let restored = HashTableDb::from_snapshot(&snap).unwrap();

        assert_eq!(restored.len(), db.len());
        assert_eq!(restored.earliest_height(), db.earliest_height());
        assert_eq!(restored.latest_height(), db.latest_height());
        assert_eq!(restored.latest_block_hash(), db.latest_block_hash());
        assert_eq!(restored.num_blocks(), db.num_blocks());

        for nwm in &nwms_1 {
            assert!(restored.contains(&nwm.nullifier));
        }
        for nwm in &nwms_2 {
            assert!(restored.contains(&nwm.nullifier));
        }
    }

    #[test]
    fn test_snapshot_checksum_tamper() {
        let mut db = HashTableDb::new();
        let nwms = make_nwms(0, 10);
        db.insert_block(1, make_hash(1), &nwms).unwrap();

        let mut snap = db.to_snapshot();
        let mid = snap.len() / 2;
        snap[mid] ^= 0xff;

        let result = HashTableDb::from_snapshot(&snap);
        assert!(result.is_err());
    }

    #[test]
    fn test_bucket_overflow() {
        let mut db = HashTableDb::new();
        let mut nwms = Vec::new();
        for i in 1..=(BUCKET_CAPACITY as u32 + 1) {
            let mut nf = [0u8; 32];
            let val = (i * NUM_BUCKETS as u32).to_le_bytes();
            nf[0..4].copy_from_slice(&val);
            nf[4] = i as u8;
            nwms.push(NullifierWithMeta {
                nullifier: nf,
                first_output_position: 0,
                action_count: 2,
            });
        }
        let result = db.insert_block(1, make_hash(1), &nwms);
        assert!(result.is_err());
        match result.unwrap_err() {
            HashTableError::BucketOverflow { .. } => {}
            other => panic!("expected BucketOverflow, got: {other}"),
        }
    }

    #[test]
    fn test_multiple_rollbacks() {
        let mut db = HashTableDb::new();
        let nwms_a = make_nwms(0, 20);
        let nwms_b = make_nwms(100, 20);
        let nwms_c = make_nwms(200, 20);
        let hash_a = make_hash(1);
        let hash_b = make_hash(2);
        let hash_c = make_hash(3);

        db.insert_block(1, hash_a, &nwms_a).unwrap();
        db.insert_block(2, hash_b, &nwms_b).unwrap();
        db.insert_block(3, hash_c, &nwms_c).unwrap();
        assert_eq!(db.len(), 60);

        db.rollback_block(&hash_c).unwrap();
        assert_eq!(db.len(), 40);
        for nwm in &nwms_c {
            assert!(!db.contains(&nwm.nullifier));
        }

        db.rollback_block(&hash_b).unwrap();
        assert_eq!(db.len(), 20);
        for nwm in &nwms_b {
            assert!(!db.contains(&nwm.nullifier));
        }

        for nwm in &nwms_a {
            assert!(db.contains(&nwm.nullifier));
        }
    }

    #[test]
    fn test_empty_block() {
        let mut db = HashTableDb::new();
        db.insert_block(1, make_hash(1), &[]).unwrap();
        assert_eq!(db.len(), 0);
        assert_eq!(db.num_blocks(), 1);
        assert_eq!(db.latest_height(), Some(1));

        let evicted = db.evict_oldest_block();
        assert_eq!(evicted, Some(1));
        assert_eq!(db.num_blocks(), 0);
    }

    #[test]
    fn test_pir_bytes_layout() {
        let mut db = HashTableDb::new();
        let nwms = make_nwms(0, 10);
        db.insert_block(1, make_hash(1), &nwms).unwrap();

        let pir = db.to_pir_bytes();
        assert_eq!(pir.len(), DB_BYTES);

        for nwm in &nwms {
            let bucket_idx = hash_to_bucket(&nwm.nullifier) as usize;
            let bucket_start = bucket_idx * BUCKET_BYTES;
            let bucket_data = &pir[bucket_start..bucket_start + BUCKET_BYTES];

            let found = bucket_data
                .chunks_exact(ENTRY_BYTES)
                .any(|chunk| &chunk[..32] == nwm.nullifier.as_slice());
            assert!(found, "nf not found in expected bucket's PIR bytes");
        }
    }

    #[test]
    fn test_pir_bytes_metadata_preserved() {
        let mut db = HashTableDb::new();
        let nwm = NullifierWithMeta {
            nullifier: make_nf(42),
            first_output_position: 12345,
            action_count: 4,
        };
        db.insert_block(500, make_hash(1), std::slice::from_ref(&nwm))
            .unwrap();

        let pir = db.to_pir_bytes();
        let bucket_idx = hash_to_bucket(&nwm.nullifier) as usize;
        let bucket_start = bucket_idx * BUCKET_BYTES;
        let bucket_data = &pir[bucket_start..bucket_start + BUCKET_BYTES];

        let entry_bytes = bucket_data
            .chunks_exact(ENTRY_BYTES)
            .find(|chunk| &chunk[..32] == nwm.nullifier.as_slice())
            .expect("nf not found");

        let entry = NullifierEntry::from_bytes(entry_bytes.try_into().unwrap());
        assert_eq!(entry.spend_height, 500);
        assert_eq!(entry.first_output_position, 12345);
        assert_eq!(entry.action_count, 4);
    }

    #[test]
    fn test_idempotent_evict() {
        let mut db = HashTableDb::new();
        assert_eq!(db.evict_oldest_block(), None);
        db.evict_to_target();
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn test_insert_after_rollback_reuses_slots() {
        let mut db = HashTableDb::new();
        let nwms_1 = make_nwms(0, 10);
        let hash_1 = make_hash(1);
        db.insert_block(1, hash_1, &nwms_1).unwrap();
        db.rollback_block(&hash_1).unwrap();

        let nwms_2 = make_nwms(0, 10);
        db.insert_block(2, make_hash(2), &nwms_2).unwrap();
        assert_eq!(db.len(), 10);
    }
}
