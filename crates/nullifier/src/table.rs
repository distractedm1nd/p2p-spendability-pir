#[path = "snapshot.rs"]
mod snapshot;

use crate::{hash_to_bucket, Nullifier, BUCKET_CAPACITY, NUM_BUCKETS};
use std::collections::{BTreeMap, HashMap};
use thiserror::Error;

const EMPTY: Nullifier = [0; 32];

#[derive(Error, Debug)]
pub enum HashTableError {
    #[error("bucket {bucket_idx} overflow")]
    BucketOverflow { bucket_idx: u32 },
    #[error("block hash not found for rollback")]
    BlockNotFound,
    #[error("snapshot error: {0}")]
    Snapshot(String),
}

pub type Result<T> = std::result::Result<T, HashTableError>;

#[derive(Clone)]
struct Bucket {
    entries: [Nullifier; BUCKET_CAPACITY],
}

impl Bucket {
    fn new() -> Self {
        Self {
            entries: [EMPTY; BUCKET_CAPACITY],
        }
    }

    fn insert(&mut self, nf: Nullifier) -> Option<u8> {
        let slot = self.entries.iter().position(|entry| *entry == EMPTY)?;
        self.entries[slot] = nf;
        Some(slot as u8)
    }
}

#[derive(Clone)]
struct BlockRecord {
    block_hash: [u8; 32],
    slots: Vec<(u32, u8)>,
}

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

    pub fn insert_block(
        &mut self,
        height: u64,
        block_hash: [u8; 32],
        nullifiers: &[Nullifier],
    ) -> Result<()> {
        let mut required = HashMap::<u32, usize>::new();
        for nf in nullifiers {
            *required.entry(hash_to_bucket(nf)).or_default() += 1;
        }
        for (bucket_idx, count) in required {
            let free = self.buckets[bucket_idx as usize]
                .entries
                .iter()
                .filter(|entry| **entry == EMPTY)
                .count();
            if count > free {
                return Err(HashTableError::BucketOverflow { bucket_idx });
            }
        }

        let mut slots = Vec::with_capacity(nullifiers.len());
        for nf in nullifiers {
            let bucket_idx = hash_to_bucket(nf);
            let slot = self.buckets[bucket_idx as usize]
                .insert(*nf)
                .expect("bucket capacity preflighted");
            slots.push((bucket_idx, slot));
            self.num_entries += 1;
        }
        self.block_index
            .insert(height, BlockRecord { block_hash, slots });
        self.block_hash_to_height.insert(block_hash, height);
        Ok(())
    }

    pub fn rollback_block(&mut self, block_hash: &[u8; 32]) -> Result<()> {
        let height = self
            .block_hash_to_height
            .remove(block_hash)
            .ok_or(HashTableError::BlockNotFound)?;
        let record = self
            .block_index
            .remove(&height)
            .ok_or(HashTableError::BlockNotFound)?;
        for (bucket, slot) in &record.slots {
            self.buckets[*bucket as usize].entries[*slot as usize] = EMPTY;
        }
        self.num_entries -= record.slots.len();
        Ok(())
    }

    pub fn evict_oldest_block(&mut self) -> Option<u64> {
        let height = *self.block_index.keys().next()?;
        let hash = self.block_index.get(&height)?.block_hash;
        self.rollback_block(&hash).ok()?;
        Some(height)
    }

    pub fn evict_to_target(&mut self) {
        while self.num_entries > crate::TARGET_SIZE {
            if self.evict_oldest_block().is_none() {
                break;
            }
        }
    }

    pub fn contains(&self, nf: &Nullifier) -> bool {
        self.buckets[hash_to_bucket(nf) as usize]
            .entries
            .contains(nf)
    }

    pub fn to_pir_bytes(&self) -> Vec<u8> {
        self.buckets
            .iter()
            .flat_map(|bucket| bucket.entries.iter().flatten().copied())
            .collect()
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
        self.block_index
            .values()
            .next_back()
            .map(|record| record.block_hash)
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

    fn nfs(start: u32, count: u32) -> Vec<Nullifier> {
        (start..start + count)
            .map(|i| {
                let mut nf = [i as u8; 32];
                nf[..4].copy_from_slice(&i.to_le_bytes());
                nf
            })
            .collect()
    }

    #[test]
    fn insert_rollback_and_snapshot() {
        let mut db = HashTableDb::new();
        let values = nfs(1, 100);
        db.insert_block(10, [10; 32], &values).unwrap();
        assert!(values.iter().all(|nf| db.contains(nf)));

        let restored = HashTableDb::from_snapshot(&db.to_snapshot()).unwrap();
        assert!(values.iter().all(|nf| restored.contains(nf)));

        db.rollback_block(&[10; 32]).unwrap();
        assert!(db.is_empty());
    }

    #[test]
    fn pir_bytes_are_raw_nullifiers() {
        let mut db = HashTableDb::new();
        let nf = nfs(42, 1)[0];
        db.insert_block(10, [10; 32], &[nf]).unwrap();
        let bytes = db.to_pir_bytes();
        assert_eq!(bytes.len(), crate::DB_BYTES);
        let bucket = hash_to_bucket(&nf) as usize;
        assert_eq!(&bytes[bucket * crate::BUCKET_BYTES..][..32], &nf);
    }

    #[test]
    fn overflowing_block_is_not_partially_inserted() {
        let values: Vec<Nullifier> = (1..=BUCKET_CAPACITY + 1)
            .map(|i| {
                let mut nf = [i as u8; 32];
                nf[..4].copy_from_slice(&((i * NUM_BUCKETS) as u32).to_le_bytes());
                nf
            })
            .collect();
        let mut db = HashTableDb::new();
        assert!(db.insert_block(10, [10; 32], &values).is_err());
        assert!(db.is_empty());
    }
}
