use crate::{BlockRecord, Bucket, HashTableDb, HashTableError, Result};
use spend_types::{BUCKET_BYTES, BUCKET_CAPACITY, ENTRY_BYTES, NUM_BUCKETS};
use std::collections::{BTreeMap, HashMap};
use xxhash_rust::xxh64::xxh64;

const SNAPSHOT_MAGIC: u64 = 0x4952_4F4E_5049_5200; // "IRONPIR\0"
const SNAPSHOT_VERSION: u32 = 2;

impl HashTableDb {
    pub fn to_snapshot(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Header: magic (8 bytes) + version (4 bytes)
        buf.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
        buf.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.block_index.len() as u64).to_le_bytes());

        // Block index (ordered by height via BTreeMap iteration)
        for (height, record) in &self.block_index {
            buf.extend_from_slice(&height.to_le_bytes());
            buf.extend_from_slice(&record.block_hash);
            buf.extend_from_slice(&(record.slots.len() as u32).to_le_bytes());
            for (bucket_idx, slot_idx) in &record.slots {
                buf.extend_from_slice(&bucket_idx.to_le_bytes());
                buf.push(*slot_idx);
                buf.push(0); // padding
            }
        }

        // Bucket data (raw 32-byte Ironwood nullifiers)
        for bucket in &self.buckets {
            for entry in &bucket.entries {
                buf.extend_from_slice(entry);
            }
        }

        // Checksum
        let checksum = xxh64(&buf, 0);
        buf.extend_from_slice(&checksum.to_le_bytes());

        buf
    }

    pub fn from_snapshot(data: &[u8]) -> Result<Self> {
        // magic(8) + version(4) + num_blocks(8)
        // + bucket_data(NUM_BUCKETS * BUCKET_BYTES) + checksum(8)
        let min_size = 8 + 4 + 8 + (NUM_BUCKETS * BUCKET_BYTES) + 8;
        if data.len() < min_size {
            return Err(HashTableError::Snapshot("data too short".into()));
        }

        // Verify checksum
        let payload = &data[..data.len() - 8];
        let stored_checksum = u64::from_le_bytes(data[data.len() - 8..].try_into().unwrap());
        let computed_checksum = xxh64(payload, 0);
        if stored_checksum != computed_checksum {
            return Err(HashTableError::Snapshot("checksum mismatch".into()));
        }

        let mut pos = 0;

        let read_u64 = |pos: &mut usize| -> Result<u64> {
            if *pos + 8 > payload.len() {
                return Err(HashTableError::Snapshot("unexpected EOF".into()));
            }
            let val = u64::from_le_bytes(payload[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            Ok(val)
        };

        let read_u32 = |pos: &mut usize| -> Result<u32> {
            if *pos + 4 > payload.len() {
                return Err(HashTableError::Snapshot("unexpected EOF".into()));
            }
            let val = u32::from_le_bytes(payload[*pos..*pos + 4].try_into().unwrap());
            *pos += 4;
            Ok(val)
        };

        let read_bytes_32 = |pos: &mut usize| -> Result<[u8; 32]> {
            if *pos + 32 > payload.len() {
                return Err(HashTableError::Snapshot("unexpected EOF".into()));
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&payload[*pos..*pos + 32]);
            *pos += 32;
            Ok(arr)
        };

        // Header: magic + version
        let magic = read_u64(&mut pos)?;
        if magic != SNAPSHOT_MAGIC {
            return Err(HashTableError::Snapshot("bad magic".into()));
        }
        let version = read_u32(&mut pos)?;
        if version != SNAPSHOT_VERSION {
            return Err(HashTableError::Snapshot(format!(
                "unsupported snapshot version {version} (expected {SNAPSHOT_VERSION})"
            )));
        }

        let num_blocks = read_u64(&mut pos)? as usize;

        // Block index
        let mut block_index = BTreeMap::new();
        let mut block_hash_to_height = HashMap::new();
        let mut num_entries = 0usize;

        for _ in 0..num_blocks {
            let height = read_u64(&mut pos)?;
            let block_hash = read_bytes_32(&mut pos)?;
            let num_slots = read_u32(&mut pos)? as usize;
            num_entries = num_entries
                .checked_add(num_slots)
                .ok_or_else(|| HashTableError::Snapshot("nullifier count overflow".into()))?;

            let mut slots = Vec::with_capacity(num_slots);
            for _ in 0..num_slots {
                let bucket_idx = read_u32(&mut pos)?;
                if pos + 2 > payload.len() {
                    return Err(HashTableError::Snapshot("unexpected EOF".into()));
                }
                let slot_idx = payload[pos];
                pos += 2; // slot_idx + padding
                slots.push((bucket_idx, slot_idx));
            }

            block_hash_to_height.insert(block_hash, height);
            block_index.insert(height, BlockRecord { block_hash, slots });
        }

        // Bucket data (raw 32-byte Ironwood nullifiers)
        let bucket_data_start = pos;
        let bucket_data_end = bucket_data_start + NUM_BUCKETS * BUCKET_BYTES;
        if bucket_data_end > payload.len() {
            return Err(HashTableError::Snapshot("bucket data truncated".into()));
        }

        let mut buckets = Vec::with_capacity(NUM_BUCKETS);
        for i in 0..NUM_BUCKETS {
            let offset = bucket_data_start + i * BUCKET_BYTES;
            let mut bucket = Bucket::new();
            for j in 0..BUCKET_CAPACITY {
                let entry_start = offset + j * ENTRY_BYTES;
                let entry: [u8; ENTRY_BYTES] = payload[entry_start..entry_start + ENTRY_BYTES]
                    .try_into()
                    .unwrap();
                bucket.entries[j] = entry;
            }
            buckets.push(bucket);
        }

        Ok(HashTableDb {
            buckets,
            block_index,
            block_hash_to_height,
            num_entries,
        })
    }
}
