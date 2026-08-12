# PIR Nullifier Spendability Checks

## Goal

Enable wallets to **instantly determine if their notes have been spent** — privately, without scanning — by issuing a single encrypted YPIR query against a bucketed hash table of recent Orchard nullifiers. A synced wallet already knows its notes' spendability from the local database; PIR targets wallets that are behind and would otherwise show stale balances until sync completes.

## Problem

When a wallet is offline, notes may be spent on-chain without the wallet's knowledge. Until the wallet scans the blocks containing the spending transactions, two problems arise:

1. **Incorrect balance display**: The wallet shows a higher spendable balance than actually exists on-chain.
2. **Failed transactions**: The user constructs a transaction using a spent note, which is rejected at broadcast because the nullifier already exists on-chain.

Traditional scanning resolves this eventually but can take 30 seconds to several minutes depending on how far behind the wallet is. PIR provides sub-second confirmation of each note's spendability — the wallet can update its balance display and prevent doomed transactions immediately, without waiting for a full scan.

### Spendability lifecycle

1. **Wallet behind** (PIR active): The wallet has unspent Orchard notes in its database. It queries the PIR server for each note's nullifier. If the server's hash table contains the nullifier, the note has been spent on-chain. The wallet upserts a row in `pir_notes` with `is_spent = 1` and excludes it from balance calculations.
2. **Wallet catches up**: Scanning confirms spends by inserting into `orchard_received_note_spends`. The `pir_notes` entry becomes redundant — `spent_notes_clause` UNIONs both sources, so deduplication is automatic.
3. **Steady state** (PIR unnecessary): The wallet is synced. New spends are detected by scanning within seconds. PIR is not queried.

PIR is a **sync-time accelerator**, not a replacement for scanning. The server needs to maintain nullifier data for a window covering recent chain history (currently ~1M nullifiers ≈ 290 days), not the full chain.

## Key data: nullifier volume and table sizing

Orchard is the only pool tracked. Sapling is excluded — its volume is declining (~13K notes/month vs Orchard's ~100K/month) and the pool is expected to sunset. Each Orchard action produces exactly one nullifier, so nullifier volume equals note commitment volume.

At the current mainnet rate of ~3,465 Orchard actions/day (measured April 2026):

- ~1M nullifiers accumulate in **~289 days** (~9.5 months)
- 6-month window: ~623,694 nullifiers
- TARGET_SIZE (1M) covers ~9.5 months of nullifiers

| Period (heights) | Orchard nullifiers | Orchard/day |
|------------------|-------------------|-------------|
| 3,089,134 → 3,123,694 | 129,776 | 4,326 |
| 3,123,694 → 3,158,254 | 144,472 | 4,816 |
| 3,158,254 → 3,192,814 | 84,360 | 2,812 |
| 3,192,814 → 3,227,374 | 83,549 | 2,785 |
| 3,227,374 → 3,261,934 | 103,900 | 3,463 |
| 3,261,934 → 3,296,494 | 77,637 | 2,588 |
| **Total** | **623,694** | **~3,465 avg** |

## Design: Bucketed Hash Table + SimplePIR

The server maintains a hash table of recent Orchard nullifiers, indexed by bucket. Each nullifier maps to a bucket via `hash_to_bucket(nf) = u32_from_le(nf[0..4]) % NUM_BUCKETS`. Nullifiers are cryptographically random (derived from a PRF), so the first 4 bytes give uniform distribution across buckets.

```
lightwalletd ──gRPC──> nf-ingest ──ChainEvent──> HashTableDb ──to_pir_bytes──> YPIR Engine
                                                                                    │
                                                                              ArcSwap (atomic)
                                                                                    │
                                                  Wallet ──HTTP──> spend-server ────┘
                                                    │
                                                    └── SpendClient::is_spent(nf) -> Option<SpendMetadata>
```

The PIR database is the hash table serialized row-major: each row is one bucket, containing up to `BUCKET_CAPACITY` entries of 41 bytes each (32-byte nullifier + 9 bytes of spend metadata). The client queries for the bucket containing its nullifier, decodes the encrypted response, and scans the bucket locally for a match. The server learns which bucket was queried but not which entry within it.

### Why a bucketed hash table

The design needs to store ~1M nullifiers in a structure where each PIR query returns enough data to determine membership. Three properties make a bucketed hash table ideal:

1. **Deterministic bucket mapping**: `hash_to_bucket` maps each nullifier to exactly one bucket. The client knows which row to query without any server interaction.
2. **No false positives**: Unlike a Bloom filter, the bucket contains full 32-byte nullifiers. A match is exact — no probability of false positives.
3. **Simple eviction**: Nullifiers are tracked per-block. When the table exceeds `TARGET_SIZE`, the oldest block's nullifiers are removed by zeroing their bucket slots. No rehashing or compaction needed.

### Why SimplePIR (not YPIR)

The nullifier table has 16,384 rows × 4,592 bytes per row = ~72 MB. SimplePIR is used instead of YPIR because the row size (4,592 bytes = 36,736 bits) exceeds SimplePIR's minimum `item_size_bits` threshold (28,672 bits). SimplePIR avoids the additional complexity and setup cost of YPIR's two-phase protocol while providing the same privacy guarantee for this database geometry.

### Table parameters

| Parameter | Value | Notes |
|-----------|-------|-------|
| `NUM_BUCKETS` | 16,384 (2^14) | Hash table rows = PIR database rows |
| `BUCKET_CAPACITY` | 112 | Max entries per bucket |
| `ENTRY_BYTES` | 41 | 32-byte nullifier + 9 bytes spend metadata |
| `BUCKET_BYTES` | 4,592 | Row size (112 × 41, exceeds SimplePIR minimum) |
| `DB_BYTES` | ~72 MB | Total PIR database (16,384 × 4,592) |
| `TARGET_SIZE` | 1,000,000 | Max nullifiers before oldest-block eviction |
| `CONFIRMATION_DEPTH` | 10 | Blocks before finalization |

**Entry format**: Each 41-byte entry contains the 32-byte nullifier followed by 9 bytes of spend metadata: `spend_height` (u32 LE), `first_output_position` (u32 LE), and `action_count` (u8). The metadata enables the wallet to immediately locate and trial-decrypt change notes from the spending transaction without waiting for block scanning (see [Change Note Discovery](#change-note-discovery) below). A future v2 path will replace the RPC block download with Decryption PIR queries.

**Bucket capacity**: At 1M nullifiers across 16,384 buckets, the average occupancy is ~61 entries per bucket. The capacity of 112 provides ~1.8× headroom. Since nullifiers are cryptographically random, bucket sizes follow a tight binomial distribution — the probability of any bucket exceeding 112 at 1M entries is negligible. If a bucket overflow occurs (bug or extreme volume), the server returns an error for that block and the block's nullifiers are not inserted.

**Load factor**: At TARGET_SIZE, the table is ~55% full (61/112 average). Empty slots are `NullifierEntry::ZERO` (all zero bytes). The zero entry cannot collide with a real nullifier — real Orchard nullifiers are outputs of a PRF keyed by the spending key, and the probability of the all-zero output is negligible (2^-256).

## Client query protocol

**Initialization** (once per session): `SpendClient::connect(url)` calls `GET /params` to fetch the `YpirScenario` JSON (PIR database geometry: 16,384 rows × 36,736 bits per row). The client validates `item_size_bits ≥ 28,672` (SimplePIR minimum) and initializes a `YPIRClient`. Then calls `GET /metadata` to fetch `SpendabilityMetadata` (height range, nullifier count, phase). This is cached for the session.

**Per-nullifier query**: For each nullifier `nf`:

1. **Compute the bucket index**: `bucket_idx = hash_to_bucket(nf)` — first 4 bytes as little-endian u32, mod 16,384.

2. **Generate an encrypted SimplePIR query**: `ypir_client.generate_query_simplepir(bucket_idx)` produces an encrypted query encoding which row to retrieve. The server processes this against all 16,384 rows and cannot determine which row was requested.

3. **Send the query** via `POST /query`. The server runs the SimplePIR online phase, multiplying the query against the database, and returns an encrypted response.

4. **Decode the response** locally to recover the bucket contents (4,592 bytes = 112 × 41-byte entries).

5. **Scan for a match**: Compare each entry's 32-byte nullifier field against `nf`. If found, extract the 9-byte metadata tail as `SpendMetadata { spend_height, first_output_position, action_count }`. The scan is `O(BUCKET_CAPACITY)` = O(112), trivial.

### Batch queries (FFI path)

`SpendClientBlocking::check_nullifiers` checks a batch of nullifiers sequentially, issuing one PIR query per nullifier. Returns `Vec<Option<SpendMetadata>>` parallel to the input — `Some(meta)` for spent nullifiers, `None` otherwise. A progress callback reports completion fraction after each query.

## Confirmation depth

The server ingests blocks up to the chain tip but the PIR database serves data up to `tip - CONFIRMATION_DEPTH` (10 blocks). This matches the wallet's confirmation policy for untrusted transfers (10 blocks) and provides reorg safety — blocks at depth 10+ are extremely unlikely to be orphaned.

The witness PIR server uses the same `CONFIRMATION_DEPTH = 10` constant from `shared/pir-types`.

## Database update strategy

The server follows a sync → follow lifecycle with per-block PIR rebuilds in follow mode.

### Sync mode (startup)

1. Load crash-safe snapshot from disk if available (resume from `latest_height + 1`).
2. Forward-sync: fetch blocks from the resume point (or `tip - 50,000` for fresh start) to the chain tip. Feed nullifiers into the hash table.
3. Backfill: if the table has fewer than `TARGET_SIZE` nullifiers, fetch earlier blocks in 50,000-block batches down to the NU5 activation height (1,687,104). This fills the table with historical nullifiers.
4. Save snapshot after sync completes.
5. Evict down to `TARGET_SIZE`, build PIR database, transition to `ServerPhase::Serving`.

During sync, `GET /health` reports progress (`current_height` / `target_height`) and `POST /query` returns 503.

### Follow mode (steady state)

1. Poll lightwalletd every 2 seconds for new blocks.
2. For each new block: insert nullifiers into the hash table, evict oldest blocks if over `TARGET_SIZE`, rebuild the PIR database (~2.3s), and atomic-swap via `ArcSwap`.
3. For reorgs: roll back orphaned blocks (remove their nullifiers by zeroing bucket slots), insert replacement blocks, rebuild PIR.
4. Save a snapshot every `snapshot_interval` blocks (default: 100).

The PIR rebuild takes ~2.3 seconds at 72 MB, well within the ~75-second block interval. The database is a fixed 72 MB regardless of fill level — empty slots are zero bytes. Rebuild time is constant.

## Server memory model

- **Hash table**: 72 MB (16,384 buckets × 4,592 bytes). Fixed allocation at startup.
- **Block index**: ~1.5 MB at 1M nullifiers across ~290K blocks. BTreeMap keyed by height; each record stores block hash + slot references.
- **Serialized PIR database**: 72 MB (identical to hash table — `to_pir_bytes()` is a direct serialization).
- **PIR engine state**: ~60 MB (SimplePIR precomputed values: offline computation + server state).
- **Total steady-state memory: ~205 MB**

The ArcSwap double-buffers the PIR state during rebuilds: the old state serves queries while the new state is being built. Peak memory during a rebuild is ~265 MB (old PIR state + new PIR state + hash table).

## Eviction

The hash table is a **fixed-capacity sliding window** over recent chain history. When `len() > TARGET_SIZE`, `evict_to_target()` removes the oldest block's nullifiers (by height order) until the count is at or below `TARGET_SIZE`.

Eviction is by **whole blocks**: all nullifiers from a block are removed together. The block index (`BTreeMap<height, BlockRecord>`) tracks which bucket slots each block's nullifiers occupy. Removal zeroes the slot entries — no compaction or rehashing.

At current volume (~3,465 nullifiers/day), `TARGET_SIZE = 1,000,000` covers ~289 days. The server's `SpendabilityMetadata` reports `earliest_height` and `latest_height` so the client knows the coverage window. If a wallet hasn't synced in over ~9 months, its notes' nullifiers may have been evicted — the wallet falls back to normal scanning.

## Crash-safe snapshots

The snapshot system provides crash safety via atomic writes:

1. **Serialize**: The hash table (bucket data + block index) is serialized into a binary format with a `SPENDPIR` magic number (u64), version field (u32), and xxHash64 checksum. The current snapshot version is 2, corresponding to 41-byte entries. Version 1 snapshots (32-byte entries) are rejected on load, triggering a full resync.
2. **Write temp file**: Data is written to `snapshot.bin.tmp`, fsynced.
3. **Atomic rename**: `snapshot.bin.tmp` → `snapshot.bin`. On POSIX systems, rename is atomic — the snapshot is either fully written or absent.

On restart, the server loads the snapshot, validates the version and checksum, and resumes from `latest_height + 1`. A corrupted, missing, or incompatible-version snapshot triggers a full sync from scratch.

Snapshots are saved after initial sync completes and periodically during follow mode (every 100 blocks by default).

## Reorg handling

**New blocks**: Nullifiers are inserted into the hash table with per-block tracking. Each block's slot references are recorded in the block index.

**Reorgs**: `ChainTracker` (from `shared/chain-ingest`) detects when a new block's `prev_hash` doesn't match the stored hash at `height - 1`. On reorg:

1. The follow loop emits a `ChainEvent::Reorg` with orphaned block hashes and replacement blocks.
2. For each orphaned block, `hashtable.rollback_block(hash)` removes its nullifiers by zeroing their bucket slots and removing the block from the index.
3. Replacement blocks are inserted normally.
4. PIR is rebuilt and swapped atomically.

Orphaned nullifiers are removed instantly — the table never serves stale data after a reorg. Because eviction and rollback both zero bucket slots without compacting, slot reuse works correctly: subsequent inserts find free slots by scanning for zero entries.

## Crates

| Crate | Description |
|-------|-------------|
| `spend-types` | Constants (`NUM_BUCKETS`, `BUCKET_CAPACITY`, `ENTRY_BYTES`, `BUCKET_BYTES`, `DB_BYTES`, `TARGET_SIZE`), types (`NullifierEntry`, `NullifierWithMeta`, `SpendMetadata`), `hash_to_bucket`, `ChainEvent`, `SpendabilityMetadata`. Re-exports shared types (`PirEngine`, `YpirScenario`, `ServerPhase`, `CONFIRMATION_DEPTH`) from `shared/pir-types`. |
| `hashtable-pir` | Bucketed hash table storing `NullifierEntry` per slot, with per-block insert/rollback, LRU eviction by height, `to_pir_bytes()` serialization (41-byte entries), and crash-safe binary snapshots (v2) with xxHash64 checksums. |
| `nf-ingest` | Compact block parser (`extract_nullifiers_with_meta` — computes `first_output_position` and `action_count` per transaction from `orchardCommitmentTreeSize` in `ChainMetadata`; Orchard only, ignores Sapling) and sync/follow loops that track `prev_tree_size` across blocks. Depends on `shared/chain-ingest` for `LwdClient` and `ChainTracker`. |
| `spend-server` | Axum HTTP server. Sync/follow lifecycle, per-block PIR rebuilds via `ArcSwap`, async snapshot I/O. `PirEngine` trait allows swapping between stub (tests) and real YPIR (production). Exposes `build_router()` for embedding in a combined server. |
| `spend-client` | `SpendClient` (async) and `SpendClientBlocking` (sync FFI wrapper). `is_spent(nf)` returns `Option<SpendMetadata>` and `check_nullifiers` returns `Vec<Option<SpendMetadata>>` — `Some(meta)` for spent nullifiers (with spend height, output position, action count), `None` otherwise. Handles YPIR SimplePIR query generation, response decoding, and bucket scanning. |

## Wallet integration

Wallet-side PIR integration spans three repositories (`zcash_client_sqlite`, `zcash-swift-wallet-sdk`, `zodl-ios`) controlled by the `spendability-pir` Cargo feature.

### Database integration

`pir_notes` stores the PIR lifecycle for each note. When a nullifier is confirmed as spent on-chain, the row's `is_spent` flag is set to 1. `spent_notes_clause` UNIONs `canonical_note_id` from spent `pir_notes` rows with `orchard_received_note_spends`, so all balance and note-selection queries automatically exclude PIR-detected spends.

### Spendability gate bypass

Three gates normally force `spendableValue` to zero during sync. When `spendability-pir` is enabled, all three are bypassed for Orchard (Sapling retains the original checks):

1. **`is_any_spendable`** (Rust): Unconditionally `true` for Orchard.
2. **`unscanned_tip_exists`** (Rust): Check skipped for Orchard.
3. **`chainTipUpdated`** (Swift): Orchard `spendableValue` preserved when `pirCompleted` flag is set.

### FFI entry point

`zcashlc_check_nullifiers_pir` (C FFI in `spendability.rs`): accepts nullifier bytes and a PIR server URL, connects to the PIR server, checks each nullifier, and returns a JSON `NullifierCheckResult` containing `spent: [Option<SpendMetadata>]` parallel to the input — `null` for unspent, `{ spend_height, first_output_position, action_count }` for spent. The caller (`SDKSynchronizer`) maps spent results to note IDs and upserts them into `pir_notes` with `is_spent = 1`.

### Change note discovery

After nullifier PIR identifies a spent note, the wallet discovers change outputs from the same spending transaction — without waiting for the scanner to reach that block. Because change notes can themselves be spent in subsequent transactions, the wallet follows the full spend chain recursively to determine the actual spendable balance.

For each spent note, the `SpendMetadata` returned by PIR provides the exact location of the transaction's Orchard actions in the commitment tree (`first_output_position`, `action_count`) and the block height (`spend_height`).

**Phase 1 — Canonical notes:** For each note in `orchard_received_notes` that PIR identifies as spent, the wallet:

1. Downloads the single compact block at `spend_height` via lightwalletd RPC
2. Extracts the `action_count` compact actions starting at `first_output_position`
3. Trial-decrypts each action using the account's Orchard FVK (both internal and external scopes)
4. Stores discovered notes in `pir_notes` (with `canonical_note_id = NULL`) at depth 1, with their full note fields (diversifier, rseed, rho, nullifier, cmx) — enough to reconstruct the note for spending once a witness is obtained

**Phase 2 — Recursive chain:** The wallet then iteratively processes provisional notes:

1. Reads all provisional notes with `pir_checked = 0`
2. PIR-checks their nullifiers to determine if they too have been spent
3. For each spent provisional, repeats the block download and trial decryption at `depth + 1`
4. Continues until no unchecked provisionals remain or a safety cap (`maxDepth`, default 20) is reached

Only active leaf nodes — provisional notes where `is_spent = 0` and `discovered_by_scanner = 0` — contribute to the wallet balance in `get_wallet_summary`. Witnessed leaves (`witness_siblings IS NOT NULL`) add to `spendable_value`; unwitnessed leaves add to `value_pending_spendability`.

When the canonical scanner later processes a block and inserts a note into `orchard_received_notes` at the same commitment tree position, the provisional row is reconciled by setting `canonical_note_id` and `discovered_by_scanner = 1` rather than deleted. This preserves the recursive chain — descendants of the reconciled note remain valid. The `is_spent` flag on the same row is picked up by `spent_notes_clause` via `canonical_note_id`, so spend status propagates automatically.

This is the v1 data source path. A future v2 will replace the RPC block download with Decryption PIR queries, removing the need to fetch the full compact block.

### Transaction list placeholders

PIR-detected spends appear as synthetic "detected spend" entries in the transaction list. These are built from a DB query (`zcashlc_get_pir_pending_spends`) that returns only PIR-detected spends not yet confirmed by scanning, so placeholders shrink and disappear automatically as scanning catches up.

## Security properties

- **Privacy**: SimplePIR guarantees the server learns nothing about which bucket was queried, therefore which nullifier was checked. The server sees encrypted query bytes and returns encrypted response bytes.
- **Correctness**: Full 32-byte nullifier comparison within 41-byte entries — no false positives. A match means the exact nullifier exists in the server's hash table and the accompanying spend metadata is authentic (derived from the same block's `ChainMetadata`). False negatives are possible only if the nullifier was evicted (note spent >9 months ago at current volume) or the server hasn't ingested the block yet.
- **Availability**: If the PIR server is down, the wallet falls back to normal scanning — balance updates are delayed until sync completes, but never blocked. The worst case when PIR is unreachable is the pre-PIR status quo. No funds are at risk: a transaction using a spent note is rejected at broadcast, not at construction.
- **Integrity**: The hash table is append-only per block and eviction-only at the oldest end. Reorgs roll back nullifiers atomically. Snapshots use xxHash64 checksums to detect corruption.

## Data sizes summary (~3,465 nullifiers/day)

- **PIR database**: ~72 MB (fixed)
- **Snapshot size**: ~108 MB (bucket data with 41-byte entries) + ~1.5 MB (block index) ≈ ~110 MB

### Performance (release mode, measured April 2026)

| Metric | Value |
|--------|-------|
| PIR rebuild | ~2.3s |
| Round-trip (client) | ~900ms |

## Method (volume analysis)

The measurements use `orchardCommitmentTreeSize` from `ChainMetadata` in compact blocks via lightwalletd (`us.zec.stardust.rest:443`), collected April 2026. Each Orchard action produces one nullifier and one note commitment, so tree size growth equals the number of Orchard actions (= number of nullifiers).

| Metric | Value |
|--------|-------|
| Chain tip | 3,296,494 |
| NU5 activation | 1,687,104 |
| Cumulative Orchard notes | 49,876,639 |
| Cumulative Sapling notes | 73,890,312 |
