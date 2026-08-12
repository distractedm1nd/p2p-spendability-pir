---
name: Decryption PIR Implementation
overview: "Implement decryption PIR end-to-end: server ingest, PIR database, server endpoints, client, and wallet SDK integration -- replacing the current lightwalletd block download path with privacy-preserving PIR queries for change note discovery."
todos:
  - id: phase-1a
    content: "Create decryption-types crate: DecryptionLeaf struct, size constants, serde, tests"
    status: done
  - id: phase-1b
    content: "Extend commitment-ingest parser: extract_decryption_leaves + combined extractor, tests"
    status: done
  - id: phase-1c
    content: "Create decryption-db crate: flat store, append/rollback, PIR DB builder, snapshot, tests"
    status: pending
  - id: phase-2a
    content: "Create decryption-server crate: Axum routes, PirEngine, rebuild_pir, PIR round-trip test"
    status: pending
  - id: phase-2b
    content: "Integrate decryption into combined-server: feature flag, follow loop, routes, health, snapshots"
    status: pending
  - id: phase-2c
    content: "CI updates: add test-decryption job, update workspace test coverage"
    status: pending
  - id: phase-3a
    content: "Create decryption-client crate: YPIR client, connect/query/decode, blocking wrapper"
    status: pending
  - id: phase-3b
    content: "Integration test: e2e mainnet decryption PIR + combined server round-trip test"
    status: pending
  - id: phase-4a
    content: "Decryption client FFI: add to SDK Rust crate, new decryption.rs module"
    status: pending
  - id: phase-4b
    content: "Replace block download path: extract_actions_from_pir + new FFI entry point"
    status: pending
  - id: phase-4c
    content: "Update Swift orchestration: replace discoverChangeAtDepth internals, add DecryptionBackend"
    status: pending
  - id: phase-4d
    content: "SDK integration test: full PIR pipeline end-to-end"
    status: pending
  - id: phase-5
    content: Review, cleanup, documentation updates, remove v1 block-download path
    status: pending
isProject: false
---

# Decryption PIR Implementation

## Context

The change note discovery pipeline currently works as:

1. **Nullifier PIR** detects a note as spent, returning `SpendMetadata` (spend_height, first_output_position, action_count)
2. **Block download** via lightwalletd fetches the full compact block at `spend_height` -- a privacy leak
3. **Trial decryption** of actions at `[first_output_position .. + action_count]` to find wallet-owned change notes

Decryption PIR replaces step 2: instead of downloading the block, the wallet queries a PIR database that stores the compact action data (nf + ephemeralKey + ciphertext) indexed by tree position. Combined with `cmx` from the existing witness PIR, this reconstructs full `CompactAction`s for trial decryption without revealing which positions were queried.

The trial-decrypt core in `[change_discovery.rs](zcash-swift-wallet-sdk/rust/src/change_discovery.rs)` (`try_decrypt_compact_actions`, `discover_notes_both_scopes`) is already source-agnostic and shared between v1/v2. Only the data-source path changes.

## Architecture Decision: Ingest Merging

**Recommendation: merge ingest, keep separate PIR databases.**

The decryption PIR shares the same sub-shard geometry as witness PIR (256 leaves per sub-shard, keyed by global tree position). Both consume the same `CompactOrchardAction` fields from the same compact blocks. The natural design:

- **Shared ingest loop** -- the combined-server's follow loop already iterates blocks once. Extend it to extract decryption leaves alongside commitments from each `CompactOrchardAction`.
- **Separate PIR databases** -- different row sizes (8,192 B/row witness vs 29,696 B/row decryption), different `PirEngine` instances, different YPIR scenarios.
- **Separate server routes** -- `/decrypt/params` and `/decrypt/query` alongside existing `/witness/params` and `/witness/query`.
- **New `decryption` feature flag** in combined-server, parallel to existing `nullifier` and `witness` flags.

The extraction lives in `commitment-ingest` (extended), since it already iterates the same actions for `cmx`. The DB and server are separate crates because they have different storage semantics (flat array vs Merkle tree).

### Crate Layout

```
witness/
  decryption-types/     -- NEW: DecryptionLeaf, row/DB size constants
  decryption-db/        -- NEW: flat position-indexed store, PIR DB builder
  decryption-server/    -- NEW: Axum routes, PirEngine integration
  decryption-client/    -- NEW: YPIR client for decryption PIR
  commitment-ingest/    -- EXTENDED: extract_decryption_leaves()
  ...existing crates unchanged...
```

---

## Phase 1: Decryption PIR Server Ingest

### 1a. `decryption-types` crate

New crate at `[witness/decryption-types/](spendability-pir/witness/decryption-types/)`.

- `DecryptionLeaf` struct: `{ nf: [u8; 32], ephemeral_key: [u8; 32], ciphertext: [u8; 52] }` -- 116 bytes total
- Constants: `DECRYPT_LEAF_BYTES = 116`, `DECRYPT_ROW_BYTES = SUBSHARD_LEAVES * 116` (29,696), `DECRYPT_DB_ROWS = L0_DB_ROWS` (8,192), `DECRYPT_DB_BYTES = DECRYPT_DB_ROWS * DECRYPT_ROW_BYTES` (~232 MB)
- Re-export shared geometry from `witness-types` (`decompose_position`, `physical_row_index`, `SUBSHARD_LEAVES`, etc.)
- Serde serialization for `DecryptionLeaf`
- Tests: geometry consistency, serde round-trip

### 1b. Extend `commitment-ingest` parser

In `[commitment-ingest/src/parser.rs](spendability-pir/witness/commitment-ingest/src/parser.rs)`, add:

```rust
pub fn extract_decryption_leaves(block: &CompactBlock) -> Vec<DecryptionLeaf> {
    // Same iteration as extract_commitments, but collects nf + ephKey + ciphertext
}
```

The existing `extract_commitments` iterates `block.vtx[].actions[]` and collects `cmx`. The new function iterates the same loop but collects `nullifier`, `ephemeral_key`, and `ciphertext`. Both produce `Vec`s with identical length and ordering (1:1 correspondence between cmx[i] and decryption_leaf[i]).

Also add a combined extractor to avoid iterating twice:

```rust
pub fn extract_commitments_and_decryption(block: &CompactBlock) -> (Vec<Hash>, Vec<DecryptionLeaf>)
```

Tests: verify 1:1 correspondence, field correctness, edge cases (short fields, empty blocks, Sapling ignored).

### 1c. `decryption-db` crate

New crate at `[witness/decryption-db/](spendability-pir/witness/decryption-db/)`. Much simpler than `CommitmentTreeDb` -- no Merkle tree, just a flat append-only store.

Key structure:

```rust
pub struct DecryptionDb {
    leaves: Vec<DecryptionLeaf>,  // append-only, indexed by (position - leaf_offset)
    blocks: Vec<BlockRecord>,      // for rollback (reuse witness_types::BlockRecord or similar)
    leaf_offset: u64,              // same window semantics as CommitmentTreeDb
}
```

Methods:

- `append_leaves(height, hash, leaves: &[DecryptionLeaf])` -- extend the store
- `rollback_to(height)` -- remove blocks after height
- `build_pir_db() -> Vec<u8>` -- serialize into row-major bytes (each row = 256 leaves x 116 bytes = 29,696 bytes)
- `subshard_leaves(shard_idx, subshard_idx) -> Vec<DecryptionLeaf>` -- 256 leaves for a sub-shard
- Snapshot save/restore (same pattern as commitment-tree-db)
- `latest_height()`, `latest_block_hash()`, `tree_size()` -- bookkeeping

Tests: append/rollback, PIR DB row layout, window offset, snapshot round-trip.

---

## Phase 2: Decryption PIR Server

### 2a. `decryption-server` crate

New crate at `[witness/decryption-server/](spendability-pir/witness/decryption-server/)`. Follows the same pattern as `[witness-server](spendability-pir/witness/witness-server/src/server.rs)`:

- `AppState<P: PirEngine>` with `ArcSwap<Option<PirState<P>>>`
- Routes: `/health`, `/params`, `/query`
- No `/broadcast` -- decryption PIR doesn't need broadcast data (the sub-shard roots and cap tree come from witness PIR)
- No `/metadata` -- unless useful for diagnostics
- `rebuild_pir(engine, db, scenario)` -- builds PIR from `DecryptionDb::build_pir_db()`
- `run_sync_only(config, engine)` -- initial sync phase (reuse `chain-ingest` LwdClient)
- `sync_range(...)` -- catch-up helper for combined server

`YpirScenario`: `{ num_items: 8192, item_size_bits: 29696 * 8 }` (same row count as witness, different row size).

Tests: PIR round-trip test (analogous to `[witness-server/tests/pir_round_trip.rs](spendability-pir/witness/witness-server/tests/pir_round_trip.rs)`).

### 2b. Integrate into combined-server

In `[combined-server/Cargo.toml](spendability-pir/combined-server/Cargo.toml)`, add:

```toml
[features]
default = ["nullifier", "witness", "decryption"]
decryption = ["dep:decryption-server", "dep:decryption-types", "dep:decryption-db"]
```

In `[combined-server/src/server.rs](spendability-pir/combined-server/src/server.rs)`:

- Add `#[cfg(feature = "decryption")]` blocks parallel to the witness blocks
- In the follow loop, extract decryption leaves alongside commitments (using `extract_commitments_and_decryption` or two calls)
- Append decryption leaves to `DecryptionDb` in the Extend/Reorg arms
- Rebuild decryption PIR alongside witness PIR
- Nest routes under `/decrypt`
- Add decryption to health endpoint, catch-up logic, and snapshot persistence

### 2c. CI updates

Add `test-decryption` job to `[.github/workflows/ci.yml](spendability-pir/.github/workflows/ci.yml)` covering:

- `decryption-types`, `decryption-db`, `decryption-server` unit tests
- PIR round-trip test

---

## Phase 3: Decryption PIR Client

### 3a. `decryption-client` crate

New crate at `[witness/decryption-client/](spendability-pir/witness/decryption-client/)`. Same pattern as `[witness-client](spendability-pir/witness/witness-client/src/lib.rs)`:

```rust
pub struct DecryptionClient {
    http: reqwest::Client,
    base_url: String,
    scenario: YpirScenario,
    ypir_client: YPIRClient,
    window_start_shard: u32,
    window_shard_count: u32,
}
```

Methods:

- `connect(url) -> Result<Self>` -- fetch `/decrypt/params`, initialize YPIR client
- `get_decryption_leaves(position: u64) -> Result<Vec<DecryptionLeaf>>` -- query for a sub-shard, return 256 leaves
- `DecryptionClientBlocking` -- blocking wrapper for FFI

No broadcast data needed -- the decryption client only needs to know the window bounds (from `/decrypt/params` or a metadata endpoint).

The client returns raw `DecryptionLeaf` values. Pairing with `cmx` (from witness PIR) and building `CompactAction`s happens at the caller level.

### 3b. Integration test

New test at `decryption-client/tests/e2e_mainnet.rs`:

- Spin up combined-server with all three subsystems against real mainnet data
- Ingest a range of blocks
- Query decryption PIR for known positions
- Verify returned leaves match the block data
- Verify leaves pair correctly with witness PIR cmx values to form valid CompactActions

### 3c. Combined PIR integration test

New test at `combined-server/tests/decryption_round_trip.rs`:

- Full pipeline: ingest blocks, query nullifier PIR for a known spent note, use metadata to query witness + decryption PIR in parallel, reconstruct CompactActions, trial-decrypt
- This exercises the complete flow that the wallet SDK will use

---

## Phase 4: Wallet SDK / iOS Integration

### 4a. Decryption client FFI

In `[zcash-swift-wallet-sdk/rust/](zcash-swift-wallet-sdk/rust/)`:

- Add decryption-client dependency to Cargo.toml
- New module `decryption.rs` (parallel to `[witness.rs](zcash-swift-wallet-sdk/rust/src/witness.rs)`): FFI functions for decryption PIR queries

### 4b. Replace block download path

In `[change_discovery.rs](zcash-swift-wallet-sdk/rust/src/change_discovery.rs)`:

- New function `extract_actions_from_pir(decryption_leaves, witness_cmxs, first_output_position, action_count) -> Vec<(u64, CompactAction)>`
- This replaces `extract_actions_from_block` -- instead of parsing a compact block, it pairs `DecryptionLeaf` data with `cmx` from witness PIR to build `CompactAction::from_parts(nf, cmx, ephemeral_key, ciphertext)`
- The downstream `discover_notes_both_scopes` call remains identical

### 4c. New FFI entry point

In `[lib.rs](zcash-swift-wallet-sdk/rust/src/lib.rs)`:

- New `zcashlc_discover_change_notes_pir(...)` that:
  1. Queries decryption PIR for the sub-shard
  2. Queries witness PIR for the same sub-shard (in parallel)
  3. Pairs leaves to build CompactActions
  4. Trial-decrypts with the account's FVK
  5. Stores discovered provisional notes
- This replaces the block-download path while reusing the same trial-decrypt and DB-insert logic

### 4d. Update Swift orchestration

In `[SDKSynchronizer.swift](zcash-swift-wallet-sdk/Sources/ZcashLightClientKit/Synchronizer/SDKSynchronizer.swift)`:

- Replace `discoverChangeAtDepth` implementation: instead of `service.blockRange(height...height)`, call the new decryption PIR FFI
- The function signature remains the same -- only the internal data source changes
- Add `DecryptionBackend.swift` (parallel to `SpendabilityBackend.swift` and `WitnessBackend.swift`)

### 4e. Integration test

Update the existing PIR integration test to exercise the full flow:

- Nullifier PIR check -> decryption + witness PIR parallel query -> trial decrypt -> provisional note storage
- Verify the provisional note data matches what the block-download path would have produced

---

## Phase 5: Review, Cleanup, Documentation

- Update `[docs/pir_wallet_integration.md](spendability-pir/docs/pir_wallet_integration.md)`: add decryption PIR architecture, endpoints, data flow
- Update `[nullifier/README.md](spendability-pir/nullifier/README.md)`: change "future v2" references to point at the implementation
- Add `witness/decryption-*/README.md` files for new crates
- Update root `[README.md](spendability-pir/README.md)`: add decryption crates to workspace layout
- Update `[plans/wip/change_note_tracking_review_af2ca4fd.plan.md](spendability-pir/plans/wip/change_note_tracking_review_af2ca4fd.plan.md)`: mark decryption-pir-database, decryption-pir-client, sdk-orchestration todos as complete
- CI: ensure all new crates are covered in test jobs
- Remove v1 block-download code path (or gate behind a feature flag for fallback)
- Final review pass for code quality, error handling, and documentation
