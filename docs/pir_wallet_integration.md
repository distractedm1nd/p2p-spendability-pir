# PIR Wallet Integration Architecture

This document describes the wallet-side integration of two PIR
(Private Information Retrieval) subsystems for the Zcash iOS wallet:

- **Nullifier PIR**: ephemeral gate check — detect whether any canonical Orchard
  notes have already been spent. If so, skip PIR and fall back to scanning.
- **Witness PIR**: make canonical Orchard notes spendable at app startup,
  before sync catches up.

PIR is a **startup-only canonical-note accelerator**, not an alternative note-discovery pipeline.

For server-side architecture (tree decomposition, YPIR parameters, ingestion
pipelines), see [note-witness/README.md](../note-witness/README.md).

## Goals

- Allow already-known Orchard notes to be spendable immediately at app startup,
  before sync catches up with the shard tree.
- Keep send-time behavior reliable by refreshing proposal-selected witnesses
  when PIR anchors are stale.

## Non-Goals

This simplified design explicitly does **not** do any of the following:

- Run during wallet recovery.
- Re-run during ongoing sync or in response to newly discovered notes.
- Query for change.
- Discover or maintain provisional notes.
- Recursively follow spend chains (DAG-sync style).
- Create PIR-derived activity or transaction-history entries.
- Let PIR create new balance-bearing notes.
- Persist PIR-detected spend state in the wallet database.

If value has moved into change that the scanner has not discovered yet, the
wallet may **underreport** total Orchard value until scanning catches up. That
is an accepted tradeoff for simplicity and consistency.

## Gate Model

PIR runs once at app startup, on notes already in the wallet from prior
scanning. If the wallet is restoring from seed, PIR is not triggered — there
are no pre-existing notes to accelerate.

Nullifier PIR acts as a binary gate that determines whether witness PIR should
run at all:

1. Read unspent canonical Orchard notes with nullifiers from the wallet DB.
2. Query the spend-server for those nullifiers.
3. If **any** note is reported as spent → skip PIR entirely, fall back to
   standard scanning.
4. If **no** notes are spent → proceed to witness PIR.

The rationale: if any notes have been spent elsewhere (mnemonic reuse, another
wallet), the wallet is already in an incomplete-picture state that PIR cannot
fully resolve without additional wallet complexity. For the time being, we let
the scanner handle it. The primary benefit of PIR is for wallets that have been
idle — you restart after two weeks and can spend immediately without waiting
for sync.

The nullifier check is ephemeral: the result is used to decide the code path
but is not persisted to the database.

## System Overview

The simplified system spans four repositories and three runtime environments:

```text
Server-side
  spend-server      -> nullifier PIR
  witness-server    -> witness PIR

Wallet Rust layer (libzcashlc)
  spendability.rs   -> network-only nullifier PIR FFI
  witness.rs        -> network-only witness PIR FFI
  lib.rs            -> DB-facing zcashlc_* helpers

Wallet DB / logic
  zcash_client_sqlite
    pir_witness_data
    shard_scanned_condition
  zcash_client_backend
    pir_orchard_witnesses

Swift SDK / app
  zcash-swift-wallet-sdk
    SpendabilityBackend
    WitnessBackend
    ZcashRustBackend
    SDKSynchronizer
  zodl-ios
    RootInitialization
    PIRDebugStore
```

## Repository Map

| Repository | Role | Key PIR code |
|---|---|---|
| `spendability-pir` | PIR server and client library | `crates/server/`, `crates/client/`, `crates/nullifier/`, `crates/witness/` |
| `zcash-swift-wallet-sdk` | Rust FFI and Swift orchestration | `rust/src/spendability.rs`, `rust/src/witness.rs`, `rust/src/lib.rs`, `Sources/ZcashLightClientKit/Synchronizer/SDKSynchronizer.swift` |
| `zcash_client_sqlite` | Wallet DB integration | `src/wallet/spendability_pir.rs`, `src/wallet/common.rs`, `src/wallet.rs` |
| `zcash_client_backend` | Transaction construction fallback | `src/data_api/wallet.rs` |
| `zodl-ios` | App-level triggers and diagnostics | `RootInitialization.swift`, `PIRDebugStore.swift` |

## Spendability Model

PIR only operates on **canonical Orchard notes** that the scanner already knows
about and has stored in `orchard_received_notes`.

That means:

- **Nullifier PIR** may detect that a canonical note has already been spent
  (used as a gate check, not persisted).
- **Witness PIR** may attach a witness to a canonical note.
- PIR does **not** create new notes.
- PIR does **not** surface change before the scanner finds it.
- PIR does **not** write synthetic transaction history.

## Nullifier PIR

### Problem Solved

When a canonical Orchard note has already been spent on-chain (e.g. mnemonic
reuse, another wallet), the wallet should not attempt the PIR witness path
because the state is already incomplete. The nullifier gate check detects this
condition early.

### Flow

1. Read unspent canonical Orchard notes with nullifiers from the wallet DB.
2. Query the spend-server for those nullifiers.
3. Return the result to the caller (any-spent flag + details for diagnostics).

No database writes occur. The result is ephemeral.

### FFI Surface

Network-only:

- `zcashlc_check_nullifiers_pir`

DB-facing (read-only):

- `zcashlc_get_unspent_orchard_notes_for_pir`

### Swift Orchestration

`SDKSynchronizer.checkWalletSpendability(pirServerUrl, progress)`:

1. Read canonical Orchard notes via `getUnspentOrchardNotesForPIR()`.
2. Query PIR on a detached task.
3. Return `SpendabilityResult` with `anySpent` flag.

The app calls this once at startup. If `anySpent` is true, witness PIR is not
dispatched.

## Witness PIR

### Problem Solved

Even after the scanner discovers a canonical Orchard note, the note may remain
temporarily unspendable until the local shard tree catches up. Witness PIR lets
the wallet fetch a Merkle path at startup and treat that note as spendable
before sync completes.

### Flow

1. Read canonical Orchard notes that lack witnesses.
2. Query the witness-server for authentication paths.
3. Validate each witness against the locally stored Orchard note.
4. Store the witness in `pir_witness_data`.

### FFI Surface

Network-only:

- `zcashlc_fetch_pir_witnesses`

DB-facing:

- `zcashlc_get_notes_needing_pir_witness`
- `zcashlc_insert_pir_witnesses`
- `zcashlc_get_pir_witness_notes_for_proposal`
- `zcashlc_get_pir_witnessed_notes` (debug-only)

### Swift Orchestration

`SDKSynchronizer.fetchNoteWitnesses(pirServerUrl, progress)`:

1. Read canonical notes via `getNotesNeedingPIRWitness()`.
2. Query the witness-server on a detached task.
3. Insert validated witnesses via `insertPIRWitnesses()`.

The app calls this once at startup, after the nullifier gate passes.

## Send-Time Witness Handling

PIR witness data can become stale between the startup fetch and the moment the
user tries to send. The SDK addresses this in two stages:

### Pre-alignment

Before the first transaction construction attempt, `alignProposalWitnesses`
re-fetches PIR witnesses for the proposal-selected Orchard notes so they share
a single anchor height. This runs only when the wallet is not fully synced and
the proposal carries a `Proposal.PIRWitnessConfig` (set by the caller before calling
`createProposedTransactions`).

### Error-triggered retry

If transaction construction still fails because selected Orchard PIR witnesses
do not agree on a single anchor, the SDK performs one targeted retry:

1. Extract Orchard notes selected by the current proposal.
2. Re-fetch witnesses for just those notes using the proposal's `pirWitnessConfig.serverURL`.
3. Insert the refreshed witnesses.
4. Retry transaction construction once.

This retry is intentionally narrow:

- It only runs for known PIR anchor mismatch failures.
- It only targets proposal-selected Orchard notes.
- It does not loop.
- If the proposal has no `pirWitnessConfig`, refresh cannot run, or the server
  returns no witnesses, the original error is surfaced.

## Database Model

The simplified design uses one table for PIR witness data. Nullifier PIR is
stateless on the wallet side.

### `pir_witness_data`

Stores validated PIR witnesses for canonical Orchard notes.

- Keyed by canonical received-note ID.
- Contains 32 authentication path siblings plus anchor height/root.
- Replaced only when the incoming witness is at least as new as the stored one.
- Cleared by `truncate_to_height` on reorg/rescan.

## Balance and Spendability Effects

When the nullifier gate check passes (no notes spent), witness PIR **upgrades**
Orchard notes from pending-spendability to spendable by attaching witnesses.

PIR must not create new balance-bearing notes.

### Spendability Gates

Without PIR, Orchard spending is blocked by four practical gates:

1. `is_any_spendable`
2. `unscanned_tip_exists`
3. `chainTipUpdated`
4. `shard_scanned_condition`

With the simplified PIR design:

- `is_any_spendable` is bypassed for Orchard when PIR is enabled.
- `unscanned_tip_exists` is bypassed for Orchard when PIR is enabled.
- `chainTipUpdated` no longer zeros Orchard `spendableValue`; only Sapling and
  transparent balances are withheld until the chain tip update runs.
- `shard_scanned_condition` accepts canonical Orchard notes with PIR witness
  data.

Sapling and transparent behavior remain unchanged.

## Transaction Builder Witness Source

The `use_pir_witnesses` flag on `create_proposed_transactions` selects the
Orchard witness source. When `true`, the builder reads witnesses directly from
`pir_witness_data` via `pir_orchard_witnesses()` instead of computing them from
the local ShardTree. The SDK derives this flag from the proposal's
`Proposal.PIRWitnessConfig`: `false` when fully synced or no config is attached, `true`
otherwise.

All selected Orchard PIR witnesses must agree on a single anchor root. If they
do not, transaction construction fails and the Swift layer may run the targeted
refresh described above.

## Concurrency Model

The PIR network calls do not hold SQLite connections. Database reads and writes
go through the same wallet-side serialization path as other `zcashlc_*`
operations.

This keeps the simplified model straightforward:

- PIR reads canonical note metadata.
- PIR writes witness metadata only (no spent-note state).
- Normal scanning remains the only source of newly discovered notes and
  transaction history.

## Error Handling

### Nullifier PIR

- If the spend-server is unreachable, the gate check fails closed: witness PIR
  is skipped, the wallet falls back to scanner-only behavior.

### Witness PIR

- If witness fetch fails, no witness rows are written.
- The wallet falls back to shard-tree-only behavior until the scanner catches
  up.

### Send-Time Refresh

- If proposal-scoped witness refresh cannot run, returns no witnesses, or still
  yields mismatched anchors, the original send failure is surfaced.

In all cases, failure degrades to normal scanning behavior rather than corrupting
wallet state.

## Feature Flag Strategy

One Rust feature is the main integration toggle:

### `spendability-pir`

- Enables PIR nullifier gate check integration.
- Enables Orchard-specific spendability behavior in the wallet summary.
- Enables witness-backed Orchard coin selection before full shard scanning.
- Enables transaction-builder PIR witness path.

## Dependency Graph

```text
spendability-pir/crates/client ---> zcash-swift-wallet-sdk/rust (libzcashlc)
                                              |
                                              v
                                      zcash_client_sqlite
                                        spendability_pir.rs
                                        common.rs
                                              |
                                              v
                                      zcash_client_backend
                                        pir_orchard_witnesses
                                              |
                                              v
                                      zcash-swift-wallet-sdk
                                              |
                                              v
                                      zodl-ios
```

## Summary

The simplified PIR design is deliberately conservative:

- It accelerates Orchard spendability at startup for idle wallets, before sync
  catches up.
- It only operates on notes already in the wallet — it is not triggered during
  recovery or ongoing sync.
- It detects mnemonic reuse / external spending and falls back gracefully.
- It avoids recursive note discovery and synthetic history.
- It keeps no persistent spent-note state — nullifier PIR is a stateless gate.
