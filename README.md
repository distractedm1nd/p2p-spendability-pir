# Spendability PIR

Private spendability checks for Zcash wallets using single-server Private Information Retrieval (PIR). Two subsystems let a wallet determine note status instantly — privately, with sub-second latency, no sync required.

**Nullifier PIR** — detects spent notes by querying a bucketed hash table of recent Orchard nullifiers via SimplePIR. Prevents stale balances and failed transactions while the wallet is behind.

**Witness PIR** — fetches Merkle authentication paths for newly discovered notes via YPIR, enabling immediate spendability before the local ShardTree is complete.

Both are sync-time accelerators: once the wallet catches up, PIR is unnecessary. If the server is unreachable, the wallet falls back to standard scanning with no loss of funds or correctness.

## Documentation

- [Nullifier PIR](nullifier/README.md) — hash table design, server architecture, client protocol, parameters
- [Witness PIR](witness/README.md) — tree decomposition, broadcast + PIR tiers, witness reconstruction
- [Wallet Integration](docs/pir_wallet_integration.md) — FFI contracts, database schema, feature flags, spendability gates
- [Zakura P2P Node](zakura-server/README.md) — embedded `zakurad`, native request/response protocol, deployment

## Workspace

```
spendability-pir/
├── shared/
│   ├── pir-types/            # PirEngine trait, YpirScenario, ServerPhase, CONFIRMATION_DEPTH
│   └── chain-ingest/         # LwdClient, ChainTracker, sync/follow streams
├── nullifier/
│   ├── spend-types/          # Constants, hash_to_bucket, ChainEvent, SpendabilityMetadata
│   ├── hashtable-pir/        # Bucketed hash table with per-block insert/rollback, snapshots
│   ├── nf-ingest/            # Compact block parser, nullifier extraction
│   ├── spend-server/         # Axum HTTP server, YPIR serving, ArcSwap rebuild
│   └── spend-client/         # SpendClient with is_spent(nf) API
├── witness/
│   ├── witness-types/        # Tree constants, PirWitness, BroadcastData
│   ├── commitment-ingest/    # Orchard note commitment extraction
│   ├── commitment-tree-db/   # In-memory Merkle tree, sub-shard decomposition
│   ├── witness-server/       # Axum HTTP server, broadcast + YPIR serving
│   └── witness-client/       # WitnessClient with get_witness(position) API
├── combined-server/          # Owns shared AppStates; normal YPIR binary hosts HTTP + Zakura
├── zakura-server/            # Native Zakura PIR transport library
└── proto/                    # Shared protobuf definitions
```

## Quick Start

### Build

```bash
cargo build                                    # library crates (no YPIR)
cargo build --features ypir -p spend-server    # nullifier server with YPIR
cargo build --features ypir -p witness-server  # witness server with YPIR
```

### Run (nullifier server)

```bash
cargo run -p spend-server --features ypir --release -- \
    --lwd-url http://localhost:9067 \
    --data-dir ./data \
    --listen 0.0.0.0:8080
```

### Test

```bash
cargo test --workspace                                  # fast (~10s, no YPIR)
cargo test --workspace --all-features --release         # full (~3min, with YPIR)
```

## Performance (release mode)

|                | Nullifier PIR | Witness PIR |
|----------------|---------------|-------------|
| PIR database   | ~56 MB        | ~64 MB      |
| Rebuild time   | ~3s           | ~3.5s       |
| Query latency  | ~65ms         | ~96ms       |
| Upload         | 672 KB        | 605 KB      |
| Download       | 12 KB         | 36 KB       |

## License

MIT
