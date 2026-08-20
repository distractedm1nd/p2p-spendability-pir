# Spendability PIR

Private spendability checks for Zcash wallets using single-server Private Information Retrieval (PIR). Two subsystems let a wallet determine note status instantly — privately, with sub-second latency, no sync required.

**Nullifier PIR** — detects spent notes by querying a bucketed hash table of recent Ironwood nullifiers via SimplePIR.

**Witness PIR** — fetches Merkle authentication paths for newly discovered notes via YPIR, enabling immediate spendability before the local ShardTree is complete.

Both are sync-time accelerators: once the wallet catches up, PIR is unnecessary. If the server is unreachable, the wallet falls back to standard scanning with no loss of funds or correctness.

## Documentation

- [Nullifier PIR](docs/nullifier.md) — hash table design, server architecture, client protocol, parameters
- [Witness PIR](docs/witness.md) — tree decomposition, broadcast + PIR tiers, witness reconstruction
- [Wallet Integration](docs/pir_wallet_integration.md) — FFI contracts, database schema, feature flags, spendability gates
- [Zakura P2P Node](docs/p2p.md) — embedded `zakurad`, native request/response protocol, deployment

## Workspace

```
spendability-pir/
├── crates/
│   ├── protocol/             # Shared client/server wire contracts
│   ├── nullifier/            # Nullifier types, hash table, snapshots
│   ├── witness/              # Witness types, commitment tree, snapshots
│   ├── client/               # Nullifier and witness wallet clients
│   └── server/               # Ingest, HTTP, P2P, and the spend-server binary
└── proto/                    # Protobuf definitions used by server ingest
```

## Quick Start

### Build

```bash
cargo build -p spendability-pir-server --release
```

### Run

```bash
cargo run -p spendability-pir-server --release -- \
    --zcash-network main \
    --lwd-url http://localhost:9067 \
    --data-dir ./data \
    --listen 0.0.0.0:8080
```

### Test

```bash
cargo test --workspace
cargo test --workspace --release
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
