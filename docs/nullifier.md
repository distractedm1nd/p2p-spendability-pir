# Ironwood Nullifier PIR

Private membership checks against recent confirmed Ironwood nullifiers.

```text
lightwalletd
  → validated compact blocks
  → CompactTx.ironwoodActions
  → fixed-capacity hash table
  → SimplePIR
  → wallet boolean: spent / not spent
```

## Dataset contract

- Pool: `ironwood`
- Dataset version: `2`
- Mainnet activation: `3,428,143`
- Testnet activation: `4,134,000`
- Entry: one raw 32-byte nullifier
- Bucket: 112 entries, 3,584 bytes / 28,672 bits
- Database: 16,384 buckets, 56 MiB
- Published height: `chain_tip - 10`

The server requires an explicit Zcash network and validates that lightwalletd
reports that network, target height, and a non-empty Ironwood tree state.
Malformed nullifiers, hashes, ranges, and chain links fail ingestion.

`dataset.json` binds persisted data to the network, pool, and dataset
version. Existing snapshots without that marker are rejected.

## Run

```bash
cargo run -p spendability-pir-server --release -- \
  --zcash-network main \
  --lwd-url https://us.zec.stardust.rest:443 \
  --data-dir ./data
```
