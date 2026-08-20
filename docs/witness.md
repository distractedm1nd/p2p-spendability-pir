# Ironwood Witness PIR

Private retrieval of note-commitment authentication paths for the Ironwood pool.

```text
lightwalletd
  → validated CompactTx.ironwoodActions
  → independent Ironwood commitment tree
  → subshard PIR rows + broadcast roots
  → 32-level witness
```

## Protocol contract

Ironwood is a distinct pool using the Orchard protocol's existing tree shape and
hash primitives:

- separate tree and positions beginning at NU6.3;
- depth 32;
- `MerkleHashOrchard` Sinsemilla hashing and empty roots;
- 2^16-leaf completed subtrees;
- compact actions from protobuf tag 9 only;
- tree size from `ChainMetadata.ironwoodCommitmentTreeSize` tag 3;
- subtree roots from `ShieldedProtocol.ironwood` value 2;
- frontier from `TreeState.ironwoodTree` tag 7.

The server requires an explicit network, starts at that network's NU6.3
activation height, serves only confirmed blocks, and rejects malformed hashes,
commitments, action fields, tree sizes, ranges, and old/unlabelled snapshots.

This contract follows ZIP 229, ZIP 258, `orchard` 0.15, and the current
`lightwallet-protocol` definitions.

## PIR Params

- Shard: 65,536 leaves
- Subshard: 256 leaves
- Row: 8,192 bytes
- Active window: up to 16 shards / 4,096 rows
- Database: 32 MiB
- YPIR: 0.2.0, polynomial degree 4,096

## Run

```bash
cargo run -p spendability-pir-server --release -- \
  --zcash-network main \
  --lwd-url https://us.zec.stardust.rest:443 \
  --data-dir ./data
```
