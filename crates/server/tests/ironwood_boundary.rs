use spendability_pir_client::witness::reconstruct::reconstruct_witness;
use spendability_pir_server::ingest::extract_commitments;
use spendability_pir_server::ingest::proto::{
    ChainMetadata, CompactBlock, CompactOrchardAction, CompactTx,
};
use witness_pir::CommitmentTreeDb;
use witness_pir::SUBSHARD_ROW_BYTES;

#[test]
fn tag_nine_block_reconstructs_ironwood_witness() {
    let commitment = |value: u64| {
        let mut cmx = [0; 32];
        cmx[..8].copy_from_slice(&value.to_le_bytes());
        CompactOrchardAction {
            nullifier: vec![value as u8; 32],
            cmx: cmx.to_vec(),
            ephemeral_key: vec![value as u8; 32],
            ciphertext: vec![value as u8; 52],
        }
    };
    let block = CompactBlock {
        height: 3_428_143,
        hash: vec![1; 32],
        prev_hash: vec![0; 32],
        vtx: vec![CompactTx {
            actions: vec![commitment(99)],
            ironwood_actions: vec![commitment(1), commitment(2)],
            ..Default::default()
        }],
        chain_metadata: Some(ChainMetadata {
            ironwood_commitment_tree_size: 2,
            ..Default::default()
        }),
        ..Default::default()
    };

    let commitments = extract_commitments(&block).unwrap();
    assert_eq!(commitments.len(), 2);
    let mut tree = CommitmentTreeDb::new();
    tree.append_commitments(block.height, [1; 32], &commitments);
    let expected_root = tree.tree_root();
    let (database, broadcast) = tree.build_pir_db_and_broadcast(block.height);

    let witness =
        reconstruct_witness(1, 0, 0, 1, &database[..SUBSHARD_ROW_BYTES], &broadcast).unwrap();
    assert_eq!(witness.anchor_root, expected_root);
}
