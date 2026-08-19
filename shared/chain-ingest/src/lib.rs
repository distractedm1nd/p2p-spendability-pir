//! Shared chain ingest infrastructure for PIR services.
//!
//! Provides the lightwalletd gRPC client ([`LwdClient`]), reorg-detecting
//! protobuf types, dataset identity, and validated sync/follow streams used by
//! both PIR subsystems.

pub mod client;
pub mod dataset;
pub mod ironwood;
pub mod pipeline;
pub mod proto;

pub use client::{ClientError, LwdClient};
pub use dataset::{ensure_ironwood_dataset, DatasetError, DATASET_MARKER_FILENAME};
pub use ironwood::{
    extract_ironwood_nullifiers, nu6_3_activation_height, require_ironwood_tree_state,
    validate_ironwood_tree_state, EndpointError, IronwoodError, NU6_3_MAINNET_ACTIVATION_HEIGHT,
    NU6_3_TESTNET_ACTIVATION_HEIGHT,
};
pub use pipeline::{follow_blocks, sync_blocks, BlockEvent, PipelineError, ValidatedBlock};
