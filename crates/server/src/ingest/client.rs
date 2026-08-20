//! Lightwalletd gRPC client wrapper.
//!
//! Provides [`LwdClient`] for connecting to a lightwalletd instance and
//! fetching compact blocks and chain tip information.

use super::proto::compact_tx_streamer_client::CompactTxStreamerClient;
use super::proto::{
    BlockId, BlockRange, ChainSpec, CompactBlock, Empty, GetSubtreeRootsArg, SubtreeRoot, TreeState,
};
use thiserror::Error;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("failed to connect to any endpoint")]
    NoEndpointAvailable,
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("gRPC status error: {0}")]
    Status(#[from] tonic::Status),
}

pub type Result<T> = std::result::Result<T, ClientError>;

/// Wrapper around the lightwalletd gRPC client.
pub struct LwdClient {
    inner: CompactTxStreamerClient<Channel>,
}

impl LwdClient {
    /// Connect to the first reachable endpoint from the given list.
    /// Automatically enables TLS for `https://` endpoints.
    pub async fn connect(endpoints: &[String]) -> Result<Self> {
        for endpoint in endpoints {
            let result = if endpoint.starts_with("https://") {
                let tls = ClientTlsConfig::new().with_native_roots();
                let ep = Endpoint::from_shared(endpoint.clone())?.tls_config(tls)?;
                CompactTxStreamerClient::connect(ep).await
            } else {
                CompactTxStreamerClient::connect(endpoint.clone()).await
            };

            match result {
                Ok(client) => {
                    tracing::info!(endpoint, "connected to lightwalletd");
                    return Ok(Self { inner: client });
                }
                Err(e) => {
                    tracing::warn!(endpoint, error = %e, "failed to connect, trying next");
                }
            }
        }
        Err(ClientError::NoEndpointAvailable)
    }

    /// Wrap an existing tonic channel (useful for testing with mock servers).
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: CompactTxStreamerClient::new(channel),
        }
    }

    /// Get the height and hash of the chain tip.
    pub async fn get_latest_block(&mut self) -> Result<(u64, Vec<u8>)> {
        let response = self.inner.get_latest_block(ChainSpec {}).await?;
        let block_id = response.into_inner();
        Ok((block_id.height, block_id.hash))
    }

    /// Stream compact blocks in the given range (inclusive).
    pub async fn get_block_range(&mut self, start: u64, end: u64) -> Result<Vec<CompactBlock>> {
        let range = BlockRange {
            start: Some(BlockId {
                height: start,
                hash: vec![],
            }),
            end: Some(BlockId {
                height: end,
                hash: vec![],
            }),
            pool_types: vec![],
        };

        let response = self.inner.get_block_range(range).await?;
        let mut stream = response.into_inner();
        let mut blocks = Vec::new();

        while let Some(block) = stream.next().await {
            blocks.push(block?);
        }

        Ok(blocks)
    }

    /// Get the tree state (frontier) at a specific block height.
    pub async fn get_tree_state(&mut self, height: u64) -> Result<TreeState> {
        let response = self
            .inner
            .get_tree_state(BlockId {
                height,
                hash: vec![],
            })
            .await?;
        Ok(response.into_inner())
    }

    /// Get the latest tree state (at the chain tip).
    pub async fn get_latest_tree_state(&mut self) -> Result<TreeState> {
        let response = self.inner.get_latest_tree_state(Empty {}).await?;
        Ok(response.into_inner())
    }

    /// Get completed subtree (shard) roots for a shielded protocol.
    ///
    /// `protocol`: 0 = Sapling, 1 = Orchard, 2 = Ironwood.
    /// Returns roots starting from `start_index`, up to `max_entries`.
    pub async fn get_subtree_roots(
        &mut self,
        protocol: i32,
        start_index: u32,
        max_entries: u32,
    ) -> Result<Vec<SubtreeRoot>> {
        let response = self
            .inner
            .get_subtree_roots(GetSubtreeRootsArg {
                start_index,
                shielded_protocol: protocol,
                max_entries,
            })
            .await?;
        let mut stream = response.into_inner();
        let mut roots = Vec::new();
        while let Some(root) = stream.next().await {
            roots.push(root?);
        }
        Ok(roots)
    }
}
