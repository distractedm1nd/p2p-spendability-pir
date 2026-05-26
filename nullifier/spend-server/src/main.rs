use clap::Parser;
use spend_server::server;
use spend_server::state::ServerConfig;
use spend_types::{YpirScenario, BUCKET_BYTES, CONFIRMATION_DEPTH, NUM_BUCKETS, TARGET_SIZE};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[cfg(feature = "ipir")]
use spend_server::pir_ipir::IpirPirEngine as SelectedPirEngine;
#[cfg(not(any(feature = "ipir", feature = "ypir")))]
use spend_server::pir_stub::StubPirEngine as SelectedPirEngine;
#[cfg(all(not(feature = "ipir"), feature = "ypir"))]
use spend_server::pir_ypir::YpirPirEngine as SelectedPirEngine;

#[derive(Parser)]
#[command(name = "spend-server", about = "Private nullifier spendability server")]
struct Cli {
    /// Directory for snapshots and hint cache
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// lightwalletd gRPC endpoint(s), can be repeated
    #[arg(long, required = true)]
    lwd_url: Vec<String>,

    /// HTTP listen address
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: SocketAddr,

    /// Target nullifier count before eviction
    #[arg(long, default_value_t = TARGET_SIZE)]
    target_size: usize,

    /// Blocks between snapshots
    #[arg(long, default_value_t = 100)]
    snapshot_interval: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    std::fs::create_dir_all(&cli.data_dir)?;

    let config = ServerConfig {
        target_size: cli.target_size,
        confirmation_depth: CONFIRMATION_DEPTH,
        snapshot_interval: cli.snapshot_interval,
        data_dir: cli.data_dir,
        lwd_urls: cli.lwd_url,
        listen_addr: cli.listen,
    };

    tracing::info!(
        listen = %config.listen_addr,
        lwd_endpoints = ?config.lwd_urls,
        target_size = config.target_size,
        data_dir = %config.data_dir.display(),
        "starting spend-server",
    );

    let scenario = YpirScenario {
        num_items: NUM_BUCKETS as u64,
        item_size_bits: (BUCKET_BYTES * 8) as u64,
    };
    #[cfg(feature = "ipir")]
    let engine = Arc::new(SelectedPirEngine::new(&scenario)?);
    #[cfg(all(not(feature = "ipir"), feature = "ypir"))]
    let engine = Arc::new(SelectedPirEngine::new(&scenario));
    #[cfg(not(any(feature = "ipir", feature = "ypir")))]
    let engine = Arc::new(SelectedPirEngine);

    server::run(config, engine).await?;

    Ok(())
}
