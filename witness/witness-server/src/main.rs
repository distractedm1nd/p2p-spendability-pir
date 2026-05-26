use clap::Parser;
#[cfg(any(feature = "ipir", feature = "ypir"))]
use pir_types::YpirScenario;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use witness_server::server;
use witness_server::state::{ServerConfig, DEFAULT_WINDOW_SHARD_LIMIT};
#[cfg(any(feature = "ipir", feature = "ypir"))]
use witness_types::{L0_DB_ROWS, SUBSHARD_ROW_BYTES};

#[cfg(feature = "ipir")]
use witness_server::pir_ipir::IpirPirEngine as SelectedPirEngine;
#[cfg(not(any(feature = "ipir", feature = "ypir")))]
use witness_server::pir_stub::StubPirEngine as SelectedPirEngine;
#[cfg(all(not(feature = "ipir"), feature = "ypir"))]
use witness_server::pir_ypir::YpirPirEngine as SelectedPirEngine;

#[derive(Parser)]
#[command(
    name = "witness-server",
    about = "Private note commitment witness server"
)]
struct Cli {
    /// Directory for snapshots
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// lightwalletd gRPC endpoint(s), can be repeated
    #[arg(long, required = true)]
    lwd_url: Vec<String>,

    /// HTTP listen address
    #[arg(long, default_value = "0.0.0.0:8081")]
    listen: SocketAddr,

    /// Blocks between snapshots
    #[arg(long, default_value_t = 100)]
    snapshot_interval: u64,

    /// Number of shards to keep locally during smart sync bootstrap.
    #[arg(long, default_value_t = DEFAULT_WINDOW_SHARD_LIMIT)]
    window_shard_limit: usize,
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
        snapshot_interval: cli.snapshot_interval,
        data_dir: cli.data_dir,
        lwd_urls: cli.lwd_url,
        listen_addr: cli.listen,
        window_shard_limit: cli.window_shard_limit,
    };

    tracing::info!(
        listen = %config.listen_addr,
        lwd_endpoints = ?config.lwd_urls,
        data_dir = %config.data_dir.display(),
        window_shard_limit = config.effective_window_shard_limit(),
        "starting witness-server",
    );

    #[cfg(any(feature = "ipir", feature = "ypir"))]
    let scenario = YpirScenario {
        num_items: L0_DB_ROWS as u64,
        item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
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
