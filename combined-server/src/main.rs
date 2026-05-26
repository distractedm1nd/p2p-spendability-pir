use clap::Parser;
#[cfg(any(feature = "ipir", feature = "ypir"))]
use pir_types::YpirScenario;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[cfg(all(feature = "nullifier", feature = "ipir"))]
use spend_server::pir_ipir::IpirPirEngine as NfPirEngine;
#[cfg(all(feature = "nullifier", not(any(feature = "ipir", feature = "ypir"))))]
use spend_server::pir_stub::StubPirEngine as NfPirEngine;
#[cfg(all(feature = "nullifier", not(feature = "ipir"), feature = "ypir"))]
use spend_server::pir_ypir::YpirPirEngine as NfPirEngine;
#[cfg(feature = "nullifier")]
use spend_types::TARGET_SIZE;
#[cfg(all(feature = "nullifier", any(feature = "ipir", feature = "ypir")))]
use spend_types::{BUCKET_BYTES, NUM_BUCKETS};
#[cfg(all(feature = "witness", feature = "ipir"))]
use witness_server::pir_ipir::IpirPirEngine as WitPirEngine;
#[cfg(all(feature = "witness", not(any(feature = "ipir", feature = "ypir"))))]
use witness_server::pir_stub::StubPirEngine as WitPirEngine;
#[cfg(all(feature = "witness", not(feature = "ipir"), feature = "ypir"))]
use witness_server::pir_ypir::YpirPirEngine as WitPirEngine;
#[cfg(all(feature = "witness", any(feature = "ipir", feature = "ypir")))]
use witness_types::{L0_DB_ROWS, SUBSHARD_ROW_BYTES};

#[derive(Parser)]
#[command(name = "spend-server", about = "Zcash PIR server")]
struct Cli {
    /// Directory for snapshots (creates nullifier/ and witness/ subdirectories)
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// lightwalletd gRPC endpoint(s), can be repeated
    #[arg(long, required = true)]
    lwd_url: Vec<String>,

    /// HTTP listen address
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: SocketAddr,

    /// Target nullifier count before eviction
    #[cfg(feature = "nullifier")]
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

    #[cfg(feature = "nullifier")]
    std::fs::create_dir_all(cli.data_dir.join("nullifier"))?;
    #[cfg(feature = "witness")]
    std::fs::create_dir_all(cli.data_dir.join("witness"))?;

    let config = combined_server::server::CombinedConfig {
        #[cfg(feature = "nullifier")]
        target_size: cli.target_size,
        snapshot_interval: cli.snapshot_interval,
        data_dir: cli.data_dir,
        lwd_urls: cli.lwd_url,
        listen_addr: cli.listen,
    };

    let features: Vec<&str> = vec![
        #[cfg(feature = "nullifier")]
        "nullifier",
        #[cfg(feature = "witness")]
        "witness",
    ];

    tracing::info!(
        listen = %config.listen_addr,
        lwd_endpoints = ?config.lwd_urls,
        subsystems = ?features,
        data_dir = %config.data_dir.display(),
        "starting spend-server",
    );

    #[cfg(feature = "nullifier")]
    let nf_engine = {
        #[cfg(any(feature = "ipir", feature = "ypir"))]
        let nf_scenario = YpirScenario {
            num_items: NUM_BUCKETS as u64,
            item_size_bits: (BUCKET_BYTES * 8) as u64,
        };
        #[cfg(feature = "ipir")]
        let engine = NfPirEngine::new(&nf_scenario)?;
        #[cfg(all(not(feature = "ipir"), feature = "ypir"))]
        let engine = NfPirEngine::new(&nf_scenario);
        #[cfg(not(any(feature = "ipir", feature = "ypir")))]
        let engine = NfPirEngine;
        Arc::new(engine)
    };

    #[cfg(feature = "witness")]
    let wit_engine = {
        #[cfg(any(feature = "ipir", feature = "ypir"))]
        let wit_scenario = YpirScenario {
            num_items: L0_DB_ROWS as u64,
            item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
        };
        #[cfg(feature = "ipir")]
        let engine = WitPirEngine::new(&wit_scenario)?;
        #[cfg(all(not(feature = "ipir"), feature = "ypir"))]
        let engine = WitPirEngine::new(&wit_scenario);
        #[cfg(not(any(feature = "ipir", feature = "ypir")))]
        let engine = WitPirEngine;
        Arc::new(engine)
    };

    combined_server::server::run(
        config,
        #[cfg(feature = "nullifier")]
        nf_engine,
        #[cfg(feature = "witness")]
        wit_engine,
    )
    .await?;

    Ok(())
}
