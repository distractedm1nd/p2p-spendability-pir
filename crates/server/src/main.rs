use clap::Parser;
use pir_protocol::YpirScenario;
use pir_protocol::ZcashNetwork;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use nullifier_pir::TARGET_SIZE;
use nullifier_pir::{BUCKET_BYTES, NUM_BUCKETS, YPIR_POLY_LEN as NF_YPIR_POLY_LEN};
use spendability_pir_server::pir::YpirPirEngine as NfPirEngine;
use spendability_pir_server::pir::YpirPirEngine as WitPirEngine;
use witness_pir::{L0_DB_ROWS, SUBSHARD_ROW_BYTES, YPIR_POLY_LEN as WIT_YPIR_POLY_LEN};

use spendability_pir_server::p2p::{P2pPirService, PIR_SERVICE_ID};
use spendability_pir_server::server::{create_app_states, run_with_states_until};
use tokio_util::sync::CancellationToken;
use zakura_network::zakura::{CustomService, ZakuraServiceId};

#[derive(Parser)]
#[command(name = "spend-server", about = "Zcash PIR server")]
struct Cli {
    /// Zcash network to ingest.
    #[arg(long)]
    zcash_network: ZcashNetwork,

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
    #[arg(long, default_value_t = TARGET_SIZE)]
    target_size: usize,

    /// Blocks between snapshots
    #[arg(long, default_value_t = 100)]
    snapshot_interval: u64,

    /// Zakura configuration file. If omitted, Zakura loads its conventional
    /// configuration sources and environment variables.
    #[arg(long)]
    zakura_config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let zakura_config = {
        let config = zakurad::config::ZakuradConfig::load(cli.zakura_config.clone())?;

        rayon::ThreadPoolBuilder::new()
            .num_threads(config.sync.parallel_cpu_threads)
            .thread_name(|index| format!("rayon {index}"))
            .build_global()?;

        config
    };

    let config = spendability_pir_server::server::CombinedConfig {
        zcash_network: cli.zcash_network,
        target_size: cli.target_size,
        snapshot_interval: cli.snapshot_interval,
        data_dir: cli.data_dir,
        lwd_urls: cli.lwd_url,
        listen_addr: cli.listen,
    };

    tracing::info!(
        listen = %config.listen_addr,
        lwd_endpoints = ?config.lwd_urls,
        data_dir = %config.data_dir.display(),
        "starting spend-server",
    );

    let nf_engine = {
        let nf_scenario = YpirScenario {
            num_items: NUM_BUCKETS as u64,
            item_size_bits: (BUCKET_BYTES * 8) as u64,
            poly_len: NF_YPIR_POLY_LEN,
        };
        let engine = NfPirEngine::new(&nf_scenario);
        Arc::new(engine)
    };

    let wit_engine = {
        let wit_scenario = YpirScenario {
            num_items: L0_DB_ROWS as u64,
            item_size_bits: (SUBSHARD_ROW_BYTES * 8) as u64,
            poly_len: WIT_YPIR_POLY_LEN,
        };
        let engine = WitPirEngine::new(&wit_scenario);
        Arc::new(engine)
    };

    let (nf_state, wit_state) = create_app_states(&config, nf_engine, wit_engine);
    let p2p_service = Arc::new(P2pPirService::new(wit_state.clone(), nf_state.clone()));
    let service_id = ZakuraServiceId::new(PIR_SERVICE_ID)?;
    let custom_service = CustomService {
        service: p2p_service,
        provides: vec![service_id],
        seeks: Vec::new(),
    };

    run_combined_with_zakura(config, nf_state, wit_state, zakura_config, custom_service).await?;

    Ok(())
}

async fn run_combined_with_zakura(
    config: spendability_pir_server::server::CombinedConfig,
    nf_state: Arc<spendability_pir_server::nullifier::state::AppState<NfPirEngine>>,
    wit_state: Arc<spendability_pir_server::witness::state::AppState<WitPirEngine>>,
    zakura_config: zakurad::config::ZakuradConfig,
    custom_service: CustomService,
) -> Result<(), Box<dyn std::error::Error>> {
    let shutdown = CancellationToken::new();
    let combined = run_with_states_until(config, nf_state, wit_state, shutdown.clone());
    let zakura =
        zakurad::node::run_with_services(zakura_config, vec![custom_service], shutdown.clone());

    tokio::pin!(combined);
    tokio::pin!(zakura);

    enum FirstExit<C, Z> {
        Signal(std::io::Result<()>),
        Combined(C),
        Zakura(Z),
    }

    let first_exit = tokio::select! {
        signal = tokio::signal::ctrl_c() => FirstExit::Signal(signal),
        result = &mut combined => FirstExit::Combined(result),
        result = &mut zakura => FirstExit::Zakura(result),
    };

    shutdown.cancel();

    match first_exit {
        FirstExit::Signal(signal) => {
            let (combined_result, zakura_result) = tokio::join!(combined, zakura);
            signal?;
            combined_result?;
            map_zakura_result(zakura_result)?;
        }
        FirstExit::Combined(combined_result) => {
            let zakura_result = zakura.await;
            combined_result?;
            map_zakura_result(zakura_result)?;
        }
        FirstExit::Zakura(zakura_result) => {
            let combined_result = combined.await;
            map_zakura_result(zakura_result)?;
            combined_result?;
        }
    }

    Ok(())
}

fn map_zakura_result<E>(result: Result<(), E>) -> Result<(), Box<dyn std::error::Error>>
where
    E: std::fmt::Display,
{
    result.map_err(|error| std::io::Error::other(format!("Zakura node failed: {error}")).into())
}
