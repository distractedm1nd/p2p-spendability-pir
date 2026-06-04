use clap::{Parser, Subcommand};
use serde::Deserialize;
use tracing_subscriber::EnvFilter;
use witness_client::WitnessClient;
use witness_types::{decompose_position, PirWitness};

#[derive(Parser)]
#[command(name = "witness-cli", about = "Query a witness PIR server")]
struct Cli {
    /// Base URL for witness-server.
    #[arg(long, default_value = "http://127.0.0.1:8081")]
    server: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show the server's current witness PIR metadata.
    Metadata,
    /// Fetch and verify a note commitment witness through the PIR protocol.
    Witness {
        /// Absolute Orchard commitment tree position. Defaults to tree_size - 1.
        #[arg(long)]
        position: Option<u64>,
    },
}

#[derive(Debug, Deserialize)]
struct Metadata {
    anchor_height: u64,
    tree_size: u64,
    window_start_shard: u32,
    window_shard_count: u32,
    populated_shards: u32,
    phase: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();
    let server = cli.server.trim_end_matches('/').to_string();

    match cli.command {
        Command::Metadata => {
            let metadata = fetch_metadata(&server).await?;
            print_metadata(&metadata);
        }
        Command::Witness { position } => {
            let metadata = fetch_metadata(&server).await?;
            let position = position.unwrap_or_else(|| metadata.tree_size.saturating_sub(1));
            let witness = WitnessClient::connect(&server)
                .await?
                .get_witness(position)
                .await?;
            print_witness(&metadata, &witness);
        }
    }

    Ok(())
}

async fn fetch_metadata(server: &str) -> Result<Metadata, Box<dyn std::error::Error>> {
    let metadata = reqwest::Client::new()
        .get(format!("{server}/metadata"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(metadata)
}

fn print_metadata(metadata: &Metadata) {
    println!("phase: {}", metadata.phase);
    println!("anchor_height: {}", metadata.anchor_height);
    println!("tree_size: {}", metadata.tree_size);
    println!(
        "window_shards: {}..{}",
        metadata.window_start_shard,
        metadata.window_start_shard + metadata.window_shard_count
    );
    println!("populated_shards: {}", metadata.populated_shards);
}

fn print_witness(metadata: &Metadata, witness: &PirWitness) {
    let (shard, subshard, leaf) = decompose_position(witness.position);
    println!("witness verified");
    println!("position: {}", witness.position);
    println!("shard: {shard}");
    println!("subshard: {subshard}");
    println!("leaf: {leaf}");
    println!("anchor_height: {}", witness.anchor_height);
    println!("anchor_root: {}", hex::encode(witness.anchor_root));
    println!("siblings: {}", witness.siblings.len());
    println!(
        "server_window_shards: {}..{}",
        metadata.window_start_shard,
        metadata.window_start_shard + metadata.window_shard_count
    );
}
