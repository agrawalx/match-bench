mod config;
mod errors;
mod fix;
mod kafka;
mod telemetry;
mod time;
mod worker;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    worker::run(config::Config::from_env()).await
}
