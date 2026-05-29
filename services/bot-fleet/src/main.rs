mod config;
mod errors;
mod fix;
mod kafka;
mod telemetry;
mod time;
mod worker;

use anyhow::{Context, Error, Result};
use config::Config;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = Config::from_env();
    config
        .validate()
        .map_err(Error::msg)
        .context("invalid bot-fleet configuration")?;

    worker::run(config).await
}
