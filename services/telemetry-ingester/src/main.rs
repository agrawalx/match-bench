//! Binary entrypoint.

use anyhow::Result;
use iicpc_logger_rust::loki;
use iicpc_telemetry_ingester::{config::Config, ingester};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let _loki_guard = loki::init("telemetry-ingester");
    ingester::run(Config::from_env()).await
}
