use anyhow::Result;
use iicpc_bot_fleet::{config, metrics, worker};
use iicpc_logger_rust::loki;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let _loki_guard = loki::init("bot-fleet");
    metrics::start_server();

    worker::run(config::Config::from_env()).await
}
