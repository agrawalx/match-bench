use anyhow::Result;
use iicpc_bot_fleet::{config, worker};
use iicpc_logger_rust::loki;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let _loki_guard = loki::init("bot-fleet");

    worker::run(config::Config::from_env()).await
}
