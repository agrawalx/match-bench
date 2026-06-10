//! This module starts the src service.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use anyhow::Result;
use iicpc_bot_fleet::{config, metrics, worker};
use iicpc_logger_rust::loki;

#[tokio::main(flavor = "multi_thread")]
/// main performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn main() -> Result<()> {
    let _loki_guard = loki::init("bot-fleet");
    metrics::start_server();

    worker::run(config::Config::from_env()).await
}
