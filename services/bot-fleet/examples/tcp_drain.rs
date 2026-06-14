//! tcp_drain — a pure TCP sink for measuring a bot-worker pod's max generation
//! rate. It accepts every connection and read()s + discards as fast as the
//! kernel delivers, and NEVER replies.
//!
//! Why: a normal contestant (the fix_echo_server) parses each order and writes
//! an ExecutionReport back; that per-order work slows how fast its read loop
//! drains the socket, shrinking the TCP receive window and back-pressuring the
//! bot's writes (which then time out and the task quits — see worker.rs). A
//! read-and-discard sink keeps the window wide open, so the bot's write_all
//! rarely blocks and `iicpc_bot_orders_sent` reflects the pod's real push
//! ceiling (bounded by generation + kernel/TCP path), not the sink's app logic.
//!
//! There are no acks, so eBPF/telemetry record no round-trip — that's expected;
//! the metric for this experiment is the bot-side `iicpc_bot_orders_sent` rate.
//!
//! Knobs (env):
//!   BIND — bind address (default 0.0.0.0:9898; 9898 is the eBPF-captured port
//!          and the orchestrator's TCP readiness-probe port).

use std::env;

use anyhow::{Context, Result};
use tokio::{io::AsyncReadExt, net::TcpListener};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let bind = env::var("BIND").unwrap_or_else(|_| "0.0.0.0:9898".to_string());
    let listener = TcpListener::bind(&bind)
        .await
        .with_context(|| format!("bind {bind}"))?;
    eprintln!("tcp_drain listening on {bind} (read-and-discard, no replies)");

    loop {
        let (mut stream, _peer) = listener.accept().await.context("accept")?;
        tokio::spawn(async move {
            stream.set_nodelay(true).ok();
            // 64 KiB discard buffer; the bytes are never inspected.
            let mut chunk = [0u8; 65536];
            loop {
                match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => return, // peer closed or errored
                    Ok(_) => {}               // discard and keep draining
                }
            }
        });
    }
}
