//! This module implements fix echo server behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::{env, time::Duration};

use anyhow::{Context, Result};
use iicpc_bot_fleet::fix;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::sleep,
};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
/// main performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn main() -> Result<()> {
    let bind = env::var("BIND").unwrap_or_else(|_| "127.0.0.1:9876".to_string());
    let latency_ms = env::var("LATENCY_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0u64);
    let drop_every = env::var("DROP_EVERY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0u64);

    let listener = TcpListener::bind(&bind)
        .await
        .with_context(|| format!("bind {bind}"))?;
    eprintln!(
        "fix_echo_server listening on {bind} (latency_ms={latency_ms}, drop_every={drop_every})"
    );

    let latency = Duration::from_millis(latency_ms);

    loop {
        let (stream, peer) = listener.accept().await.context("accept")?;
        eprintln!("connection from {peer}");
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, latency, drop_every).await {
                eprintln!("connection error: {err:?}");
            }
        });
    }
}

/// handle_connection performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn handle_connection(
    mut stream: TcpStream,
    latency: Duration,
    drop_every: u64,
) -> Result<()> {
    stream.set_nodelay(true).ok();

    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk = [0u8; 4096];
    let mut response_seq: u64 = 1;
    let mut order_count: u64 = 0;

    loop {
        let n = stream.read(&mut chunk).await.context("read")?;
        if n == 0 {
            return Ok(()); // peer closed
        }
        buf.extend_from_slice(&chunk[..n]);

        let (messages, consumed) = fix::parse_messages(&buf);
        for msg in messages {
            if msg.msg_type != b"D" {
                continue;
            }
            order_count += 1;
            if drop_every > 0 && order_count.is_multiple_of(drop_every) {
                continue; // intentional drop for watchdog tests
            }
            let Some(clord_id_bytes) = msg.clord_id else {
                continue;
            };
            let clord_id = std::str::from_utf8(clord_id_bytes).unwrap_or("UNKNOWN");

            if !latency.is_zero() {
                sleep(latency).await;
            }
            let frame = fix::execution_report_frame("FIX.4.2", response_seq, clord_id);
            response_seq += 1;
            stream.write_all(&frame).await.context("write ER")?;
        }
        if consumed > 0 {
            buf.drain(..consumed);
        }
    }
}
