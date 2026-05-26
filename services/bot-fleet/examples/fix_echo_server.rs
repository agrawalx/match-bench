//! fix_echo_server — a minimal FIX matcher fixture for integration tests.
//!
//! Listens on a TCP port. For every connection:
//!   - Skips the FIX Logon (35=A) message the bot sends on connect.
//!   - For every subsequent NewOrderSingle (35=D) message it reads, replies
//!     immediately with an ExecutionReport (35=8, OrdStatus=0 / New) whose
//!     ClOrdID (tag 11) mirrors the incoming order.
//!
//! Optional `--latency-ms <N>` arg delays the response by N milliseconds —
//! used by the bot-worker integration test to verify that r9 - t1 reflects
//! that delay.
//!
//! Optional `--drop-every <N>` arg drops every Nth NewOrderSingle (no reply
//! sent) — used to verify the watchdog emits timed_out=true for the lost
//! orders.

use std::{env, time::Duration};

use anyhow::{Context, Result};
use iicpc_bot_fleet::fix;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::sleep,
};

/// Knobs (read from env to avoid pulling in clap):
///   BIND       — bind address (default 127.0.0.1:9876)
///   LATENCY_MS — artificial response latency (default 0)
///   DROP_EVERY — drop every Nth NewOrderSingle reply (default 0 = drop none)
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
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
            // We answer only NewOrderSingle (35=D). Logon (35=A), heartbeat,
            // etc. are dropped silently.
            if msg.msg_type != b"D" {
                continue;
            }
            order_count += 1;
            if drop_every > 0 && order_count.is_multiple_of(drop_every) {
                continue; // intentional drop for watchdog tests
            }
            let Some(clord_id_bytes) = msg.clord_id else { continue };
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
