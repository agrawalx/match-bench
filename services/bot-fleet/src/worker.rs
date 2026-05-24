use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use futures::SinkExt;
use iicpc_schemas_rust::{BotProfile, OrderSentEvent, Protocol, ReadySignal, Side, WorkloadSpec};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    task::JoinSet,
    time::{self, Instant},
};
use tokio_tungstenite::{
    connect_async, tungstenite::Message as WsMessage, MaybeTlsStream, WebSocketStream,
};
use tracing::{error, info, warn};

use crate::{
    config::Config,
    fix::{self, OrderFrame},
    kafka::{self, KafkaProducer},
    telemetry::TelemetrySink,
    time::unix_nanos,
};

const MAX_ORDERS_PER_BOT: u32 = 100_000;

/// run starts the bot-fleet worker loop.
/// It consumes workload assignments, executes each one, and exits on Ctrl-C.
pub async fn run(config: Config) -> Result<()> {
    let producer = kafka::producer(&config.kafka_brokers)?;
    let workload_consumer = kafka::consumer(
        &config.kafka_brokers,
        &config.consumer_group,
        &[&config.workload_topic],
    )?;

    info!(
        worker_id = %config.worker_id,
        workload_topic = %config.workload_topic,
        "bot worker started"
    );

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown requested");
                return Ok(());
            }
            payload = kafka::recv_payload(&workload_consumer) => {
                let Some(payload) = payload.context("read workload assignment")? else {
                    continue;
                };
                let spec = match kafka::decode_workload(&payload) {
                    Ok(spec) => spec,
                    Err(err) => {
                        warn!(error = %err, "skipping invalid workload assignment");
                        continue;
                    }
                };

                if let Err(err) = run_workload(&config, &producer, spec).await {
                    error!(error = %err, "workload failed");
                    std::process::exit(1);
                }
            }
        }
    }
}

/// run_workload prepares one assignment, publishes readiness, waits on the
/// session barrier, fires all bots, and flushes telemetry before returning.
async fn run_workload(config: &Config, producer: &KafkaProducer, spec: WorkloadSpec) -> Result<()> {
    validate_spec(config, &spec)?;

    let barrier_group = format!(
        "{}-barrier-{}-{}",
        config.consumer_group, spec.session_id, config.worker_id
    );
    let barrier_consumer = kafka::consumer(
        &config.kafka_brokers,
        &barrier_group,
        &[&config.barrier_topic],
    )?;

    info!(
        session_id = %spec.session_id,
        protocol = ?spec.protocol,
        bot_count = spec.bot_count,
        orders_per_bot = spec.orders_per_bot,
        "preparing workload"
    );

    let connected = connect_bots(&spec).await?;
    let connected_count = connected.len() as u32;

    let ready = ReadySignal {
        session_id: spec.session_id.clone(),
        submission_id: spec.submission_id.clone(),
        worker_id: config.worker_id.clone(),
        worker_index: spec.worker_index,
        worker_count: spec.worker_count,
        bot_count: spec.bot_count,
        connected_count,
        ready_at_unix_nanos: unix_nanos(),
    };
    kafka::publish_json(
        producer,
        &config.ready_topic,
        &kafka::ready_key(&ready),
        &ready,
    )
    .await?;
    info!(session_id = %spec.session_id, connected_count, "published ready signal");

    let barrier = kafka::wait_for_barrier(
        &barrier_consumer,
        &spec.session_id,
        Duration::from_secs(120),
    )
    .await?;
    let start = instant_from_unix_nanos(barrier.target_epoch_unix_nanos);

    let telemetry = TelemetrySink::new(
        producer.clone(),
        config.orders_sent_topic.clone(),
        spec.session_id.clone(),
        config.worker_id.clone(),
        config.telemetry_channel_capacity,
        config.telemetry_flush_interval,
        config.telemetry_batch_size,
    );

    let result = fire_workload(config, spec, connected, start, telemetry.clone()).await;
    telemetry.close().await?;
    result
}

/// validate_spec enforces local worker limits and rejects identifiers that
/// would corrupt generated FIX or JSON payloads.
fn validate_spec(config: &Config, spec: &WorkloadSpec) -> Result<()> {
    if spec.bot_count == 0 {
        return Err(crate::errors::BotFleetError::ValidationError(
            "bot_count must be greater than zero".into(),
        )
        .into());
    }
    if spec.bot_count as usize > config.max_bots_per_worker {
        return Err(crate::errors::BotFleetError::ValidationError(format!(
            "bot_count {} exceeds MAX_BOTS_PER_WORKER {}",
            spec.bot_count, config.max_bots_per_worker
        ))
        .into());
    }
    if spec.orders_per_bot == 0 {
        return Err(crate::errors::BotFleetError::ValidationError(
            "orders_per_bot must be greater than zero".into(),
        )
        .into());
    }
    if spec.orders_per_bot > MAX_ORDERS_PER_BOT {
        return Err(crate::errors::BotFleetError::ValidationError(format!(
            "orders_per_bot {} exceeds MAX_ORDERS_PER_BOT {}",
            spec.orders_per_bot, MAX_ORDERS_PER_BOT
        ))
        .into());
    }
    if spec.worker_count == 0 || spec.worker_index >= spec.worker_count {
        return Err(crate::errors::BotFleetError::ValidationError(
            "invalid worker index/count".into(),
        )
        .into());
    }
    validate_identifier("session_id", &spec.session_id)?;
    validate_identifier("fix_version", &spec.fix_version)?;
    Ok(())
}

/// validate_identifier allows only simple stable tokens in wire-format fields.
fn validate_identifier(name: &str, value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));

    if !valid {
        return Err(crate::errors::BotFleetError::ValidationError(format!(
            "{name} contains unsupported characters"
        ))
        .into());
    }

    Ok(())
}

/// connect_bots opens all target connections and precomputes order payloads
/// before the barrier so the live phase stays allocation-light.
async fn connect_bots(spec: &WorkloadSpec) -> Result<Vec<ConnectedBot>> {
    let addr: SocketAddr = format!("{}:{}", spec.target_host, spec.target_port)
        .parse()
        .context("parse target socket address")?;
    let mut set = JoinSet::new();
    let shared_spec = Arc::new(spec.clone());

    for local_bot in 0..spec.bot_count {
        let spec = Arc::clone(&shared_spec);
        set.spawn(async move {
            let bot_id = global_bot_id(&spec, local_bot);
            let client = TargetClient::connect(&spec, addr).await?;
            let frames = precompute_orders(&spec, bot_id);
            Ok::<_, anyhow::Error>(ConnectedBot {
                bot_id,
                client,
                frames,
            })
        });
    }

    let mut bots = Vec::with_capacity(spec.bot_count as usize);
    while let Some(result) = set.join_next().await {
        bots.push(result.context("join bot connect task")??);
    }
    Ok(bots)
}

/// fire_workload waits until the barrier instant and then runs every connected
/// bot as an independent Tokio task.
async fn fire_workload(
    config: &Config,
    spec: WorkloadSpec,
    bots: Vec<ConnectedBot>,
    start: Instant,
    telemetry: TelemetrySink,
) -> Result<()> {
    let mut set = JoinSet::new();
    let write_timeout = Duration::from_millis(spec.write_timeout_ms);

    for bot in bots {
        let session_id = spec.session_id.clone();
        let submission_id = spec.submission_id.clone();
        let worker_id = config.worker_id.clone();
        let telemetry = telemetry.clone();
        set.spawn(async move {
            time::sleep_until(start).await;
            bot.send_all(
                &session_id,
                &submission_id,
                &worker_id,
                write_timeout,
                telemetry,
            )
            .await
        });
    }

    let mut sent = 0u64;
    while let Some(result) = set.join_next().await {
        sent += result.context("join bot send task")??;
    }
    info!(session_id = %spec.session_id, sent, "workload completed");
    Ok(())
}

/// precompute_orders deterministically renders every order frame for one bot.
fn precompute_orders(spec: &WorkloadSpec, bot_id: u64) -> Vec<OrderFrame> {
    let mut rng = SmallRng::seed_from_u64(spec.global_seed ^ bot_id);
    let mut frames = Vec::with_capacity(spec.orders_per_bot as usize);
    let profile = profile_for_bot(spec, bot_id);

    for seq in 1..=spec.orders_per_bot {
        let (price, qty, side) = order_shape(profile, seq, &mut rng);
        frames.push(fix::order_frame(
            &spec.fix_version,
            &spec.session_id,
            bot_id,
            u64::from(seq),
            price,
            qty,
            side,
        ));
    }

    frames
}

/// profile_for_bot maps a global bot id onto the configured weighted profile mix.
fn profile_for_bot(spec: &WorkloadSpec, bot_id: u64) -> BotProfile {
    let total: u32 = spec.profile_mix.iter().map(|entry| entry.weight).sum();
    if total == 0 {
        return BotProfile::MarketMaker;
    }

    let mut cursor = (bot_id % u64::from(total)) as u32;
    for entry in &spec.profile_mix {
        if cursor < entry.weight {
            return entry.profile;
        }
        cursor -= entry.weight;
    }

    BotProfile::MarketMaker
}

/// order_shape produces deterministic price, quantity, and side values for the
/// selected profile and sequence number.
fn order_shape(profile: BotProfile, seq: u32, rng: &mut SmallRng) -> (u64, u64, Side) {
    match profile {
        BotProfile::MarketMaker => {
            let side = if seq.is_multiple_of(2) {
                Side::Sell
            } else {
                Side::Buy
            };
            let spread = rng.gen_range(1..25);
            let price = match side {
                Side::Buy => 10_000 - spread,
                Side::Sell => 10_000 + spread,
            };
            (price, rng.gen_range(10..50), side)
        }
        BotProfile::AggressiveTaker => {
            let side = if rng.gen_bool(0.5) {
                Side::Buy
            } else {
                Side::Sell
            };
            let price = match side {
                Side::Buy => rng.gen_range(10_100..10_500),
                Side::Sell => rng.gen_range(9_500..9_900),
            };
            (price, rng.gen_range(50..200), side)
        }
        BotProfile::Canceller => {
            let side = if seq.is_multiple_of(2) {
                Side::Buy
            } else {
                Side::Sell
            };
            (10_000, rng.gen_range(1..20), side)
        }
    }
}

/// global_bot_id gives each local bot a stable id across worker partitions.
fn global_bot_id(spec: &WorkloadSpec, local_bot: u32) -> u64 {
    u64::from(spec.worker_index) * u64::from(spec.bot_count) + u64::from(local_bot)
}

/// instant_from_unix_nanos converts the controller's realtime epoch into a
/// local Tokio instant for sleep_until.
fn instant_from_unix_nanos(target_ns: u64) -> Instant {
    let now_ns = unix_nanos();
    if target_ns <= now_ns {
        Instant::now()
    } else {
        Instant::now() + Duration::from_nanos(target_ns - now_ns)
    }
}

/// ConnectedBot owns one established protocol client and its precomputed frames.
struct ConnectedBot {
    bot_id: u64,
    client: TargetClient,
    frames: Vec<OrderFrame>,
}

impl ConnectedBot {
    /// send_all writes every precomputed order and records telemetry after each
    /// successful socket write.
    async fn send_all(
        mut self,
        session_id: &str,
        submission_id: &str,
        worker_id: &str,
        write_timeout: Duration,
        telemetry: TelemetrySink,
    ) -> Result<u64> {
        // pre-allocate shared strings once instead
        let session_id = session_id.to_string();
        let submission_id = submission_id.to_string();
        let worker_id = worker_id.to_string();

        let mut sent = 0;
        for frame in self.frames {
            time::timeout(write_timeout, self.client.write(&frame))
                .await
                .context("timed out writing order")??;
            let send_ts_ns = unix_nanos();
            telemetry
                .record(OrderSentEvent {
                    session_id: session_id.clone(),
                    submission_id: submission_id.clone(),
                    worker_id: worker_id.clone(),
                    bot_id: self.bot_id,
                    order_id: frame.order_id,
                    send_ts_ns,
                    price: frame.price,
                    qty: frame.qty,
                    side: frame.side,
                })
                .await;
            sent += 1;
        }
        Ok(sent)
    }
}

/// TargetClient stores the active connection for the selected workload protocol.
enum TargetClient {
    Fix(TcpStream),
    Rest(TcpStream),
    Ws(WebSocketStream<MaybeTlsStream<TcpStream>>),
}

impl TargetClient {
    /// connect establishes the protocol-specific connection and performs FIX
    /// logon when required.
    async fn connect(spec: &WorkloadSpec, addr: SocketAddr) -> Result<Self> {
        let timeout = Duration::from_millis(spec.connect_timeout_ms);
        match spec.protocol {
            Protocol::Fix => {
                let mut stream = time::timeout(timeout, TcpStream::connect(addr))
                    .await
                    .context("timed out connecting FIX bot")?
                    .context("connect FIX bot")?;
                stream.set_nodelay(true).context("set TCP_NODELAY")?;
                stream
                    .write_all(&fix::logon_frame(&spec.fix_version, 1))
                    .await
                    .context("send FIX logon")?;
                Ok(Self::Fix(stream))
            }
            Protocol::Rest => {
                let stream = time::timeout(timeout, TcpStream::connect(addr))
                    .await
                    .context("timed out connecting REST bot")?
                    .context("connect REST bot")?;
                stream.set_nodelay(true).context("set TCP_NODELAY")?;
                Ok(Self::Rest(stream))
            }
            Protocol::Ws => {
                let url = format!("ws://{}:{}/", spec.target_host, spec.target_port);
                let (ws, _) = time::timeout(timeout, connect_async(url))
                    .await
                    .context("timed out connecting WS bot")?
                    .context("connect WS bot")?;
                // WebSocket path lacked nodelay
                // that FIX/REST have, causing up to 40ms Nagle coalescing delay
                if let tokio_tungstenite::MaybeTlsStream::Plain(ref tcp) = ws.get_ref() {
                    let _ = tcp.set_nodelay(true);
                }
                Ok(Self::Ws(ws))
            }
        }
    }

    /// write sends one precomputed order frame on the active protocol.
    async fn write(&mut self, frame: &OrderFrame) -> Result<()> {
        match self {
            Self::Fix(stream) => stream
                .write_all(&frame.fix)
                .await
                .context("write FIX order"),
            Self::Rest(stream) => stream
                .write_all(&frame.rest)
                .await
                .context("write REST order"),
            Self::Ws(ws) => ws
                .send(WsMessage::Text(frame.ws.clone()))
                .await
                .context("write WS order"),
        }
    }
}
