use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use futures::SinkExt;
use iicpc_schemas_rust::{
    BotProfile, OrderSentEvent, Protocol, ReadySignal, Side, WorkloadFailedEvent, WorkloadSpec,
};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::Semaphore,
    task::JoinSet,
    time::{self, Instant},
};
use tokio_tungstenite::{client_async, tungstenite::Message as WsMessage, WebSocketStream};
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
///
/// ## Workload Loop & Re-queueing design
///
/// This loop processes exactly one assignment at a time sequentially.
/// Since a worker runs in isolation, if the controller sends a new/duplicate
/// workload while one is active, it remains in the Kafka broker queue until the
/// current one completes. Re-queueing active sessions is not expected by the
/// controller; if duplicate session execution is requested sequentially, it will
/// run without concurrent overlap, keeping the metrics isolated.
pub async fn run(config: Config) -> Result<()> {
    kafka::ensure_topics(
        &config.kafka_brokers,
        &[
            &config.workload_topic,
            &config.barrier_topic,
            &config.ready_topic,
            &config.workload_failed_topic,
            &config.orders_sent_topic,
        ],
    )?;

    let producer = kafka::producer(&config.kafka_brokers)?;
    let workload_consumer = kafka::workload_consumer(
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
            message = kafka::recv_message(&workload_consumer) => {
                let message = message.context("read workload assignment")?;
                let Some(payload) = message.payload.as_deref() else {
                    warn!("skipping null workload assignment message");
                    kafka::commit_message(&workload_consumer, &message)
                        .context("commit null workload assignment")?;
                    continue;
                };
                let spec = match kafka::decode_workload(payload) {
                    Ok(spec) => spec,
                    Err(err) => {
                        warn!(error = %err, "skipping invalid workload assignment");
                        kafka::commit_message(&workload_consumer, &message)
                            .context("commit invalid workload assignment")?;
                        continue;
                    }
                };

                if let Err(err) = run_workload(&config, &producer, spec.clone()).await {
                    error!(error = %err, "workload failed");
                    if let Err(publish_err) = publish_workload_failed(&config, &producer, &spec, &err).await {
                        error!(error = %publish_err, "failed to publish workload failure");
                    }
                } else {
                    kafka::commit_message(&workload_consumer, &message)
                        .context("commit completed workload assignment")?;
                }
            }
        }
    }
}

/// run_workload prepares one assignment, publishes readiness, waits on the
/// session barrier, fires all bots, and flushes telemetry before returning.
async fn run_workload(config: &Config, producer: &KafkaProducer, spec: WorkloadSpec) -> Result<()> {
    validate_spec(config, &spec)?;

    // One stable group per worker lets every worker observe every session barrier
    // while avoiding unbounded consumer-group churn across sessions. With a single
    // consumer in this group, Kafka assigns all barrier partitions to this worker.
    let barrier_group = format!("{}-barrier-{}", config.consumer_group, config.worker_id);
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
    let close_result = telemetry.close().await;
    kafka::flush_producer(producer, Duration::from_secs(5))?;
    close_result?;
    result
}

async fn publish_workload_failed(
    config: &Config,
    producer: &KafkaProducer,
    spec: &WorkloadSpec,
    err: &anyhow::Error,
) -> Result<()> {
    let event = WorkloadFailedEvent {
        session_id: spec.session_id.clone(),
        submission_id: spec.submission_id.clone(),
        worker_id: config.worker_id.clone(),
        worker_index: spec.worker_index,
        reason: format!("{err:#}"),
        failed_at_unix_nanos: unix_nanos(),
    };
    kafka::publish_json(
        producer,
        &config.workload_failed_topic,
        &format!("{}:{}", spec.session_id, config.worker_id),
        &event,
    )
    .await
    .context("publish workload failure")
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
    if matches!(spec.target_rate_per_bot, Some(0)) {
        return Err(crate::errors::BotFleetError::ValidationError(
            "target_rate_per_bot must be greater than zero when set".into(),
        )
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
///
/// Uses a semaphore with a concurrency limit of 20 to avoid target TCP SYN floods.
/// Each permit may also run CPU-heavy frame precomputation on Tokio's blocking
/// thread pool, so peak memory scales with this concurrency limit times
/// `orders_per_bot` and the rendered frame size.
async fn connect_bots(spec: &WorkloadSpec) -> Result<Vec<ConnectedBot>> {
    let addr: SocketAddr = format!("{}:{}", spec.target_host, spec.target_port)
        .parse()
        .context("parse target socket address")?;
    let mut set = JoinSet::new();
    let shared_spec = Arc::new(spec.clone());
    let semaphore = Arc::new(Semaphore::new(20));

    for local_bot in 0..spec.bot_count {
        let spec = Arc::clone(&shared_spec);
        let sem = Arc::clone(&semaphore);
        set.spawn(async move {
            let _permit = sem
                .acquire_owned()
                .await
                .context("acquire connection permit")?;
            let bot_id = global_bot_id(&spec, local_bot);
            let client = TargetClient::connect(&spec, addr).await?;

            // Move the CPU-heavy frame precomputation to tokio::task::spawn_blocking
            // to prevent CPU starvation and latency spikes on the async executor threads.
            let frames = tokio::task::spawn_blocking(move || precompute_orders(&spec, bot_id))
                .await
                .context("spawn_blocking precompute_orders panicked")?;

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

    // Preallocate Arc<str> references exactly once for the entire fleet
    // to bypass 200 separate heap allocations during bot spawning.
    let session_id_arc = Arc::<str>::from(spec.session_id.as_str());
    let submission_id_arc = Arc::<str>::from(spec.submission_id.as_str());
    let worker_id_arc = Arc::<str>::from(config.worker_id.as_str());

    for bot in bots {
        let session_id = Arc::clone(&session_id_arc);
        let submission_id = Arc::clone(&submission_id_arc);
        let worker_id = Arc::clone(&worker_id_arc);
        let protocol = spec.protocol;
        let target_rate_per_bot = spec.target_rate_per_bot;
        let telemetry = telemetry.clone();
        set.spawn(async move {
            time::sleep_until(start).await;
            bot.send_all(
                session_id,
                submission_id,
                worker_id,
                protocol,
                target_rate_per_bot,
                write_timeout,
                telemetry,
            )
            .await
        });
    }

    let mut sent = 0u64;
    let mut failed_bots = 0;

    // A single bot write failure or timeout shouldn't abort the entire workload and terminate active tasks.
    // We catch individual bot errors, increment a failure counter, allow other tasks to complete, and report at the end.
    while let Some(result) = set.join_next().await {
        match result {
            Ok(Ok(count)) => {
                sent += count;
            }
            Ok(Err(err)) => {
                failed_bots += 1;
                error!(error = %err, "bot worker task completed with error");
            }
            Err(err) => {
                failed_bots += 1;
                error!(error = %err, "bot worker task panicked or was cancelled");
            }
        }
    }

    info!(
        session_id = %spec.session_id,
        sent,
        failed_bots,
        "workload completed"
    );

    if failed_bots > 0 {
        return Err(anyhow::anyhow!(
            "workload completed with {} failed bot task(s)",
            failed_bots
        ));
    }

    Ok(())
}

/// precompute_orders deterministically renders every order frame for one bot.
fn precompute_orders(spec: &WorkloadSpec, bot_id: u64) -> Vec<OrderFrame> {
    let mut rng = SmallRng::seed_from_u64(spec.global_seed ^ bot_id);
    let mut frames: Vec<OrderFrame> = Vec::with_capacity(spec.orders_per_bot as usize);
    let profile = profile_for_bot(spec, bot_id);

    for seq in 1..=spec.orders_per_bot {
        if profile == BotProfile::Canceller && seq % 2 == 0 && seq > 1 {
            // Cancel the previous order (seq - 1)
            let prev_frame = &frames[(seq - 2) as usize];
            frames.push(fix::cancel_frame(
                &spec.fix_version,
                &spec.session_id,
                &spec.target_host,
                bot_id,
                u64::from(seq),
                u64::from(seq - 1),
                prev_frame.price,
                prev_frame.qty,
                prev_frame.side,
            ));
        } else {
            let (price, qty, side) = order_shape(profile, seq, &mut rng);
            frames.push(fix::order_frame(
                &spec.fix_version,
                &spec.session_id,
                &spec.target_host,
                bot_id,
                u64::from(seq),
                price,
                qty,
                side,
            ));
        }
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
    (u64::from(spec.worker_index) << 32) | u64::from(local_bot)
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
    ///
    /// ## Timeout & Failure Policy
    ///
    /// A single socket write failure or timeout causes this entire bot task to terminate
    /// and exit immediately. All remaining orders for this specific bot are skipped, rather than
    /// continuing to attempt writes on a broken/degraded connection. This represents a hard-stop
    /// policy for individual bots under the assumption that connection issues are persistent.
    async fn send_all(
        mut self,
        session_id: Arc<str>,
        submission_id: Arc<str>,
        worker_id: Arc<str>,
        protocol: Protocol,
        target_rate_per_bot: Option<u32>,
        write_timeout: Duration,
        telemetry: TelemetrySink,
    ) -> Result<u64> {
        let mut sent = 0;
        let mut next_send_at = Instant::now();
        let send_interval =
            target_rate_per_bot.map(|rate| Duration::from_secs_f64(1.0 / f64::from(rate)));
        for mut frame in self.frames {
            if let Some(interval) = send_interval {
                time::sleep_until(next_send_at).await;
                next_send_at += interval;
            }
            let send_ts_ns = unix_nanos();
            frame.patch_timestamp(send_ts_ns);
            time::timeout(write_timeout, self.client.write(&frame))
                .await
                .context("timed out writing order")??;
            telemetry
                .record(OrderSentEvent {
                    session_id: session_id.clone(),
                    submission_id: submission_id.clone(),
                    worker_id: worker_id.clone(),
                    bot_id: self.bot_id,
                    order_id: frame.order_id,
                    send_ts_ns,
                    payload_type: frame.payload_type,
                    price: frame.price,
                    qty: frame.qty,
                    side: frame.side,
                    protocol,
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
    Ws(WebSocketStream<TcpStream>),
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
                let stream = time::timeout(timeout, TcpStream::connect(addr))
                    .await
                    .context("timed out connecting WS bot")?
                    .context("connect WS bot")?;
                stream.set_nodelay(true).context("set TCP_NODELAY")?;
                let url = format!("ws://{}:{}/", spec.target_host, spec.target_port);
                let (ws, _) = client_async(url, stream).await.context("WS handshake")?;
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
            // Note: frame.ws_bytes.clone() is required here because tungstenite's
            // WsMessage::Binary constructor takes an owned Vec<u8>.
            Self::Ws(ws) => ws
                .send(WsMessage::Binary(frame.ws_bytes.clone()))
                .await
                .context("write WS order"),
        }
    }
}
