//! This module implements worker behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use iicpc_schemas_rust::{
    BotProfile, OrdType, OrderSentEvent, PayloadType, Protocol, ReadySignal, Side, TaskSpec,
    WorkloadSpec,
};
use rand::{rngs::SmallRng, Rng};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::{lookup_host, TcpStream},
    sync::watch,
    task::JoinSet,
    time::{self, Instant},
};
use tokio_tungstenite::{
    connect_async, tungstenite::Message as WsMessage, MaybeTlsStream, WebSocketStream,
};
use tracing::{error, info, warn};

use crate::{
    config::Config,
    content::{self, TaskGenerator},
    fix::{self, OrderFrame},
    kafka::{self, KafkaProducer},
    metrics,
    telemetry::TelemetrySink,
    time::unix_nanos,
};

const RESPONSE_TIMEOUT_NS: u64 = 5_000_000_000;

const BARRIER_WAIT: Duration = Duration::from_secs(120);

#[derive(Clone)]
/// CancelToken stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct CancelToken {
    tx: Arc<watch::Sender<bool>>,
    rx: watch::Receiver<bool>,
}

impl CancelToken {
    /// new performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self {
            tx: Arc::new(tx),
            rx,
        }
    }

    /// cancel performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn cancel(&self) {
        let _ = self.tx.send(true);
    }

    /// is_cancelled performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    /// cancelled performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn cancelled(&self) {
        let mut rx = self.rx.clone();
        if *rx.borrow() {
            return;
        }
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                return;
            }
        }
    }
}

/// should_stop_sending performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn should_stop_sending(cancel: &CancelToken, task_end_ns: u64) -> bool {
    cancel.is_cancelled() || unix_nanos() >= task_end_ns
}

/// run starts the bot-fleet worker loop.
/// It consumes workload assignments, executes each one, and exits on a
/// shutdown signal (SIGINT/Ctrl-C or SIGTERM from kubelet).
pub async fn run(mut config: Config) -> Result<()> {
    kafka::ensure_topics(
        &config.kafka_brokers,
        &[
            &config.workload_topic,
            &config.barrier_topic,
            &config.ready_topic,
            &config.orders_sent_topic,
        ],
    )
    .await?;

    let control_producer = kafka::control_producer(&config.kafka_brokers)?;
    let telemetry_producer = kafka::telemetry_producer(&config.kafka_brokers)?;
    // Authoritative N for order_id sharding: the orders.sent topic's real partition
    // count (source of truth), falling back to the configured ORDERS_PARTITIONS if
    // metadata is unavailable. The eBPF capture derives the same N for orders.acked,
    // so an order's sent + acked co-locate on one partition.
    if let Some(n) = kafka::topic_partition_count(&telemetry_producer, &config.orders_sent_topic) {
        if n != config.orders_partitions {
            tracing::info!(
                env = config.orders_partitions,
                topic = n,
                "orders.sent partition count from metadata overrides ORDERS_PARTITIONS"
            );
        }
        config.orders_partitions = n;
    }
    let workload_consumer = kafka::consumer(
        &config.kafka_brokers,
        &config.consumer_group,
        &[&config.workload_topic],
        config.max_poll_interval,
    )?;

    info!(
        worker_id = %config.worker_id,
        workload_topic = %config.workload_topic,
        "bot worker started"
    );

    let cancel = CancelToken::new();
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let mut sigterm =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(s) => s,
                    Err(err) => {
                        error!(error = %err, "failed to install SIGTERM handler");
                        return;
                    }
                };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => info!("SIGINT received; shutting down"),
                _ = sigterm.recv() => info!("SIGTERM received; shutting down"),
            }
            cancel.cancel();
        });
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("shutdown requested");
                return Ok(());
            }
            message = kafka::recv_message(&workload_consumer) => {
                let message = message.context("read workload assignment")?;
                let Some(payload) = message.payload.as_deref() else {
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

                if let Err(err) = run_workload(
                    &config,
                    &control_producer,
                    &telemetry_producer,
                    spec,
                    cancel.clone(),
                )
                .await
                {
                    metrics::workload_error();
                    error!(error = %err, "workload failed; dropping and continuing");
                } else {
                    metrics::workload_ok();
                }
                kafka::commit_message(&workload_consumer, &message)
                    .context("commit handled workload assignment")?;
            }
        }
    }
}

/// run_workload performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn run_workload(
    config: &Config,
    control_producer: &KafkaProducer,
    telemetry_producer: &KafkaProducer,
    spec: WorkloadSpec,
    cancel: CancelToken,
) -> Result<()> {
    validate_spec(config, &spec)?;

    let barrier_group = barrier_group(&config.consumer_group, &spec.session_id, &config.worker_id);
    let barrier_consumer = kafka::consumer(
        &config.kafka_brokers,
        &barrier_group,
        &[&config.barrier_topic],
        config.max_poll_interval,
    )?;

    info!(
        session_id = %spec.session_id,
        protocol = ?spec.protocol,
        task_count = spec.tasks.len(),
        worker_index = spec.worker_index,
        worker_count = spec.worker_count,
        "preparing workload"
    );
    metrics::tasks_assigned(spec.tasks.len());

    let connected = connect_tasks(&spec).await?;
    let connected_count = connected.len() as u32;
    metrics::tasks_connected(connected.len());

    let ready = ReadySignal {
        session_id: spec.session_id.clone(),
        submission_id: spec.submission_id.clone(),
        worker_id: config.worker_id.clone(),
        worker_index: spec.worker_index,
        worker_count: spec.worker_count,
        task_count: spec.tasks.len() as u32,
        connected_count,
        ready_at_unix_nanos: unix_nanos(),
    };
    kafka::publish_json(
        control_producer,
        &config.ready_topic,
        &kafka::ready_key(&ready),
        &ready,
    )
    .await?;
    info!(session_id = %spec.session_id, connected_count, "published ready signal");

    let barrier =
        kafka::wait_for_barrier(&barrier_consumer, &spec.session_id, BARRIER_WAIT).await?;
    let barrier_epoch_ns = barrier.target_epoch_unix_nanos;

    let telemetry = TelemetrySink::new(
        telemetry_producer.clone(),
        config.orders_sent_topic.clone(),
        spec.session_id.clone(),
        config.worker_id.clone(),
        config.telemetry_channel_capacity,
        config.telemetry_flush_interval,
        config.telemetry_batch_size,
        config.orders_partitions,
    );

    let result = fire_workload(
        config,
        &spec,
        connected,
        barrier_epoch_ns,
        telemetry.clone(),
        cancel,
    )
    .await;
    telemetry.close().await?;
    result
}

/// validate_spec performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn validate_spec(config: &Config, spec: &WorkloadSpec) -> Result<()> {
    if spec.tasks.is_empty() {
        return Err(
            crate::errors::BotFleetError::ValidationError("tasks list is empty".into()).into(),
        );
    }
    if spec.tasks.len() > config.max_bots_per_worker {
        return Err(crate::errors::BotFleetError::ValidationError(format!(
            "task count {} exceeds config max_bots_per_worker {}",
            spec.tasks.len(),
            config.max_bots_per_worker
        ))
        .into());
    }
    for task in &spec.tasks {
        if task.target_rps == 0 {
            return Err(crate::errors::BotFleetError::ValidationError(format!(
                "task {} has target_rps=0",
                task.task_id
            ))
            .into());
        }
        if task.duration_ns == 0 {
            return Err(crate::errors::BotFleetError::ValidationError(format!(
                "task {} has duration_ns=0",
                task.task_id
            ))
            .into());
        }
    }
    if spec.worker_count == 0 || spec.worker_index >= spec.worker_count {
        return Err(crate::errors::BotFleetError::ValidationError(
            "invalid worker index/count".into(),
        )
        .into());
    }
    let worst_case_ns = worst_case_wall_time_ns(spec);
    let poll_ceiling_ns = config.max_poll_interval.as_nanos() as u64;
    if worst_case_ns >= poll_ceiling_ns {
        return Err(crate::errors::BotFleetError::ValidationError(format!(
            "worst-case wall time {worst_case_ns}ns (barrier wait + offset + duration + drain) \
             would meet or exceed max.poll.interval.ms ceiling {poll_ceiling_ns}ns; the \
             assignment offset commits only after the run, so Kafka would rebalance and \
             re-deliver mid-run (duplicate execution)"
        ))
        .into());
    }
    validate_identifier("session_id", &spec.session_id)?;
    validate_identifier("submission_id", &spec.submission_id)?;
    validate_identifier("fix_version", &spec.fix_version)?;
    Ok(())
}

/// barrier_group performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn barrier_group(consumer_group: &str, _session_id: &str, worker_id: &str) -> String {
    format!("{consumer_group}-barrier-{worker_id}")
}

/// worst_case_wall_time_ns performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn worst_case_wall_time_ns(spec: &WorkloadSpec) -> u64 {
    let max_task_span_ns = spec
        .tasks
        .iter()
        .map(|t| t.start_offset_ns.saturating_add(t.duration_ns))
        .max()
        .unwrap_or(0);
    (BARRIER_WAIT.as_nanos() as u64)
        .saturating_add(max_task_span_ns)
        .saturating_add(RESPONSE_TIMEOUT_NS)
}

/// validate_identifier performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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

/// connect_tasks performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn connect_tasks(spec: &WorkloadSpec) -> Result<Vec<ConnectedTask>> {
    let addr = resolve_target(&spec.target_host, spec.target_port).await?;
    let mut set = JoinSet::new();
    let shared_spec = Arc::new(spec.clone());

    for task in &spec.tasks {
        let spec = Arc::clone(&shared_spec);
        let task = task.clone();
        set.spawn(async move {
            let client = TargetClient::connect(&spec, addr).await?;
            Ok::<_, anyhow::Error>(ConnectedTask {
                task,
                target_host: spec.target_host.clone(),
                client,
            })
        });
    }

    let mut tasks = Vec::with_capacity(spec.tasks.len());
    while let Some(result) = set.join_next().await {
        match result.context("join task connect")? {
            Ok(t) => tasks.push(t),
            Err(err) => {
                metrics::connect_failure();
                warn!(error = %err, "task connect failed; dropping");
            }
        }
    }
    Ok(tasks)
}

/// resolve_target performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn resolve_target(host: &str, port: u16) -> Result<SocketAddr> {
    let target = format!("{host}:{port}");
    let mut addrs = lookup_host(&target)
        .await
        .with_context(|| format!("resolve target host {target}"))?;
    addrs
        .next()
        .ok_or_else(|| anyhow::anyhow!("no addresses resolved for target host {target}"))
}

/// fire_workload performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn fire_workload(
    config: &Config,
    spec: &WorkloadSpec,
    tasks: Vec<ConnectedTask>,
    barrier_epoch_ns: u64,
    telemetry: TelemetrySink,
    cancel: CancelToken,
) -> Result<()> {
    let mut set = JoinSet::new();
    let write_timeout = Duration::from_millis(spec.write_timeout_ms);

    for ct in tasks {
        let session_id = spec.session_id.clone();
        let submission_id = spec.submission_id.clone();
        let worker_id = config.worker_id.clone();
        let fix_version = spec.fix_version.clone();
        let global_seed = spec.global_seed;
        let telemetry = telemetry.clone();
        let cancel = cancel.clone();
        set.spawn(async move {
            ct.run(
                &session_id,
                &submission_id,
                &worker_id,
                &fix_version,
                global_seed,
                barrier_epoch_ns,
                write_timeout,
                telemetry,
                cancel,
            )
            .await
        });
    }

    let mut sent_total = 0u64;
    while let Some(result) = set.join_next().await {
        match result.context("join task send loop")? {
            Ok(sent) => sent_total += sent,
            Err(err) => {
                warn!(error = %err, "task send loop failed; dropped");
            }
        }
    }
    info!(session_id = %spec.session_id, sent = sent_total, "workload completed");
    Ok(())
}

/// ConnectedTask stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct ConnectedTask {
    task: TaskSpec,
    target_host: String,
    client: TargetClient,
}

#[derive(Clone)]
/// PendingOrder stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct PendingOrder {
    order_id: String,
    orig_order_id: String,
    target_send_ts_ns: u64,
    send_ts_ns: u64,
    /// The session's barrier epoch (unix ns), copied onto the emitted
    /// OrderSentEvent so the ingester can bucket waves deterministically (see
    /// OrderSentEvent::barrier_epoch_ns). Same value for every order in a session.
    barrier_epoch_ns: u64,
    price: u64,
    qty: u64,
    side: Side,
    payload_type: PayloadType,
    ord_type: OrdType,
}

type PendingMap = Arc<Mutex<HashMap<String, PendingOrder>>>;

impl ConnectedTask {
    #[allow(clippy::too_many_arguments)]
    /// run performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn run(
        self,
        session_id: &str,
        submission_id: &str,
        worker_id: &str,
        fix_version: &str,
        global_seed: u64,
        barrier_epoch_ns: u64,
        write_timeout: Duration,
        telemetry: TelemetrySink,
        cancel: CancelToken,
    ) -> Result<u64> {
        let ctx = TaskContext {
            session_id: session_id.to_string(),
            submission_id: submission_id.to_string(),
            worker_id: worker_id.to_string(),
            target_host: self.target_host,
            fix_version: fix_version.to_string(),
            global_seed,
            barrier_epoch_ns,
            write_timeout,
            telemetry,
            cancel,
        };

        match self.client {
            TargetClient::Fix(fix) => run_fix_task(self.task, fix, ctx).await,
            TargetClient::Rest(stream) => run_rest_task(self.task, stream, ctx).await,
            TargetClient::Ws(ws) => run_ws_task(self.task, ws, ctx).await,
        }
    }
}

/// TaskContext stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct TaskContext {
    session_id: String,
    submission_id: String,
    worker_id: String,
    target_host: String,
    fix_version: String,
    global_seed: u64,
    barrier_epoch_ns: u64,
    write_timeout: Duration,
    telemetry: TelemetrySink,
    cancel: CancelToken,
}

/// FixConnection stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct FixConnection {
    read_half: ReadHalf<TcpStream>,
    write_half: WriteHalf<TcpStream>,
}

/// run_fix_task performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn run_fix_task(task: TaskSpec, fix: FixConnection, ctx: TaskContext) -> Result<u64> {
    let task_start_ns = ctx.barrier_epoch_ns.saturating_add(task.start_offset_ns);
    let task_end_ns = task_start_ns.saturating_add(task.duration_ns);
    let drain_end_ns = task_end_ns.saturating_add(RESPONSE_TIMEOUT_NS);

    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));

    let mut set: JoinSet<Result<u64>> = JoinSet::new();

    set.spawn(fix_write_loop(
        fix.write_half,
        task.clone(),
        pending.clone(),
        ctx.session_id.clone(),
        ctx.target_host.clone(),
        ctx.fix_version.clone(),
        ctx.global_seed,
        task_start_ns,
        task_end_ns,
        ctx.write_timeout,
        ctx.cancel.clone(),
    ));
    set.spawn(fix_read_loop(
        fix.read_half,
        task.task_id,
        pending.clone(),
        ctx.telemetry.clone(),
        ctx.session_id.clone(),
        ctx.submission_id.clone(),
        ctx.worker_id.clone(),
        drain_end_ns,
    ));
    set.spawn(watchdog_loop(
        task.task_id,
        pending.clone(),
        ctx.telemetry.clone(),
        ctx.session_id.clone(),
        ctx.submission_id.clone(),
        ctx.worker_id.clone(),
        drain_end_ns,
    ));

    let mut sent_total = 0u64;
    while let Some(result) = set.join_next().await {
        match result.context("join FIX sub-task")? {
            Ok(n) => sent_total += n,
            Err(err) => {
                warn!(task_id = task.task_id, error = %err, "FIX sub-task ended with error");
            }
        }
    }
    Ok(sent_total)
}

/// mix_from_task performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn mix_from_task(task: &TaskSpec) -> content::OrderMix {
    content::OrderMix {
        market_pct: task.market_pct,
        cancel_pct: task.cancel_pct,
        replace_pct: task.replace_pct,
    }
}

/// render_frame performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn render_frame(
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    task_id: u64,
    action: &content::Action,
) -> fix::OrderFrame {
    use content::Action;
    match action {
        Action::NewLimit {
            seq,
            price,
            qty,
            side,
        } => fix::order_frame(
            fix_version,
            session_id,
            target_host,
            task_id,
            u64::from(*seq),
            *price,
            *qty,
            *side,
        ),
        Action::NewMarket { seq, qty, side } => fix::market_frame(
            fix_version,
            session_id,
            target_host,
            task_id,
            u64::from(*seq),
            *qty,
            *side,
        ),
        Action::Cancel {
            seq,
            orig_order_id,
            price,
            qty,
            side,
        } => fix::cancel_frame(
            fix_version,
            session_id,
            target_host,
            task_id,
            u64::from(*seq),
            orig_order_id,
            *price,
            *qty,
            *side,
        ),
        Action::Replace {
            seq,
            orig_order_id,
            price,
            qty,
            side,
        } => fix::replace_frame(
            fix_version,
            session_id,
            target_host,
            task_id,
            u64::from(*seq),
            orig_order_id,
            *price,
            *qty,
            *side,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
/// fix_write_loop performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn fix_write_loop(
    mut write_half: WriteHalf<TcpStream>,
    task: TaskSpec,
    pending: PendingMap,
    session_id: String,
    target_host: String,
    fix_version: String,
    global_seed: u64,
    task_start_ns: u64,
    task_end_ns: u64,
    // Intentionally unused: a per-write timeout is wrong for a load generator (see the write
    // site) — it turns sink backpressure into permanent task death and collapses aggregate load.
    _write_timeout: Duration,
    cancel: CancelToken,
) -> Result<u64> {
    time::sleep_until(instant_from_unix_nanos(task_start_ns)).await;

    // BOT_WRITE_BATCH coalesces up to N already-DUE orders into a single write_all,
    // so one write() syscall + one reactor round-trip amortises across many orders —
    // the dominant per-order cost found by profiling (CPU-bound, ~42us/order, mostly
    // the per-order write().await). Default 64; 1 reproduces legacy per-order sends.
    // Pacing is unchanged: only orders past their schedule are batched (catch-up), so
    // an under-the-ceiling task still paces normally and just writes batches of one.
    static MAX_WRITE_BATCH: LazyLock<usize> = LazyLock::new(|| {
        std::env::var("BOT_WRITE_BATCH")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(64)
    });
    let batch_max = *MAX_WRITE_BATCH;

    // Closed-loop in-flight cap (per task). A slow contestant — or a stalled telemetry path —
    // makes the per-task pending map (sent-but-unacked orders) grow without bound and OOMs the
    // worker. Cap in-flight orders: at the cap the send loop backpressures instead of firing
    // more, pacing this connection to the contestant's real ack rate. A healthy contestant
    // keeps in-flight at ~rate×RTT (orders of magnitude below the cap → never engages); only an
    // over-driven slow contestant is throttled, to its honest sustainable rate. Default 10k/task
    // (~2.4 MiB) → bounded worker memory that fits a small node, and well above
    // per_task_rate × RESPONSE_TIMEOUT for any realistic rate, so it never false-throttles
    // (e.g. an 835k/500-task drain sits at ~8.4k in-flight, under 10k).
    static MAX_INFLIGHT: LazyLock<usize> = LazyLock::new(|| {
        std::env::var("BOT_MAX_INFLIGHT_PER_TASK")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(10_000)
    });
    let max_inflight = *MAX_INFLIGHT;

    let interval_ns = 1_000_000_000_u64 / u64::from(task.target_rps);
    let mut next_send_ns = task_start_ns;
    let barrier_epoch_ns = task_start_ns.saturating_sub(task.start_offset_ns);
    let mut generator = TaskGenerator::new(
        session_id.clone(),
        u64::from(task.task_id),
        task.profile,
        mix_from_task(&task),
        global_seed ^ u64::from(task.task_id),
    );
    let mut sent: u64 = 0;

    // Reused across iterations to avoid per-batch allocation.
    let mut frames: Vec<OrderFrame> = Vec::with_capacity(batch_max);
    let mut targets: Vec<u64> = Vec::with_capacity(batch_max);
    let mut batch_buf: Vec<u8> = Vec::with_capacity(batch_max * 256);

    loop {
        if should_stop_sending(&cancel, task_end_ns) {
            break;
        }

        // In-flight backpressure: if too many orders are awaiting acks (contestant can't keep
        // up, or telemetry is stalled), wait for pending to drain — via acks or watchdog
        // eviction — before issuing more, instead of growing memory unbounded.
        while pending.lock().expect("pending map poisoned").len() >= max_inflight {
            if should_stop_sending(&cancel, task_end_ns) {
                return Ok(sent);
            }
            tokio::select! {
                _ = time::sleep(Duration::from_millis(1)) => {}
                _ = cancel.cancelled() => return Ok(sent),
            }
        }

        // Pace: park only when genuinely ahead of the next due order.
        if next_send_ns > unix_nanos() {
            tokio::select! {
                _ = time::sleep_until(instant_from_unix_nanos(next_send_ns)) => {}
                _ = cancel.cancelled() => break,
            }
        }

        // Collect every order that is now due (up to batch_max) into one buffer.
        frames.clear();
        targets.clear();
        batch_buf.clear();
        let now_ns = unix_nanos();
        while frames.len() < batch_max && next_send_ns <= now_ns {
            let action = generator.next();
            let mut frame = render_frame(
                &fix_version,
                &session_id,
                &target_host,
                u64::from(task.task_id),
                &action,
            );
            frame.patch_timestamp(unix_nanos());
            batch_buf.extend_from_slice(&frame.fix);
            targets.push(next_send_ns);
            frames.push(frame);
            next_send_ns = next_send_ns.saturating_add(interval_ns);
        }
        if frames.is_empty() {
            continue;
        }
        let count = frames.len();

        // Insert the whole batch under a single lock.
        {
            let mut map = pending.lock().expect("pending map poisoned");
            for (frame, &target) in frames.iter().zip(targets.iter()) {
                map.insert(
                    frame.order_id.clone(),
                    PendingOrder {
                        order_id: frame.order_id.clone(),
                        orig_order_id: frame.orig_order_id.clone(),
                        target_send_ts_ns: target,
                        send_ts_ns: 0, // patched on successful write
                        barrier_epoch_ns,
                        price: frame.price,
                        qty: frame.qty,
                        side: frame.side,
                        payload_type: frame.payload_type,
                        ord_type: frame.ord_type,
                    },
                );
            }
        }

        let write_start_ns = unix_nanos();
        // Block on the write rather than imposing a per-write timeout. When the contestant
        // can't drain fast enough its TCP receive window fills and write_all stalls; TCP flow
        // control then paces THIS task down to the contestant's real service rate, so aggregate
        // load plateaus at the sink's capacity instead of overshooting. A short timeout here did
        // the opposite: write_all isn't cancel-safe, so a timeout left a partial frame and forced
        // the task to exit — under sustained backpressure every loaded task exited at once and the
        // offered load collapsed to zero (the symptom we saw on ramp; a drain sink never fills the
        // window so it never tripped). Cancellation still tears the loop down promptly at session
        // end, and a genuinely dead peer surfaces as a write error below.
        let write_res = tokio::select! {
            res = write_half.write_all(&batch_buf) => res,
            _ = cancel.cancelled() => break,
        };
        match write_res {
            Ok(()) => {
                let send_ts_ns = unix_nanos();
                metrics::orders_sent_by(count);
                metrics::observe_write(send_ts_ns.saturating_sub(write_start_ns), count);
                {
                    let mut map = pending.lock().expect("pending map poisoned");
                    for frame in frames.iter() {
                        if let Some(p) = map.get_mut(&frame.order_id) {
                            p.send_ts_ns = send_ts_ns;
                        }
                    }
                }
                for &target in targets.iter() {
                    metrics::observe_slip(send_ts_ns.saturating_sub(target));
                }
                sent += count as u64;
            }
            Err(err) => {
                {
                    let mut map = pending.lock().expect("pending map poisoned");
                    for frame in frames.iter() {
                        map.remove(&frame.order_id);
                    }
                }
                metrics::order_write_error();
                warn!(
                    task_id = task.task_id,
                    error = %err, "FIX batch write failed; task writer exiting"
                );
                return Ok(sent);
            }
        }
    }

    Ok(sent)
}

#[allow(clippy::too_many_arguments)]
/// fix_read_loop performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn fix_read_loop(
    mut read_half: ReadHalf<TcpStream>,
    task_id: u32,
    pending: PendingMap,
    telemetry: TelemetrySink,
    session_id: String,
    submission_id: String,
    worker_id: String,
    drain_end_ns: u64,
) -> Result<u64> {
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk = [0u8; 4096];

    loop {
        let now_ns = unix_nanos();
        if now_ns >= drain_end_ns {
            break;
        }
        let remaining = Duration::from_nanos(drain_end_ns - now_ns);

        let n = match time::timeout(remaining, read_half.read(&mut chunk)).await {
            Ok(Ok(0)) => {
                break;
            }
            Ok(Ok(n)) => n,
            Ok(Err(err)) => {
                warn!(task_id, error = %err, "FIX read failed; reader exiting");
                break;
            }
            Err(_) => break, // drain deadline reached
        };
        buf.extend_from_slice(&chunk[..n]);

        let (messages, consumed) = fix::parse_messages(&buf);
        for msg in messages {
            if msg.msg_type != b"8" {
                continue;
            }
            let Some(clord_id_bytes) = msg.clord_id else {
                continue;
            };
            let Ok(clord_id) = std::str::from_utf8(clord_id_bytes) else {
                continue;
            };

            let pending_order = {
                let mut map = pending.lock().expect("pending map poisoned");
                map.remove(clord_id)
            };
            let Some(p) = pending_order else { continue };

            let recv_done_ts_ns = unix_nanos();
            telemetry
                .record(OrderSentEvent {
                    session_id: session_id.clone(),
                    submission_id: submission_id.clone(),
                    worker_id: worker_id.clone(),
                    task_id,
                    order_id: p.order_id,
                    target_send_ts_ns: p.target_send_ts_ns,
                    send_ts_ns: p.send_ts_ns,
                    recv_done_ts_ns,
                    timed_out: false,
                    price: p.price,
                    qty: p.qty,
                    side: p.side,
                    payload_type: p.payload_type,
                    ord_type: p.ord_type,
                    orig_order_id: p.orig_order_id,
                    barrier_epoch_ns: p.barrier_epoch_ns,
                })
                .await;
        }

        if consumed > 0 {
            buf.drain(..consumed);
        }

        if buf.len() > 1_048_576 {
            warn!(task_id, "FIX read buffer overflow; resetting");
            buf.clear();
        }
    }

    Ok(0)
}

#[allow(clippy::too_many_arguments)]
/// watchdog_loop performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn watchdog_loop(
    task_id: u32,
    pending: PendingMap,
    telemetry: TelemetrySink,
    session_id: String,
    submission_id: String,
    worker_id: String,
    drain_end_ns: u64,
) -> Result<u64> {
    const WATCHDOG_TICK: Duration = Duration::from_millis(250);

    loop {
        let now_ns = unix_nanos();
        let last_tick = now_ns >= drain_end_ns;

        let to_evict: Vec<PendingOrder> = {
            let mut map = pending.lock().expect("pending map poisoned");
            let mut evicted = Vec::new();
            map.retain(|_, p| {
                let age_ns = if p.send_ts_ns == 0 {
                    0
                } else {
                    now_ns.saturating_sub(p.send_ts_ns)
                };
                let expired = age_ns >= RESPONSE_TIMEOUT_NS || last_tick;
                if expired {
                    evicted.push(p.clone());
                    false
                } else {
                    true
                }
            });
            evicted
        };

        for p in to_evict {
            telemetry
                .record(OrderSentEvent {
                    session_id: session_id.clone(),
                    submission_id: submission_id.clone(),
                    worker_id: worker_id.clone(),
                    task_id,
                    order_id: p.order_id,
                    target_send_ts_ns: p.target_send_ts_ns,
                    send_ts_ns: p.send_ts_ns,
                    recv_done_ts_ns: 0,
                    timed_out: true,
                    price: p.price,
                    qty: p.qty,
                    side: p.side,
                    payload_type: p.payload_type,
                    ord_type: p.ord_type,
                    orig_order_id: p.orig_order_id,
                    barrier_epoch_ns: p.barrier_epoch_ns,
                })
                .await;
        }

        if last_tick {
            break;
        }
        time::sleep(WATCHDOG_TICK).await;
    }

    Ok(0)
}

/// RwWriter enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
enum RwWriter {
    Rest(WriteHalf<TcpStream>),
    Ws(SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, WsMessage>),
}

impl RwWriter {
    /// write_order performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn write_order(&mut self, frame: &OrderFrame) -> Result<()> {
        match self {
            Self::Rest(w) => w.write_all(&frame.rest).await.context("write REST order"),
            Self::Ws(s) => s
                .send(WsMessage::Binary(frame.ws_bytes.clone()))
                .await
                .context("write WS order"),
        }
    }
}

/// run_rest_task performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn run_rest_task(task: TaskSpec, stream: TcpStream, ctx: TaskContext) -> Result<u64> {
    let (read_half, write_half) = tokio::io::split(stream);
    run_readwrite_task(
        task,
        RwWriter::Rest(write_half),
        ReadSource::Rest(read_half),
        ctx,
    )
    .await
}

/// run_ws_task performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn run_ws_task(
    task: TaskSpec,
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    ctx: TaskContext,
) -> Result<u64> {
    let (sink, stream) = ws.split();
    run_readwrite_task(task, RwWriter::Ws(sink), ReadSource::Ws(stream), ctx).await
}

/// ReadSource enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
enum ReadSource {
    Rest(ReadHalf<TcpStream>),
    Ws(SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>),
}

/// run_readwrite_task performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn run_readwrite_task(
    task: TaskSpec,
    writer: RwWriter,
    reader: ReadSource,
    ctx: TaskContext,
) -> Result<u64> {
    let task_start_ns = ctx.barrier_epoch_ns.saturating_add(task.start_offset_ns);
    let task_end_ns = task_start_ns.saturating_add(task.duration_ns);
    let drain_end_ns = task_end_ns.saturating_add(RESPONSE_TIMEOUT_NS);

    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let mut set: JoinSet<Result<u64>> = JoinSet::new();

    set.spawn(rw_write_loop(
        writer,
        task.clone(),
        pending.clone(),
        ctx.session_id.clone(),
        ctx.target_host.clone(),
        ctx.fix_version.clone(),
        ctx.global_seed,
        task_start_ns,
        task_end_ns,
        ctx.write_timeout,
        ctx.cancel.clone(),
    ));
    match reader {
        ReadSource::Rest(read_half) => set.spawn(rest_read_loop(
            read_half,
            task.task_id,
            pending.clone(),
            ctx.telemetry.clone(),
            ctx.session_id.clone(),
            ctx.submission_id.clone(),
            ctx.worker_id.clone(),
            drain_end_ns,
        )),
        ReadSource::Ws(stream) => set.spawn(ws_read_loop(
            stream,
            task.task_id,
            pending.clone(),
            ctx.telemetry.clone(),
            ctx.session_id.clone(),
            ctx.submission_id.clone(),
            ctx.worker_id.clone(),
            drain_end_ns,
        )),
    };
    set.spawn(watchdog_loop(
        task.task_id,
        pending.clone(),
        ctx.telemetry.clone(),
        ctx.session_id.clone(),
        ctx.submission_id.clone(),
        ctx.worker_id.clone(),
        drain_end_ns,
    ));

    let mut sent_total = 0u64;
    while let Some(result) = set.join_next().await {
        match result.context("join REST/WS sub-task")? {
            Ok(n) => sent_total += n,
            Err(err) => {
                warn!(task_id = task.task_id, error = %err, "REST/WS sub-task ended with error");
            }
        }
    }
    Ok(sent_total)
}

#[allow(clippy::too_many_arguments)]
/// rw_write_loop performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn rw_write_loop(
    mut writer: RwWriter,
    task: TaskSpec,
    pending: PendingMap,
    session_id: String,
    target_host: String,
    fix_version: String,
    global_seed: u64,
    task_start_ns: u64,
    task_end_ns: u64,
    write_timeout: Duration,
    cancel: CancelToken,
) -> Result<u64> {
    time::sleep_until(instant_from_unix_nanos(task_start_ns)).await;

    let interval_ns = 1_000_000_000_u64 / u64::from(task.target_rps);
    let mut next_send_ns = task_start_ns;
    let mut generator = TaskGenerator::new(
        session_id.clone(),
        u64::from(task.task_id),
        task.profile,
        mix_from_task(&task),
        global_seed ^ u64::from(task.task_id),
    );
    let mut sent: u64 = 0;

    loop {
        if should_stop_sending(&cancel, task_end_ns) {
            break;
        }
        let target_send_ts_ns = next_send_ns;
        // Coordinated-omission catch-up pacing: only park on the timer when genuinely
        // AHEAD of schedule. If the deadline has already passed (we're behind), send
        // the due order immediately instead of sleeping. tokio's timer wheel quantizes
        // every sleep to ~1ms — even past-due ones — which otherwise pins each task
        // near ~1k/s and, with many tasks contending the timer driver, collapses
        // aggregate throughput while CPU sits idle. Sending due orders immediately is
        // the *correct* CO behaviour: target_send_ts_ns stays the fixed schedule, so
        // schedule_slip / response_time still measure real lateness (engine/network
        // backpressure), not a load-generator timer artifact. should_stop_sending at
        // the loop head keeps cancel/task-end responsive without the sleep.
        if next_send_ns > unix_nanos() {
            tokio::select! {
                _ = time::sleep_until(instant_from_unix_nanos(next_send_ns)) => {}
                _ = cancel.cancelled() => break,
            }
        }

        let action = generator.next();
        let seq = action.seq();
        let frame = render_frame(
            &fix_version,
            &session_id,
            &target_host,
            u64::from(task.task_id),
            &action,
        );

        {
            let mut map = pending.lock().expect("pending map poisoned");
            map.insert(
                frame.order_id.clone(),
                PendingOrder {
                    order_id: frame.order_id.clone(),
                    orig_order_id: frame.orig_order_id.clone(),
                    target_send_ts_ns,
                    send_ts_ns: 0,
                    barrier_epoch_ns: task_start_ns.saturating_sub(task.start_offset_ns),
                    price: frame.price,
                    qty: frame.qty,
                    side: frame.side,
                    payload_type: frame.payload_type,
                    ord_type: frame.ord_type,
                },
            );
        }

        let write_start_ns = unix_nanos();
        match time::timeout(write_timeout, writer.write_order(&frame)).await {
            Ok(Ok(())) => {
                let send_ts_ns = unix_nanos();
                metrics::order_sent();
                metrics::observe_write(send_ts_ns.saturating_sub(write_start_ns), 1);
                metrics::observe_slip(send_ts_ns.saturating_sub(target_send_ts_ns));
                if let Some(p) = pending
                    .lock()
                    .expect("pending map poisoned")
                    .get_mut(&frame.order_id)
                {
                    p.send_ts_ns = send_ts_ns;
                }
                sent += 1;
            }
            Ok(Err(err)) => {
                pending
                    .lock()
                    .expect("pending map poisoned")
                    .remove(&frame.order_id);
                metrics::order_write_error();
                warn!(task_id = task.task_id, seq, error = %err, "REST/WS write failed; writer exiting");
                return Ok(sent);
            }
            Err(_) => {
                pending
                    .lock()
                    .expect("pending map poisoned")
                    .remove(&frame.order_id);
                metrics::order_write_error();
                warn!(
                    task_id = task.task_id,
                    seq, "REST/WS write timeout; writer exiting"
                );
                return Ok(sent);
            }
        }

        next_send_ns = next_send_ns.saturating_add(interval_ns);
    }

    Ok(sent)
}

#[allow(clippy::too_many_arguments)]
/// emit_response performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn emit_response(
    telemetry: &TelemetrySink,
    pending: &PendingMap,
    session_id: &str,
    submission_id: &str,
    worker_id: &str,
    task_id: u32,
    clord_id: &str,
) {
    let pending_order = {
        let mut map = pending.lock().expect("pending map poisoned");
        map.remove(clord_id)
    };
    let Some(p) = pending_order else { return };
    let recv_done_ts_ns = unix_nanos();
    telemetry
        .record(OrderSentEvent {
            session_id: session_id.to_string(),
            submission_id: submission_id.to_string(),
            worker_id: worker_id.to_string(),
            task_id,
            order_id: p.order_id,
            target_send_ts_ns: p.target_send_ts_ns,
            send_ts_ns: p.send_ts_ns,
            recv_done_ts_ns,
            timed_out: false,
            price: p.price,
            qty: p.qty,
            side: p.side,
            payload_type: p.payload_type,
            ord_type: p.ord_type,
            orig_order_id: p.orig_order_id,
            barrier_epoch_ns: p.barrier_epoch_ns,
        })
        .await;
}

/// clordid_from_json performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn clordid_from_json(body: &[u8]) -> Option<String> {
    #[derive(serde::Deserialize)]
    /// Resp stores the state passed across this module boundary.
    /// Keep field changes compatible with callers and serialized contracts.
    struct Resp {
        cl_ord_id: Option<String>,
    }
    serde_json::from_slice::<Resp>(body)
        .ok()
        .and_then(|r| r.cl_ord_id)
}

/// next_http_response performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn next_http_response(buf: &[u8]) -> Option<(Option<String>, usize)> {
    let hdr_end = find_subslice(buf, b"\r\n\r\n")?;
    let body_start = hdr_end + 4;
    let headers = &buf[..hdr_end];
    if header_is_chunked(headers) {
        let rel = find_subslice(&buf[body_start..], b"0\r\n\r\n")?;
        let total = body_start + rel + 5;
        let chunk_body = dechunk_first(&buf[body_start..total]);
        return Some((clordid_from_json(&chunk_body), total));
    }
    let content_len = header_content_length(headers).unwrap_or(0);
    let total = body_start + content_len;
    if buf.len() < total {
        return None;
    }
    Some((clordid_from_json(&buf[body_start..total]), total))
}

/// find_subslice performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

/// header_content_length performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn header_content_length(headers: &[u8]) -> Option<usize> {
    let lower: Vec<u8> = headers.iter().map(u8::to_ascii_lowercase).collect();
    let i = find_subslice(&lower, b"content-length:")?;
    let rest = &headers[i + b"content-length:".len()..];
    let end = rest.iter().position(|&b| b == b'\r').unwrap_or(rest.len());
    std::str::from_utf8(&rest[..end]).ok()?.trim().parse().ok()
}

/// header_is_chunked performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn header_is_chunked(headers: &[u8]) -> bool {
    let lower: Vec<u8> = headers.iter().map(u8::to_ascii_lowercase).collect();
    match find_subslice(&lower, b"transfer-encoding:") {
        Some(i) => {
            let rest = &lower[i + b"transfer-encoding:".len()..];
            let end = rest.iter().position(|&b| b == b'\r').unwrap_or(rest.len());
            find_subslice(&rest[..end], b"chunked").is_some()
        }
        None => false,
    }
}

/// dechunk_first performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn dechunk_first(body: &[u8]) -> Vec<u8> {
    let Some(crlf) = find_subslice(body, b"\r\n") else {
        return Vec::new();
    };
    let size = std::str::from_utf8(&body[..crlf])
        .ok()
        .and_then(|s| usize::from_str_radix(s.trim(), 16).ok())
        .unwrap_or(0);
    let start = crlf + 2;
    let end = (start + size).min(body.len());
    body[start..end].to_vec()
}

#[allow(clippy::too_many_arguments)]
/// rest_read_loop performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn rest_read_loop(
    mut read_half: ReadHalf<TcpStream>,
    task_id: u32,
    pending: PendingMap,
    telemetry: TelemetrySink,
    session_id: String,
    submission_id: String,
    worker_id: String,
    drain_end_ns: u64,
) -> Result<u64> {
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk = [0u8; 4096];

    loop {
        let now_ns = unix_nanos();
        if now_ns >= drain_end_ns {
            break;
        }
        let remaining = Duration::from_nanos(drain_end_ns - now_ns);
        let n = match time::timeout(remaining, read_half.read(&mut chunk)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => n,
            Ok(Err(err)) => {
                warn!(task_id, error = %err, "REST read failed; reader exiting");
                break;
            }
            Err(_) => break,
        };
        buf.extend_from_slice(&chunk[..n]);

        while let Some((clord, consumed)) = next_http_response(&buf) {
            if let Some(clord_id) = clord {
                emit_response(
                    &telemetry,
                    &pending,
                    &session_id,
                    &submission_id,
                    &worker_id,
                    task_id,
                    &clord_id,
                )
                .await;
            }
            buf.drain(..consumed);
        }

        if buf.len() > 1_048_576 {
            warn!(task_id, "REST read buffer overflow; resetting");
            buf.clear();
        }
    }

    Ok(0)
}

#[allow(clippy::too_many_arguments)]
/// ws_read_loop performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn ws_read_loop(
    mut stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    task_id: u32,
    pending: PendingMap,
    telemetry: TelemetrySink,
    session_id: String,
    submission_id: String,
    worker_id: String,
    drain_end_ns: u64,
) -> Result<u64> {
    loop {
        let now_ns = unix_nanos();
        if now_ns >= drain_end_ns {
            break;
        }
        let remaining = Duration::from_nanos(drain_end_ns - now_ns);
        let msg = match time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(err))) => {
                warn!(task_id, error = %err, "WS read failed; reader exiting");
                break;
            }
            Ok(None) => break, // stream closed
            Err(_) => break,   // drain deadline
        };
        let body: Vec<u8> = match msg {
            WsMessage::Text(t) => t.into_bytes(),
            WsMessage::Binary(b) => b,
            _ => continue, // ping/pong/close carry no execution report
        };
        if let Some(clord_id) = clordid_from_json(&body) {
            emit_response(
                &telemetry,
                &pending,
                &session_id,
                &submission_id,
                &worker_id,
                task_id,
                &clord_id,
            )
            .await;
        }
    }

    Ok(0)
}

/// order_shape performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.

pub(crate) fn order_shape(profile: BotProfile, seq: u32, rng: &mut SmallRng) -> (u64, u64, Side) {
    match profile {
        BotProfile::Hft => {
            let side = if seq % 2 == 0 { Side::Sell } else { Side::Buy };
            // Price straddles the 10_000 mid (both sides draw from the same band),
            // so a buy can land above a resting sell — and a sell below a resting
            // bid — and they cross and trade. This churns the reference order book
            // (filled orders leave it), keeping the validator's book bounded, and
            // produces realistic fills instead of a permanently two-sided book that
            // never matches. Orders far from the mid still rest; near it they cross.
            let offset = rng.gen_range(0i64..25) - 12; // -12..=+12 around the mid
            let price = (10_000_i64 + offset) as u64;
            (price, rng.gen_range(10..50), side)
        }
        BotProfile::Retail => {
            let side = if rng.gen_bool(0.5) {
                Side::Buy
            } else {
                Side::Sell
            };
            let drift = rng.gen_range(0..50);
            let price = match side {
                Side::Buy => 10_000 - drift,
                Side::Sell => 10_000 + drift,
            };
            (price, rng.gen_range(1..10), side)
        }
        BotProfile::Institutional => {
            let side = if rng.gen_bool(0.5) {
                Side::Buy
            } else {
                Side::Sell
            };
            let drift = rng.gen_range(20..100);
            let price = match side {
                Side::Buy => 10_000 - drift,
                Side::Sell => 10_000 + drift,
            };
            (price, rng.gen_range(100..500), side)
        }
    }
}

/// instant_from_unix_nanos performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn instant_from_unix_nanos(target_ns: u64) -> Instant {
    let now_ns = unix_nanos();
    if target_ns <= now_ns {
        Instant::now()
    } else {
        Instant::now() + Duration::from_nanos(target_ns - now_ns)
    }
}

/// TargetClient enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
enum TargetClient {
    Fix(FixConnection),
    Rest(TcpStream),
    Ws(WebSocketStream<MaybeTlsStream<TcpStream>>),
}

impl TargetClient {
    /// connect performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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
                let (read_half, write_half) = tokio::io::split(stream);
                Ok(Self::Fix(FixConnection {
                    read_half,
                    write_half,
                }))
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
                if let tokio_tungstenite::MaybeTlsStream::Plain(ref tcp) = ws.get_ref() {
                    let _ = tcp.set_nodelay(true);
                }
                Ok(Self::Ws(ws))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    /// http_response_framed_by_content_length_and_clordid_extracted performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn http_response_framed_by_content_length_and_clordid_extracted() {
        let body = br#"{"cl_ord_id":"sess_1_2_O","exec_type":"2","fill_qty":5,"fill_price":10000}"#;
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            std::str::from_utf8(body).unwrap()
        );
        let (clord, consumed) = next_http_response(resp.as_bytes()).expect("complete response");
        assert_eq!(clord.as_deref(), Some("sess_1_2_O"));
        assert_eq!(consumed, resp.len());
    }

    #[test]
    /// http_two_pipelined_responses_drain_in_order performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn http_two_pipelined_responses_drain_in_order() {
        let one = "HTTP/1.1 200 OK\r\nContent-Length: 22\r\n\r\n{\"cl_ord_id\":\"ord-A\"}\r\n";
        let two = "HTTP/1.1 200 OK\r\nContent-Length: 22\r\n\r\n{\"cl_ord_id\":\"ord-B\"}\r\n";
        let mut buf = format!("{one}{two}").into_bytes();
        let (a, n1) = next_http_response(&buf).expect("first");
        assert_eq!(a.as_deref(), Some("ord-A"));
        buf.drain(..n1);
        let (b, _) = next_http_response(&buf).expect("second");
        assert_eq!(b.as_deref(), Some("ord-B"));
    }

    #[test]
    /// http_incomplete_response_returns_none performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn http_incomplete_response_returns_none() {
        let partial = "HTTP/1.1 200 OK\r\nContent-Length: 50\r\n\r\n{\"cl_ord_id\":";
        assert!(next_http_response(partial.as_bytes()).is_none());
    }

    #[test]
    /// http_chunked_response_clordid_extracted performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn http_chunked_response_clordid_extracted() {
        let json = "{\"cl_ord_id\":\"ord-C\"}";
        let resp = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
            json.len(),
            json
        );
        let (clord, _) = next_http_response(resp.as_bytes()).expect("chunked complete");
        assert_eq!(clord.as_deref(), Some("ord-C"));
    }

    #[test]
    /// ws_json_payload_clordid_extracted performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn ws_json_payload_clordid_extracted() {
        let payload = br#"{"cl_ord_id":"ord-ws-1","exec_type":"0"}"#;
        assert_eq!(clordid_from_json(payload).as_deref(), Some("ord-ws-1"));
        assert_eq!(clordid_from_json(b"not json"), None);
        assert_eq!(clordid_from_json(b"{\"other\":1}"), None);
    }

    #[tokio::test]
    /// resolves_hostname_target performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn resolves_hostname_target() {
        let addr = resolve_target("localhost", 9876)
            .await
            .expect("localhost must resolve");
        assert_eq!(addr.port(), 9876);
        assert!(addr.ip().is_loopback(), "expected loopback, got {addr}");
    }

    #[tokio::test]
    /// resolves_ip_literal_target performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn resolves_ip_literal_target() {
        let addr = resolve_target("127.0.0.1", 8080)
            .await
            .expect("ip literal must resolve");
        assert_eq!(addr, "127.0.0.1:8080".parse::<SocketAddr>().unwrap());
    }

    #[tokio::test]
    /// cluster_dns_name_reaches_resolver performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn cluster_dns_name_reaches_resolver() {
        let err = resolve_target("algo-sess-123.sandbox.svc.cluster.local", 8080)
            .await
            .expect_err("unresolvable off-cluster");
        assert!(
            err.to_string().contains("resolve target host"),
            "expected a resolver error, got: {err}"
        );
    }

    /// valid_spec performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn valid_spec() -> WorkloadSpec {
        WorkloadSpec {
            session_id: "sess-1".into(),
            submission_id: "sub-1".into(),
            contestant_id: "team-1".into(),
            target_host: "127.0.0.1".into(),
            target_port: 8080,
            protocol: Protocol::Fix,
            worker_index: 0,
            worker_count: 1,
            global_seed: 42,
            fix_version: "FIX.4.2".into(),
            connect_timeout_ms: 1500,
            write_timeout_ms: 250,
            barrier_epoch_ns: 0,
            tasks: vec![TaskSpec {
                task_id: 1,
                profile: BotProfile::Hft,
                target_rps: 10,
                start_offset_ns: 0,
                duration_ns: 1_000_000_000,
                market_pct: 0,
                cancel_pct: 0,
                replace_pct: 0,
            }],
        }
    }

    #[test]
    /// validate_spec_rejects_submission_id_with_wire_unsafe_chars performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn validate_spec_rejects_submission_id_with_wire_unsafe_chars() {
        let mut spec = valid_spec();
        spec.submission_id = "sub/1".into();

        let err = validate_spec(&Config::default(), &spec).expect_err("spec should be rejected");
        assert!(err.to_string().contains("submission_id"));
    }

    #[test]
    /// cancel_replace_frames_carry_orig_order_id_new_orders_empty performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn cancel_replace_frames_carry_orig_order_id_new_orders_empty() {
        use content::Action;

        let new = Action::NewLimit {
            seq: 1,
            price: 10_000,
            qty: 5,
            side: Side::Buy,
        };
        let frame = render_frame("FIX.4.2", "sess1", "host", 7, &new);
        assert_eq!(frame.orig_order_id, "", "new limit must have empty orig");

        let market = Action::NewMarket {
            seq: 2,
            qty: 5,
            side: Side::Buy,
        };
        let frame = render_frame("FIX.4.2", "sess1", "host", 7, &market);
        assert_eq!(frame.orig_order_id, "", "market must have empty orig");

        let cancel = Action::Cancel {
            seq: 3,
            orig_order_id: "sess1_7_1_O".into(),
            price: 10_000,
            qty: 5,
            side: Side::Buy,
        };
        let frame = render_frame("FIX.4.2", "sess1", "host", 7, &cancel);
        assert_eq!(frame.orig_order_id, "sess1_7_1_O");

        let replace = Action::Replace {
            seq: 4,
            orig_order_id: "sess1_7_1_O".into(),
            price: 10_001,
            qty: 5,
            side: Side::Buy,
        };
        let frame = render_frame("FIX.4.2", "sess1", "host", 7, &replace);
        assert_eq!(frame.orig_order_id, "sess1_7_1_O");

        let pending = PendingOrder {
            order_id: frame.order_id.clone(),
            orig_order_id: frame.orig_order_id.clone(),
            target_send_ts_ns: 0,
            send_ts_ns: 0,
            barrier_epoch_ns: 0,
            price: frame.price,
            qty: frame.qty,
            side: frame.side,
            payload_type: frame.payload_type,
            ord_type: frame.ord_type,
        };
        assert_eq!(pending.orig_order_id, "sess1_7_1_O");
    }

    #[test]
    /// barrier_group_is_per_worker_not_per_session performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn barrier_group_is_per_worker_not_per_session() {
        let a = barrier_group("bot-fleet", "sess-1", "worker-7");
        let b = barrier_group("bot-fleet", "sess-2", "worker-7");

        assert!(
            !a.contains("sess-1"),
            "barrier group must not embed session_id, got {a}"
        );
        assert_eq!(
            a, b,
            "barrier group must be stable across sessions for one worker"
        );
        assert_eq!(a, "bot-fleet-barrier-worker-7");
    }

    #[tokio::test]
    /// cancelled_token_stops_send_loop performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn cancelled_token_stops_send_loop() {
        let cancel = CancelToken::new();
        assert!(!cancel.is_cancelled(), "fresh token is not cancelled");

        cancel.cancel();
        assert!(cancel.is_cancelled(), "cancelled token reports cancelled");

        assert!(
            should_stop_sending(&cancel, u64::MAX),
            "cancelled token must stop the send loop before the deadline"
        );
        let live = CancelToken::new();
        assert!(!should_stop_sending(&live, u64::MAX));
        assert!(should_stop_sending(&live, 0));

        tokio::time::timeout(Duration::from_secs(1), cancel.cancelled())
            .await
            .expect("cancelled() must resolve promptly on a cancelled token");
    }

    #[test]
    /// validate_spec_rejects_duration_that_would_breach_poll_interval performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn validate_spec_rejects_duration_that_would_breach_poll_interval() {
        let config = Config::default();

        let mut ok = valid_spec();
        ok.tasks[0].duration_ns = 1_000_000_000; // 1s
        validate_spec(&config, &ok).expect("short workload must be accepted");

        let mut over = valid_spec();
        over.tasks[0].duration_ns = config.max_poll_interval.as_nanos() as u64;
        let err =
            validate_spec(&config, &over).expect_err("over-ceiling workload must be rejected");
        assert!(
            err.to_string().contains("max.poll.interval.ms")
                || err.to_string().contains("poll interval"),
            "error should reference the poll-interval ceiling, got: {err}"
        );

        assert!(worst_case_wall_time_ns(&over) > over.tasks[0].duration_ns);
    }

    #[test]
    /// ready_key_groups_by_session_and_worker performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn ready_key_groups_by_session_and_worker() {
        let signal = ReadySignal {
            session_id: "sess-1".into(),
            submission_id: "sub-1".into(),
            worker_id: "worker-1".into(),
            worker_index: 0,
            worker_count: 1,
            task_count: 1,
            connected_count: 1,
            ready_at_unix_nanos: 123,
        };

        assert_eq!(kafka::ready_key(&signal), "sess-1:worker-1");
    }
}
