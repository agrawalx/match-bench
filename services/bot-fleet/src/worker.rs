use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use futures::SinkExt;
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

/// MAX_TASKS_PER_WORKER caps the number of TCP connections one worker pod
/// holds for a single session. Matches the controller's MaxTasksPerWorker
/// constant (services/bot-fleet-controller/internal/controller/runner.go).
const MAX_TASKS_PER_WORKER: usize = 1000;

/// RESPONSE_TIMEOUT_NS bounds how long a FIX task waits for a response
/// before the watchdog evicts the pending entry and emits OrderSentEvent
/// with timed_out=true.
///
/// 5s is the same default the validator/ingester use for "lost response"
/// semantics. Tuning knob — judges can shorten for faster CO detection
/// (at the cost of more false timeouts on legitimately slow contestants)
/// or lengthen for higher tolerance.
const RESPONSE_TIMEOUT_NS: u64 = 5_000_000_000;

/// BARRIER_WAIT bounds how long run_workload blocks on the barrier before
/// firing. It is part of the worst-case wall time the L39 poll-interval guard
/// accounts for, so it is a named constant shared between the guard and the
/// wait_for_barrier call.
const BARRIER_WAIT: Duration = Duration::from_secs(120);

/// CancelToken is a clonable shutdown signal threaded into the send loops.
/// A SIGTERM/SIGINT handler cancels it so in-flight workloads stop sending and
/// TelemetrySink::close runs to flush the final batch before the pod exits
/// (L43). Built on tokio::sync::watch so every loop can both poll it
/// (`is_cancelled`) and await it (`cancelled`) without an extra dependency.
#[derive(Clone)]
struct CancelToken {
    tx: Arc<watch::Sender<bool>>,
    rx: watch::Receiver<bool>,
}

impl CancelToken {
    fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self {
            tx: Arc::new(tx),
            rx,
        }
    }

    /// cancel flips the token; idempotent across repeated signals.
    fn cancel(&self) {
        let _ = self.tx.send(true);
    }

    /// is_cancelled is the cheap poll the send loops check each iteration.
    fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    /// cancelled resolves once the token is cancelled. Resolves immediately if
    /// it is already cancelled, so a loop racing it against a sleep cannot hang
    /// past shutdown.
    async fn cancelled(&self) {
        let mut rx = self.rx.clone();
        if *rx.borrow() {
            return;
        }
        // changed() errors only if the sender dropped; the Arc keeps it alive
        // for the token's lifetime, so a wait-then-recheck loop is sufficient.
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                return;
            }
        }
    }
}

/// should_stop_sending is the send-loop guard: stop when the token is cancelled
/// (graceful shutdown) or the task's wall-clock deadline has passed. Factored
/// out so the cancellation plumbing is unit-testable without a live socket.
fn should_stop_sending(cancel: &CancelToken, task_end_ns: u64) -> bool {
    cancel.is_cancelled() || unix_nanos() >= task_end_ns
}

/// run starts the bot-fleet worker loop.
/// It consumes workload assignments, executes each one, and exits on a
/// shutdown signal (SIGINT/Ctrl-C or SIGTERM from kubelet).
pub async fn run(config: Config) -> Result<()> {
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

    // Two producers — control-plane (bot.ready) gets acks=all + idempotence
    // per architecture §6.5; telemetry (orders.sent) gets acks=1 + batching
    // for throughput. Splitting also keeps a slow control-plane broker from
    // back-pressuring the telemetry queue and vice-versa.
    let control_producer = kafka::control_producer(&config.kafka_brokers)?;
    let telemetry_producer = kafka::telemetry_producer(&config.kafka_brokers)?;
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

    // L43: handle SIGTERM (kubelet's graceful-stop signal) as well as
    // SIGINT/Ctrl-C. As PID 1 with no SIGTERM handler the worker would ignore
    // the signal and be SIGKILLed at the grace deadline, truncating the final
    // telemetry batch. A background task watches both signals and cancels the
    // shared token; in-flight send loops observe it and stop, after which
    // TelemetrySink::close flushes the last batch.
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
                    // Log + continue rather than process::exit(1). The
                    // previous behaviour killed the pod on the first bad
                    // workload, forcing a restart and a KEDA reschedule
                    // delay. Continuing keeps the worker available for the
                    // next benchmark.requested message — the controller's
                    // fan-in path already handles the missing worker via
                    // ReadyDeadline + degraded fan-in, so dropping this one
                    // workload is harmless at the fleet level.
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

/// run_workload prepares one assignment, publishes readiness, waits for the
/// barrier, and runs every TaskSpec as an independent tokio task.
///
/// Lifecycle (matches architecture_v2.md §"Load Scenarios" → Task lifecycle):
///   1. Pre-warm: open every TaskSpec's TCP connection at barrier time.
///      Cold-start cost is paid before any task fires so the spike/ramp
///      measurements are not contaminated by connect latency.
///   2. Publish ReadySignal once every connection is open.
///   3. Wait for BarrierEvent; the controller's barrier_epoch_ns becomes
///      the absolute go-time.
///   4. Each task spawns its own tokio task that:
///        - sleeps until (barrier_epoch + task.start_offset_ns)
///        - paces sends at task.target_rps (fixed-interval, no jitter)
///        - exits at (barrier_epoch + task.start_offset_ns + task.duration_ns)
///   5. Telemetry flushes on workload exit.
async fn run_workload(
    config: &Config,
    control_producer: &KafkaProducer,
    telemetry_producer: &KafkaProducer,
    spec: WorkloadSpec,
    cancel: CancelToken,
) -> Result<()> {
    validate_spec(config, &spec)?;

    // L41: a stable per-worker barrier group. wait_for_barrier already filters
    // by session_id, so the group need not be unique per session — a per-session
    // group would be created fresh every run and never deleted, leaking consumer
    // groups on the broker.
    let barrier_group = barrier_group(&config.consumer_group, &spec.session_id, &config.worker_id);
    let barrier_consumer = kafka::consumer(
        &config.kafka_brokers,
        &barrier_group,
        &[&config.barrier_topic],
    )?;

    info!(
        session_id = %spec.session_id,
        protocol = ?spec.protocol,
        task_count = spec.tasks.len(),
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
    // ReadySignal is control-plane — use the acks=all producer so a leader
    // failure between ack and replication cannot silently drop the signal
    // and stall the controller's fan-in.
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

    // TelemetrySink uses the acks=1 producer — orders.sent is high-volume
    // and loss-tolerant; rare drops surface as HDR-histogram gaps, not as
    // wrong scores.
    let telemetry = TelemetrySink::new(
        telemetry_producer.clone(),
        config.orders_sent_topic.clone(),
        spec.session_id.clone(),
        config.worker_id.clone(),
        config.telemetry_channel_capacity,
        config.telemetry_flush_interval,
        config.telemetry_batch_size,
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
    // Always close the sink — even on a cancelled run — so the final telemetry
    // batch is flushed before the worker exits (L43).
    telemetry.close().await?;
    result
}

/// validate_spec enforces local worker limits and rejects identifiers that
/// would corrupt generated FIX or JSON payloads.
fn validate_spec(config: &Config, spec: &WorkloadSpec) -> Result<()> {
    if spec.tasks.is_empty() {
        return Err(
            crate::errors::BotFleetError::ValidationError("tasks list is empty".into()).into(),
        );
    }
    if spec.tasks.len() > MAX_TASKS_PER_WORKER {
        return Err(crate::errors::BotFleetError::ValidationError(format!(
            "task count {} exceeds MAX_TASKS_PER_WORKER {}",
            spec.tasks.len(),
            MAX_TASKS_PER_WORKER
        ))
        .into());
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
    // L39: the workload-assignment offset is committed only after run_workload
    // returns. If the worst-case wall time (barrier wait + last task's
    // offset+duration + response drain) exceeds the consumer's
    // max.poll.interval.ms, Kafka rebalances the partition mid-run and
    // re-delivers the assignment — duplicate execution. Reject such a spec up
    // front rather than committing before the work (which would drop
    // at-least-once delivery).
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

/// barrier_group builds the barrier consumer group id. It is intentionally
/// per-worker (not per-session): wait_for_barrier filters by session_id, so a
/// session-scoped group would only leak a fresh, never-deleted consumer group
/// on the broker for every run (L41). `_session_id` is accepted to keep the
/// call site explicit about what is deliberately excluded.
fn barrier_group(consumer_group: &str, _session_id: &str, worker_id: &str) -> String {
    format!("{consumer_group}-barrier-{worker_id}")
}

/// worst_case_wall_time_ns is the longest run_workload can block before its
/// Kafka offset is committed: the barrier wait, plus the latest-finishing
/// task's (start_offset + duration), plus the post-deadline response drain.
/// The L39 guard compares this against the consumer's max.poll.interval.ms.
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

/// connect_tasks opens one TCP connection per TaskSpec at barrier time. All
/// connections are pre-warmed before any task fires (the actual sending is
/// gated by per-task start_offset_ns inside the firing loop), so cold-start
/// latency cannot leak into spike or ramp measurements.
async fn connect_tasks(spec: &WorkloadSpec) -> Result<Vec<ConnectedTask>> {
    // The controller supplies target_host as the algo Service's cluster DNS
    // name (algo-{session_id}.sandbox.svc.cluster.local), not a numeric IP, so
    // we must resolve it — SocketAddr's FromStr only accepts IP literals and
    // would reject every in-cluster target. lookup_host handles both DNS names
    // and bare IPs, so local examples that pass 127.0.0.1 still work. Resolve
    // once here and share the result across every task's connection.
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
                // Connection failure for one task does not fail the workload —
                // log + drop and continue. The dropped task contributes to
                // the error-rate metric visible in Grafana via the slog stream.
                metrics::connect_failure();
                warn!(error = %err, "task connect failed; dropping");
            }
        }
    }
    Ok(tasks)
}

/// resolve_target turns the controller-supplied (host, port) into a concrete
/// SocketAddr. host is the algo Service's cluster DNS name in production
/// (algo-{session_id}.sandbox.svc.cluster.local) and a bare IP in local
/// examples; lookup_host handles both. This must NOT be `host:port`.parse()
/// — SocketAddr::FromStr only accepts numeric IP literals and rejects every
/// in-cluster DNS target, which would silently drop every workload.
async fn resolve_target(host: &str, port: u16) -> Result<SocketAddr> {
    let target = format!("{host}:{port}");
    let mut addrs = lookup_host(&target)
        .await
        .with_context(|| format!("resolve target host {target}"))?;
    addrs
        .next()
        .ok_or_else(|| anyhow::anyhow!("no addresses resolved for target host {target}"))
}

/// fire_workload spawns one tokio task per ConnectedTask. Each tokio task
/// waits until its task-specific start time, then paces sends at the task's
/// target_rps until its duration expires.
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
                // Mid-flight task failure (TCP reset, write timeout). Logged
                // here; counted as a dropped task. Does not fail the
                // workload — partial data is still informative and the
                // controller's status pipeline reports session completion
                // regardless of per-task errors.
                warn!(error = %err, "task send loop failed; dropped");
            }
        }
    }
    info!(session_id = %spec.session_id, sent = sent_total, "workload completed");
    Ok(())
}

/// ConnectedTask owns one established connection and its TaskSpec. For FIX
/// the underlying TcpStream has been split into independent read/write halves
/// so the response-capture path can drain inbound bytes in parallel with the
/// fixed-interval pacer.
struct ConnectedTask {
    task: TaskSpec,
    target_host: String,
    client: TargetClient,
}

/// PendingOrder is the per-order context the FIX write loop hands off to the
/// reader/watchdog. We keep the full OrderSentEvent payload here (price, qty,
/// side, order_id) so that the eventual emission — whether on first response
/// or watchdog timeout — has everything it needs without round-tripping back
/// to the writer.
#[derive(Clone)]
struct PendingOrder {
    order_id: String,
    /// Bot-authoritative cancel/replace target (the original ClOrdID), empty
    /// for new/market orders. Carried so the reader/watchdog can stamp it onto
    /// the emitted OrderSentEvent without round-tripping back to the writer.
    orig_order_id: String,
    target_send_ts_ns: u64,
    send_ts_ns: u64,
    price: u64,
    qty: u64,
    side: Side,
    payload_type: PayloadType,
    ord_type: OrdType,
}

/// PendingMap is a shared map of ClOrdID → PendingOrder. Locked with a std
/// Mutex; lock guards never cross an await boundary in any of the three
/// loops that share it, so the std (non-async) mutex is safe and faster than
/// tokio::sync::Mutex for this access pattern.
type PendingMap = Arc<Mutex<HashMap<String, PendingOrder>>>;

impl ConnectedTask {
    /// run dispatches to the protocol-specific task driver. FIX gets the
    /// full three-loop (writer + reader + watchdog) setup so r9 is captured.
    /// REST/WS keep the legacy single-loop write-only path with r9=0 and
    /// timed_out=false (response capture for non-FIX protocols is a v2 item).
    #[allow(clippy::too_many_arguments)]
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
            TargetClient::Rest(stream) => {
                run_writeonly_task(self.task, WriteOnly::Rest(stream), ctx).await
            }
            TargetClient::Ws(ws) => run_writeonly_task(self.task, WriteOnly::Ws(ws), ctx).await,
        }
    }
}

/// TaskContext bundles the workload-level immutable knobs the task drivers
/// need. Avoids 10-parameter function signatures.
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

/// FixConnection holds the two halves of a split TcpStream so writer and
/// reader loops can own their respective ends independently.
struct FixConnection {
    read_half: ReadHalf<TcpStream>,
    write_half: WriteHalf<TcpStream>,
}

/// run_fix_task spawns the three tokio tasks (writer, reader, watchdog) and
/// waits for them to terminate. Writer exits at task deadline; reader and
/// watchdog continue for RESPONSE_TIMEOUT past the task deadline so stragglers
/// either get matched or get evicted with timed_out=true.
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
    set.spawn(fix_watchdog_loop(
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
                // Sub-task error (e.g. one of the three loops returned an
                // error rather than panicking). Logged but not propagated —
                // the other loops keep running, and missing r9 manifests as
                // timed_out=true via the watchdog.
                warn!(task_id = task.task_id, error = %err, "FIX sub-task ended with error");
            }
        }
    }
    Ok(sent_total)
}

/// fix_write_loop is the fixed-interval pacer. Identical to the legacy
/// loop in shape, but instead of emitting OrderSentEvent on each write it
/// hands off PendingOrder to the shared pending map. Emission is the
/// reader/watchdog's job.
/// mix_from_task reads the per-task order-type mix percentages off the TaskSpec.
fn mix_from_task(task: &TaskSpec) -> content::OrderMix {
    content::OrderMix {
        market_pct: task.market_pct,
        cancel_pct: task.cancel_pct,
        replace_pct: task.replace_pct,
    }
}

/// render_frame turns a generated content::Action into the wire frame (FIX +
/// REST + WS payloads) via the fix builders. A cancel/replace passes the resting
/// order's full ClOrdID as OrigClOrdID so tag 41 matches the original tag 11.
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
        // Stop at the task deadline or on a shutdown signal (L43). Breaking
        // here lets the writer exit so the reader/watchdog can drain and
        // TelemetrySink::close can flush the final batch.
        if should_stop_sending(&cancel, task_end_ns) {
            break;
        }

        // t0 — the schedule's intended fire time. Captured BEFORE the sleep
        // so it reflects the schedule, not what the clock actually says when
        // we wake up. Under coordinated omission `send_ts_ns - target_send_ts_ns`
        // grows monotonically; that gap is the CO signal.
        let target_send_ts_ns = next_send_ns;

        // Race the inter-send sleep against cancellation so a SIGTERM during a
        // low-rps pacing gap stops the loop promptly instead of after the gap.
        tokio::select! {
            _ = time::sleep_until(instant_from_unix_nanos(next_send_ns)) => {}
            _ = cancel.cancelled() => break,
        }

        let action = generator.next();
        let seq = action.seq();
        let mut frame = render_frame(
            &fix_version,
            &session_id,
            &target_host,
            u64::from(task.task_id),
            &action,
        );
        // Set FIX SendingTime (tag 52) to the actual transmit instant.
        // The frame builders emit a fixed-width epoch placeholder; patch_timestamp
        // rewrites those 21 bytes and delta-fixes the checksum in place.
        // Without this, every order ships SendingTime=19700101-00:00:00.000 and
        // a contestant FIX engine validating tag 52 freshness rejects it.
        frame.patch_timestamp(unix_nanos());

        // The pending insert must happen BEFORE the write so that a fast
        // contestant cannot reply before we've recorded the pending entry.
        // We patch send_ts_ns after the write succeeds.
        {
            let mut map = pending.lock().expect("pending map poisoned");
            map.insert(
                frame.order_id.clone(),
                PendingOrder {
                    order_id: frame.order_id.clone(),
                    orig_order_id: frame.orig_order_id.clone(),
                    target_send_ts_ns,
                    send_ts_ns: 0, // patched on successful write
                    price: frame.price,
                    qty: frame.qty,
                    side: frame.side,
                    payload_type: frame.payload_type,
                    ord_type: frame.ord_type,
                },
            );
        }

        match time::timeout(write_timeout, write_half.write_all(&frame.fix)).await {
            Ok(Ok(())) => {
                let send_ts_ns = unix_nanos();
                metrics::order_sent();
                // Patch send_ts_ns. Skipping the patch on a fast-arrived
                // response is acceptable — the response branch only reads
                // send_ts_ns which was 0; an r9 < t1 anomaly in the rare
                // contestant-faster-than-vDSO case is harmless.
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
                // Write failed — purge the pending entry we just inserted
                // so the watchdog does not emit a bogus timeout for it.
                pending
                    .lock()
                    .expect("pending map poisoned")
                    .remove(&frame.order_id);
                metrics::order_write_error();
                warn!(
                    task_id = task.task_id,
                    seq, error = %err, "FIX write failed; task writer exiting"
                );
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
                    seq, "FIX write timeout; task writer exiting"
                );
                return Ok(sent);
            }
        }

        next_send_ns = next_send_ns.saturating_add(interval_ns);
    }

    Ok(sent)
}

/// fix_read_loop drains the read half, parses ExecutionReport (35=8)
/// messages, matches ClOrdID to the pending map, and emits OrderSentEvent
/// with timed_out=false. Subsequent reports for the same ClOrdID are dropped
/// because the first remove() consumed the entry — that's the "first
/// response wins" rule.
///
/// Exits at `drain_end_ns` (= task_end + RESPONSE_TIMEOUT) so late stragglers
/// past the task deadline still get matched. Anything still pending at that
/// point is the watchdog's responsibility.
#[allow(clippy::too_many_arguments)]
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
                // Peer closed the connection — no more responses will arrive.
                // Stop reading; watchdog will time out any stragglers.
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
            // Only ExecutionReport (MsgType=8) carries an r9 signal. Other
            // FIX traffic (heartbeats, logon acks) is silently dropped.
            if msg.msg_type != b"8" {
                continue;
            }
            let Some(clord_id_bytes) = msg.clord_id else {
                continue;
            };
            let Ok(clord_id) = std::str::from_utf8(clord_id_bytes) else {
                continue;
            };

            // Lock scope: remove the entry, then drop the guard BEFORE the
            // telemetry.record().await call. Holding a std Mutex across an
            // await would risk deadlock under contention.
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
                })
                .await;
        }

        // Drop the consumed prefix and keep the carry-over tail in place.
        if consumed > 0 {
            buf.drain(..consumed);
        }

        // Guard against unbounded buffer growth — a contestant streaming
        // unparseable garbage without ever closing the socket would otherwise
        // pin memory forever. 1 MiB is a generous ceiling for ~6 KiB FIX
        // messages; if we hit it, drop the prefix and resync on the next
        // 8=FIX boundary by clearing the buffer.
        if buf.len() > 1_048_576 {
            warn!(task_id, "FIX read buffer overflow; resetting");
            buf.clear();
        }
    }

    Ok(0)
}

/// fix_watchdog_loop evicts pending orders whose send_ts_ns is older than
/// RESPONSE_TIMEOUT_NS. Evicted orders emit with timed_out=true and
/// recv_done_ts_ns=0 — the explicit marker that the response was lost rather
/// than received at instant 0.
///
/// Runs every WATCHDOG_TICK_MS until drain_end_ns. On final tick (when no
/// more sends are happening) it flushes everything still pending, regardless
/// of age.
#[allow(clippy::too_many_arguments)]
async fn fix_watchdog_loop(
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

        // Collect everything to evict, then drop the lock before awaiting
        // telemetry.record() for each one.
        let to_evict: Vec<PendingOrder> = {
            let mut map = pending.lock().expect("pending map poisoned");
            let mut evicted = Vec::new();
            map.retain(|_, p| {
                // send_ts_ns == 0 means the write hasn't returned yet; don't
                // evict — the writer is mid-call. The 5s timeout below would
                // catch it on the next tick anyway.
                let age_ns = if p.send_ts_ns == 0 {
                    0
                } else {
                    now_ns.saturating_sub(p.send_ts_ns)
                };
                // L40: on the FINAL tick evict every remaining entry — including one
                // whose write was still in flight (send_ts_ns == 0). The old guard
                // (`&& p.send_ts_ns > 0`) dropped such an order entirely (counted as
                // neither matched nor timed_out) before the loop broke; recording it
                // as timed_out is the correct offered-order accounting.
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

/// WriteOnly wraps the REST and WS connections that share the legacy
/// single-loop path (no response capture in v1).
enum WriteOnly {
    Rest(TcpStream),
    Ws(WebSocketStream<MaybeTlsStream<TcpStream>>),
}

impl WriteOnly {
    async fn write(&mut self, frame: &OrderFrame) -> Result<()> {
        match self {
            Self::Rest(stream) => stream
                .write_all(&frame.rest)
                .await
                .context("write REST order"),
            Self::Ws(ws) => ws
                .send(WsMessage::Binary(frame.ws_bytes.clone()))
                .await
                .context("write WS order"),
        }
    }
}

/// run_writeonly_task is the legacy single-loop path for REST and WS. The
/// emitted OrderSentEvent has recv_done_ts_ns=0 and timed_out=false; the
/// ingester is expected to treat that pair as "r9 not captured for this
/// protocol" rather than "lost response."
async fn run_writeonly_task(
    task: TaskSpec,
    mut client: WriteOnly,
    ctx: TaskContext,
) -> Result<u64> {
    let task_start_ns = ctx.barrier_epoch_ns.saturating_add(task.start_offset_ns);
    let task_end_ns = task_start_ns.saturating_add(task.duration_ns);
    time::sleep_until(instant_from_unix_nanos(task_start_ns)).await;

    let interval_ns = 1_000_000_000_u64 / u64::from(task.target_rps);
    let mut next_send_ns = task_start_ns;
    let mut generator = TaskGenerator::new(
        ctx.session_id.clone(),
        u64::from(task.task_id),
        task.profile,
        mix_from_task(&task),
        ctx.global_seed ^ u64::from(task.task_id),
    );
    let mut sent: u64 = 0;

    loop {
        // Stop at the task deadline or on a shutdown signal (L43).
        if should_stop_sending(&ctx.cancel, task_end_ns) {
            break;
        }
        let target_send_ts_ns = next_send_ns;

        tokio::select! {
            _ = time::sleep_until(instant_from_unix_nanos(next_send_ns)) => {}
            _ = ctx.cancel.cancelled() => break,
        }

        let action = generator.next();
        let seq = action.seq();
        // REST/WS transports send frame.rest / frame.ws_bytes, which do not
        // carry FIX tag 52, so no patch_timestamp is needed here.
        let frame = render_frame(
            &ctx.fix_version,
            &ctx.session_id,
            &ctx.target_host,
            u64::from(task.task_id),
            &action,
        );

        match time::timeout(ctx.write_timeout, client.write(&frame)).await {
            Ok(Ok(())) => {
                let send_ts_ns = unix_nanos();
                ctx.telemetry
                    .record(OrderSentEvent {
                        session_id: ctx.session_id.clone(),
                        submission_id: ctx.submission_id.clone(),
                        worker_id: ctx.worker_id.clone(),
                        task_id: task.task_id,
                        order_id: frame.order_id,
                        target_send_ts_ns,
                        send_ts_ns,
                        recv_done_ts_ns: 0,
                        timed_out: false,
                        price: frame.price,
                        qty: frame.qty,
                        side: frame.side,
                        payload_type: frame.payload_type,
                        ord_type: frame.ord_type,
                        orig_order_id: frame.orig_order_id,
                    })
                    .await;
                sent += 1;
            }
            Ok(Err(err)) => {
                warn!(task_id = task.task_id, seq, error = %err, "task write failed; task exiting");
                return Ok(sent);
            }
            Err(_) => {
                warn!(
                    task_id = task.task_id,
                    seq, "task write timeout; task exiting"
                );
                return Ok(sent);
            }
        }

        next_send_ns = next_send_ns.saturating_add(interval_ns);
    }

    Ok(sent)
}

/// order_shape produces deterministic price, quantity, and side values for
/// the given profile and sequence number. v1 shapes are participant-typical
/// but not exhaustive — judges can rebalance via the scenarios table without
/// editing this function.
pub(crate) fn order_shape(profile: BotProfile, seq: u32, rng: &mut SmallRng) -> (u64, u64, Side) {
    match profile {
        BotProfile::Hft => {
            // Market-making behaviour: tight spread around 10_000, alternating
            // side per sequence so the book stays roughly balanced.
            let side = if seq % 2 == 0 { Side::Sell } else { Side::Buy };
            let spread = rng.gen_range(1..25);
            let price = match side {
                Side::Buy => 10_000 - spread,
                Side::Sell => 10_000 + spread,
            };
            (price, rng.gen_range(10..50), side)
        }
        BotProfile::Retail => {
            // Retail: small, random side, modest sizes, close to mid.
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
            // Institutional: large block sizes, conservative pricing further
            // from mid to exercise the depth of the book.
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

/// TargetClient stores the active connection for the selected workload
/// protocol. The FIX variant carries the connection already split into
/// read/write halves so the response capture path can drain inbound bytes in
/// parallel with the fixed-interval pacer.
enum TargetClient {
    Fix(FixConnection),
    Rest(TcpStream),
    Ws(WebSocketStream<MaybeTlsStream<TcpStream>>),
}

impl TargetClient {
    /// connect establishes the protocol-specific connection and performs FIX
    /// logon when required. For FIX it then splits the TcpStream into read +
    /// write halves so the response capture path can drain inbound bytes in
    /// parallel with the fixed-interval pacer.
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
                // split() consumes the stream and hands ownership of each
                // half to its respective tokio task — the writer paces sends,
                // the reader drains responses. They share the same socket
                // but no further synchronisation is needed since reads and
                // writes are independent in the kernel.
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
                // WebSocket path lacked nodelay
                // that FIX/REST have, causing up to 40ms Nagle coalescing delay
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

    // Regression: the controller passes target_host as a DNS name
    // (algo-{session_id}.sandbox.svc.cluster.local), never a numeric IP.
    // The old code did `"host:port".parse::<SocketAddr>()`, whose FromStr
    // only accepts IP literals, so every in-cluster workload failed to
    // connect. resolve_target must resolve a hostname. "localhost" is the
    // portable stand-in that's guaranteed resolvable in CI.
    #[tokio::test]
    async fn resolves_hostname_target() {
        let addr = resolve_target("localhost", 9876)
            .await
            .expect("localhost must resolve");
        assert_eq!(addr.port(), 9876);
        assert!(addr.ip().is_loopback(), "expected loopback, got {addr}");
    }

    // Bare IP literals (used by the local examples) must keep working.
    #[tokio::test]
    async fn resolves_ip_literal_target() {
        let addr = resolve_target("127.0.0.1", 8080)
            .await
            .expect("ip literal must resolve");
        assert_eq!(addr, "127.0.0.1:8080".parse::<SocketAddr>().unwrap());
    }

    // The exact production-shaped name that broke under SocketAddr::parse.
    // It won't resolve off-cluster, so we assert we get a clean resolver
    // error (not a parse rejection) — i.e. the code path now reaches DNS.
    #[tokio::test]
    async fn cluster_dns_name_reaches_resolver() {
        let err = resolve_target("algo-sess-123.sandbox.svc.cluster.local", 8080)
            .await
            .expect_err("unresolvable off-cluster");
        assert!(
            err.to_string().contains("resolve target host"),
            "expected a resolver error, got: {err}"
        );
    }

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
    fn validate_spec_rejects_submission_id_with_wire_unsafe_chars() {
        let mut spec = valid_spec();
        spec.submission_id = "sub/1".into();

        let err = validate_spec(&Config::default(), &spec).expect_err("spec should be rejected");
        assert!(err.to_string().contains("submission_id"));
    }

    // Repro for **H13** (producer side): a cancel/replace must carry the
    // targeted resting order's ClOrdID through to OrderSentEvent.orig_order_id
    // so the validator can key its reference engine off the bot-authoritative
    // target. A new/market order must emit an empty orig_order_id.
    #[test]
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

        // The PendingOrder the writer hands off (and thus the emitted
        // OrderSentEvent) copies orig_order_id verbatim from the frame.
        let pending = PendingOrder {
            order_id: frame.order_id.clone(),
            orig_order_id: frame.orig_order_id.clone(),
            target_send_ts_ns: 0,
            send_ts_ns: 0,
            price: frame.price,
            qty: frame.qty,
            side: frame.side,
            payload_type: frame.payload_type,
            ord_type: frame.ord_type,
        };
        assert_eq!(pending.orig_order_id, "sess1_7_1_O");
    }

    // Repro for **L41**: the barrier consumer group must be stable per worker,
    // not per session. A per-session group `{group}-barrier-{session}-{worker}`
    // is created fresh for every run and never deleted, leaking consumer groups
    // on the broker. wait_for_barrier already filters by session_id, so a stable
    // per-worker group is sufficient.
    #[test]
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

    // Repro for **L43**: a SIGTERM-driven shutdown must be able to stop the
    // send loops promptly so TelemetrySink::close can flush the final batch
    // before the pod exits. The send loops gate on a CancelToken; an
    // already-cancelled token must make the loop guard return "stop" without
    // performing another send. (A full async fire path needs live TCP, so we
    // unit-test the cancellation plumbing the loops actually consult.)
    #[tokio::test]
    async fn cancelled_token_stops_send_loop() {
        let cancel = CancelToken::new();
        assert!(!cancel.is_cancelled(), "fresh token is not cancelled");

        cancel.cancel();
        assert!(cancel.is_cancelled(), "cancelled token reports cancelled");

        // The send loops break when the token is cancelled; assert the guard.
        assert!(
            should_stop_sending(&cancel, u64::MAX),
            "cancelled token must stop the send loop before the deadline"
        );
        // An un-cancelled token before the deadline keeps sending.
        let live = CancelToken::new();
        assert!(!should_stop_sending(&live, u64::MAX));
        // A passed deadline stops regardless of cancellation.
        assert!(should_stop_sending(&live, 0));

        // cancelled() resolves immediately on an already-cancelled token, so a
        // loop awaiting it does not hang past shutdown.
        tokio::time::timeout(Duration::from_secs(1), cancel.cancelled())
            .await
            .expect("cancelled() must resolve promptly on a cancelled token");
    }

    // Repro for **L39**: the workload-assignment offset is committed only after
    // run_workload returns (barrier wait + scenario duration + drain). If that
    // worst-case wall time exceeds the consumer's max.poll.interval.ms, Kafka
    // rebalances the partition mid-run and re-delivers the assignment to another
    // worker — duplicate execution. validate_spec must reject a spec whose
    // worst-case wall time would breach the configured poll bound, and accept
    // one that fits.
    #[test]
    fn validate_spec_rejects_duration_that_would_breach_poll_interval() {
        let config = Config::default();

        // In-bounds: short duration fits comfortably under the poll ceiling.
        let mut ok = valid_spec();
        ok.tasks[0].duration_ns = 1_000_000_000; // 1s
        validate_spec(&config, &ok).expect("short workload must be accepted");

        // Over-ceiling: a duration so long that barrier-wait + duration + drain
        // exceeds max.poll.interval.ms must be rejected before it runs.
        let mut over = valid_spec();
        over.tasks[0].duration_ns = config.max_poll_interval.as_nanos() as u64;
        let err =
            validate_spec(&config, &over).expect_err("over-ceiling workload must be rejected");
        assert!(
            err.to_string().contains("max.poll.interval.ms")
                || err.to_string().contains("poll interval"),
            "error should reference the poll-interval ceiling, got: {err}"
        );

        // The worst-case wall time accounts for barrier wait + offset + duration
        // + drain, so it strictly exceeds the raw duration.
        assert!(worst_case_wall_time_ns(&over) > over.tasks[0].duration_ns);
    }

    #[test]
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
