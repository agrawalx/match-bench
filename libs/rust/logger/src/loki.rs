use std::{
    cell::RefCell,
    env, fs,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, RecvTimeoutError, SyncSender, TryRecvError},
        Arc, Mutex, Once,
    },
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use tracing::{
    field::Visit,
    span::{Attributes, Id, Record},
    Event, Level, Span, Subscriber,
};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{
    fmt, layer::Context, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer,
};

const DEFAULT_SERVICE_NAME: &str = "iicpc";
const DEFAULT_QUEUE_SIZE: usize = 10_000;
const DEFAULT_BATCH_SIZE: usize = 256;
const DEFAULT_BATCH_WAIT: Duration = Duration::from_millis(500);
const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_RETRIES: usize = 3;

static WARN_BATCH_SIZE_ONCE: Once = Once::new();

thread_local! {
    static CONTEXT_ATTRS: RefCell<Vec<Attr>> = const { RefCell::new(Vec::new()) };
    static JSON_BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    static LOKI_VALUES: RefCell<Vec<[String; 2]>> = const { RefCell::new(Vec::new()) };
}

#[derive(Clone, Debug)]
pub struct Config {
    pub service_name: String,
    pub environment: String,
    pub instance_id: String,
    pub version: String,
    pub loki_url: String,
    pub level: String,
    pub queue_size: usize,
    pub batch_size: usize,
    pub batch_wait: Duration,
    pub http_timeout: Duration,
    pub max_retries: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            environment: env_or("ENVIRONMENT", env_or("APP_ENV", "local")),
            instance_id: cached_hostname(),
            version: env_or("SERVICE_VERSION", "dev"),
            loki_url: env::var("LOKI_URL").unwrap_or_default(),
            level: env_or("LOG_LEVEL", "info"),
            queue_size: DEFAULT_QUEUE_SIZE,
            batch_size: DEFAULT_BATCH_SIZE,
            batch_wait: DEFAULT_BATCH_WAIT,
            http_timeout: DEFAULT_HTTP_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }
}

pub fn default_config() -> Config {
    Config::default()
}

pub fn init(service_name: &str) -> Option<LokiGuard> {
    let mut config = default_config();
    config.service_name = service_name.to_string();
    init_with_config(config)
}

pub fn init_with_config(config: Config) -> Option<LokiGuard> {
    let config = normalize_config(config);
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(level_filter(&config.level)));
    let fmt_layer = fmt::layer().json();

    if config.loki_url.trim().is_empty() {
        if let Err(err) = tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .try_init()
        {
            eprintln!("logger init warning: {err}");
        }
        return None;
    }

    let client = LokiClient::new_with_config(config.clone());
    let guard = client.guard();
    if let Err(err) = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(LokiLayer { client, config })
        .try_init()
    {
        eprintln!("logger init warning: {err}");
    }
    Some(guard)
}

fn normalize_config(mut config: Config) -> Config {
    let defaults = default_config();
    if config.service_name.is_empty() {
        config.service_name = defaults.service_name;
    }
    if config.environment.is_empty() {
        config.environment = defaults.environment;
    }
    if config.instance_id.is_empty() {
        config.instance_id = defaults.instance_id;
    }
    if config.version.is_empty() {
        config.version = defaults.version;
    }
    if config.level.is_empty() {
        config.level = defaults.level;
    }
    if config.queue_size == 0 {
        config.queue_size = defaults.queue_size;
    }
    if config.batch_size == 0 {
        config.batch_size = defaults.batch_size;
    }
    if config.batch_size > config.queue_size {
        WARN_BATCH_SIZE_ONCE.call_once(|| {
            eprintln!(
                "loki client warning: BatchSize ({}) is larger than QueueSize ({}). Capping BatchSize to QueueSize ({}) to ensure flushing by count works.",
                config.batch_size, config.queue_size, config.queue_size
            );
        });
        config.batch_size = config.queue_size;
    }
    if config.batch_wait.is_zero() {
        config.batch_wait = defaults.batch_wait;
    }
    if config.http_timeout.is_zero() {
        config.http_timeout = defaults.http_timeout;
    }
    if config.max_retries == 0 {
        config.max_retries = defaults.max_retries;
    }
    config
}

#[derive(Clone)]
pub struct LokiClient {
    tx: SyncSender<WorkerMessage>,
    closed: Arc<AtomicBool>,
    queue_drops: Arc<AtomicU64>,
    send_drops: Arc<AtomicU64>,
    worker: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl LokiClient {
    pub fn new(base_url: &str) -> Self {
        let mut config = default_config();
        config.loki_url = base_url.to_string();
        Self::new_with_config(config)
    }

    pub fn new_with_config(config: Config) -> Self {
        let config = normalize_config(config);
        if !config.loki_url.trim().is_empty() {
            let parsed = reqwest::Url::parse(&config.loki_url)
                .unwrap_or_else(|err| panic!("invalid Loki URL {:?}: {err}", config.loki_url));
            if parsed.scheme().is_empty() || parsed.host_str().is_none() {
                panic!(
                    "invalid Loki URL {:?}: scheme and host are required",
                    config.loki_url
                );
            }
        }

        let (tx, rx) = mpsc::sync_channel(config.queue_size);
        let closed = Arc::new(AtomicBool::new(false));
        let queue_drops = Arc::new(AtomicU64::new(0));
        let send_drops = Arc::new(AtomicU64::new(0));
        let worker_closed = Arc::clone(&closed);
        let worker_drops = Arc::clone(&queue_drops);
        let worker_send_drops = Arc::clone(&send_drops);

        let handle = thread::spawn(move || {
            run_worker(rx, config, worker_closed, worker_drops, worker_send_drops);
        });

        Self {
            tx,
            closed,
            queue_drops,
            send_drops,
            worker: Arc::new(Mutex::new(Some(handle))),
        }
    }

    pub fn guard(&self) -> LokiGuard {
        LokiGuard {
            tx: self.tx.clone(),
            closed: Arc::clone(&self.closed),
            queue_drops: Arc::clone(&self.queue_drops),
            send_drops: Arc::clone(&self.send_drops),
            worker: Arc::clone(&self.worker),
        }
    }

    pub fn close(&self) {
        close_worker(
            &self.tx,
            &self.closed,
            &self.queue_drops,
            &self.send_drops,
            &self.worker,
        );
    }

    pub fn queue_drops(&self) -> u64 {
        self.queue_drops.load(Ordering::Relaxed)
    }

    pub fn send_drops(&self) -> u64 {
        self.send_drops.load(Ordering::Relaxed)
    }

    fn queue(&self, entry: LokiEntry) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        if self.tx.try_send(WorkerMessage::Entry(entry)).is_err() {
            let drops = self.queue_drops.fetch_add(1, Ordering::Relaxed) + 1;
            if drops == 1 || drops % 1000 == 0 {
                eprintln!("loki queue full, dropping Rust log line (dropped {drops} so far)");
            }
        }
    }
}

pub struct LokiGuard {
    tx: SyncSender<WorkerMessage>,
    closed: Arc<AtomicBool>,
    queue_drops: Arc<AtomicU64>,
    send_drops: Arc<AtomicU64>,
    worker: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl LokiGuard {
    pub fn close(&self) {
        close_worker(
            &self.tx,
            &self.closed,
            &self.queue_drops,
            &self.send_drops,
            &self.worker,
        );
    }
}

impl Drop for LokiGuard {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Attr {
    key: String,
    value: Value,
}

#[derive(Clone, Debug, Default)]
pub struct LogContext {
    attrs: Vec<Attr>,
}

impl LogContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_attrs(attrs: impl IntoIterator<Item = Attr>) -> Self {
        Self {
            attrs: attrs.into_iter().collect(),
        }
    }

    pub fn child(&self, attrs: impl IntoIterator<Item = Attr>) -> Self {
        let mut merged = self.attrs.clone();
        merged.extend(attrs);
        Self { attrs: merged }
    }

    pub fn scope<T>(&self, f: impl FnOnce() -> T) -> T {
        with_attrs(self.attrs.clone(), f)
    }

    pub fn span(&self) -> Span {
        let loki_context = serde_json::to_string(&self.attrs).unwrap_or_else(|_| "[]".to_string());
        tracing::span!(Level::INFO, "loki_context", loki_context = %loki_context)
    }
}

impl Attr {
    pub fn string(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: Value::String(value.into()),
        }
    }

    pub fn u64(key: impl Into<String>, value: u64) -> Self {
        Self {
            key: key.into(),
            value: Value::Number(Number::from(value)),
        }
    }

    pub fn i64(key: impl Into<String>, value: i64) -> Self {
        Self {
            key: key.into(),
            value: Value::Number(Number::from(value)),
        }
    }

    pub fn bool(key: impl Into<String>, value: bool) -> Self {
        Self {
            key: key.into(),
            value: Value::Bool(value),
        }
    }
}

pub fn with_attrs<T>(attrs: impl IntoIterator<Item = Attr>, f: impl FnOnce() -> T) -> T {
    let attrs = attrs.into_iter().collect::<Vec<_>>();
    CONTEXT_ATTRS.with(|slot| {
        let old_len = {
            let mut current = slot.borrow_mut();
            let old_len = current.len();
            current.extend(attrs);
            old_len
        };
        let result = f();
        slot.borrow_mut().truncate(old_len);
        result
    })
}

struct LokiLayer {
    client: LokiClient,
    config: Config,
}

impl<S> Layer<S> for LokiLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let mut visitor = SpanAttrVisitor::default();
        attrs.record(&mut visitor);
        if !visitor.attrs.is_empty() {
            span.extensions_mut().insert(visitor.attrs);
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let mut visitor = SpanAttrVisitor::default();
        values.record(&mut visitor);
        if visitor.attrs.is_empty() {
            return;
        }
        let mut extensions = span.extensions_mut();
        if let Some(existing) = extensions.get_mut::<Vec<Attr>>() {
            existing.extend(visitor.attrs);
        } else {
            extensions.insert(visitor.attrs);
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let now = SystemTime::now();
        let mut visitor = JsonVisitor::new(&self.config);
        if let Some(scope) = _ctx.event_scope(event) {
            for span in scope.from_root() {
                if let Some(attrs) = span.extensions().get::<Vec<Attr>>() {
                    for attr in attrs {
                        visitor.fields.insert(attr.key.clone(), attr.value.clone());
                    }
                }
            }
        }
        event.record(&mut visitor);

        let meta = event.metadata();
        visitor.fields.insert(
            "level".to_string(),
            Value::String(level_str(meta.level()).to_string()),
        );
        visitor.fields.insert(
            "target".to_string(),
            Value::String(meta.target().to_string()),
        );

        if let Some(line) = serialize_log_line(&visitor.fields) {
            self.client.queue(LokiEntry {
                timestamp_ns: unix_nanos(now),
                line,
            });
        }
    }
}

struct JsonVisitor {
    fields: Map<String, Value>,
}

impl JsonVisitor {
    fn new(config: &Config) -> Self {
        let mut fields = Map::new();
        fields.insert(
            "service_name".to_string(),
            Value::String(config.service_name.clone()),
        );
        fields.insert(
            "environment".to_string(),
            Value::String(config.environment.clone()),
        );
        fields.insert(
            "instance".to_string(),
            Value::String(config.instance_id.clone()),
        );
        if !config.version.is_empty() {
            fields.insert("version".to_string(), Value::String(config.version.clone()));
        }
        CONTEXT_ATTRS.with(|slot| {
            for attr in slot.borrow().iter() {
                fields.insert(attr.key.clone(), attr.value.clone());
            }
        });
        Self { fields }
    }
}

#[derive(Default)]
struct SpanAttrVisitor {
    attrs: Vec<Attr>,
}

impl Visit for SpanAttrVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "loki_context" {
            if let Ok(attrs) = serde_json::from_str::<Vec<Attr>>(value) {
                self.attrs.extend(attrs);
            }
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "loki_context" {
            let rendered = format!("{value:?}");
            let trimmed = rendered.trim_matches('"');
            if let Ok(attrs) = serde_json::from_str::<Vec<Attr>>(trimmed) {
                self.attrs.extend(attrs);
            }
        }
    }
}

impl Visit for JsonVisitor {
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), Value::Bool(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), Value::Number(Number::from(value)));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), Value::Number(Number::from(value)));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), Value::String(value.to_string()));
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.fields
            .insert(field.name().to_string(), Value::String(value.to_string()));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(
            field.name().to_string(),
            Value::String(format!("{value:?}")),
        );
    }
}

#[derive(Clone)]
struct LokiEntry {
    timestamp_ns: u128,
    line: String,
}

enum WorkerMessage {
    Entry(LokiEntry),
    Shutdown,
}

#[derive(Serialize)]
struct PushRequest<'a> {
    streams: [Stream<'a>; 1],
}

#[derive(Serialize)]
struct Stream<'a> {
    stream: Labels<'a>,
    values: &'a [[String; 2]],
}

#[derive(Serialize)]
struct Labels<'a> {
    service_name: &'a str,
    environment: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    version: &'a str,
}

fn run_worker(
    rx: mpsc::Receiver<WorkerMessage>,
    config: Config,
    closed: Arc<AtomicBool>,
    queue_drops: Arc<AtomicU64>,
    send_drops: Arc<AtomicU64>,
) {
    let client = reqwest::blocking::Client::builder()
        .timeout(config.http_timeout)
        .build()
        .unwrap_or_else(|err| panic!("build Loki HTTP client: {err}"));
    let push_url = loki_push_url(&config.loki_url);
    let mut batch = Vec::with_capacity(config.batch_size);

    loop {
        match rx.recv_timeout(config.batch_wait) {
            Ok(WorkerMessage::Entry(entry)) => {
                batch.push(entry);
                while batch.len() < config.batch_size {
                    match rx.try_recv() {
                        Ok(WorkerMessage::Entry(entry)) => batch.push(entry),
                        Ok(WorkerMessage::Shutdown) => {
                            closed.store(true, Ordering::Relaxed);
                            break;
                        }
                        Err(_) => break,
                    }
                }
            }
            Ok(WorkerMessage::Shutdown) => {
                closed.store(true, Ordering::Relaxed);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }

        if !batch.is_empty() {
            if let Err(err) = push_batch_with_retries(&client, &push_url, &config, &closed, &batch)
            {
                send_drops.fetch_add(batch.len() as u64, Ordering::Relaxed);
                eprintln!("loki push failed for Rust logs: {err}");
            }
            batch.clear();
        }

        if closed.load(Ordering::Relaxed) {
            loop {
                match rx.try_recv() {
                    Ok(WorkerMessage::Entry(entry)) => batch.push(entry),
                    Ok(WorkerMessage::Shutdown) => {}
                    Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                }
            }
            if !batch.is_empty() {
                if let Err(err) =
                    push_batch_with_retries(&client, &push_url, &config, &closed, &batch)
                {
                    send_drops.fetch_add(batch.len() as u64, Ordering::Relaxed);
                    eprintln!("loki final push failed for Rust logs: {err}");
                }
            }
            if queue_drops.load(Ordering::Relaxed) > 0 {
                break;
            }
            break;
        }
    }
}

fn push_batch_with_retries(
    client: &reqwest::blocking::Client,
    push_url: &str,
    config: &Config,
    closed: &AtomicBool,
    batch: &[LokiEntry],
) -> Result<(), PushError> {
    let mut delay = Duration::from_millis(100);
    for attempt in 1..=config.max_retries {
        if attempt > 1 {
            if sleep_with_jitter(delay, closed) {
                return Ok(());
            }
            delay = delay.saturating_mul(2);
        }

        match push_batch(client, push_url, config, batch) {
            Ok(()) => return Ok(()),
            Err(PushError::Permanent(message)) => return Err(PushError::Permanent(message)),
            Err(err) => {
                if closed.load(Ordering::Relaxed) {
                    return Ok(());
                }
                eprintln!(
                    "loki push failed attempt {}/{}: {}",
                    attempt, config.max_retries, err
                );
                if attempt == config.max_retries {
                    return Err(err);
                }
            }
        }
    }
    Ok(())
}

fn push_batch(
    client: &reqwest::blocking::Client,
    push_url: &str,
    config: &Config,
    batch: &[LokiEntry],
) -> Result<(), PushError> {
    LOKI_VALUES.with(|slot| {
        let mut values = slot.borrow_mut();
        values.clear();
        values.extend(
            batch
                .iter()
                .map(|entry| [entry.timestamp_ns.to_string(), entry.line.clone()]),
        );
        let request = PushRequest {
            streams: [Stream {
                stream: Labels {
                    service_name: &config.service_name,
                    environment: &config.environment,
                    version: &config.version,
                },
                values: &values,
            }],
        };

        let response = client.post(push_url).json(&request).send()?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        let body = response.text().unwrap_or_default();
        if status.is_client_error() && status.as_u16() != 429 {
            return Err(PushError::Permanent(format!(
                "loki rejected payload with status {}: {}",
                status.as_u16(),
                body
            )));
        }

        Err(PushError::Retryable(format!(
            "loki push returned status {}: {}",
            status.as_u16(),
            body
        )))
    })
}

#[derive(Debug)]
enum PushError {
    Retryable(String),
    Permanent(String),
    Http(reqwest::Error),
}

impl From<reqwest::Error> for PushError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}

impl core::fmt::Display for PushError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Retryable(message) | Self::Permanent(message) => f.write_str(message),
            Self::Http(err) => write!(f, "{err}"),
        }
    }
}

fn close_worker(
    tx: &SyncSender<WorkerMessage>,
    closed: &AtomicBool,
    queue_drops: &AtomicU64,
    send_drops: &AtomicU64,
    worker: &Mutex<Option<JoinHandle<()>>>,
) {
    if closed.swap(true, Ordering::SeqCst) {
        return;
    }
    let _ = tx.send(WorkerMessage::Shutdown);
    if let Some(handle) = worker.lock().ok().and_then(|mut guard| guard.take()) {
        let _ = handle.join();
    }
    let drops = queue_drops.load(Ordering::Relaxed);
    if drops > 0 {
        eprintln!("loki queue dropped {drops} Rust log lines");
    }
    let drops = send_drops.load(Ordering::Relaxed);
    if drops > 0 {
        eprintln!("loki sender dropped {drops} Rust log lines");
    }
}

fn sleep_with_jitter(delay: Duration, closed: &AtomicBool) -> bool {
    let jitter_max = delay / 2;
    let jitter = if jitter_max.is_zero() {
        Duration::ZERO
    } else {
        let max_millis = jitter_max.as_millis() as u64;
        Duration::from_millis(rand::thread_rng().gen_range(0..=max_millis))
    };
    let deadline = delay + jitter;
    let step = Duration::from_millis(25);
    let mut slept = Duration::ZERO;
    while slept < deadline {
        if closed.load(Ordering::Relaxed) {
            return true;
        }
        let remaining = deadline.saturating_sub(slept);
        let nap = if remaining < step { remaining } else { step };
        thread::sleep(nap);
        slept += nap;
    }
    closed.load(Ordering::Relaxed)
}

fn serialize_log_line(fields: &Map<String, Value>) -> Option<String> {
    JSON_BUF.with(|slot| {
        let mut buf = slot.borrow_mut();
        buf.clear();
        serde_json::to_writer(&mut *buf, fields).ok()?;
        String::from_utf8(buf.clone()).ok()
    })
}

fn loki_push_url(base_url: &str) -> String {
    if base_url.ends_with("/loki/api/v1/push") {
        base_url.to_string()
    } else {
        format!("{}/loki/api/v1/push", base_url.trim_end_matches('/'))
    }
}

fn level_filter(value: &str) -> &'static str {
    match value.trim().to_ascii_lowercase().as_str() {
        "debug" => "debug",
        "warn" | "warning" => "warn",
        "error" => "error",
        "trace" => "trace",
        _ => "info",
    }
}

fn level_str(level: &Level) -> &'static str {
    match *level {
        Level::ERROR => "ERROR",
        Level::WARN => "WARN",
        Level::INFO => "INFO",
        Level::DEBUG => "DEBUG",
        Level::TRACE => "TRACE",
    }
}

fn unix_nanos(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

fn env_or(key: &str, fallback: impl Into<String>) -> String {
    env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| fallback.into())
}

fn cached_hostname() -> String {
    env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            fs::read_to_string("/etc/hostname")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| "local".to_string())
}
