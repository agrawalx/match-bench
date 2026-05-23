package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultServiceName = "iicpc"

type contextAttrsKey struct{}

// Config controls application logging and optional Loki shipping.
// Label fields should stay low-cardinality so Loki indexes remain healthy.
type Config struct {
	ServiceName string
	Environment string
	InstanceID  string
	Version     string
	LokiURL     string
	Level       slog.Level
	QueueSize   int
	BatchSize   int
	BatchWait   time.Duration
	HTTPTimeout time.Duration
	MaxRetries  int
}

// DefaultConfig returns production-shaped defaults with Loki disabled unless
// LOKI_URL is provided through the environment.
func DefaultConfig() Config {
	host, _ := os.Hostname()
	return Config{
		ServiceName: defaultServiceName,
		Environment: envOr("ENVIRONMENT", envOr("APP_ENV", "local")),
		InstanceID:  host,
		Version:     envOr("SERVICE_VERSION", "dev"),
		LokiURL:     os.Getenv("LOKI_URL"),
		Level:       parseLevel(envOr("LOG_LEVEL", "info")),
		QueueSize:   10000,
		BatchSize:   256,
		BatchWait:   500*time.Millisecond,
		HTTPTimeout: 5*time.Second,
		MaxRetries:  3,
	}
}

// NewProductionLogger builds the process logger used by services.
// It always writes JSON to stdout and optionally mirrors batches to Loki.
func NewProductionLogger(cfg Config) (*slog.Logger, *LokiClient) {
	cfg = normalizeConfig(cfg)

	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.Level,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				attr.Value = slog.StringValue(attr.Value.Time().UTC().Format(time.RFC3339Nano))
			}
			return attr
		},
	}).WithAttrs(serviceAttrs(cfg))

	var handler slog.Handler = base
	var client *LokiClient
	if cfg.LokiURL != "" {
		client = NewLokiClientWithConfig(cfg)
		handler = NewLokiHandler(client, handler)
	}
	handler = &ContextHandler{next: handler}

	return slog.New(handler), client
}

// WithAttrs returns a child context whose attributes are appended to every
// slog record handled by ContextHandler.
func WithAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	if len(attrs) == 0 {
		return ctx
	}
	existing, _ := ctx.Value(contextAttrsKey{}).([]slog.Attr)
	merged := make([]slog.Attr, 0, len(existing)+len(attrs))
	merged = append(merged, existing...)
	merged = append(merged, attrs...)
	return context.WithValue(ctx, contextAttrsKey{}, merged)
}

// ContextHandler injects attributes carried in context into slog records.
type ContextHandler struct {
	next slog.Handler
}

// Enabled delegates level filtering to the wrapped handler.
func (h *ContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

// Handle adds request-scoped context attributes before writing a record.
func (h *ContextHandler) Handle(ctx context.Context, record slog.Record) error {
	if attrs, ok := ctx.Value(contextAttrsKey{}).([]slog.Attr); ok && len(attrs) > 0 {
		cloned := record.Clone()
		cloned.AddAttrs(attrs...)
		return h.next.Handle(ctx, cloned)
	}
	return h.next.Handle(ctx, record)
}

// WithAttrs returns a handler with static attributes attached.
func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{next: h.next.WithAttrs(attrs)}
}

// WithGroup returns a handler with the named attribute group attached.
func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{next: h.next.WithGroup(name)}
}

// LokiEntry represents a serialized JSON log line queued for Loki.
type LokiEntry struct {
	Timestamp time.Time
	Line      string
}

// LokiClient handles bounded queueing, batching, retrying, and sending logs to
// Grafana Loki's push API.
type LokiClient struct {
	url        string
	labels     map[string]string
	attrs      []slog.Attr
	httpClient *http.Client
	ch         chan LokiEntry
	doneCh     chan struct{}
	closeOnce  sync.Once
	ctx        context.Context
	cancel     context.CancelFunc
	queueDrops atomic.Uint64
	sendDrops  atomic.Uint64
	batchSize  int
	batchWait  time.Duration
	maxRetries int
}

// NewLokiClient keeps the old constructor while using production defaults.
func NewLokiClient(baseURL string) *LokiClient {
	cfg := DefaultConfig()
	cfg.LokiURL = baseURL
	return NewLokiClientWithConfig(cfg)
}

// NewLokiClientWithConfig creates and starts a Loki client.
func NewLokiClientWithConfig(cfg Config) *LokiClient {
	cfg = normalizeConfig(cfg)
	ctx, cancel := context.WithCancel(context.Background())

	c := &LokiClient{
		url:        lokiPushURL(cfg.LokiURL),
		labels:     lokiLabels(cfg),
		attrs:      serviceAttrs(cfg),
		httpClient: &http.Client{Timeout: cfg.HTTPTimeout},
		ch:         make(chan LokiEntry, cfg.QueueSize),
		doneCh:     make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
		batchSize:  cfg.BatchSize,
		batchWait:  cfg.BatchWait,
		maxRetries: cfg.MaxRetries,
	}

	go c.start()
	return c
}

// QueueLog pushes a serialized log entry to the bounded Loki queue.
func (c *LokiClient) QueueLog(t time.Time, line string) {
	select {
	case c.ch <- LokiEntry{Timestamp: t, Line: line}:
	default:
		c.queueDrops.Add(1)
		fmt.Fprintf(os.Stderr, "loki queue full, dropping log line\n")
	}
}

// Close gracefully stops the worker and flushes any remaining queued logs.
func (c *LokiClient) Close() {
	c.closeOnce.Do(func() {
		close(c.ch)
		<-c.doneCh
		c.cancel()
		if dropped := c.queueDrops.Load(); dropped > 0 {
			fmt.Fprintf(os.Stderr, "loki queue dropped %d log lines\n", dropped)
		}
		if dropped := c.sendDrops.Load(); dropped > 0 {
			fmt.Fprintf(os.Stderr, "loki sender dropped %d log lines\n", dropped)
		}
	})
}

type lokiPushRequest struct {
	Streams []lokiStream `json:"streams"`
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

func (c *LokiClient) start() {
	ticker := time.NewTicker(c.batchWait)
	defer ticker.Stop()

	batch := make([]LokiEntry, 0, c.batchSize)
	for {
		select {
		case entry, ok := <-c.ch:
			if !ok {
				c.flush(batch)
				close(c.doneCh)
				return
			}
			batch = append(batch, entry)
			if len(batch) >= c.batchSize {
				c.flush(batch)
				batch = make([]LokiEntry, 0, c.batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				c.flush(batch)
				batch = make([]LokiEntry, 0, c.batchSize)
			}
		}
	}
}

func (c *LokiClient) flush(batch []LokiEntry) {
	if len(batch) == 0 {
		return
	}

	values := make([][]string, 0, len(batch))
	for _, entry := range batch {
		values = append(values, []string{
			strconv.FormatInt(entry.Timestamp.UnixNano(), 10),
			entry.Line,
		})
	}

	payload := lokiPushRequest{
		Streams: []lokiStream{{
			Stream: c.labels,
			Values: values,
		}},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		c.sendDrops.Add(uint64(len(batch)))
		fmt.Fprintf(os.Stderr, "loki marshal failed: %v\n", err)
		return
	}

	delay := 100 * time.Millisecond
	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		if attempt > 1 {
			timer := time.NewTimer(delay)
			select {
			case <-c.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			delay *= 2
		}

		req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.url, bytes.NewReader(data))
		if err != nil {
			c.sendDrops.Add(uint64(len(batch)))
			fmt.Fprintf(os.Stderr, "loki request creation failed: %v\n", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			fmt.Fprintf(os.Stderr, "loki push failed attempt %d/%d: %v\n", attempt, c.maxRetries, err)
			continue
		}

		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return
		}
		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			c.sendDrops.Add(uint64(len(batch)))
			fmt.Fprintf(os.Stderr, "loki rejected payload with status %d: %s\n", resp.StatusCode, string(body))
			return
		}
		fmt.Fprintf(os.Stderr, "loki push returned status %d attempt %d/%d: %s\n", resp.StatusCode, attempt, c.maxRetries, string(body))
	}

	c.sendDrops.Add(uint64(len(batch)))
	fmt.Fprintf(os.Stderr, "loki permanently dropped batch of %d log lines\n", len(batch))
}

type lokiWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *lokiWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

type slogStep struct {
	isGroup bool
	group   string
	attrs   []slog.Attr
}

// LokiHandler mirrors structured slog records to Loki while preserving stdout.
type LokiHandler struct {
	client *LokiClient
	next   slog.Handler
	steps  []slogStep
	writer *lokiWriter
	render slog.Handler
}

// NewLokiHandler wraps a LokiClient and fallback handler.
func NewLokiHandler(client *LokiClient, fallback slog.Handler) *LokiHandler {
	writer := &lokiWriter{}
	render := slog.NewJSONHandler(writer, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				attr.Value = slog.StringValue(attr.Value.Time().UTC().Format(time.RFC3339Nano))
			}
			return attr
		},
	}).WithAttrs(client.attrs)

	return &LokiHandler{
		client: client,
		next:   fallback,
		writer: writer,
		render: render,
	}
}

// Enabled delegates level filtering to the wrapped handler.
func (h *LokiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

// Handle writes to stdout and queues the same structured record for Loki.
func (h *LokiHandler) Handle(ctx context.Context, record slog.Record) error {
	err := h.next.Handle(ctx, record)
	if h.client == nil {
		return err
	}

	h.writer.mu.Lock()
	h.writer.buf.Reset()
	jsonErr := h.render.Handle(ctx, record)
	line := strings.TrimSuffix(h.writer.buf.String(), "\n")
	h.writer.mu.Unlock()

	if jsonErr == nil && line != "" {
		h.client.QueueLog(record.Time, line)
	}

	return err
}

// WithAttrs returns a handler with static attributes attached.
func (h *LokiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	steps := append(append([]slogStep(nil), h.steps...), slogStep{
		attrs: attrs,
	})
	return h.with(h.next.WithAttrs(attrs), steps)
}

// WithGroup returns a handler with a named attribute group attached.
func (h *LokiHandler) WithGroup(name string) slog.Handler {
	steps := append(append([]slogStep(nil), h.steps...), slogStep{
		isGroup: true,
		group:   name,
	})
	return h.with(h.next.WithGroup(name), steps)
}

func (h *LokiHandler) with(next slog.Handler, steps []slogStep) slog.Handler {
	writer := &lokiWriter{}
	render := slog.NewJSONHandler(writer, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				attr.Value = slog.StringValue(attr.Value.Time().UTC().Format(time.RFC3339Nano))
			}
			return attr
		},
	}).WithAttrs(h.client.attrs)

	var handler slog.Handler = render
	for _, step := range steps {
		if step.isGroup {
			handler = handler.WithGroup(step.group)
		} else {
			handler = handler.WithAttrs(step.attrs)
		}
	}

	return &LokiHandler{
		client: h.client,
		next:   next,
		steps:  steps,
		writer: writer,
		render: handler,
	}
}

func normalizeConfig(cfg Config) Config {
	defaults := DefaultConfig()
	if cfg.ServiceName == "" {
		cfg.ServiceName = defaults.ServiceName
	}
	if cfg.Environment == "" {
		cfg.Environment = defaults.Environment
	}
	if cfg.InstanceID == "" {
		cfg.InstanceID = defaults.InstanceID
	}
	if cfg.Version == "" {
		cfg.Version = defaults.Version
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaults.QueueSize
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaults.BatchSize
	}
	if cfg.BatchWait <= 0 {
		cfg.BatchWait = defaults.BatchWait
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = defaults.HTTPTimeout
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaults.MaxRetries
	}
	return cfg
}

func lokiPushURL(baseURL string) string {
	if strings.HasSuffix(baseURL, "/loki/api/v1/push") {
		return baseURL
	}
	return strings.TrimSuffix(baseURL, "/") + "/loki/api/v1/push"
}

func lokiLabels(cfg Config) map[string]string {
	labels := map[string]string{
		"service_name": cfg.ServiceName,
		"service":      cfg.ServiceName,
		"environment":  cfg.Environment,
	}
	if cfg.Version != "" {
		labels["version"] = cfg.Version
	}
	return labels
}

func serviceAttrs(cfg Config) []slog.Attr {
	attrs := []slog.Attr{
		slog.String("service_name", cfg.ServiceName),
		slog.String("service", cfg.ServiceName),
		slog.String("environment", cfg.Environment),
		slog.String("instance", cfg.InstanceID),
	}
	if cfg.Version != "" {
		attrs = append(attrs, slog.String("version", cfg.Version))
	}
	return attrs
}

func parseLevel(value string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
