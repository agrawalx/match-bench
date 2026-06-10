// Package logger defines shared library behavior for loki.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultServiceName = "iicpc"

var cachedHostname = func() string {
	h, _ := os.Hostname()
	return h
}()

var slicePool = sync.Pool{
	New: func() any {
		s := make([][]string, 0, 256)
		return &s
	},
}

var warnBatchSizeOnce sync.Once

// contextAttrsKey groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type contextAttrsKey struct{}

// Config groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
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

// DefaultConfig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func DefaultConfig() Config {
	return Config{
		ServiceName: defaultServiceName,
		Environment: envOr("ENVIRONMENT", envOr("APP_ENV", "local")),
		InstanceID:  cachedHostname,
		Version:     envOr("SERVICE_VERSION", "dev"),
		LokiURL:     os.Getenv("LOKI_URL"),
		Level:       parseLevel(envOr("LOG_LEVEL", "info")),
		QueueSize:   10000,
		BatchSize:   256,
		BatchWait:   500 * time.Millisecond,
		HTTPTimeout: 5 * time.Second,
		MaxRetries:  3,
	}
}

// normalizeConfig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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
	if cfg.BatchSize > cfg.QueueSize {
		warnBatchSizeOnce.Do(func() {
			fmt.Fprintf(os.Stderr, "loki client warning: BatchSize (%d) is larger than QueueSize (%d). Capping BatchSize to QueueSize (%d) to ensure flushing by count works.\n", cfg.BatchSize, cfg.QueueSize, cfg.QueueSize)
		})
		cfg.BatchSize = cfg.QueueSize
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

// lokiPushURL performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func lokiPushURL(baseURL string) string {
	if strings.HasSuffix(baseURL, "/loki/api/v1/push") {
		return baseURL
	}
	return strings.TrimSuffix(baseURL, "/") + "/loki/api/v1/push"
}

// lokiLabels performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func lokiLabels(cfg Config) map[string]string {
	labels := map[string]string{
		"service_name": cfg.ServiceName,
		"environment":  cfg.Environment,
	}
	if cfg.Version != "" {
		labels["version"] = cfg.Version
	}
	return labels
}

// serviceAttrs performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func serviceAttrs(cfg Config) []slog.Attr {
	attrs := []slog.Attr{
		slog.String("service_name", cfg.ServiceName),
		slog.String("environment", cfg.Environment),
		slog.String("instance", cfg.InstanceID),
	}
	if cfg.Version != "" {
		attrs = append(attrs, slog.String("version", cfg.Version))
	}
	return attrs
}

// parseLevel performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// envOr performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// NewProductionLogger performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// NewLokiClient performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewLokiClient(baseURL string) *LokiClient {
	cfg := DefaultConfig()
	cfg.LokiURL = baseURL
	return NewLokiClientWithConfig(cfg)
}

// NewLokiClientWithConfig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewLokiClientWithConfig(cfg Config) *LokiClient {
	cfg = normalizeConfig(cfg)

	if cfg.LokiURL != "" {
		parsed, err := url.Parse(cfg.LokiURL)
		if err != nil {
			panic(fmt.Sprintf("invalid Loki URL %q: %v", cfg.LokiURL, err))
		}
		if parsed.Scheme == "" || parsed.Host == "" {
			panic(fmt.Sprintf("invalid Loki URL %q: scheme and host are required", cfg.LokiURL))
		}
	}

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

// WithAttrs performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// ContextHandler groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ContextHandler struct {
	next slog.Handler
}

// Enabled applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (h *ContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

// Handle applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (h *ContextHandler) Handle(ctx context.Context, record slog.Record) error {
	if attrs, ok := ctx.Value(contextAttrsKey{}).([]slog.Attr); ok && len(attrs) > 0 {
		cloned := record.Clone()
		cloned.AddAttrs(attrs...)
		return h.next.Handle(ctx, cloned)
	}
	return h.next.Handle(ctx, record)
}

// WithAttrs applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{next: h.next.WithAttrs(attrs)}
}

// WithGroup applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{next: h.next.WithGroup(name)}
}

// LokiEntry groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type LokiEntry struct {
	Timestamp time.Time
	Line      string
}

// lokiPushRequest groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type lokiPushRequest struct {
	Streams []lokiStream `json:"streams"`
}

// lokiStream groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

// LokiClient groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type LokiClient struct {
	url        string
	labels     map[string]string // only Loki-specific
	attrs      []slog.Attr       // useful to both Loki and slog
	httpClient *http.Client
	ch         chan LokiEntry
	doneCh     chan struct{}
	closeOnce  sync.Once
	closed     atomic.Bool
	ctx        context.Context
	cancel     context.CancelFunc
	queueDrops atomic.Uint64 // logs dropped due to full queue
	sendDrops  atomic.Uint64 // logs dropped due to HTTP errors
	batchSize  int
	batchWait  time.Duration
	maxRetries int
}

// QueueLog applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *LokiClient) QueueLog(t time.Time, line string) {
	if c.closed.Load() {
		return
	}

	defer func() {
		if r := recover(); r != nil {
		}
	}()

	select {
	case c.ch <- LokiEntry{Timestamp: t, Line: line}:
	default:
		drops := c.queueDrops.Add(1)
		if drops == 1 || drops%1000 == 0 {
			fmt.Fprintf(os.Stderr, "loki queue full, dropping log line (dropped %d so far)\n", drops)
		}
	}
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *LokiClient) Close() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
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

// QueueDrops applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *LokiClient) QueueDrops() uint64 {
	return c.queueDrops.Load()
}

// SendDrops applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *LokiClient) SendDrops() uint64 {
	return c.sendDrops.Load()
}

// start applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				c.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

// flush applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *LokiClient) flush(batch []LokiEntry) {
	if len(batch) == 0 {
		return
	}

	pValues := slicePool.Get().(*[][]string)
	values := (*pValues)[:0]
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
		for i := range values {
			values[i] = nil
		}
		*pValues = values
		slicePool.Put(pValues)

		c.sendDrops.Add(uint64(len(batch)))
		fmt.Fprintf(os.Stderr, "loki marshal failed: %v\n", err)
		return
	}

	for i := range values {
		values[i] = nil
	}
	*pValues = values
	slicePool.Put(pValues)

	delay := 100 * time.Millisecond
	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		if attempt > 1 {
			jitter := time.Duration(rand.Int64N(int64(delay / 2)))
			timer := time.NewTimer(delay + jitter)
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

// renderState groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type renderState struct {
	buf     *bytes.Buffer
	handler slog.Handler
}

// slogStep groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type slogStep struct {
	isGroup bool
	group   string
	attrs   []slog.Attr
}

// LokiHandler groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type LokiHandler struct {
	client *LokiClient
	next   slog.Handler
	steps  []slogStep
	pool   *sync.Pool
}

// NewLokiHandler performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewLokiHandler(client *LokiClient, fallback slog.Handler) *LokiHandler {
	return buildLokiHandler(client, fallback, nil)
}

// buildLokiHandler performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func buildLokiHandler(client *LokiClient, next slog.Handler, steps []slogStep) *LokiHandler {
	pool := &sync.Pool{
		New: func() any {
			buf := new(bytes.Buffer)
			render := slog.NewJSONHandler(buf, &slog.HandlerOptions{
				Level: slog.LevelDebug,
				ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
					if attr.Key == slog.TimeKey {
						attr.Value = slog.StringValue(attr.Value.Time().UTC().Format(time.RFC3339Nano))
					}
					return attr
				},
			}).WithAttrs(client.attrs)

			var handler slog.Handler = render
			for _, step := range steps {
				if step.isGroup {
					handler = handler.WithGroup(step.group)
				} else {
					handler = handler.WithAttrs(step.attrs)
				}
			}

			return &renderState{
				buf:     buf,
				handler: handler,
			}
		},
	}

	return &LokiHandler{
		client: client,
		next:   next,
		steps:  steps,
		pool:   pool,
	}
}

// Enabled applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (h *LokiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

// Handle applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (h *LokiHandler) Handle(ctx context.Context, record slog.Record) error {
	err := h.next.Handle(ctx, record)
	if h.client == nil {
		return err
	}

	state := h.pool.Get().(*renderState)
	state.buf.Reset()

	jsonErr := state.handler.Handle(ctx, record)
	line := strings.TrimSuffix(state.buf.String(), "\n")

	if state.buf.Cap() <= 65536 {
		h.pool.Put(state)
	}

	if jsonErr == nil && line != "" {
		h.client.QueueLog(record.Time, line)
	}

	return err
}

// WithAttrs applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (h *LokiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	steps := append(append([]slogStep(nil), h.steps...), slogStep{
		attrs: attrs,
	})
	return buildLokiHandler(h.client, h.next.WithAttrs(attrs), steps)
}

// WithGroup applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (h *LokiHandler) WithGroup(name string) slog.Handler {
	steps := append(append([]slogStep(nil), h.steps...), slogStep{
		isGroup: true,
		group:   name,
	})
	return buildLokiHandler(h.client, h.next.WithGroup(name), steps)
}
