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
	"strings"
	"sync"
	"time"
)

// LokiEntry represents a single log entry queued for Loki.
type LokiEntry struct {
	Timestamp time.Time
	Level     string
	Message   string
}

// LokiClient handles batching, retrying, and sending logs to Grafana Loki.
type LokiClient struct {
	url        string
	appName    string
	httpClient *http.Client
	ch         chan LokiEntry
	doneCh     chan struct{}
	closeOnce  sync.Once
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	sem        chan struct{}
}

// NewLokiClient creates and starts a new Loki client.
func NewLokiClient(baseURL string) *LokiClient {
	url := baseURL
	if !strings.HasSuffix(url, "/loki/api/v1/push") {
		url = strings.TrimSuffix(url, "/") + "/loki/api/v1/push"
	}

	appName := os.Getenv("LOKI_APP_NAME")
	if appName == "" {
		appName = "submission-api"
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &LokiClient{
		url:     url,
		appName: appName,
		httpClient: &http.Client{
			Timeout: 5 * time.Second, // Robust 5s timeout under load
		},
		ch:     make(chan LokiEntry, 10000), // Capacity increased to 10k to withstand bursts
		doneCh: make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
		sem:    make(chan struct{}, 10), // Limit to 10 concurrent HTTP batch push goroutines
	}

	go c.start()
	return c
}

// QueueLog pushes a log entry to the channel.
func (c *LokiClient) QueueLog(t time.Time, level, message string) {
	select {
	case c.ch <- LokiEntry{Timestamp: t, Level: level, Message: message}:
	default:
		// Safe fallback output if queue overflows under massive volume
		fmt.Fprintf(os.Stderr, "Loki log queue full (overflow), discarding log: [%s] %s\n", level, message)
	}
}

// Close gracefully stops the worker, flushing any remaining logs in the queue.
func (c *LokiClient) Close() {
	c.closeOnce.Do(func() {
		close(c.ch)
		<-c.doneCh
		c.cancel()
	})
}

type LokiPushRequest struct {
	Streams []LokiStream `json:"streams"`
}

type LokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

func (c *LokiClient) start() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var batch []LokiEntry
	maxBatchSize := 100

	flush := func() {
		if len(batch) == 0 {
			return
		}
		c.wg.Add(1)
		go func(b []LokiEntry) {
			defer c.wg.Done()

			// Bound concurrency using a semaphore channel
			select {
			case c.sem <- struct{}{}:
				defer func() { <-c.sem }()
			case <-c.ctx.Done():
				return // Abort if client is closing
			}

			c.sendBatch(b)
		}(batch)
		batch = nil
	}

	for {
		select {
		case entry, ok := <-c.ch:
			if !ok {
				flush()
				c.wg.Wait() // Wait for all background batches to complete or cancel
				close(c.doneCh)
				return
			}
			batch = append(batch, entry)
			if len(batch) >= maxBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (c *LokiClient) sendBatch(batch []LokiEntry) {
	streamsMap := make(map[string][][]string)
	for _, entry := range batch {
		nsStr := fmt.Sprintf("%d", entry.Timestamp.UnixNano())
		streamsMap[entry.Level] = append(streamsMap[entry.Level], []string{nsStr, entry.Message})
	}

	var streams []LokiStream
	for lvl, vals := range streamsMap {
		streams = append(streams, LokiStream{
			Stream: map[string]string{
				"app":   c.appName,
				"level": lvl,
			},
			Values: vals,
		})
	}

	payload := LokiPushRequest{Streams: streams}
	data, err := json.Marshal(payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Loki client failed to marshal batch: %v\n", err)
		return
	}

	maxRetries := 3
	backoff := 100 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			// Sleep respecting context cancellation (leak-free pattern)
			t := time.NewTimer(backoff)
			select {
			case <-c.ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			backoff *= 2
		}

		if c.ctx.Err() != nil {
			return
		}

		req, err := http.NewRequestWithContext(c.ctx, "POST", c.url, bytes.NewReader(data))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Loki client failed to create request: %v\n", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			if c.ctx.Err() != nil {
				return // Clean exit on cancel
			}
			fmt.Fprintf(os.Stderr, "Loki client failed to push logs (attempt %d/%d): %v\n", i+1, maxRetries, err)
			continue
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			resp.Body.Close()
			return // Successfully pushed
		}

		// Short-circuit retry loops on non-retryable 4xx client errors (excluding 429 rate limits)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 429 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			fmt.Fprintf(os.Stderr, "Loki push rejected payload with status %d (non-retryable): %s\n", resp.StatusCode, string(body))
			return
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Fprintf(os.Stderr, "Loki push returned status %d (attempt %d/%d): %s\n", resp.StatusCode, i+1, maxRetries, string(body))
	}

	fmt.Fprintf(os.Stderr, "Loki client permanently dropped batch of %d logs after %d failed attempts\n", len(batch), maxRetries)
}

// LokiWriter intercepts and stores serialized JSON logs before queuing them.
// A sync.Mutex is used to prevent concurrent writes and buffer corruption if
// multiple goroutines log concurrently using the same logger/handler instance.
type LokiWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *LokiWriter) Write(p []byte) (int, error) {
	w.buf.Write(p)
	return len(p), nil
}

// slogStep preserves the exact chronological history of WithAttrs/WithGroup calls
// to guarantee order and nesting correctness during jsonHandler reconstruction.
type slogStep struct {
	isGroup bool
	group   string
	attrs   []slog.Attr
}

// LokiHandler implements slog.Handler.
type LokiHandler struct {
	client      *LokiClient
	fallback    slog.Handler
	steps       []slogStep
	writer      *LokiWriter
	jsonHandler slog.Handler
}

// NewLokiHandler wraps a LokiClient and a fallback handler.
func NewLokiHandler(client *LokiClient, fallback slog.Handler) *LokiHandler {
	w := &LokiWriter{}
	jsonHandler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})

	return &LokiHandler{
		client:      client,
		fallback:    fallback,
		writer:      w,
		jsonHandler: jsonHandler,
	}
}

func (h *LokiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.fallback.Enabled(ctx, level)
}

func (h *LokiHandler) Handle(ctx context.Context, record slog.Record) error {
	// 1. Log to standard output first.
	fallbackErr := h.fallback.Handle(ctx, record)

	// 2. Format the log line using pre-built and pre-allocated jsonHandler.
	h.writer.mu.Lock()
	h.writer.buf.Reset()
	jsonErr := h.jsonHandler.Handle(ctx, record)
	var logLine string
	if jsonErr == nil {
		logLine = h.writer.buf.String()
	}
	h.writer.mu.Unlock()

	// 3. Queue the log line asynchronously if formatting succeeded.
	if jsonErr == nil && logLine != "" {
		logLine = strings.TrimSuffix(logLine, "\n")
		h.client.QueueLog(record.Time, record.Level.String(), logLine)
	}

	return fallbackErr
}

func (h *LokiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Reconstruct state history in correct chronological order
	newSteps := append(append([]slogStep(nil), h.steps...), slogStep{
		isGroup: false,
		attrs:   attrs,
	})

	w := &LokiWriter{}
	jsonHandler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})

	// Replay all structural steps exactly as they were called chronologically
	var handler slog.Handler = jsonHandler
	for _, step := range newSteps {
		if step.isGroup {
			handler = handler.WithGroup(step.group)
		} else {
			handler = handler.WithAttrs(step.attrs)
		}
	}

	return &LokiHandler{
		client:      h.client,
		fallback:    h.fallback.WithAttrs(attrs),
		steps:       newSteps,
		writer:      w,
		jsonHandler: handler,
	}
}

func (h *LokiHandler) WithGroup(name string) slog.Handler {
	// Reconstruct state history in correct chronological order
	newSteps := append(append([]slogStep(nil), h.steps...), slogStep{
		isGroup: true,
		group:   name,
	})

	w := &LokiWriter{}
	jsonHandler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})

	// Replay all structural steps exactly as they were called chronologically
	var handler slog.Handler = jsonHandler
	for _, step := range newSteps {
		if step.isGroup {
			handler = handler.WithGroup(step.group)
		} else {
			handler = handler.WithAttrs(step.attrs)
		}
	}

	return &LokiHandler{
		client:      h.client,
		fallback:    h.fallback.WithGroup(name),
		steps:       newSteps,
		writer:      w,
		jsonHandler: handler,
	}
}
