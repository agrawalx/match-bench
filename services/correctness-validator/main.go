// correctness-validator replays a completed session's order log in TCP-delivery
// order through a reference matching engine and scores the contestant's reported
// fills against it (post-run, not latency-sensitive).
//
// Flow: consume benchmark.status.updated -> on `completed`, enqueue the session
// -> a worker (after a settle delay) drains the session's orders.sent +
// orders.acked from Kafka, runs the deterministic pipeline (assemble -> order by
// effective_t3 -> validate), writes the summary + violation log to PostgreSQL,
// and publishes a CorrectnessScoreEvent to scores.correctness. Idempotent via the
// summary row.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/iicpc/correctness-validator/internal/pipeline"
	"github.com/iicpc/correctness-validator/internal/publisher"
	"github.com/iicpc/correctness-validator/internal/source"
	"github.com/iicpc/correctness-validator/internal/store"
	"github.com/iicpc/libs/logger"
	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
)

func main() {
	logCfg := logger.DefaultConfig()
	logCfg.ServiceName = "correctness-validator"
	log, lokiClient := logger.NewProductionLogger(logCfg)
	slog.SetDefault(log)
	if lokiClient != nil {
		defer lokiClient.Close()
	}

	port := envOr("PORT", "8080")
	dbURL := mustEnv("DATABASE_URL", log)
	kafkaBrokers := mustEnv("KAFKA_BROKERS", log)
	statusGroup := envOr("KAFKA_STATUS_GROUP", "correctness-validator")
	settleDelay := time.Duration(envInt("SETTLE_DELAY_MS", 10000)) * time.Millisecond
	concurrency := envInt("VALIDATOR_CONCURRENCY", 4)
	brokers := parseBrokers(kafkaBrokers)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	st, err := store.New(ctx, dbURL)
	if err != nil {
		log.Error("postgres init failed", "error", err)
		os.Exit(1)
	}
	defer st.Close()
	pub := publisher.New(kafkaBrokers)
	defer pub.Close()

	v := &validator{
		log:         log,
		brokers:     brokers,
		store:       st,
		pub:         pub,
		settleDelay: settleDelay,
	}

	// Bounded worker pool of completed sessions to validate.
	jobs := make(chan string, 256)
	for i := 0; i < concurrency; i++ {
		go func() {
			for sessionID := range jobs {
				v.validateSession(ctx, sessionID)
			}
		}()
	}

	go runTriggerConsumer(ctx, brokers, statusGroup, jobs, log)

	var ready atomic.Bool
	ready.Store(true)
	r := chi.NewRouter()
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready.Load() && st.Healthcheck(ctx) == nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server", "error", err)
		}
	}()
	log.Info("correctness-validator started", "status_group", statusGroup, "settle_ms", settleDelay.Milliseconds(), "concurrency", concurrency)

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}

type validator struct {
	log         *slog.Logger
	brokers     []string
	store       *store.Store
	pub         *publisher.Publisher
	settleDelay time.Duration
}

func (v *validator) validateSession(ctx context.Context, sessionID string) {
	log := v.log.With("session_id", sessionID)
	if done, err := v.store.HasSummary(ctx, sessionID); err != nil {
		log.Error("idempotency check", "error", err)
		return
	} else if done {
		log.Info("session already validated; skipping")
		return
	}

	// Settle: let the eBPF reader flush its terminal events (> its 5s eviction)
	// before snapshotting the Kafka watermark.
	select {
	case <-time.After(v.settleDelay):
	case <-ctx.Done():
		return
	}

	sents, ackeds, err := source.DrainSession(ctx, v.brokers, sessionID)
	if err != nil {
		log.Error("drain session", "error", err)
		return
	}
	report, contestant := pipeline.Run(sents, ackeds)
	rec := store.Record{
		SessionID:    sessionID,
		ContestantID: contestant,
		Report:       report,
		ComputedAtNS: uint64(time.Now().UnixNano()),
	}
	if err := v.store.Save(ctx, rec); err != nil {
		log.Error("save correctness", "error", err)
		return
	}
	if err := v.pub.Publish(ctx, topics.CorrectnessScoreEvent{
		SessionID:        sessionID,
		ContestantID:     contestant,
		ValidFills:       report.ValidFills,
		TotalFills:       report.TotalFills,
		CorrectnessScore: report.CorrectnessScore(),
		ViolationCount:   report.ViolationCount(),
		ComputedAtNS:     rec.ComputedAtNS,
	}); err != nil {
		log.Error("publish correctness score", "error", err)
		return
	}
	log.Info("session validated",
		"contestant_id", contestant,
		"total_fills", report.TotalFills,
		"valid_fills", report.ValidFills,
		"score", report.CorrectnessScore(),
		"violations", report.ViolationCount(),
		"sent", len(sents), "acked", len(ackeds))
}

func runTriggerConsumer(ctx context.Context, brokers []string, group string, jobs chan<- string, log *slog.Logger) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        group,
		Topic:          topics.TopicBenchmarkStatusUpdated,
		MinBytes:       1,
		MaxBytes:       1 << 20,
		MaxWait:        200 * time.Millisecond,
		CommitInterval: 0, // manual commit
	})
	defer reader.Close()
	log.Info("benchmark.status.updated consumer started", "group", group)
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("fetch benchmark.status.updated", "error", err)
			continue
		}
		var ev topics.BenchmarkStatusUpdated
		if err := json.Unmarshal(m.Value, &ev); err != nil {
			log.Error("unmarshal benchmark.status.updated", "error", err)
			_ = reader.CommitMessages(ctx, m) // poison message — don't block
			continue
		}
		if ev.Status == topics.RunStatusCompleted {
			select {
			case jobs <- ev.SessionID:
			case <-ctx.Done():
				return
			}
		}
		// Commit immediately: the heavy work is recomputable from Kafka and guarded
		// by the summary-row idempotency check, so at-least-once dispatch is safe.
		if err := reader.CommitMessages(ctx, m); err != nil {
			log.Warn("commit benchmark.status.updated", "error", err)
		}
	}
}

// ---- small helpers (env) ---------------------------------------------------

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func mustEnv(key string, log *slog.Logger) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		log.Error("missing required env", "key", key)
		os.Exit(1)
	}
	return v
}

func envInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func parseBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
