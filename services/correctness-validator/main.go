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
	"fmt"
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
	"github.com/iicpc/correctness-validator/internal/validate"
	"github.com/iicpc/libs/logger"
	"github.com/iicpc/libs/metrics"
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

	// Prometheus metrics endpoint. Bound on its own listener so a scrape target
	// resolves even if the main HTTP mux is busy; bind failures are logged but
	// non-fatal (the service's real work is Kafka-driven, not HTTP).
	metricsSrv, err := metrics.StartServer("0.0.0.0:9090")
	if err != nil {
		log.Error("metrics server start failed", "addr", "0.0.0.0:9090", "error", err)
	} else {
		defer metricsSrv.Close()
		log.Info("metrics server started", "addr", "0.0.0.0:9090")
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

	// `concurrency` consumer goroutines in one group. Each fetches a status
	// message, validates synchronously, and commits the offset ONLY after the
	// score is durably persisted + published — so a crash mid-validation
	// re-delivers (at-least-once) rather than silently dropping the session.
	// Partition-level parallelism across the group; the summary-row claim keeps
	// reprocessing idempotent.
	for i := 0; i < concurrency; i++ {
		go v.runStatusConsumer(ctx, brokers, statusGroup)
	}

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
	r.Handle("/metrics", metrics.Handler())
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
	// inflight counts sessions concurrently held in memory; exported as the
	// validator_inflight_sessions gauge to watch the OOM-prone concurrency.
	inflight atomic.Int64
}

// recordViolations fans the per-session report counts out to the
// validator_violations_total counter, one increment per violation type.
func recordViolations(report validate.Report) {
	if report.Overfills > 0 {
		metrics.Counter("validator_violations_total", "Correctness violations detected by type.", metrics.Labels("type", "overfill"), float64(report.Overfills))
	}
	if report.PhantomFills > 0 {
		metrics.Counter("validator_violations_total", "Correctness violations detected by type.", metrics.Labels("type", "phantom"), float64(report.PhantomFills))
	}
	if report.PriceViolations > 0 {
		metrics.Counter("validator_violations_total", "Correctness violations detected by type.", metrics.Labels("type", "price"), float64(report.PriceViolations))
	}
	if report.TimeViolations > 0 {
		metrics.Counter("validator_violations_total", "Correctness violations detected by type.", metrics.Labels("type", "time"), float64(report.TimeViolations))
	}
	if report.SelfTrades > 0 {
		metrics.Counter("validator_violations_total", "Correctness violations detected by type.", metrics.Labels("type", "self_trade"), float64(report.SelfTrades))
	}
}

// validateSession drains, replays, scores, persists, and publishes one session.
// Returns nil only when the result is durably persisted AND (re)published, so the
// caller can safely commit the Kafka offset. Idempotent: the summary-row claim in
// store.Save means exactly one worker publishes the primary score for a session,
// while a redelivery that finds the summary already present re-publishes it
// (at-least-once) without redoing the work.
func (v *validator) validateSession(ctx context.Context, sessionID string) error {
	log := v.log.With("session_id", sessionID)

	// In-flight gauge: how many sessions are concurrently held in memory. This
	// service OOM'd draining large sessions, so concurrency × per-session buffer
	// is the memory-pressure signal operators watch.
	metrics.Gauge("validator_inflight_sessions", "Correctness-validator sessions currently being validated.", nil, float64(v.inflight.Add(1)))
	defer func() { metrics.Gauge("validator_inflight_sessions", "Correctness-validator sessions currently being validated.", nil, float64(v.inflight.Add(-1))) }()

	if done, err := v.store.HasSummary(ctx, sessionID); err != nil {
		metrics.Counter("validator_validation_errors_total", "Correctness-validator validation errors by stage.", metrics.Labels("stage", "idempotency"), 1)
		metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "error"), 1)
		return fmt.Errorf("idempotency check: %w", err)
	} else if done {
		ev, ok, err := v.store.LoadScore(ctx, sessionID)
		if err != nil {
			metrics.Counter("validator_validation_errors_total", "Correctness-validator validation errors by stage.", metrics.Labels("stage", "load_score"), 1)
			metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "error"), 1)
			return err
		}
		if ok {
			if err := v.pub.Publish(ctx, ev); err != nil {
				metrics.Counter("validator_validation_errors_total", "Correctness-validator validation errors by stage.", metrics.Labels("stage", "republish"), 1)
				metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "error"), 1)
				return fmt.Errorf("re-publish score: %w", err)
			}
			metrics.Counter("validator_scores_published_total", "Correctness scores published to scores.correctness.", nil, 1)
		}
		metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "skipped_done"), 1)
		log.Info("session already validated; re-published score")
		return nil
	}

	// Settle: let the eBPF reader flush its terminal events (> its 5s eviction)
	// before snapshotting the Kafka watermark.
	select {
	case <-time.After(v.settleDelay):
	case <-ctx.Done():
		return ctx.Err()
	}

	drainStart := time.Now()
	sents, ackeds, err := source.DrainSession(ctx, v.brokers, sessionID)
	if err != nil {
		metrics.Counter("validator_validation_errors_total", "Correctness-validator validation errors by stage.", metrics.Labels("stage", "drain"), 1)
		metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "error"), 1)
		return fmt.Errorf("drain session: %w", err)
	}
	// Drain cost + buffered volume: the memory the deterministic pipeline holds
	// for one session is proportional to (len(sents)+len(ackeds)).
	metrics.Histogram("validator_drain_duration_seconds", "Correctness-validator per-session Kafka drain duration in seconds.", nil, metrics.SinceSeconds(drainStart))
	metrics.Counter("validator_events_drained_total", "Correctness-validator events drained from Kafka by topic.", metrics.Labels("topic", "orders_sent"), float64(len(sents)))
	metrics.Counter("validator_events_drained_total", "Correctness-validator events drained from Kafka by topic.", metrics.Labels("topic", "orders_acked"), float64(len(ackeds)))
	metrics.Histogram("validator_session_events_buffered", "Events (orders.sent + orders.acked) buffered in memory per validated session.", nil, float64(len(sents)+len(ackeds)))

	report, contestant := pipeline.Run(sents, ackeds)
	recordViolations(report)
	rec := store.Record{
		SessionID:    sessionID,
		ContestantID: contestant,
		Report:       report,
		ComputedAtNS: uint64(time.Now().UnixNano()),
	}
	inserted, err := v.store.Save(ctx, rec)
	if err != nil {
		metrics.Counter("validator_validation_errors_total", "Correctness-validator validation errors by stage.", metrics.Labels("stage", "save"), 1)
		metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "error"), 1)
		return fmt.Errorf("save correctness: %w", err)
	}
	if !inserted {
		// Lost the claim race to a concurrent worker; it owns the publish.
		metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "skipped_claimed"), 1)
		log.Info("session already claimed by another worker; skipping publish")
		return nil
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
		// Summary is persisted; leaving the offset uncommitted re-delivers and the
		// HasSummary fast-path above re-publishes — at-least-once delivery.
		metrics.Counter("validator_validation_errors_total", "Correctness-validator validation errors by stage.", metrics.Labels("stage", "publish"), 1)
		metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "error"), 1)
		return fmt.Errorf("publish correctness score: %w", err)
	}
	metrics.Counter("validator_scores_published_total", "Correctness scores published to scores.correctness.", nil, 1)
	metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "success"), 1)
	log.Info("session validated",
		"contestant_id", contestant,
		"total_fills", report.TotalFills,
		"valid_fills", report.ValidFills,
		"score", report.CorrectnessScore(),
		"violations", report.ViolationCount(),
		"sent", len(sents), "acked", len(ackeds))
	return nil
}

// runStatusConsumer is one member of the benchmark.status.updated consumer group.
// It validates each `completed` session synchronously and commits the offset ONLY
// after validateSession succeeds, downgrading nothing: a crash or shutdown before
// the commit re-delivers the message (at-least-once), and the per-session claim
// makes reprocessing idempotent.
func (v *validator) runStatusConsumer(ctx context.Context, brokers []string, group string) {
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
	v.log.Info("benchmark.status.updated consumer started", "group", group)
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			v.log.Error("fetch benchmark.status.updated", "error", err)
			continue
		}
		var ev topics.BenchmarkStatusUpdated
		if err := json.Unmarshal(m.Value, &ev); err != nil {
			v.log.Error("unmarshal benchmark.status.updated", "error", err)
			_ = reader.CommitMessages(ctx, m) // poison message — don't block the partition
			continue
		}
		if ev.Status == topics.RunStatusCompleted {
			if err := v.validateSession(ctx, ev.SessionID); err != nil {
				if ctx.Err() != nil {
					return // shutdown: leave uncommitted for redelivery
				}
				v.log.Error("validate session; will retry on redelivery", "session_id", ev.SessionID, "error", err)
				continue // do NOT commit — retry on next poll / redelivery
			}
		}
		if err := reader.CommitMessages(ctx, m); err != nil {
			v.log.Warn("commit benchmark.status.updated", "error", err)
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
