// Package main starts the correctness-validator service.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package main

import (
	"context"
	"encoding/json"
	"errors"
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
	"github.com/iicpc/correctness-validator/internal/publisher"
	"github.com/iicpc/correctness-validator/internal/source"
	"github.com/iicpc/correctness-validator/internal/store"
	"github.com/iicpc/correctness-validator/internal/validate"
	"github.com/iicpc/libs/logger"
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
)

// main performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func main() {
	logCfg := logger.DefaultConfig()
	logCfg.ServiceName = "correctness-validator"
	log, lokiClient := logger.NewProductionLogger(logCfg)
	slog.SetDefault(log)
	if lokiClient != nil {
		defer lokiClient.Close()
	}

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
	validationTimeout := time.Duration(envInt("VALIDATION_TIMEOUT_MS", 60000)) * time.Millisecond
	concurrency := envInt("VALIDATOR_CONCURRENCY", 4)
	validate.AggressiveFillToleranceNs = uint64(envInt("AGGRESSIVE_FILL_TOLERANCE_US", 0)) * 1000
	reorderWindow := envInt("REORDER_WINDOW", 0) // 0 -> source.DefaultReorderWindow
	// VALIDATOR_MODE: full (default) is the current book-replay behavior; invariants
	// is the pass-2 book-free mode (docs/multi-contestant-audit.md §5, P-F). This is a
	// blunt global switch: no per-session signal (e.g. scenario name from
	// BenchmarkRequested.ScenarioID) is threaded through the store/status-updated path
	// today, and wiring one up would mean a DB lookup of scenario name by scenario_id
	// on every session — out of scope here; accepted deviation, see final report.
	validatorMode := envOr("VALIDATOR_MODE", "full")
	crossFlowWindowUs := uint64(envInt("CROSS_FLOW_WINDOW_US", int(validate.DefaultCrossFlowWindowUs)))
	brokers := parseBrokers(kafkaBrokers)
	if err := checkTimeoutConfig(validationTimeout, settleDelay); err != nil {
		log.Error("invalid validation timeout config", "validation_timeout_ms", validationTimeout.Milliseconds(), "settle_ms", settleDelay.Milliseconds(), "error", err)
		os.Exit(1)
	}

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
		log:               log,
		brokers:           brokers,
		store:             st,
		pub:               pub,
		settleDelay:       settleDelay,
		validationTimeout: validationTimeout,
		reorderWindow:     reorderWindow,
		mode:              validatorMode,
		crossFlowWindowUs: crossFlowWindowUs,
	}

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
	log.Info("correctness-validator started", "status_group", statusGroup, "settle_ms", settleDelay.Milliseconds(), "validation_timeout_ms", validationTimeout.Milliseconds(), "concurrency", concurrency)

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}

// validator groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type validator struct {
	log               *slog.Logger
	brokers           []string
	store             *store.Store
	pub               *publisher.Publisher
	settleDelay       time.Duration
	validationTimeout time.Duration
	reorderWindow     int
	mode              string // "full" | "invariants" (VALIDATOR_MODE)
	crossFlowWindowUs uint64
	inflight          atomic.Int64
}

// recordViolations performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// validateSession applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (v *validator) validateSession(ctx context.Context, sessionID string) error {
	log := v.log.With("session_id", sessionID)

	metrics.Gauge("validator_inflight_sessions", "Correctness-validator sessions currently being validated.", nil, float64(v.inflight.Add(1)))
	defer func() {
		metrics.Gauge("validator_inflight_sessions", "Correctness-validator sessions currently being validated.", nil, float64(v.inflight.Add(-1)))
	}()

	status, exists, err := v.store.SummaryStatus(ctx, sessionID)
	if err != nil {
		metrics.Counter("validator_validation_errors_total", "Correctness-validator validation errors by stage.", metrics.Labels("stage", "idempotency"), 1)
		metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "error"), 1)
		return fmt.Errorf("idempotency check: %w", err)
	}
	switch {
	case exists && status == store.StatusScored:
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
	case exists && status == store.StatusTimedOut:
		log.Info("session has timed_out placeholder; re-running validation")
	}

	select {
	case <-time.After(v.settleDelay):
	case <-ctx.Done():
		return ctx.Err()
	}

	drainStart := time.Now()
	var (
		counts     source.StreamCounts
		contestant string
		report     validate.Report
	)
	if v.mode == "invariants" {
		iv := validate.NewInvariantsValidator(v.crossFlowWindowUs)
		counts, contestant, err = source.StreamSession(ctx, v.brokers, sessionID, v.reorderWindow,
			iv.Apply,
			iv.AddUnmatched,
		)
		if err != nil {
			metrics.Counter("validator_validation_errors_total", "Correctness-validator validation errors by stage.", metrics.Labels("stage", "drain"), 1)
			metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "error"), 1)
			return fmt.Errorf("stream session: %w", err)
		}
		report = iv.Finish()
	} else {
		sv := validate.NewStreamValidator()
		counts, contestant, err = source.StreamSession(ctx, v.brokers, sessionID, v.reorderWindow,
			sv.Apply,
			func(id string, qty uint64, price int64) {
				sv.AddPhantom(validate.ReportedFill{OrderID: id, Qty: qty, Price: price})
			},
		)
		if err != nil {
			metrics.Counter("validator_validation_errors_total", "Correctness-validator validation errors by stage.", metrics.Labels("stage", "drain"), 1)
			metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "error"), 1)
			return fmt.Errorf("stream session: %w", err)
		}
		report = sv.Finish()
	}
	metrics.Histogram("validator_drain_duration_seconds", "Correctness-validator per-session Kafka drain duration in seconds.", nil, metrics.SinceSeconds(drainStart))
	metrics.Counter("validator_events_drained_total", "Correctness-validator events drained from Kafka by topic.", metrics.Labels("topic", "orders_sent"), float64(counts.SentEvents))
	metrics.Counter("validator_events_drained_total", "Correctness-validator events drained from Kafka by topic.", metrics.Labels("topic", "orders_acked"), float64(counts.AckedEvents))

	recordViolations(report)
	rec := store.Record{
		SessionID:    sessionID,
		ContestantID: contestant,
		Report:       report,
		Status:       store.StatusScored,
		SentCount:    counts.SentEvents,
		AckedCount:   counts.AckedEvents,
		MatchedCount: counts.MatchedOrders,
		ComputedAtNS: uint64(time.Now().UnixNano()),
	}
	claimed, err := v.store.Save(ctx, rec)
	if err != nil {
		metrics.Counter("validator_validation_errors_total", "Correctness-validator validation errors by stage.", metrics.Labels("stage", "save"), 1)
		metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "error"), 1)
		return fmt.Errorf("save correctness: %w", err)
	}
	if !claimed {
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
		SentCount:        counts.SentEvents,
		AckedCount:       counts.AckedEvents,
		MatchedCount:     counts.MatchedOrders,
		JitterP50US:      report.Jitter.P50Us,
		JitterP99US:      report.Jitter.P99Us,
		JitterP999US:     report.Jitter.P999Us,
		JitterMaxUS:      report.Jitter.MaxUs,
		JitterInvRate:    report.Jitter.InversionRate,
	}); err != nil {
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
		"sent", counts.SentEvents, "acked", counts.AckedEvents, "matched", counts.MatchedOrders)
	return nil
}

// timeoutRecord performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func timeoutRecord(sessionID string, nowNS uint64) store.Record {
	return store.Record{
		SessionID:    sessionID,
		ContestantID: "",
		Report:       validate.Report{},
		Status:       store.StatusTimedOut,
		ComputedAtNS: nowNS,
	}
}

// recordValidationTimeout applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (v *validator) recordValidationTimeout(ctx context.Context, sessionID string) error {
	status, exists, err := v.store.SummaryStatus(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("idempotency check: %w", err)
	}
	if !exists {
		if _, err := v.store.Save(ctx, timeoutRecord(sessionID, uint64(time.Now().UnixNano()))); err != nil {
			return fmt.Errorf("save timeout placeholder: %w", err)
		}
	} else {
		v.log.Info("timeout fallback found existing summary; re-publishing it", "session_id", sessionID, "status", status)
	}
	ev, ok, err := v.store.LoadScore(ctx, sessionID)
	if err != nil {
		return err
	}
	if ok {
		return v.pub.Publish(ctx, ev)
	}
	return nil
}

// runStatusConsumer applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (v *validator) runStatusConsumer(ctx context.Context, brokers []string, group string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        group,
		Topic:          topics.TopicBenchmarkStatusUpdated,
		MinBytes:       1,
		MaxBytes:       1 << 20,
		MaxWait:        200 * time.Millisecond,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
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
			_ = reader.CommitMessages(ctx, m)
			continue
		}
		if ev.Status == topics.RunStatusCompleted {
			validateCtx, cancel := context.WithTimeout(ctx, v.validationTimeout)
			err := v.validateSession(validateCtx, ev.SessionID)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if errors.Is(err, context.DeadlineExceeded) {
					fallbackCtx, fallbackCancel := context.WithTimeout(context.Background(), 10*time.Second)
					fallbackErr := v.recordValidationTimeout(fallbackCtx, ev.SessionID)
					fallbackCancel()
					if fallbackErr == nil {
						metrics.Counter("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", metrics.Labels("result", "timed_out"), 1)
						v.log.Warn("validation timed out; recorded timed_out placeholder (re-validated on redelivery)", "session_id", ev.SessionID, "timeout_ms", v.validationTimeout.Milliseconds())
						err = nil
					} else {
						v.log.Error("record validation timeout fallback", "session_id", ev.SessionID, "error", fallbackErr)
						err = fallbackErr
					}
				}
			}
			if err != nil {
				v.log.Error("validate session; will retry on redelivery", "session_id", ev.SessionID, "error", err)
				continue
			}
		}
		if err := reader.CommitMessages(ctx, m); err != nil {
			v.log.Warn("commit benchmark.status.updated", "error", err)
		}
	}
}

// checkTimeoutConfig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func checkTimeoutConfig(validationTimeout, settleDelay time.Duration) error {
	if validationTimeout <= 0 {
		return fmt.Errorf("VALIDATION_TIMEOUT_MS must be > 0, got %d", validationTimeout.Milliseconds())
	}
	if validationTimeout <= settleDelay {
		return fmt.Errorf("VALIDATION_TIMEOUT_MS (%d) must exceed SETTLE_DELAY_MS (%d)", validationTimeout.Milliseconds(), settleDelay.Milliseconds())
	}
	return nil
}

// envOr performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// mustEnv performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func mustEnv(key string, log *slog.Logger) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		log.Error("missing required env", "key", key)
		os.Exit(1)
	}
	return v
}

// envInt performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// parseBrokers performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func parseBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
