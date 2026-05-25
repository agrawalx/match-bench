package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/iicpc/bot-fleet-controller/internal/orchestrator"
	"github.com/iicpc/bot-fleet-controller/internal/store"
	"github.com/iicpc/schemas/topics"
)

// Scenario is the workload shape the controller publishes for each session.
// v1 is hardcoded — there is no scenarios table yet (open question #1 in
// BOT_FLEET_PIPELINE.md §9). When that lands, replace the hardcoded value
// in main.go with a per-submission lookup.
type Scenario struct {
	WorkerCount      uint32
	BotCount         uint32
	OrdersPerBot     uint32
	GlobalSeed       uint64
	FIXVersion       string
	ProfileMix       []topics.BotProfileWeight
	ConnectTimeoutMS uint64
	WriteTimeoutMS   uint64

	// Operational deadlines for the controller (not part of WorkloadSpec).
	DeployDeadline   time.Duration
	ReadyDeadline    time.Duration
	BarrierSafetyGap time.Duration
	RunDuration      time.Duration
}

// HarborConfig produces the production image ref for a submission.
// Mirrors services/build-worker/internal/k8s/spawner.go:202 — the controller
// must use the same naming scheme the spawner pushed under.
type HarborConfig struct {
	Endpoint string // e.g. ghcr.io or harbor.example.com
	Project  string // e.g. iicpc or your ghcr username
}

func (h HarborConfig) ImageRef(submissionID string) string {
	return fmt.Sprintf("%s/%s/%s:latest", h.Endpoint, h.Project, submissionID)
}

// Runner drives one session through the lifecycle described in
// BOT_FLEET_PIPELINE.md §3.
type Runner struct {
	sessions *SessionManager
	store    *store.Store
	orch     *orchestrator.Client
	producer *Producer
	scenario Scenario
	harbor   HarborConfig
	log      *slog.Logger
}

func NewRunner(sessions *SessionManager, st *store.Store, orch *orchestrator.Client, producer *Producer, scenario Scenario, harbor HarborConfig, log *slog.Logger) *Runner {
	return &Runner{
		sessions: sessions,
		store:    st,
		orch:     orch,
		producer: producer,
		scenario: scenario,
		harbor:   harbor,
		log:      log,
	}
}

// Start kicks off a per-session goroutine. The runner takes responsibility
// for cleaning up the session map entry and the orchestrator slot.
func (r *Runner) Start(parent context.Context, req topics.BenchmarkRequested) {
	sess := &Session{
		SessionID:     req.SessionID,
		SubmissionID:  req.SubmissionID,
		ContestantID:  req.ContestantID,
		WorkerCount:   r.scenario.WorkerCount,
		BotCount:      r.scenario.BotCount,
		OrdersPerBot:  r.scenario.OrdersPerBot,
		ReadyReceived: make(map[uint32]topics.ReadySignal),
		readyCh:       make(chan topics.ReadySignal, int(r.scenario.WorkerCount)*2+1),
		CreatedAt:     time.Now().UTC(),
	}

	ctx, cancel := context.WithCancel(parent)
	sess.cancel = cancel

	if _, existed := r.sessions.Add(sess); existed {
		r.log.Info("session already tracked — ignoring duplicate benchmark.requested", "session_id", req.SessionID)
		cancel()
		return
	}

	go r.run(ctx, sess)
}

// run is the per-session goroutine. Walks through the lifecycle defined in
// CONVENTIONS.md §9 and BOT_FLEET_PIPELINE.md §3, publishing
// benchmark.status.updated at every transition.
func (r *Runner) run(ctx context.Context, sess *Session) {
	log := r.log.With("session_id", sess.SessionID, "submission_id", sess.SubmissionID)

	defer func() {
		sess.cancel()
		r.sessions.Drop(sess.SessionID)
	}()

	// 1. Look up submission so we know the protocol/port.
	sub, err := r.store.GetSubmission(ctx, sess.SubmissionID)
	if err != nil {
		r.fail(ctx, sess, "lookup submission: "+err.Error(), log)
		return
	}
	if sub == nil {
		r.fail(ctx, sess, "submission not found", log)
		return
	}

	// 2. Deploy slot.
	r.transition(ctx, sess, topics.RunStatusDeploying, "allocating sandbox slot", log)
	image := r.harbor.ImageRef(sess.SubmissionID)
	if _, err := r.orch.CreateSlot(ctx, sess.SessionID, image, sub.Port); err != nil {
		r.fail(ctx, sess, "create slot: "+err.Error(), log)
		return
	}
	sess.SlotID = sess.SessionID

	slot, err := r.orch.WaitForReady(ctx, sess.SessionID, r.scenario.DeployDeadline, 500*time.Millisecond)
	if err != nil || slot.State != orchestrator.StateReady {
		msg := "slot did not become ready"
		if err != nil {
			msg = msg + ": " + err.Error()
		} else {
			msg = msg + ": " + slot.Message
		}
		r.fail(ctx, sess, msg, log)
		r.releaseSlot(ctx, sess, log)
		return
	}
	sess.Endpoint = &slot.Endpoint

	// 3. Publish WorkloadSpec to each worker.
	specs := r.buildWorkloadSpecs(sess, sub)
	if err := r.producer.PublishWorkloadSpec(ctx, specs); err != nil {
		r.fail(ctx, sess, "publish workload specs: "+err.Error(), log)
		r.releaseSlot(ctx, sess, log)
		return
	}
	r.transition(ctx, sess, topics.RunStatusWaitingReady, "fanning in ready signals", log)

	// 4. Fan-in bot.ready until all worker_count signals arrive or the
	// deadline expires. If we received at least one signal at the deadline
	// we proceed (degraded benchmark) rather than failing outright.
	if err := r.awaitReady(ctx, sess, log); err != nil {
		r.fail(ctx, sess, err.Error(), log)
		r.releaseSlot(ctx, sess, log)
		return
	}

	// 5. Publish barrier.
	target := uint64(time.Now().Add(r.scenario.BarrierSafetyGap).UnixNano())
	if err := r.producer.PublishBarrier(ctx, sess.SessionID, target); err != nil {
		r.fail(ctx, sess, "publish barrier: "+err.Error(), log)
		r.releaseSlot(ctx, sess, log)
		return
	}
	r.transition(ctx, sess, topics.RunStatusBarrierFired, "barrier published", log)
	r.transition(ctx, sess, topics.RunStatusRunning, "bots firing", log)

	// 6. Bound the run. Workers self-terminate after sending orders_per_bot;
	// this is the upper bound on how long we wait before cleanup. No
	// workload-completed signal exists yet (open question #2 in
	// BOT_FLEET_PIPELINE.md §9).
	select {
	case <-time.After(r.scenario.RunDuration):
	case <-ctx.Done():
		log.Info("context cancelled during run; cleaning up")
	}

	// 7. Cleanup + complete.
	r.releaseSlot(ctx, sess, log)
	r.transition(ctx, sess, topics.RunStatusCompleted, "run completed", log)
}

// transition publishes a status update and records the new status on the
// session entry. Failures to publish are logged but do not crash the runner
// — the controller's view of state stays correct even if the status stream
// is degraded.
func (r *Runner) transition(ctx context.Context, sess *Session, status, message string, log *slog.Logger) {
	sess.Status = status
	sess.Message = message
	evt := topics.BenchmarkStatusUpdated{
		SessionID:    sess.SessionID,
		SubmissionID: sess.SubmissionID,
		Status:       status,
		Message:      message,
		UpdatedAt:    time.Now().UTC(),
	}
	if err := r.producer.PublishStatus(ctx, evt); err != nil {
		log.Error("publish status failed", "status", status, "error", err)
	} else {
		log.Info("session transition", "status", status, "message", message)
	}
}

// fail is the unified failure path. Marks the session failed via the same
// status pipeline so submission-api writes it through and the partial unique
// index releases the submission for re-trigger.
func (r *Runner) fail(ctx context.Context, sess *Session, message string, log *slog.Logger) {
	log.Error("session failed", "message", message)
	r.transition(ctx, sess, topics.RunStatusFailed, message, log)
}

// releaseSlot is best-effort. Logging only — if the orchestrator is down we
// would leak a pod, but the controller cannot do better from here.
func (r *Runner) releaseSlot(ctx context.Context, sess *Session, log *slog.Logger) {
	if sess.SlotID == "" {
		return
	}
	// Use a fresh context so cleanup runs even when the parent is cancelled.
	releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.orch.DeleteSlot(releaseCtx, sess.SlotID); err != nil {
		log.Error("release slot failed", "slot_id", sess.SlotID, "error", err)
	}
}

// awaitReady blocks until WorkerCount signals arrive or the ready deadline
// passes. Returns an error only if no signals at all arrive — partial
// readiness is allowed (degraded benchmark is preferable to no benchmark).
func (r *Runner) awaitReady(ctx context.Context, sess *Session, log *slog.Logger) error {
	deadline := time.NewTimer(r.scenario.ReadyDeadline)
	defer deadline.Stop()

	for uint32(len(sess.ReadyReceived)) < sess.WorkerCount {
		select {
		case sig := <-sess.readyCh:
			sess.ReadyReceived[sig.WorkerIndex] = sig
			log.Info("ready signal received",
				"worker_index", sig.WorkerIndex,
				"connected", sig.ConnectedCount,
				"total_received", len(sess.ReadyReceived),
				"expected", sess.WorkerCount,
			)
		case <-deadline.C:
			if len(sess.ReadyReceived) == 0 {
				return errors.New("no ready signals before deadline")
			}
			log.Warn("ready deadline expired with partial fan-in",
				"received", len(sess.ReadyReceived),
				"expected", sess.WorkerCount,
			)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// buildWorkloadSpecs assembles one WorkloadSpec per worker_index from the
// session metadata + the hardcoded scenario.
func (r *Runner) buildWorkloadSpecs(sess *Session, sub *store.SubmissionInfo) []topics.WorkloadSpec {
	specs := make([]topics.WorkloadSpec, 0, r.scenario.WorkerCount)
	for i := uint32(0); i < r.scenario.WorkerCount; i++ {
		specs = append(specs, topics.WorkloadSpec{
			SessionID:        sess.SessionID,
			SubmissionID:     sess.SubmissionID,
			ContestantID:     sub.ContestantID,
			TargetHost:       sess.Endpoint.Host,
			TargetPort:       uint16(sess.Endpoint.Port),
			Protocol:         sub.Protocol,
			WorkerIndex:      i,
			WorkerCount:      r.scenario.WorkerCount,
			BotCount:         r.scenario.BotCount,
			OrdersPerBot:     r.scenario.OrdersPerBot,
			GlobalSeed:       r.scenario.GlobalSeed,
			FIXVersion:       r.scenario.FIXVersion,
			ProfileMix:       r.scenario.ProfileMix,
			ConnectTimeoutMS: r.scenario.ConnectTimeoutMS,
			WriteTimeoutMS:   r.scenario.WriteTimeoutMS,
		})
	}
	return specs
}
