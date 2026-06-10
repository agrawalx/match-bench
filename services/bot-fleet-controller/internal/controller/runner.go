// Package controller implements runner behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/iicpc/bot-fleet-controller/internal/orchestrator"
	"github.com/iicpc/bot-fleet-controller/internal/store"
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
)

const DefaultMaxTasksPerWorker = 1000

// RunConfig groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type RunConfig struct {
	GlobalSeed       uint64
	FIXVersion       string
	ConnectTimeoutMS uint64
	WriteTimeoutMS   uint64

	DeployDeadline   time.Duration
	ReadyDeadline    time.Duration
	BarrierSafetyGap time.Duration

	MaxTasksPerWorker int
}

// Runner groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Runner struct {
	sessions  *SessionManager
	store     *store.Store
	orch      *orchestrator.Client
	producer  *Producer
	runConfig RunConfig
	log       *slog.Logger
}

// NewRunner performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewRunner(sessions *SessionManager, st *store.Store, orch *orchestrator.Client, producer *Producer, runConfig RunConfig, log *slog.Logger) *Runner {
	return &Runner{
		sessions:  sessions,
		store:     st,
		orch:      orch,
		producer:  producer,
		runConfig: runConfig,
		log:       log,
	}
}

// Run applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) Run(parent context.Context, req topics.BenchmarkRequested) {
	log := r.log.With(
		"session_id", req.SessionID,
		"submission_id", req.SubmissionID,
		"run_group_id", req.RunGroupID,
		"scenario_id", req.ScenarioID,
	)

	if status, serr := r.store.RunStatus(parent, req.SessionID); serr != nil {
		log.Warn("run-status precheck failed; proceeding", "error", serr)
	} else if status == topics.RunStatusCompleted || status == topics.RunStatusFailed {
		log.Info("benchmark.requested for an already-terminal run; skipping redelivery", "status", status)
		return
	}

	scenario, err := r.store.LoadScenario(parent, req.ScenarioID)
	if err != nil {
		log.Error("load scenario failed", "error", err)
		r.publishFailure(parent, req, "load scenario: "+err.Error(), log)
		return
	}
	if scenario == nil || len(scenario.TaskSpecs) == 0 {
		r.publishFailure(parent, req, "scenario has zero tasks", log)
		return
	}

	workerCount := computeWorkerCount(len(scenario.TaskSpecs), r.runConfig.MaxTasksPerWorker)
	log = log.With("scenario_name", scenario.Name, "total_tasks", len(scenario.TaskSpecs), "worker_count", workerCount)
	metrics.Counter("sessions_started_total", "Benchmark sessions started by scenario.", metrics.Labels("scenario_name", scenario.Name), 1)

	sess := &Session{
		SessionID:     req.SessionID,
		SubmissionID:  req.SubmissionID,
		ContestantID:  req.ContestantID,
		RunGroupID:    req.RunGroupID,
		WorkerCount:   workerCount,
		ReadyReceived: make(map[uint32]topics.ReadySignal),
		readyCh:       make(chan topics.ReadySignal, int(workerCount)*2+1),
		CreatedAt:     time.Now().UTC(),
	}

	ctx, cancel := context.WithCancel(parent)
	sess.cancel = cancel
	defer cancel()

	if _, existed := r.sessions.Add(sess); existed {
		log.Info("session already tracked — ignoring duplicate benchmark.requested")
		return
	}
	defer r.sessions.Drop(sess.SessionID)

	r.runSession(ctx, sess, scenario, workerCount, log)
}

// runSession applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) runSession(
	ctx context.Context,
	sess *Session,
	scenario *topics.Scenario,
	workerCount uint32,
	log *slog.Logger,
) {
	sessionStart := time.Now()
	result := "failed"
	defer func() {
		labels := metrics.Labels("scenario_name", scenario.Name, "result", result)
		metrics.Counter("sessions_completed_total", "Benchmark sessions completed by scenario and result.", labels, 1)
		metrics.Histogram("session_duration_seconds", "Benchmark session duration in seconds.", labels, metrics.SinceSeconds(sessionStart))
	}()
	stageStart := time.Now()
	sub, err := r.store.GetSubmission(ctx, sess.SubmissionID)
	recordSessionStage("load_submission", stageStart, err)
	if err != nil {
		r.fail(ctx, sess, "lookup submission: "+err.Error(), log)
		return
	}
	if sub == nil {
		r.fail(ctx, sess, "submission not found", log)
		return
	}
	if sub.ImageRef == "" {
		r.fail(ctx, sess, "submission has no built image ref", log)
		return
	}

	r.transition(ctx, sess, topics.RunStatusDeploying, "allocating sandbox slot", log)
	image := sub.ImageRef
	stageStart = time.Now()
	if _, err := r.orch.CreateSlot(ctx, sess.SessionID, sess.ContestantID, image, sub.Port); err != nil {
		recordSessionStage("create_slot", stageStart, err)
		r.fail(ctx, sess, "create slot: "+err.Error(), log)
		return
	}
	recordSessionStage("create_slot", stageStart, nil)
	sess.SlotID = sess.SessionID

	stageStart = time.Now()
	slot, err := r.orch.WaitForReady(ctx, sess.SessionID, r.runConfig.DeployDeadline, 500*time.Millisecond)
	recordSessionStage("wait_slot_ready", stageStart, err)
	if err != nil || slot.State != orchestrator.StateReady {
		msg := "slot did not become ready"
		if err != nil {
			msg = msg + ": " + err.Error()
		} else {
			msg = msg + ": " + slot.Message
		}
		r.fail(ctx, sess, msg, log)
		r.releaseSlot(sess, log)
		return
	}
	sess.Endpoint = &slot.Endpoint

	specs := r.buildWorkloadSpecs(sess, sub, scenario, workerCount)
	stageStart = time.Now()
	if err := r.producer.PublishWorkloadSpec(ctx, specs); err != nil {
		recordSessionStage("publish_workload", stageStart, err)
		r.fail(ctx, sess, "publish workload specs: "+err.Error(), log)
		r.releaseSlot(sess, log)
		return
	}
	recordSessionStage("publish_workload", stageStart, nil)
	r.transition(ctx, sess, topics.RunStatusWaitingReady, "fanning in ready signals", log)

	stageStart = time.Now()
	if err := r.awaitReady(ctx, sess, log); err != nil {
		recordSessionStage("await_ready", stageStart, err)
		r.fail(ctx, sess, err.Error(), log)
		r.releaseSlot(sess, log)
		return
	}
	recordSessionStage("await_ready", stageStart, nil)

	barrierEpochNs := uint64(time.Now().Add(r.runConfig.BarrierSafetyGap).UnixNano())
	stageStart = time.Now()
	if err := r.producer.PublishBarrier(ctx, sess.SessionID, barrierEpochNs); err != nil {
		recordSessionStage("publish_barrier", stageStart, err)
		r.fail(ctx, sess, "publish barrier: "+err.Error(), log)
		r.releaseSlot(sess, log)
		return
	}
	recordSessionStage("publish_barrier", stageStart, nil)
	r.transition(ctx, sess, topics.RunStatusBarrierFired, "barrier published", log)
	r.transition(ctx, sess, topics.RunStatusRunning, "bots firing", log)

	totalDuration := time.Duration(scenario.DurationNs) + r.runConfig.BarrierSafetyGap
	select {
	case <-time.After(totalDuration):
	case <-ctx.Done():
		log.Info("context cancelled during run; marking failed and cleaning up")
		r.releaseSlot(sess, log)
		failCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		r.fail(failCtx, sess, "controller shutdown during run", log)
		return
	}

	stageStart = time.Now()
	r.releaseSlot(sess, log)
	recordSessionStage("release_slot", stageStart, nil)
	r.transition(ctx, sess, topics.RunStatusCompleted, "run completed", log)
	result = "completed"
}

// computeWorkerCount performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func computeWorkerCount(totalTasks, maxTasksPerWorker int) uint32 {
	if totalTasks <= 0 {
		return 1
	}
	if maxTasksPerWorker <= 0 {
		maxTasksPerWorker = DefaultMaxTasksPerWorker
	}
	count := (totalTasks + maxTasksPerWorker - 1) / maxTasksPerWorker
	return uint32(count)
}

// transition applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) transition(ctx context.Context, sess *Session, status, message string, log *slog.Logger) {
	sess.Status = status
	sess.Message = message
	evt := topics.BenchmarkStatusUpdated{
		SessionID:    sess.SessionID,
		SubmissionID: sess.SubmissionID,
		RunGroupID:   sess.RunGroupID,
		Status:       status,
		Message:      message,
		UpdatedAt:    time.Now().UTC(),
	}
	if err := r.producer.PublishStatus(ctx, evt); err != nil {
		log.Error("publish status failed", "status", status, "error", err)
	} else {
		metrics.Counter("session_transitions_total", "Session transitions published by status.", metrics.Labels("status", status), 1)
		log.Info("session transition", "status", status, "message", message)
	}
}

// fail applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) fail(_ context.Context, sess *Session, message string, log *slog.Logger) {
	log.Error("session failed", "message", message)
	pubCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r.transition(pubCtx, sess, topics.RunStatusFailed, message, log)
}

// publishFailure applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) publishFailure(_ context.Context, req topics.BenchmarkRequested, message string, log *slog.Logger) {
	pubCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	evt := topics.BenchmarkStatusUpdated{
		SessionID:    req.SessionID,
		SubmissionID: req.SubmissionID,
		RunGroupID:   req.RunGroupID,
		Status:       topics.RunStatusFailed,
		Message:      message,
		UpdatedAt:    time.Now().UTC(),
	}
	if err := r.producer.PublishStatus(pubCtx, evt); err != nil {
		log.Error("publish early failure status", "error", err, "message", message)
	}
}

// releaseSlot applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) releaseSlot(sess *Session, log *slog.Logger) {
	if sess.SlotID == "" {
		return
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.orch.DeleteSlot(releaseCtx, sess.SlotID); err != nil {
		log.Error("release slot failed", "slot_id", sess.SlotID, "error", err)
	}
}

// awaitReady applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) awaitReady(ctx context.Context, sess *Session, log *slog.Logger) error {
	deadline := time.NewTimer(r.runConfig.ReadyDeadline)
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
				metrics.Counter("ready_none_total", "Sessions with no ready signals before deadline.", nil, 1)
				return errors.New("no ready signals before deadline")
			}
			metrics.Counter("ready_partial_total", "Sessions with partial ready fan-in.", nil, 1)
			log.Warn("ready deadline expired with partial fan-in",
				"received", len(sess.ReadyReceived),
				"expected", sess.WorkerCount,
			)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	metrics.Counter("ready_signals_total", "Ready fan-in completions by result.", metrics.Labels("result", "full"), 1)
	return nil
}

// buildWorkloadSpecs applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) buildWorkloadSpecs(
	sess *Session,
	sub *store.SubmissionInfo,
	scenario *topics.Scenario,
	workerCount uint32,
) []topics.WorkloadSpec {
	tasksByWorker := make([][]topics.TaskSpec, workerCount)
	for i := range tasksByWorker {
		tasksByWorker[i] = make([]topics.TaskSpec, 0)
	}
	for i, ts := range scenario.TaskSpecs {
		shard := uint32(i) % workerCount
		tasksByWorker[shard] = append(tasksByWorker[shard], ts)
	}

	specs := make([]topics.WorkloadSpec, 0, workerCount)
	for i := uint32(0); i < workerCount; i++ {
		specs = append(specs, topics.WorkloadSpec{
			SessionID:        sess.SessionID,
			SubmissionID:     sess.SubmissionID,
			ContestantID:     sub.ContestantID,
			TargetHost:       sess.Endpoint.Host,
			TargetPort:       uint16(sess.Endpoint.Port),
			Protocol:         sub.Protocol,
			WorkerIndex:      i,
			WorkerCount:      workerCount,
			GlobalSeed:       r.runConfig.GlobalSeed,
			FIXVersion:       r.runConfig.FIXVersion,
			ConnectTimeoutMS: r.runConfig.ConnectTimeoutMS,
			WriteTimeoutMS:   r.runConfig.WriteTimeoutMS,
			Tasks:            tasksByWorker[i],
		})
	}
	return specs
}

// recordSessionStage performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordSessionStage(stage string, start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := metrics.Labels("stage", stage, "result", result)
	metrics.Counter("session_stage_total", "Benchmark session stages by result.", labels, 1)
	metrics.Histogram("session_stage_duration_seconds", "Benchmark session stage duration in seconds.", labels, metrics.SinceSeconds(start))
}
