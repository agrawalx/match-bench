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

// DefaultMaxTasksPerWorker is the per-pod task ceiling that drives WorkerCount
// when MAX_TASKS_PER_WORKER is unset. 1,000 per the load-scenarios design
// (architecture_v2.md §"Load Scenarios" → Service-by-service touchpoints).
//
// WorkerCount for a session is ceil(total_tasks_in_scenario / MaxTasksPerWorker).
// Tasks are sharded round-robin by task_id across the WorkerCount worker pods.
//
// This is the operator's per-pod-concentration knob: raise it (e.g. above a
// scenario's total task count) to pin ALL load on a single bot pod and measure
// that pod's raw generation ceiling; lower it to fan the load across more pods.
// Two hard couplings:
//   - WorkerCount must be <= the workload.assignments partition count (24 per
//     topic-init): each spec is pinned to partition worker_index%N, and
//     PublishWorkloadSpec fails the run outright if WorkerCount exceeds N.
//   - WorkerCount must be <= the bot-fleet replica count, because each pod
//     runs assignments serially — if WorkerCount exceeds the number of pods,
//     some pod is handed two assignments and the second misses its barrier.
//     Pre-scale the fleet (KEDA minReplicaCount) for multi-worker scenarios.
const DefaultMaxTasksPerWorker = 1000

// RunConfig holds the deployment-wide operational deadlines that are not part
// of any one scenario. They tune the controller's lifecycle (slot-deploy
// timeout, ready-fan-in timeout, barrier safety gap) and are read once from
// env at startup. Distinct from a Scenario, which is per-run and per-session.
type RunConfig struct {
	GlobalSeed       uint64
	FIXVersion       string
	ConnectTimeoutMS uint64
	WriteTimeoutMS   uint64

	DeployDeadline   time.Duration
	ReadyDeadline    time.Duration
	BarrierSafetyGap time.Duration

	// MaxTasksPerWorker is the per-pod task ceiling that drives WorkerCount
	// (ceil(total_tasks / MaxTasksPerWorker)). Read from MAX_TASKS_PER_WORKER;
	// defaults to DefaultMaxTasksPerWorker. See DefaultMaxTasksPerWorker for the
	// WorkerCount<=replicas coupling.
	MaxTasksPerWorker int
}

// Runner drives one session through the benchmark lifecycle. Each
// benchmark.requested message produces one Runner.Run invocation.
//
// Sessions are processed SERIALLY by the benchmark.requested consumer: the
// consumer calls Runner.Run synchronously and only pulls the next Kafka
// message after the current session completes. With one controller replica
// this gives every session exclusive use of the orchestrator + bot fleet,
// which is what we want for clean metrics. Cross-group parallelism is a v2
// concern; v1 cap is one in-flight session globally.
//
// Within a session the lifecycle is:
//
//  1. Look up submission (protocol, port) and scenario (TaskSpec list,
//     duration).
//  2. Allocate a sandbox slot via the orchestrator (HTTP POST /slots);
//     poll until state=ready or DeployDeadline.
//  3. Compute WorkerCount = ceil(len(TaskSpecs) / MaxTasksPerWorker), shard
//     tasks round-robin by task_id, publish one WorkloadSpec per worker on
//     workload.assignments.
//  4. Fan in bot.ready from each worker, up to ReadyDeadline. Partial
//     fan-in is acceptable — the run proceeds degraded.
//  5. Compute barrier_epoch_ns = now + BarrierSafetyGap and publish
//     BarrierEvent on the barrier topic. Workers unblock at that instant.
//  6. Sleep for scenario.DurationNs. Workers self-terminate when each
//     TaskSpec's duration expires; the controller's wait is the wall-clock
//     upper bound.
//  7. DELETE the slot and publish status=completed.
//
// On any error in 1–5: fail via PublishStatus(failed) and release the slot
// if it was allocated. The runner returns after step 7; the consumer then
// commits the Kafka offset and pulls the next message.
type Runner struct {
	sessions  *SessionManager
	store     *store.Store
	orch      *orchestrator.Client
	producer  *Producer
	runConfig RunConfig
	log       *slog.Logger
}

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

// Run drives one session to completion synchronously. The benchmark.requested
// consumer is the only caller; it blocks here until the session finishes (or
// fails) before pulling the next message.
//
// On a benchmark.requested redelivery for a session_id that is already
// tracked in the SessionManager, Run is a no-op — protects against Kafka
// re-delivery duplicating a session that is already mid-flight from an
// earlier process.
func (r *Runner) Run(parent context.Context, req topics.BenchmarkRequested) {
	log := r.log.With(
		"session_id", req.SessionID,
		"submission_id", req.SubmissionID,
		"run_group_id", req.RunGroupID,
		"scenario_id", req.ScenarioID,
	)

	// Idempotency against redelivery: the benchmark.requested offset is committed
	// only after Run returns, so a crash between the final status publish and the
	// commit re-delivers this message. The in-memory session map is empty after a
	// restart, so guard on the durable run status — re-running a terminal session
	// would spawn a duplicate algo pod and duplicate orders.sent/acked telemetry.
	if status, serr := r.store.RunStatus(parent, req.SessionID); serr != nil {
		log.Warn("run-status precheck failed; proceeding", "error", serr)
	} else if status == topics.RunStatusCompleted || status == topics.RunStatusFailed {
		log.Info("benchmark.requested for an already-terminal run; skipping redelivery", "status", status)
		return
	}

	// Load scenario before touching anything else — a missing/garbled scenario
	// is the only error we cannot recover from, and discovering it later (after
	// allocating a slot) leaks resources.
	scenario, err := r.store.LoadScenario(parent, req.ScenarioID)
	if err != nil {
		log.Error("load scenario failed", "error", err)
		// Best-effort failure publish; the run row exists in submission-api's
		// database with status='requested' and will stick there until the
		// recovery sweep marks it failed on next restart. Publishing here
		// short-circuits that wait.
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

// runSession executes steps 1–7 of the lifecycle for one session.
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
	// Step 1 — look up submission.
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

	// Step 2 — allocate sandbox slot.
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

	// Step 3 — publish per-worker WorkloadSpecs.
	//
	// Note on the barrier epoch: previously this was computed here (before
	// fan-in) and embedded in each WorkloadSpec so workers could fall back
	// to it if the BarrierEvent was lost. That was buggy — fan-in can take
	// up to ReadyDeadline (30s default), so by the time workers received
	// the BarrierEvent the embedded epoch was ~30s in the past, and the
	// workers' instant_from_unix_nanos returned Instant::now() (fire
	// immediately). Workers also never actually read the embedded field;
	// wait_for_barrier always uses BarrierEvent.target_epoch_unix_nanos.
	// Now we compute the epoch AFTER fan-in (step 5) so it stays fresh.
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

	// Step 4 — fan-in ready signals.
	stageStart = time.Now()
	if err := r.awaitReady(ctx, sess, log); err != nil {
		recordSessionStage("await_ready", stageStart, err)
		r.fail(ctx, sess, err.Error(), log)
		r.releaseSlot(sess, log)
		return
	}
	recordSessionStage("await_ready", stageStart, nil)

	// Step 5 — compute barrier epoch and publish BarrierEvent.
	// BarrierSafetyGap of 500ms is comfortable here because fan-in is done
	// and all workers are blocked on wait_for_barrier; the only variance
	// left is Kafka delivery time of the BarrierEvent itself (~ms).
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

	// Step 6 — wait for scenario duration. Workers self-terminate when each
	// TaskSpec's duration expires; the controller waits the scenario's
	// wall-clock duration plus a small safety margin so it doesn't tear down
	// the slot before the last task finishes sending. The safety margin is
	// the same BarrierSafetyGap that delayed the firing window.
	totalDuration := time.Duration(scenario.DurationNs) + r.runConfig.BarrierSafetyGap
	select {
	case <-time.After(totalDuration):
	case <-ctx.Done():
		// Shutdown (or parent cancellation) interrupted the run before its
		// scenario duration elapsed. The slot is about to be torn down and
		// the bot workers have not finished their schedule, so this is a
		// failure, not a completion — reporting 'completed' here would write
		// a terminal success that the recovery sweep can never correct.
		// Release the slot and fail via a fresh (non-cancelled) context so
		// the status publish itself isn't aborted by the same cancellation.
		log.Info("context cancelled during run; marking failed and cleaning up")
		r.releaseSlot(sess, log)
		failCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		r.fail(failCtx, sess, "controller shutdown during run", log)
		return
	}

	// Step 7 — cleanup + complete.
	stageStart = time.Now()
	r.releaseSlot(sess, log)
	recordSessionStage("release_slot", stageStart, nil)
	r.transition(ctx, sess, topics.RunStatusCompleted, "run completed", log)
	result = "completed"
}

// computeWorkerCount = ceil(totalTasks / maxTasksPerWorker), clamped to >= 1.
// Used at session start to determine how many bot-worker pods need to be in
// the consumer group for this session. A non-positive maxTasksPerWorker falls
// back to DefaultMaxTasksPerWorker.
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

// transition publishes a status update and records the new status on the
// session entry. Failures to publish are logged but do not crash the runner —
// the controller's internal view of state stays correct even if the status
// stream is degraded.
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

// fail is the unified failure path. Marks the session failed via the same
// status pipeline so submission-api writes it through and the parent
// run-group's status rolls up to 'failed'.
func (r *Runner) fail(_ context.Context, sess *Session, message string, log *slog.Logger) {
	log.Error("session failed", "message", message)
	// The terminal failure MUST be published even when the session ctx was
	// cancelled by shutdown (SIGTERM mid-step) — otherwise the run row is left
	// non-terminal until the next recovery sweep. Detach from the cancellable
	// session ctx, mirroring releaseSlot and the step-6 failCtx.
	pubCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r.transition(pubCtx, sess, topics.RunStatusFailed, message, log)
}

// publishFailure is a thin wrapper for the early-failure path where we have
// not yet built a Session object (e.g. scenario load failed). Constructs a
// minimal BenchmarkStatusUpdated event directly so submission-api can
// record the failure without us going through transition().
func (r *Runner) publishFailure(_ context.Context, req topics.BenchmarkRequested, message string, log *slog.Logger) {
	// Detached from the caller's (cancellable) ctx so the failure still publishes
	// during shutdown — same rationale as fail().
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

// releaseSlot is best-effort. Logging only — if the orchestrator is down we
// would leak a pod, but the controller cannot do better from here.
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

// awaitReady blocks until WorkerCount signals arrive or the ready deadline
// passes. Returns an error only if no signals at all arrive — partial
// readiness is allowed (degraded benchmark is preferable to no benchmark).
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

// buildWorkloadSpecs shards the scenario's task list across WorkerCount worker
// pods round-robin by task_id, then assembles one WorkloadSpec per worker.
//
// Round-robin (rather than contiguous chunks) keeps each worker's TaskSpec
// distribution close to the scenario's overall profile mix — important for
// rate fairness when a worker has fewer tasks than its peers.
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

// recordSessionStage exposes blocking controller lifecycle stages.
func recordSessionStage(stage string, start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := metrics.Labels("stage", stage, "result", result)
	metrics.Counter("session_stage_total", "Benchmark session stages by result.", labels, 1)
	metrics.Histogram("session_stage_duration_seconds", "Benchmark session stage duration in seconds.", labels, metrics.SinceSeconds(start))
}
