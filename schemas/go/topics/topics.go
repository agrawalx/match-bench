package topics

import "time"

const (
	TopicSubmissionBuildRequested = "submission.build.requested"
	TopicSubmissionStatusUpdated  = "submission.status.updated"
	TopicBenchmarkRequested       = "benchmark.requested"
	TopicBenchmarkStatusUpdated   = "benchmark.status.updated"
	TopicWorkloadAssignments      = "workload.assignments"
	TopicBarrier                  = "barrier"
	TopicBotReady                 = "bot.ready"
	TopicWorkloadFailed           = "workload.failed"
	TopicOrdersSent               = "orders.sent"
	TopicOrdersAcked              = "orders.acked"
	TopicScoresCorrectness        = "scores.correctness"
	TopicLeaderboardUpdates       = "leaderboard.updates"
	TelemetryPriceScale           = uint64(1_000_000_000)
)

// SubmissionBuildRequested is published to "submission.build.requested"
// by the submission-api after the artifact is stored in MinIO and metadata
// is written to PostgreSQL. Consumed by: build-worker.
type SubmissionBuildRequested struct {
	SubmissionID string    `json:"submission_id"`
	ContestantID string    `json:"contestant_id"` // reserved; empty until OAuth is added
	ArtifactPath string    `json:"artifact_path"` // MinIO object path: submissions/{id}/artifact.zip
	Language     string    `json:"language"`      // cpp | rust | go
	Protocol     string    `json:"protocol"`      // FIX | REST | WS
	Port         int       `json:"port"`          // port the algorithm listens on
	BuildType    string    `json:"build_type"`    // cmake | cargo | go
	BuildTarget  string    `json:"build_target"`  // binary name declared in benchmark.yaml
	TeamName     string    `json:"team_name"`
	SHA256       string    `json:"sha256"`
	RequestedAt  time.Time `json:"requested_at"`
}

const (
	StatusUploaded  = "uploaded"
	StatusBuilding  = "building"
	StatusScanned   = "scanned"
	StatusSBOMReady = "sbom_ready"
	StatusReady     = "ready"
	StatusFailed    = "failed"
)

// Run status values for the runs table.
//
// HARD INVARIANT: terminal values (completed, failed) may only be written by
// the bot-fleet-controller — via a BenchmarkStatusUpdated message published
// to "benchmark.status.updated" and consumed by submission-api. The only
// exception is the controller's startup recovery sweep, which writes
// 'failed' directly to release the partial unique index on
// runs(submission_id) WHERE status NOT IN ('completed','failed').
//
// Non-terminal states are owned by the controller's per-session goroutine
// driving the run lifecycle: deploying → waiting_ready → barrier_fired →
// running, then a terminal value.
const (
	RunStatusRequested    = "requested"     // submission-api accepted the click
	RunStatusDeploying    = "deploying"     // controller is allocating a sandbox slot
	RunStatusWaitingReady = "waiting_ready" // workload published, fanning in bot.ready
	RunStatusBarrierFired = "barrier_fired" // barrier event published
	RunStatusRunning      = "running"       // bots actively firing
	RunStatusCompleted    = "completed"     // controller: terminal success
	RunStatusFailed       = "failed"        // controller: terminal failure
)

// BenchmarkRequested is published to "benchmark.requested" by submission-api when
// the user clicks "start benchmark" on a submission. Consumed by: bot-fleet-controller.
// Key: session_id (so a future multi-replica controller could shard by session).
//
// One "start benchmark" click expands into a run-group with multiple child sessions
// (one per scenario in the scenarios table — constant, spike, ramp in v1). The
// submission-api publishes one BenchmarkRequested per session, all sharing the same
// RunGroupID. Each message carries the ScenarioID the controller should load.
type BenchmarkRequested struct {
	SessionID    string    `json:"session_id"`    // UUID v7, minted by submission-api
	SubmissionID string    `json:"submission_id"` // referenced submission (must be 'ready')
	ContestantID string    `json:"contestant_id"` // reserved; empty until OAuth
	RunGroupID   string    `json:"run_group_id"`  // parent group; shared across the run-group's sessions
	ScenarioID   string    `json:"scenario_id"`   // which row in scenarios table this session runs
	RequestedAt  time.Time `json:"requested_at"`
}

// BenchmarkStatusUpdated is published to "benchmark.status.updated" by
// bot-fleet-controller on every state transition. Consumed by:
//   - submission-api: refreshes the runs row in PostgreSQL. This consumer
//     is the only writer of the terminal 'completed' and 'failed' values
//     into runs.status (see the hard invariant on RunStatus* constants
//     above). All non-terminal transitions are written by the controller's
//     per-session goroutine.
//   - sse-gateway (fan-out to frontend SSE clients) — future, not built yet.
//
// Key: session_id.
type BenchmarkStatusUpdated struct {
	SessionID    string    `json:"session_id"`
	SubmissionID string    `json:"submission_id"` // included for consumers that index by submission
	RunGroupID   string    `json:"run_group_id"`  // parent group; allows the frontend / SSE to correlate sibling sessions
	Status       string    `json:"status"`        // one of RunStatus* constants above
	Message      string    `json:"message"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// CorrectnessScoreEvent is published to "scores.correctness" (JSON) by the
// correctness-validator after the post-run order-book replay. Consumed by the
// scoring service for the hard correctness gate. Key: session_id.
type CorrectnessScoreEvent struct {
	SessionID        string  `json:"session_id"`
	ContestantID     string  `json:"contestant_id"`
	ValidFills       uint64  `json:"valid_fills"`
	TotalFills       uint64  `json:"total_fills"`
	CorrectnessScore float64 `json:"correctness_score"` // valid_fills / total_fills
	ViolationCount   uint32  `json:"violation_count"`
	ComputedAtNS     uint64  `json:"computed_at_ns"`
}

// SubmissionStatusUpdated is published to "submission.status.updated"
// by the build-worker on every state transition. Consumed by: submission-api (status queries).
type SubmissionStatusUpdated struct {
	SubmissionID string    `json:"submission_id"`
	Status       string    `json:"status"` // uploaded | building | scanned | sbom_ready | ready | failed
	Message      string    `json:"message"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// WorkloadSpec is published to "workload.assignments" by the test controller.
// Each message is keyed by session_id:worker_index and consumed by one bot-fleet worker.
//
// The flat (BotCount, OrdersPerBot, ProfileMix) shape is replaced by a list of
// TaskSpec values. Every TaskSpec is one tokio task in the worker = one TCP
// connection = one constant-rate sender. Load-pattern variation (spike, ramp)
// emerges from the schedule of TaskSpecs: tasks start at their StartOffsetNs
// and stop after DurationNs. Bots never change behavior mid-flight.
type WorkloadSpec struct {
	SessionID        string `json:"session_id"`
	SubmissionID     string `json:"submission_id"`
	ContestantID     string `json:"contestant_id"`
	TargetHost       string `json:"target_host"` // IP of contestant pod
	TargetPort       uint16 `json:"target_port"`
	Protocol         string `json:"protocol"` // FIX | REST | WS
	WorkerIndex      uint32 `json:"worker_index"`
	WorkerCount      uint32 `json:"worker_count"` // Total worker pods
	GlobalSeed       uint64 `json:"global_seed"`
	FIXVersion       string `json:"fix_version"`
	ConnectTimeoutMS uint64 `json:"connect_timeout_ms"`
	WriteTimeoutMS   uint64 `json:"write_timeout_ms"`
	// barrier_epoch_ns was historically carried here as a "fallback" if the
	// BarrierEvent was lost, but workers never read this field — they wait
	// for BarrierEvent (kafka::wait_for_barrier) and use its target_epoch_unix_nanos.
	// Removed because computing the epoch before fan-in meant it was stale
	// (up to ReadyDeadline seconds in the past) by the time workers received
	// the BarrierEvent. The controller now computes the epoch AFTER fan-in
	// using the same BarrierSafetyGap, so the BarrierEvent value is fresh.
	Tasks []TaskSpec `json:"tasks"` // this worker's slice of the scenario's task list
}

// TaskSpec is one sender: one tokio task, one TCP connection, one constant rate.
//
// All tasks pre-open their TCP connection at barrier time (avoids cold-start jitter
// contaminating spike measurements). Each task sleeps until BarrierEpochNs +
// StartOffsetNs, then sends at TargetRPS via fixed-interval pacing until
// BarrierEpochNs + StartOffsetNs + DurationNs.
type TaskSpec struct {
	TaskID        uint32 `json:"task_id"`
	Profile       string `json:"profile"`         // hft | retail | institutional
	TargetRPS     uint32 `json:"target_rps"`      // orders per second, constant for this task's lifetime
	StartOffsetNs uint64 `json:"start_offset_ns"` // relative to barrier epoch
	DurationNs    uint64 `json:"duration_ns"`     // how long this task fires
	// Order-type mix as a percentage of messages sent by this task. The limit
	// fraction is implied: 100 - MarketPct - CancelPct - ReplacePct. Source:
	// architecture_v2.md Bot Profiles. Omitted (zero) decodes as all-limit,
	// which preserves the pre-mix behaviour for older scenarios.
	MarketPct  uint8 `json:"market_pct"`
	CancelPct  uint8 `json:"cancel_pct"`
	ReplacePct uint8 `json:"replace_pct"`
}

// Scenario is the controller-side representation of a row in the scenarios table.
// Not a Kafka message — included here because the bot-fleet-controller and
// submission-api both read/write the same shape, and JSONB column storage uses
// the same field names.
type Scenario struct {
	ScenarioID string     `json:"scenario_id"`
	Name       string     `json:"name"`        // constant | spike | ramp
	DurationNs uint64     `json:"duration_ns"` // wall-time of the session
	TaskSpecs  []TaskSpec `json:"task_specs"`  // full task list — sharded across worker pods by the controller
}

// BarrierEvent is published to "barrier" once all workers have reported ready.
type BarrierEvent struct {
	SessionID            string `json:"session_id"`
	TargetEpochUnixNanos uint64 `json:"target_epoch_unix_nanos"`
}

// ReadySignal is published by each bot-fleet worker to "bot.ready".
//
// TaskCount is the number of TaskSpec entries assigned to this worker. The
// old name BotCount referred to the pre-scenario bot_count knob and is gone;
// the JSON key is now "task_count" to match what the Rust producer emits.
type ReadySignal struct {
	SessionID        string `json:"session_id"`
	SubmissionID     string `json:"submission_id"`
	WorkerID         string `json:"worker_id"`
	WorkerIndex      uint32 `json:"worker_index"`
	WorkerCount      uint32 `json:"worker_count"`
	TaskCount        uint32 `json:"task_count"`
	ConnectedCount   uint32 `json:"connected_count"`
	ReadyAtUnixNanos uint64 `json:"ready_at_unix_nanos"`
}

// OrderSentBatch is MessagePack-encoded on "orders.sent" to avoid per-order Kafka writes.
type OrderSentBatch struct {
	SessionID string           `json:"session_id" msgpack:"session_id"`
	WorkerID  string           `json:"worker_id" msgpack:"worker_id"`
	Events    []OrderSentEvent `json:"events" msgpack:"events"`
}

// OrderSentEvent records one outbound order with the three bot-side
// timestamps the telemetry-ingester needs to detect coordinated omission.
//
// Timestamp definitions (all CLOCK_REALTIME nanoseconds, bot-side):
//   - TargetSendTSNS (t0): the schedule's intended fire time. Deterministic
//     from barrier_epoch + task.start_offset + seq*(1e9/target_rps).
//     Captured BEFORE sleep_until — never a clock read. The gap
//     SendTSNS - TargetSendTSNS IS coordinated omission, by definition.
//   - SendTSNS (t1): wall-clock immediately after the TCP write returned.
//   - RecvDoneTSNS (r9): wall-clock immediately after the FIRST response for
//     this order was read off the socket. Subsequent ExecutionReports for
//     the same ClOrdID (partial fills, final fills) are ignored.
//
// TimedOut=true with RecvDoneTSNS=0 means the watchdog evicted the order at
// the 5s deadline because no response ever arrived. Distinguishes "lost
// response" from "response at exactly t=0", which would otherwise be
// ambiguous.
//
// REST/WS caveat: response capture is FIX-only in v1. For REST and WS the
// bot emits with RecvDoneTSNS=0 and TimedOut=false (legacy behaviour).
//
// task_id replaces the old bot_id field (per-task loop, not per-bot loop).
type OrderSentEvent struct {
	SessionID      string `json:"session_id" msgpack:"session_id"`
	SubmissionID   string `json:"submission_id" msgpack:"submission_id"`
	WorkerID       string `json:"worker_id" msgpack:"worker_id"`
	TaskID         uint32 `json:"task_id" msgpack:"task_id"`
	OrderID        string `json:"order_id" msgpack:"order_id"`
	TargetSendTSNS uint64 `json:"target_send_ts_ns" msgpack:"target_send_ts_ns"`
	SendTSNS       uint64 `json:"send_ts_ns" msgpack:"send_ts_ns"`
	RecvDoneTSNS   uint64 `json:"recv_done_ts_ns" msgpack:"recv_done_ts_ns"`
	TimedOut       bool   `json:"timed_out" msgpack:"timed_out"`
	Price          uint64 `json:"price" msgpack:"price"`
	Qty            uint64 `json:"qty" msgpack:"qty"`
	Side           string `json:"side" msgpack:"side"`                 // BUY | SELL
	PayloadType    string `json:"payload_type" msgpack:"payload_type"` // NEW | CANCEL | REPLACE — lets the validator/ingester separate cancels from new orders (cancel throughput).
	OrdType        string `json:"ord_type" msgpack:"ord_type"`         // LIMIT | MARKET (FIX tag 40) — distinguishes market from limit new orders, which share payload_type=NEW.
}

// OrderAckedBatch is MessagePack-encoded on "orders.acked" by the eBPF
// latency publisher. Consumers join these kernel-side response timestamps
// with OrderSentEvent on (session_id, order_id).
type OrderAckedBatch struct {
	SessionID    string            `json:"session_id" msgpack:"session_id"`
	ContestantID string            `json:"contestant_id" msgpack:"contestant_id"`
	Events       []OrderAckedEvent `json:"events" msgpack:"events"`
}

// OrderAckedEvent records kernel-side request ingress and response egress
// timestamps. The primary contestant metric is PodServiceTimeNS.
type OrderAckedEvent struct {
	SessionID           string `json:"session_id" msgpack:"session_id"`
	ContestantID        string `json:"contestant_id" msgpack:"contestant_id"`
	OrderID             string `json:"order_id" msgpack:"order_id"`
	SrcIP               uint32 `json:"src_ip" msgpack:"src_ip"`
	SrcPort             uint16 `json:"src_port" msgpack:"src_port"`
	TCPSeq              uint32 `json:"tcp_seq" msgpack:"tcp_seq"`
	T3XDPIngressNS      uint64 `json:"t3_xdp_ingress_ns" msgpack:"t3_xdp_ingress_ns"`
	T7XDPEgressNS       uint64 `json:"t7_xdp_egress_ns" msgpack:"t7_xdp_egress_ns"`
	PodServiceTimeNS    uint64 `json:"pod_service_time_ns" msgpack:"pod_service_time_ns"`
	ExecType            string `json:"exec_type" msgpack:"exec_type"`
	FillQty             uint64 `json:"fill_qty" msgpack:"fill_qty"`
	FillPrice           uint64 `json:"fill_price" msgpack:"fill_price"` // fixed-point, scaled by TelemetryPriceScale
	OrigOrderID         string `json:"orig_order_id" msgpack:"orig_order_id"`
	ReorderingDetected  bool   `json:"reordering_detected" msgpack:"reordering_detected"`
	RetransmissionCount uint32 `json:"retransmission_count" msgpack:"retransmission_count"`
}
