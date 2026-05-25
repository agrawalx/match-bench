package topics

import "time"

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
type BenchmarkRequested struct {
	SessionID    string    `json:"session_id"`    // UUID v7, minted by submission-api
	SubmissionID string    `json:"submission_id"` // referenced submission (must be 'ready')
	ContestantID string    `json:"contestant_id"` // reserved; empty until OAuth
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
// Key: session_id.
type BenchmarkStatusUpdated struct {
	SessionID    string    `json:"session_id"`
	SubmissionID string    `json:"submission_id"` // included for consumers that index by submission
	Status       string    `json:"status"`        // one of RunStatus* constants above
	Message      string    `json:"message"`
	UpdatedAt    time.Time `json:"updated_at"`
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
type WorkloadSpec struct {
	SessionID        string             `json:"session_id"`
	SubmissionID     string             `json:"submission_id"`
	ContestantID     string             `json:"contestant_id"`
	TargetHost       string             `json:"target_host"` // IP of contestant pod
	TargetPort       uint16             `json:"target_port"`
	Protocol         string             `json:"protocol"` // FIX | REST | WS
	WorkerIndex      uint32             `json:"worker_index"`
	WorkerCount      uint32             `json:"worker_count"` // Total worker pods
	BotCount         uint32             `json:"bot_count"`
	OrdersPerBot     uint32             `json:"orders_per_bot"` // Currently, every bot will run for 60s
	GlobalSeed       uint64             `json:"global_seed"`
	FIXVersion       string             `json:"fix_version"`
	ProfileMix       []BotProfileWeight `json:"profile_mix"`
	ConnectTimeoutMS uint64             `json:"connect_timeout_ms"`
	WriteTimeoutMS   uint64             `json:"write_timeout_ms"`
}

type BotProfileWeight struct {
	Profile string `json:"profile"` // market_maker | aggressive_taker | canceller
	Weight  uint32 `json:"weight"`
}

// BarrierEvent is published to "barrier" once all workers have reported ready.
type BarrierEvent struct {
	SessionID            string `json:"session_id"`
	TargetEpochUnixNanos uint64 `json:"target_epoch_unix_nanos"`
}

// ReadySignal is published by each bot-fleet worker to "bot.ready".
type ReadySignal struct {
	SessionID        string `json:"session_id"`
	SubmissionID     string `json:"submission_id"`
	WorkerID         string `json:"worker_id"`
	WorkerIndex      uint32 `json:"worker_index"`
	WorkerCount      uint32 `json:"worker_count"`
	BotCount         uint32 `json:"bot_count"`
	ConnectedCount   uint32 `json:"connected_count"`
	ReadyAtUnixNanos uint64 `json:"ready_at_unix_nanos"`
}

// OrderSentBatch is MessagePack-encoded on "orders.sent" to avoid per-order Kafka writes.
type OrderSentBatch struct {
	SessionID string           `json:"session_id" msgpack:"session_id"`
	WorkerID  string           `json:"worker_id" msgpack:"worker_id"`
	Events    []OrderSentEvent `json:"events" msgpack:"events"`
}

type OrderSentEvent struct {
	SessionID    string `json:"session_id" msgpack:"session_id"`
	SubmissionID string `json:"submission_id" msgpack:"submission_id"`
	WorkerID     string `json:"worker_id" msgpack:"worker_id"`
	BotID        uint64 `json:"bot_id" msgpack:"bot_id"`
	OrderID      string `json:"order_id" msgpack:"order_id"`
	SendTSNS     uint64 `json:"send_ts_ns" msgpack:"send_ts_ns"`
	Price        uint64 `json:"price" msgpack:"price"`
	Qty          uint64 `json:"qty" msgpack:"qty"`
	Side         string `json:"side" msgpack:"side"` // BUY | SELL
}
