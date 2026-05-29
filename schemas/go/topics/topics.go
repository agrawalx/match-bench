package topics

import "time"


const (
	StatusUploaded  = "uploaded"
	StatusBuilding  = "building"
	StatusScanned   = "scanned"
	StatusSBOMReady = "sbom_ready"
	StatusReady     = "ready"
	StatusFailed    = "failed"
)

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

// SubmissionStatusUpdated is published to "submission.status.updated"
// by the build-worker on every state transition. Consumed by: submission-api (status queries).
type SubmissionStatusUpdated struct {
	SubmissionID string    `json:"submission_id"`
	Status       string    `json:"status"` // uploaded | building | scanned | sbom_ready | ready | failed
	Message      string    `json:"message"`
	UpdatedAt    time.Time `json:"updated_at"`
}

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
//
// Key: session_id.
type BenchmarkStatusUpdated struct {
	SessionID    string    `json:"session_id"`
	SubmissionID string    `json:"submission_id"` // included for consumers that index by submission
	Status       string    `json:"status"`        // one of RunStatus* constants above
	Message      string    `json:"message"`
	UpdatedAt    time.Time `json:"updated_at"`
}


// WorkloadSpec is published to "workload.assignments" by the test controller.
// Each message is keyed by session_id:worker_index and consumed by one bot-fleet worker.
//
// OrdersPerBot and TargetRatePerBot are mutually exclusive control modes:
//   - If TargetRatePerBot > 0, the worker fires at that rate (orders/sec) for a
//     fixed 60-second window, ignoring OrdersPerBot entirely.
//   - If TargetRatePerBot == 0, the worker sends exactly OrdersPerBot orders as
//     fast as the target allows (unbounded rate, count-limited).
//
// FIXVersion is always sent by the Go controller; the Rust-side default of "FIX.4.2"
// is a fallback that should never be exercised in production but ensures the worker
// stays runnable if a spec is manually injected without this field.
type WorkloadSpec struct {
	SessionID        string             `json:"session_id"`
	SubmissionID     string             `json:"submission_id"`
	ContestantID     string             `json:"contestant_id"`
	TargetHost       string             `json:"target_host"` // IP of contestant pod
	TargetPort       uint16             `json:"target_port"`
	Protocol         string             `json:"protocol"` // FIX | REST | WS
	WorkerIndex      uint32             `json:"worker_index"`
	WorkerCount      uint32             `json:"worker_count"`
	BotCount         uint32             `json:"bot_count"`
	OrdersPerBot     uint32             `json:"orders_per_bot"`          // used when TargetRatePerBot == 0; see doc above
	TargetRatePerBot *uint32            `json:"target_rate_per_bot,omitempty"` // orders/sec per bot; 0/absent = count mode
	GlobalSeed       uint64             `json:"global_seed"`
	FIXVersion       string             `json:"fix_version"`             // always set by controller; default "FIX.4.2"
	ProfileMix       []BotProfileWeight `json:"profile_mix"`
	ConnectTimeoutMS uint64             `json:"connect_timeout_ms"`
	WriteTimeoutMS   uint64             `json:"write_timeout_ms"`
}

type BotProfileWeight struct {
	Profile string `json:"profile"` // market_maker | aggressive_taker | canceller
	Weight  uint32 `json:"weight"`
}

// BarrierEvent is published to "barrier" once all workers have reported ready.
//
// TargetEpochUnixNanos is the absolute nanosecond timestamp at which all workers
// must simultaneously open fire. Consumers must handle the case where this timestamp
// is in the past at the time of consumption (e.g. slow consumer, Kafka replay):
// in that case the worker should begin firing immediately rather than waiting.
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

// WorkloadFailedEvent is published by bot-fleet to "workload.failed".
type WorkloadFailedEvent struct {
	SessionID         string `json:"session_id"`
	SubmissionID      string `json:"submission_id"`
	WorkerID          string `json:"worker_id"`
	WorkerIndex       uint32 `json:"worker_index"`
	Reason            string `json:"reason"`
	FailedAtUnixNanos uint64 `json:"failed_at_unix_nanos"`
}

// OrderSentBatch is MessagePack-encoded on "orders.sent" to avoid per-order Kafka writes.
// Both json and msgpack tags are present for debugging convenience (e.g. logging the
// struct as JSON in tests or local runs). In production, only MessagePack is used.
// If the encoding ever changes, rename both tag sets together to avoid silent divergence.
type OrderSentBatch struct {
	SessionID string           `json:"session_id" msgpack:"session_id"`
	WorkerID  string           `json:"worker_id" msgpack:"worker_id"`
	Events    []OrderSentEvent `json:"events" msgpack:"events"`
}

// OrderSentEvent records one outbound order timestamp.
//
// session_id, submission_id, and worker_id are duplicated from the OrderSentBatch
// envelope so that individual events remain self-contained for downstream analytics
// consumers (e.g. a results processor reading events from a flattened table).
// If events are never processed outside the batch context, these three fields can
// be removed to reduce wire size on this high-frequency path.
type OrderSentEvent struct {
	SessionID    string `json:"session_id" msgpack:"session_id"`
	SubmissionID string `json:"submission_id" msgpack:"submission_id"`
	WorkerID     string `json:"worker_id" msgpack:"worker_id"`
	BotID        uint64 `json:"bot_id" msgpack:"bot_id"`
	OrderID      string `json:"order_id" msgpack:"order_id"`
	SendTSNS     uint64 `json:"send_ts_ns" msgpack:"send_ts_ns"`
	PayloadType  string `json:"payload_type" msgpack:"payload_type"` // NEW | CANCEL | REPLACE
	Price        uint64 `json:"price" msgpack:"price"`
	Qty          uint64 `json:"qty" msgpack:"qty"`
	Side         string `json:"side" msgpack:"side"`         // BUY | SELL
	Protocol     string `json:"protocol" msgpack:"protocol"` // FIX | REST | WS
}
