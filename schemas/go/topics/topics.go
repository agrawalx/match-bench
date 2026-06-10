// Package topics defines shared schema contracts for topics.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
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

// SubmissionBuildRequested groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
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

const (
	RunStatusRequested    = "requested"     // submission-api accepted the click
	RunStatusDeploying    = "deploying"     // controller is allocating a sandbox slot
	RunStatusWaitingReady = "waiting_ready" // workload published, fanning in bot.ready
	RunStatusBarrierFired = "barrier_fired" // barrier event published
	RunStatusRunning      = "running"       // bots actively firing
	RunStatusCompleted    = "completed"     // controller: terminal success
	RunStatusFailed       = "failed"        // controller: terminal failure
)

// BenchmarkRequested groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type BenchmarkRequested struct {
	SessionID    string    `json:"session_id"`    // UUID v7, minted by submission-api
	SubmissionID string    `json:"submission_id"` // referenced submission (must be 'ready')
	ContestantID string    `json:"contestant_id"` // reserved; empty until OAuth
	RunGroupID   string    `json:"run_group_id"`  // parent group; shared across the run-group's sessions
	ScenarioID   string    `json:"scenario_id"`   // which row in scenarios table this session runs
	RequestedAt  time.Time `json:"requested_at"`
}

// BenchmarkStatusUpdated groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type BenchmarkStatusUpdated struct {
	SessionID    string    `json:"session_id"`
	SubmissionID string    `json:"submission_id"` // included for consumers that index by submission
	RunGroupID   string    `json:"run_group_id"`  // parent group; allows the frontend / SSE to correlate sibling sessions
	Status       string    `json:"status"`        // one of RunStatus* constants above
	Message      string    `json:"message"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// CorrectnessScoreEvent groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type CorrectnessScoreEvent struct {
	SessionID        string  `json:"session_id"`
	ContestantID     string  `json:"contestant_id"`
	ValidFills       uint64  `json:"valid_fills"`
	TotalFills       uint64  `json:"total_fills"`
	CorrectnessScore float64 `json:"correctness_score"` // valid_fills / total_fills
	ViolationCount   uint32  `json:"violation_count"`
	ComputedAtNS     uint64  `json:"computed_at_ns"`
	SentCount        uint64  `json:"sent_count"`    // orders.sent events drained for the session
	AckedCount       uint64  `json:"acked_count"`   // orders.acked events drained (post-dedup)
	MatchedCount     uint64  `json:"matched_count"` // distinct orders present in both streams
}

// LeaderboardUpdateEvent groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type LeaderboardUpdateEvent struct {
	RunGroupID           string  `json:"run_group_id"`
	SubmissionID         string  `json:"submission_id"`
	ContestantID         string  `json:"contestant_id"`
	TeamName             string  `json:"team_name"`
	Rank                 int64   `json:"rank"`
	RankDelta            int64   `json:"rank_delta"`
	PeakSustainedTPS     uint64  `json:"peak_sustained_tps"`
	P99NSAtPeakTPS       uint64  `json:"p99_ns_at_peak_tps"`
	SpikeRecoveryNS      uint64  `json:"spike_recovery_ns"`
	TotalCorrectness     float64 `json:"total_correctness"`
	Disqualified         bool    `json:"disqualified"`
	DisqualificationCode string  `json:"disqualification_code,omitempty"`
	UpdatedAtNS          uint64  `json:"updated_at_ns"`
}

// SubmissionStatusUpdated groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type SubmissionStatusUpdated struct {
	SubmissionID string    `json:"submission_id"`
	Status       string    `json:"status"` // uploaded | building | scanned | sbom_ready | ready | failed
	Message      string    `json:"message"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// WorkloadSpec groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type WorkloadSpec struct {
	SessionID        string     `json:"session_id"`
	SubmissionID     string     `json:"submission_id"`
	ContestantID     string     `json:"contestant_id"`
	TargetHost       string     `json:"target_host"` // IP of contestant pod
	TargetPort       uint16     `json:"target_port"`
	Protocol         string     `json:"protocol"` // FIX | REST | WS
	WorkerIndex      uint32     `json:"worker_index"`
	WorkerCount      uint32     `json:"worker_count"` // Total worker pods
	GlobalSeed       uint64     `json:"global_seed"`
	FIXVersion       string     `json:"fix_version"`
	ConnectTimeoutMS uint64     `json:"connect_timeout_ms"`
	WriteTimeoutMS   uint64     `json:"write_timeout_ms"`
	Tasks            []TaskSpec `json:"tasks"` // this worker's slice of the scenario's task list
}

// TaskSpec groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type TaskSpec struct {
	TaskID        uint32 `json:"task_id"`
	Profile       string `json:"profile"`         // hft | retail | institutional
	TargetRPS     uint32 `json:"target_rps"`      // orders per second, constant for this task's lifetime
	StartOffsetNs uint64 `json:"start_offset_ns"` // relative to barrier epoch
	DurationNs    uint64 `json:"duration_ns"`     // how long this task fires
	MarketPct     uint8  `json:"market_pct"`
	CancelPct     uint8  `json:"cancel_pct"`
	ReplacePct    uint8  `json:"replace_pct"`
}

// Scenario groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Scenario struct {
	ScenarioID string     `json:"scenario_id"`
	Name       string     `json:"name"`        // constant | spike | ramp
	DurationNs uint64     `json:"duration_ns"` // wall-time of the session
	TaskSpecs  []TaskSpec `json:"task_specs"`  // full task list — sharded across worker pods by the controller
}

// BarrierEvent groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type BarrierEvent struct {
	SessionID            string `json:"session_id"`
	TargetEpochUnixNanos uint64 `json:"target_epoch_unix_nanos"`
}

// ReadySignal groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
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

// OrderSentBatch groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type OrderSentBatch struct {
	SessionID string           `json:"session_id" msgpack:"session_id"`
	WorkerID  string           `json:"worker_id" msgpack:"worker_id"`
	Events    []OrderSentEvent `json:"events" msgpack:"events"`
}

// OrderSentEvent groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
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
	OrigOrderID    string `json:"orig_order_id" msgpack:"orig_order_id"`
}

// OrderAckedBatch groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type OrderAckedBatch struct {
	SessionID    string            `json:"session_id" msgpack:"session_id"`
	ContestantID string            `json:"contestant_id" msgpack:"contestant_id"`
	Events       []OrderAckedEvent `json:"events" msgpack:"events"`
}

// OrderAckedEvent groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
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
