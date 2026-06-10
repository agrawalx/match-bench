package score

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/iicpc/schemas/topics"
)

const (
	DefaultCorrectnessDQThreshold = 0.99
	DefaultMaxErrorRate           = 0.01
	DefaultMaxP99NS               = uint64(1_000_000)
	DefaultWaveDurationNS         = uint64(20_000_000_000)
	DefaultMaxScheduledWaves      = uint64(10_000)
	// DefaultMinCoverage is the telemetry-completeness threshold: the minimum
	// per-session matched/sent coverage below which violation-based DQ is
	// suppressed and the run is flagged IncompleteTelemetry. 0.90 mirrors the
	// scoring_config seed.
	DefaultMinCoverage = 0.90
)

type Config struct {
	CorrectnessDQThreshold float64
	MaxErrorRate           float64
	MaxP99NS               uint64
	WaveDurationNS         uint64
	MinCoverage            float64
}

func (c Config) WithDefaults() Config {
	if !isPositiveFinite(c.CorrectnessDQThreshold) || c.CorrectnessDQThreshold > 1 {
		c.CorrectnessDQThreshold = DefaultCorrectnessDQThreshold
	}
	if !isPositiveFinite(c.MaxErrorRate) {
		c.MaxErrorRate = DefaultMaxErrorRate
	}
	if c.MaxP99NS == 0 {
		c.MaxP99NS = DefaultMaxP99NS
	}
	if c.WaveDurationNS == 0 {
		c.WaveDurationNS = DefaultWaveDurationNS
	}
	if !isPositiveFinite(c.MinCoverage) || c.MinCoverage > 1 {
		c.MinCoverage = DefaultMinCoverage
	}
	return c
}

func isPositiveFinite(v float64) bool {
	return v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0)
}

type Correctness struct {
	SessionID      string
	ValidFills     uint64
	TotalFills     uint64
	ViolationCount uint64
	// Telemetry-completeness counters from the validator's drain. SentCount is
	// orders.sent events drained, AckedCount is orders.acked events (post-
	// dedup), MatchedCount is distinct orders present in BOTH streams — the
	// replay's actual inputs. All zero = unknown (pre-counter rows or timed_out
	// placeholders), which the coverage gate treats as ungateable.
	SentCount    uint64
	AckedCount   uint64
	MatchedCount uint64
}

type MetricRow struct {
	WaveIndex int
	P99NS     uint64
	TPS1S     float64
	ErrorRate float64
}

type Session struct {
	SessionID  string
	Scenario   string
	TaskSpecs  []topics.TaskSpec
	Correct    Correctness
	Metrics    []MetricRow
	DurationNS uint64
}

type Input struct {
	RunGroupID   string
	SubmissionID string
	ContestantID string
	TeamName     string
	Sessions     []Session
	Config       Config
}

type WaveResult struct {
	WaveIndex  int    `json:"wave_index"`
	OfferedRPS uint64 `json:"offered_rps"`
	P99NS      uint64 `json:"p99_ns"`
	Passed     bool   `json:"passed"`
	Reason     string `json:"reason,omitempty"`
}

type Result struct {
	RunGroupID           string  `json:"run_group_id"`
	SubmissionID         string  `json:"submission_id"`
	ContestantID         string  `json:"contestant_id"`
	TeamName             string  `json:"team_name"`
	PeakSustainedTPS     uint64  `json:"peak_sustained_tps"`
	P99AtPeakNS          uint64  `json:"p99_at_peak_ns"`
	SpikeRecoveryNS      uint64  `json:"spike_recovery_ns"`
	TotalCorrectness     float64 `json:"total_correctness"`
	Disqualified         bool    `json:"disqualified"`
	DisqualificationCode string  `json:"disqualification_code,omitempty"`
	// IncompleteTelemetry marks a verdict whose correctness inputs failed the
	// per-session coverage gate (matched/sent below scoring_config.min_coverage).
	// Violation-based DQ is suppressed for such runs; the reason names the first
	// offending session so the leaderboard can surface it from score_detail.
	IncompleteTelemetry       bool         `json:"incomplete_telemetry"`
	IncompleteTelemetryReason string       `json:"incomplete_telemetry_reason,omitempty"`
	Waves                     []WaveResult `json:"waves,omitempty"`
}

var ErrMissingRampSession = errors.New("missing ramp session")

func Compute(in Input) (Result, error) {
	cfg := in.Config.WithDefaults()
	res := Result{
		RunGroupID:       in.RunGroupID,
		SubmissionID:     in.SubmissionID,
		ContestantID:     in.ContestantID,
		TeamName:         in.TeamName,
		TotalCorrectness: aggregateCorrectness(in.Sessions),
	}

	// Telemetry-completeness gate (v1). Coverage is matched_count/sent_count:
	// of the orders the bot fleet reports firing (orders.sent is the bot-side
	// ground truth of what existed), the fraction the validator could join with
	// at least one kernel-side orders.acked response — the orders that actually
	// entered the replay. This is the most defensible ratio the validator can
	// measure: a lost acked event removes its order from matched_count directly
	// (the silent-exclusion failure this gate exists to catch), while
	// acked_count/sent_count would be distorted by multi-response orders
	// (partial fills emit several acked events per order). The ratio is
	// conservative toward the contestant: genuine non-response also lowers it,
	// but a false flag only suppresses violation DQ — it never improves a score.
	// sent_count==0 means the counters are unknown (rows written before the
	// counters existed, or a timed_out placeholder whose drain never ran), so
	// coverage is unknowable and the gate must not retroactively reflag those
	// runs. The first offending session (deterministic LoadInput order) names
	// the reason persisted in score_detail.
	for i := range in.Sessions {
		c := in.Sessions[i].Correct
		if c.SentCount == 0 {
			continue
		}
		coverage := min(float64(c.MatchedCount)/float64(c.SentCount), 1.0)
		if coverage < cfg.MinCoverage {
			res.IncompleteTelemetry = true
			res.IncompleteTelemetryReason = fmt.Sprintf(
				"session %s telemetry coverage %.4f (matched %d / sent %d) below min_coverage %g",
				in.Sessions[i].SessionID, coverage, c.MatchedCount, c.SentCount, cfg.MinCoverage)
			break
		}
	}

	var ramp *Session
	disqualificationCode := ""
	// The correctness-RATIO gates below stay active even when the run is
	// flagged IncompleteTelemetry: valid/total is a ratio over the fills that
	// WERE observed, so uniform telemetry loss leaves it roughly unbiased.
	// Caveat: non-uniform loss (e.g. one worker's flushes dropped wholesale)
	// can still skew the ratio — v1 accepts that, because suppressing the
	// correctness gate entirely would let a cheating engine hide behind lossy
	// telemetry.
	if res.TotalCorrectness < cfg.CorrectnessDQThreshold {
		disqualificationCode = "correctness_below_threshold"
	}
	for i := range in.Sessions {
		s := &in.Sessions[i]
		if s.Correct.TotalFills > 0 {
			sessionCorrectness := float64(s.Correct.ValidFills) / float64(s.Correct.TotalFills)
			sessionCorrectness = min(sessionCorrectness, 1.0)
			if sessionCorrectness < cfg.CorrectnessDQThreshold && disqualificationCode == "" {
				disqualificationCode = "session_correctness_below_threshold"
			}
		}
		if s.Scenario == "ramp" {
			ramp = s
		}
	}
	if ramp == nil {
		// A disqualified run-group without a ramp session is still a terminal
		// result: surface the DQ instead of an error, otherwise no scores row is
		// ever written and PendingRunGroups re-enqueues the group forever.
		if disqualificationCode != "" {
			res.Disqualified = true
			res.DisqualificationCode = disqualificationCode
			return res, nil
		}
		return res, ErrMissingRampSession
	}
	if ramp.Correct.ViolationCount > 0 && disqualificationCode == "" && !res.IncompleteTelemetry {
		// v1 approximation from ROADMAP.md: session-level correctness gate is
		// applied to every wave until correctness_violations carries wave buckets.
		// Suppressed when the telemetry-completeness gate fired: absolute
		// violation counts are unsound on incomplete data — a lost orders.sent
		// flush turns every one of its acks into a false phantom violation —
		// so a DQ on them would punish telemetry loss, not the contestant.
		disqualificationCode = "ramp_session_violation"
	}

	res.SpikeRecoveryNS = spikeRecoveryNS(in.Sessions, cfg.WaveDurationNS)

	schedule := WaveSchedule(ramp.TaskSpecs, cfg.WaveDurationNS)
	metrics := summarizeMetrics(ramp.Metrics)
	for _, wave := range schedule {
		wr := WaveResult{WaveIndex: wave.WaveIndex, OfferedRPS: wave.OfferedRPS}
		m, ok := metrics[wave.WaveIndex]
		if !ok || m.Count == 0 {
			wr.Passed = false
			wr.Reason = "missing_metrics"
			res.Waves = append(res.Waves, wr)
			break
		}
		wr.P99NS = m.MaxP99NS
		switch {
		case m.MaxErrorRate > cfg.MaxErrorRate:
			wr.Reason = "error_rate"
		case m.MaxP99NS > cfg.MaxP99NS:
			wr.Reason = "p99_latency"
		default:
			wr.Passed = true
			res.PeakSustainedTPS = wave.OfferedRPS
			res.P99AtPeakNS = m.MaxP99NS
		}
		res.Waves = append(res.Waves, wr)
		if !wr.Passed {
			break
		}
	}
	if disqualificationCode != "" {
		res.Disqualified = true
		res.DisqualificationCode = disqualificationCode
	}
	return res, nil
}

func aggregateCorrectness(sessions []Session) float64 {
	var valid, total float64
	for _, s := range sessions {
		valid += float64(s.Correct.ValidFills)
		total += float64(s.Correct.TotalFills)
	}
	if total == 0 {
		return 0
	}
	return min(valid/total, 1.0)
}

type WaveOffer struct {
	WaveIndex  int
	OfferedRPS uint64
}

func WaveSchedule(tasks []topics.TaskSpec, waveDurationNS uint64) []WaveOffer {
	if waveDurationNS == 0 {
		waveDurationNS = DefaultWaveDurationNS
	}
	var maxEnd uint64
	for _, t := range tasks {
		if end := saturatingAdd(t.StartOffsetNs, t.DurationNs); end > maxEnd {
			maxEnd = end
		}
	}
	if maxEnd == 0 {
		return nil
	}
	waves64 := ((maxEnd - 1) / waveDurationNS) + 1
	if waves64 > DefaultMaxScheduledWaves {
		waves64 = DefaultMaxScheduledWaves
	}
	waves := int(waves64)
	out := make([]WaveOffer, 0, waves)
	for wave := 0; wave < waves; wave++ {
		start := uint64(wave) * waveDurationNS
		end := saturatingAdd(start, waveDurationNS)
		var offered float64
		for _, t := range tasks {
			taskStart := t.StartOffsetNs
			taskEnd := saturatingAdd(t.StartOffsetNs, t.DurationNs)
			overlap := overlapNS(start, end, taskStart, taskEnd)
			if overlap == 0 {
				continue
			}
			offered += float64(t.TargetRPS) * (float64(overlap) / float64(waveDurationNS))
		}
		if offered <= 0 {
			continue
		}
		out = append(out, WaveOffer{WaveIndex: wave, OfferedRPS: uint64(math.Round(offered))})
	}
	return out
}

func saturatingAdd(a, b uint64) uint64 {
	if math.MaxUint64-a < b {
		return math.MaxUint64
	}
	return a + b
}

func overlapNS(aStart, aEnd, bStart, bEnd uint64) uint64 {
	start := max(aStart, bStart)
	end := min(aEnd, bEnd)
	if end <= start {
		return 0
	}
	return end - start
}

type MetricSummary struct {
	Count        int
	MaxP99NS     uint64
	MaxErrorRate float64
}

func summarizeMetrics(rows []MetricRow) map[int]MetricSummary {
	out := make(map[int]MetricSummary)
	for _, row := range rows {
		m := out[row.WaveIndex]
		m.Count++
		m.MaxP99NS = max(m.MaxP99NS, row.P99NS)
		if row.ErrorRate > m.MaxErrorRate {
			m.MaxErrorRate = row.ErrorRate
		}
		out[row.WaveIndex] = m
	}
	return out
}

// spikeRecoveryNS estimates the Session-2 (spike) p99 recovery time: the elapsed
// time from the spike's peak-p99 wave until p99 returns within 10% of the
// pre-spike baseline (the first wave's p99). It is a secondary tiebreaker only.
//
// Resolution is one wave (WaveDurationNS, ~20s) because the metrics store
// aggregates per wave, not per second — this is a coarse proxy, not a precise
// recovery time. Returns 0 when there is no spike session, no metrics, or p99
// never rose meaningfully above baseline; returns the full observed post-peak
// span when it rose but never recovered.
func spikeRecoveryNS(sessions []Session, waveDurationNS uint64) uint64 {
	if waveDurationNS == 0 {
		waveDurationNS = DefaultWaveDurationNS
	}
	var spike *Session
	for i := range sessions {
		if sessions[i].Scenario == "spike" {
			spike = &sessions[i]
			break
		}
	}
	if spike == nil {
		return 0
	}
	sum := summarizeMetrics(spike.Metrics)
	if len(sum) == 0 {
		return 0
	}
	waves := make([]int, 0, len(sum))
	for w := range sum {
		waves = append(waves, w)
	}
	sort.Ints(waves)

	baseline := sum[waves[0]].MaxP99NS
	if baseline == 0 {
		return 0
	}
	threshold := uint64(float64(baseline) * 1.10)

	peakWave, peakP99 := waves[0], sum[waves[0]].MaxP99NS
	for _, w := range waves {
		if sum[w].MaxP99NS > peakP99 {
			peakP99, peakWave = sum[w].MaxP99NS, w
		}
	}
	if peakP99 <= threshold {
		return 0 // never rose meaningfully above baseline
	}
	for _, w := range waves {
		if w > peakWave && sum[w].MaxP99NS <= threshold {
			return uint64(w-peakWave) * waveDurationNS
		}
	}
	// rose above baseline but never recovered within the observed window
	last := waves[len(waves)-1]
	return uint64(last-peakWave+1) * waveDurationNS
}

func SortResults(results []Result) {
	sort.SliceStable(results, func(i, j int) bool {
		a, b := results[i], results[j]
		// Disqualified results keep their measured peak for transparency but
		// must never outrank a clean result, so DQ is the primary key.
		if a.Disqualified != b.Disqualified {
			return b.Disqualified
		}
		if a.PeakSustainedTPS != b.PeakSustainedTPS {
			return a.PeakSustainedTPS > b.PeakSustainedTPS
		}
		if a.P99AtPeakNS != b.P99AtPeakNS {
			return a.P99AtPeakNS < b.P99AtPeakNS
		}
		if a.SpikeRecoveryNS != b.SpikeRecoveryNS {
			return a.SpikeRecoveryNS < b.SpikeRecoveryNS
		}
		if a.TotalCorrectness != b.TotalCorrectness {
			return a.TotalCorrectness > b.TotalCorrectness
		}
		return a.RunGroupID < b.RunGroupID
	})
}
