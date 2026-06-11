// Package read implements store behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package read

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maxChartPoints         = 20000
	maxViolationRows       = 1000
	maxLeaderboardRows     = 500
	defaultLeaderboardRows = 100
	maxLeaderboardOffset   = 100000
)

const rankedOrder = `disqualified ASC, peak_sustained_tps DESC, p99_at_peak_ns ASC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC`

// Store groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Store struct {
	meta      *pgxpool.Pool
	timescale *pgxpool.Pool
}

// New performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func New(ctx context.Context, metadataURL, timescaleURL string) (*Store, error) {
	meta, err := pgxpool.New(ctx, metadataURL)
	if err != nil {
		return nil, err
	}
	ts, err := pgxpool.New(ctx, timescaleURL)
	if err != nil {
		meta.Close()
		return nil, err
	}
	return &Store{meta: meta, timescale: ts}, nil
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) Close() {
	s.meta.Close()
	s.timescale.Close()
}

// Healthcheck applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) Healthcheck(ctx context.Context) error {
	if err := s.meta.Ping(ctx); err != nil {
		return err
	}
	return s.timescale.Ping(ctx)
}

// LeaderboardResponse groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type LeaderboardResponse struct {
	Source     string           `json:"source"`
	Rows       []LeaderboardRow `json:"rows"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

// LeaderboardQuery groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type LeaderboardQuery struct {
	Limit        int
	Sort         string
	Order        string
	Cursor       string
	RunGroupID   string
	SubmissionID string
	ContestantID string
	TeamID       string
	TeamName     string
}

// LeaderboardRow groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type LeaderboardRow struct {
	Rank                 int64   `json:"rank"`
	RunGroupID           string  `json:"run_group_id"`
	SubmissionID         string  `json:"submission_id"`
	ContestantID         string  `json:"contestant_id"`
	TeamName             string  `json:"team_name"`
	PeakSustainedTPS     uint64  `json:"peak_sustained_tps"`
	P99NSAtPeakTPS       uint64  `json:"p99_ns_at_peak_tps"`
	SpikeRecoveryNS      uint64  `json:"spike_recovery_ns"`
	TotalCorrectness     float64 `json:"total_correctness"`
	Disqualified         bool    `json:"disqualified"`
	DisqualificationCode string  `json:"disqualification_code,omitempty"`
	RankDelta            int64   `json:"rank_delta"`
	ComputedAtUnixNS     int64   `json:"computed_at_ns"`
}

// Leaderboard applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) Leaderboard(ctx context.Context, q LeaderboardQuery) (LeaderboardResponse, error) {
	limit := q.Limit
	if limit <= 0 || limit > maxLeaderboardRows {
		limit = defaultLeaderboardRows
	}
	offset, err := decodeLeaderboardCursor(q.Cursor)
	if err != nil {
		return LeaderboardResponse{}, err
	}
	contestantFilter := q.ContestantID
	if contestantFilter == "" {
		contestantFilter = q.TeamID
	}
	orderBy := leaderboardOrderBy(q.Sort, q.Order)
	rows, err := s.meta.Query(ctx, `
SELECT rank, run_group_id, submission_id, contestant_id, team_name, peak_sustained_tps,
       p99_at_peak_ns, spike_recovery_ns, total_correctness, disqualified,
       disqualification_code, COALESCE(rank_delta,0), computed_at
  FROM (
	SELECT ROW_NUMBER() OVER (ORDER BY `+rankedOrder+`) AS rank,
	       run_group_id, submission_id, contestant_id, team_name, peak_sustained_tps,
	       p99_at_peak_ns, spike_recovery_ns, total_correctness, disqualified,
	       disqualification_code, rank_delta, computed_at
	  FROM scores
  ) ranked
 WHERE ($1='' OR run_group_id=$1)
   AND ($2='' OR submission_id=$2)
   AND ($3='' OR contestant_id=$3)
   AND ($4='' OR team_name ILIKE '%' || $4 || '%')
 ORDER BY `+orderBy+`
 LIMIT $5 OFFSET $6`, q.RunGroupID, q.SubmissionID, contestantFilter, q.TeamName, limit+1, offset)
	if err != nil {
		if isUndefinedTable(err) {
			return LeaderboardResponse{Source: "frozen", Rows: []LeaderboardRow{}}, nil
		}
		return LeaderboardResponse{}, err
	}
	defer rows.Close()
	resp := LeaderboardResponse{Source: "frozen", Rows: []LeaderboardRow{}}
	hasMore := false
	for rows.Next() {
		var r LeaderboardRow
		var peak, p99, recovery int64
		var computed time.Time
		if err := rows.Scan(&r.Rank, &r.RunGroupID, &r.SubmissionID, &r.ContestantID, &r.TeamName, &peak, &p99,
			&recovery, &r.TotalCorrectness, &r.Disqualified, &r.DisqualificationCode, &r.RankDelta, &computed); err != nil {
			return resp, err
		}
		r.PeakSustainedTPS = nonNegativeUint64(peak)
		r.P99NSAtPeakTPS = nonNegativeUint64(p99)
		r.SpikeRecoveryNS = nonNegativeUint64(recovery)
		r.ComputedAtUnixNS = computed.UnixNano()
		if len(resp.Rows) < limit {
			resp.Rows = append(resp.Rows, r)
		} else {
			hasMore = true
		}
	}
	if err := rows.Err(); err != nil {
		return resp, err
	}
	if hasMore {
		resp.NextCursor = encodeLeaderboardCursor(offset + limit)
	}
	return resp, nil
}

// isUndefinedTable performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// leaderboardCursor groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type leaderboardCursor struct {
	Offset int `json:"offset"`
}

// RunDetail groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type RunDetail struct {
	RunGroupID string           `json:"run_group_id"`
	Score      *LeaderboardRow  `json:"score,omitempty"`
	Sessions   []SessionDetail  `json:"sessions"`
	Violations []ViolationEntry `json:"violations"`
}

// SessionDetail groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type SessionDetail struct {
	SessionID string        `json:"session_id"`
	Scenario  string        `json:"scenario"`
	Status    string        `json:"status"`
	Timeline  []MetricPoint `json:"timeline"`
}

// MetricPoint groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type MetricPoint struct {
	TimeUnixNS     int64   `json:"time_unix_ns"`
	WaveIndex      int     `json:"wave_index"`
	P50NS          uint64  `json:"p50_ns"`
	P90NS          uint64  `json:"p90_ns"`
	P99NS          uint64  `json:"p99_ns"`
	RTP50NS        uint64  `json:"rt_p50_ns"`
	RTP90NS        uint64  `json:"rt_p90_ns"`
	RTP99NS        uint64  `json:"rt_p99_ns"`
	TPS1S          float64 `json:"tps_1s"`
	ErrorRate      float64 `json:"error_rate"`
	HDREncoded     string  `json:"hdr_encoded,omitempty"`
	RTHDREncoded   string  `json:"rt_hdr_encoded,omitempty"`
	SlipHDREncoded string  `json:"slip_hdr_encoded,omitempty"`
}

// ViolationEntry groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ViolationEntry struct {
	SessionID     string `json:"session_id"`
	ContestantID  string `json:"contestant_id"`
	ViolationType string `json:"violation_type"`
	OrderID       string `json:"order_id"`
	Detail        string `json:"detail"`
	DetectedAtNS  int64  `json:"detected_at_ns"`
}

// RunDetail applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) RunDetail(ctx context.Context, runGroupID string) (RunDetail, error) {
	d := RunDetail{RunGroupID: runGroupID}
	scoreRow, ok, err := s.scoreForRunGroup(ctx, runGroupID)
	if err != nil {
		return d, err
	}
	if ok {
		d.Score = &scoreRow
	}
	rows, err := s.meta.Query(ctx, `
SELECT r.session_id, sc.name, r.status
  FROM runs r JOIN scenarios sc ON sc.scenario_id=r.scenario_id
 WHERE r.run_group_id=$1
 ORDER BY sc.sort_order, sc.name`, runGroupID)
	if err != nil {
		return d, err
	}
	defer rows.Close()
	for rows.Next() {
		var sd SessionDetail
		if err := rows.Scan(&sd.SessionID, &sd.Scenario, &sd.Status); err != nil {
			return d, err
		}
		sd.Timeline, err = s.Chart(ctx, sd.SessionID)
		if err != nil {
			return d, err
		}
		if sd.Timeline == nil {
			sd.Timeline = []MetricPoint{}
		}
		d.Sessions = append(d.Sessions, sd)
	}
	if err := rows.Err(); err != nil {
		return d, err
	}
	if d.Sessions == nil {
		d.Sessions = []SessionDetail{}
	}
	d.Violations, err = s.Violations(ctx, runGroupID)
	if d.Violations == nil {
		d.Violations = []ViolationEntry{}
	}
	return d, err
}

// Chart applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) Chart(ctx context.Context, sessionID string) ([]MetricPoint, error) {
	rows, err := s.timescale.Query(ctx, `
SELECT EXTRACT(EPOCH FROM time) * 1000000000, wave_index,
       COALESCE(p50_ns,0), COALESCE(p90_ns,0), COALESCE(p99_ns,0),
       COALESCE(rt_p50_ns,0), COALESCE(rt_p90_ns,0), COALESCE(rt_p99_ns,0),
       COALESCE(tps_1s,0), COALESCE(error_rate,0), hdr_encoded, rt_hdr_encoded, slip_hdr_encoded
  FROM metrics
 WHERE session_id=$1
 ORDER BY time
 LIMIT $2`, sessionID, maxChartPoints)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MetricPoint
	for rows.Next() {
		var p MetricPoint
		var p50, p90, p99, rt50, rt90, rt99 int64
		var nsFloat float64
		var hdr, rtHdr, slipHdr []byte
		if err := rows.Scan(&nsFloat, &p.WaveIndex, &p50, &p90, &p99, &rt50, &rt90, &rt99, &p.TPS1S, &p.ErrorRate, &hdr, &rtHdr, &slipHdr); err != nil {
			return nil, err
		}
		p.TimeUnixNS = int64(nsFloat)
		p.P50NS, p.P90NS, p.P99NS = nonNegativeUint64(p50), nonNegativeUint64(p90), nonNegativeUint64(p99)
		p.RTP50NS, p.RTP90NS, p.RTP99NS = nonNegativeUint64(rt50), nonNegativeUint64(rt90), nonNegativeUint64(rt99)
		if len(hdr) > 0 {
			p.HDREncoded = base64.StdEncoding.EncodeToString(hdr)
		}
		if len(rtHdr) > 0 {
			p.RTHDREncoded = base64.StdEncoding.EncodeToString(rtHdr)
		}
		if len(slipHdr) > 0 {
			p.SlipHDREncoded = base64.StdEncoding.EncodeToString(slipHdr)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Violations applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) Violations(ctx context.Context, runGroupID string) ([]ViolationEntry, error) {
	rows, err := s.meta.Query(ctx, `
SELECT v.session_id, v.contestant_id, v.violation_type, v.order_id,
       COALESCE(v.detail,''), v.detected_at
  FROM correctness_violations v
  JOIN runs r ON r.session_id=v.session_id
 WHERE r.run_group_id=$1
 ORDER BY v.detected_at, v.id
 LIMIT $2`, runGroupID, maxViolationRows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ViolationEntry
	for rows.Next() {
		var v ViolationEntry
		var detected time.Time
		if err := rows.Scan(&v.SessionID, &v.ContestantID, &v.ViolationType, &v.OrderID, &v.Detail, &detected); err != nil {
			return nil, err
		}
		v.DetectedAtNS = detected.UnixNano()
		out = append(out, v)
	}
	return out, rows.Err()
}

// ActiveSession groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ActiveSession struct {
	SessionID string `json:"session_id"`
	Scenario  string `json:"scenario"`
	Status    string `json:"status"`
}

// ActiveRun groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ActiveRun struct {
	RunGroupID string          `json:"run_group_id"`
	TeamName   string          `json:"team_name"`
	Sessions   []ActiveSession `json:"sessions"`
}

// ActiveRuns applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) ActiveRuns(ctx context.Context) ([]ActiveRun, error) {
	rows, err := s.meta.Query(ctx, `
SELECT rg.run_group_id, COALESCE(sub.team_name,''), r.session_id, sc.name, r.status
  FROM run_groups rg
  JOIN runs r ON r.run_group_id = rg.run_group_id
  JOIN scenarios sc ON sc.scenario_id = r.scenario_id
  LEFT JOIN submissions sub ON sub.submission_id = rg.submission_id
 WHERE rg.run_group_id IN (
   SELECT run_group_id FROM runs WHERE status NOT IN ('completed','failed')
 )
 ORDER BY rg.created_at DESC, sc.sort_order, sc.name
 LIMIT 500`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	idx := map[string]int{}
	var out []ActiveRun
	for rows.Next() {
		var rgID, team string
		var sess ActiveSession
		if err := rows.Scan(&rgID, &team, &sess.SessionID, &sess.Scenario, &sess.Status); err != nil {
			return nil, err
		}
		i, ok := idx[rgID]
		if !ok {
			i = len(out)
			idx[rgID] = i
			out = append(out, ActiveRun{RunGroupID: rgID, TeamName: team})
		}
		out[i].Sessions = append(out[i].Sessions, sess)
	}
	return out, rows.Err()
}

// String applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) String() string { return fmt.Sprintf("read.Store(%p)", s) }

// scoreForRunGroup applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) scoreForRunGroup(ctx context.Context, runGroupID string) (LeaderboardRow, bool, error) {
	rows, err := s.meta.Query(ctx, `
SELECT rank, run_group_id, submission_id, contestant_id, team_name, peak_sustained_tps,
       p99_at_peak_ns, spike_recovery_ns, total_correctness, disqualified,
       disqualification_code, COALESCE(rank_delta,0), computed_at
  FROM (
	SELECT ROW_NUMBER() OVER (ORDER BY `+rankedOrder+`) AS rank,
	       run_group_id, submission_id, contestant_id, team_name, peak_sustained_tps,
	       p99_at_peak_ns, spike_recovery_ns, total_correctness, disqualified,
	       disqualification_code, rank_delta, computed_at
	  FROM scores
  ) ranked
 WHERE run_group_id=$1`, runGroupID)
	if err != nil {
		return LeaderboardRow{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return LeaderboardRow{}, false, rows.Err()
	}
	var r LeaderboardRow
	var peak, p99, recovery int64
	var computed time.Time
	if err := rows.Scan(&r.Rank, &r.RunGroupID, &r.SubmissionID, &r.ContestantID, &r.TeamName, &peak, &p99,
		&recovery, &r.TotalCorrectness, &r.Disqualified, &r.DisqualificationCode, &r.RankDelta, &computed); err != nil {
		return LeaderboardRow{}, false, err
	}
	r.PeakSustainedTPS = nonNegativeUint64(peak)
	r.P99NSAtPeakTPS = nonNegativeUint64(p99)
	r.SpikeRecoveryNS = nonNegativeUint64(recovery)
	r.ComputedAtUnixNS = computed.UnixNano()
	return r, true, rows.Err()
}

// nonNegativeUint64 performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func nonNegativeUint64(v int64) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v)
}

// leaderboardOrderBy performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func leaderboardOrderBy(sortField, order string) string {
	sortField = strings.ToLower(strings.TrimSpace(sortField))
	sortField = strings.ReplaceAll(sortField, "-", "_")
	order = strings.ToLower(strings.TrimSpace(order))
	type sortSpec struct {
		column      string
		defaultDesc bool
		tiebreak    string
	}
	specs := map[string]sortSpec{
		"":                   {column: "rank", defaultDesc: false, tiebreak: "run_group_id ASC"},
		"rank":               {column: "rank", defaultDesc: false, tiebreak: "run_group_id ASC"},
		"peak_tps":           {column: "peak_sustained_tps", defaultDesc: true, tiebreak: "p99_at_peak_ns ASC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC"},
		"peak_sustained_tps": {column: "peak_sustained_tps", defaultDesc: true, tiebreak: "p99_at_peak_ns ASC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC"},
		"p99":                {column: "p99_at_peak_ns", defaultDesc: false, tiebreak: "peak_sustained_tps DESC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC"},
		"p99_at_peak":        {column: "p99_at_peak_ns", defaultDesc: false, tiebreak: "peak_sustained_tps DESC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC"},
		"p99_ns_at_peak_tps": {column: "p99_at_peak_ns", defaultDesc: false, tiebreak: "peak_sustained_tps DESC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC"},
		"spike_recovery":     {column: "spike_recovery_ns", defaultDesc: false, tiebreak: "peak_sustained_tps DESC, p99_at_peak_ns ASC, total_correctness DESC, run_group_id ASC"},
		"spike_recovery_ns":  {column: "spike_recovery_ns", defaultDesc: false, tiebreak: "peak_sustained_tps DESC, p99_at_peak_ns ASC, total_correctness DESC, run_group_id ASC"},
		"total_score":        {column: "total_correctness", defaultDesc: true, tiebreak: "peak_sustained_tps DESC, p99_at_peak_ns ASC, spike_recovery_ns ASC, run_group_id ASC"},
		"total_correctness":  {column: "total_correctness", defaultDesc: true, tiebreak: "peak_sustained_tps DESC, p99_at_peak_ns ASC, spike_recovery_ns ASC, run_group_id ASC"},
		"correctness":        {column: "total_correctness", defaultDesc: true, tiebreak: "peak_sustained_tps DESC, p99_at_peak_ns ASC, spike_recovery_ns ASC, run_group_id ASC"},
		"team_name":          {column: "team_name", defaultDesc: false, tiebreak: "rank ASC, run_group_id ASC"},
		"computed_at":        {column: "computed_at", defaultDesc: true, tiebreak: "rank ASC, run_group_id ASC"},
	}
	spec, ok := specs[sortField]
	if !ok {
		spec = specs[""]
	}
	dir := "ASC"
	if spec.defaultDesc {
		dir = "DESC"
	}
	switch order {
	case "asc":
		dir = "ASC"
	case "desc":
		dir = "DESC"
	}
	return "disqualified ASC, " + spec.column + " " + dir + ", " + spec.tiebreak
}

// encodeLeaderboardCursor performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func encodeLeaderboardCursor(offset int) string {
	payload, _ := json.Marshal(leaderboardCursor{Offset: offset})
	return base64.RawURLEncoding.EncodeToString(payload)
}

// decodeLeaderboardCursor performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func decodeLeaderboardCursor(cursor string) (int, error) {
	if strings.TrimSpace(cursor) == "" {
		return 0, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, fmt.Errorf("invalid cursor")
	}
	var decoded leaderboardCursor
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return 0, fmt.Errorf("invalid cursor")
	}
	if decoded.Offset < 0 || decoded.Offset > maxLeaderboardOffset {
		return 0, fmt.Errorf("invalid cursor")
	}
	return decoded.Offset, nil
}
