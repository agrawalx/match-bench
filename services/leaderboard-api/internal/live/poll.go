// Package live polls Redis for per-wave contestant metrics and rebroadcasts them over SSE.
package live

import (
	"context"
	"log/slog"
	"strconv"
	"time"
)

const defaultPollInterval = 1 * time.Second

// SessionSource discovers which sessions are currently active and which contestant owns each.
type SessionSource interface {
	ActiveSessionContestants(ctx context.Context) ([]ActiveSessionContestant, error)
}

// ActiveSessionContestant is the minimal shape the poller needs from the read store.
type ActiveSessionContestant struct {
	SessionID    string
	ContestantID string
}

// RedisReader is the subset of redis.Client the poller depends on.
type RedisReader interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	HGetAll(ctx context.Context, key string) (map[string]string, error)
}

// LiveBroadcaster is the subset of sse.Broker the poller depends on.
type LiveBroadcaster interface {
	BroadcastLive(payload any)
}

// LiveMetrics is the payload broadcast to SSE clients on the "live_metrics" event.
type LiveMetrics struct {
	ContestantID string  `json:"contestant_id"`
	SessionID    string  `json:"session_id"`
	WaveIndex    int64   `json:"wave_index"`
	P50NS        int64   `json:"p50_ns"`
	P99NS        int64   `json:"p99_ns"`
	P999NS       int64   `json:"p999_ns"`
	TPS1s        float64 `json:"tps_1s"`
	ErrorRate    float64 `json:"error_rate"`
	UpdatedAtNS  int64   `json:"updated_at_ns"`
}

// Poller periodically discovers active sessions and rebroadcasts their latest live metrics.
type Poller struct {
	store    SessionSource
	redis    RedisReader
	broker   LiveBroadcaster
	interval time.Duration
	log      *slog.Logger
}

// New constructs a Poller. If interval is zero, defaultPollInterval is used.
func New(store SessionSource, redis RedisReader, broker LiveBroadcaster, interval time.Duration, log *slog.Logger) *Poller {
	if interval <= 0 {
		interval = defaultPollInterval
	}
	if log == nil {
		log = slog.Default()
	}
	return &Poller{store: store, redis: redis, broker: broker, interval: interval, log: log}
}

// Run ticks on the configured interval, polling active sessions until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tick(ctx)
		}
	}
}

func (p *Poller) tick(ctx context.Context) {
	sessions, err := p.store.ActiveSessionContestants(ctx)
	if err != nil {
		p.log.Warn("live poller: fetch active sessions failed", "error", err)
		return
	}
	for _, sc := range sessions {
		p.pollSession(ctx, sc)
	}
}

func (p *Poller) pollSession(ctx context.Context, sc ActiveSessionContestant) {
	waveRaw, found, err := p.redis.Get(ctx, "live:"+sc.SessionID+":latest")
	if err != nil || !found {
		if err != nil {
			p.log.Warn("live poller: get latest wave failed", "session_id", sc.SessionID, "error", err)
		}
		return
	}
	waveIndex, err := strconv.ParseInt(string(waveRaw), 10, 64)
	if err != nil {
		p.log.Warn("live poller: parse wave index failed", "session_id", sc.SessionID, "error", err)
		return
	}

	key := "contestant:" + sc.ContestantID + ":" + sc.SessionID + ":" + strconv.FormatInt(waveIndex, 10)
	fields, err := p.redis.HGetAll(ctx, key)
	if err != nil {
		p.log.Warn("live poller: hgetall failed", "key", key, "error", err)
		return
	}
	if len(fields) == 0 {
		return
	}

	metrics, err := parseLiveMetrics(sc.ContestantID, sc.SessionID, waveIndex, fields)
	if err != nil {
		p.log.Warn("live poller: parse metrics failed", "key", key, "error", err)
		return
	}
	p.broker.BroadcastLive(metrics)
}

func parseLiveMetrics(contestantID, sessionID string, waveIndex int64, fields map[string]string) (LiveMetrics, error) {
	p50, err := strconv.ParseInt(fields["p50_ns"], 10, 64)
	if err != nil {
		return LiveMetrics{}, err
	}
	p99, err := strconv.ParseInt(fields["p99_ns"], 10, 64)
	if err != nil {
		return LiveMetrics{}, err
	}
	p999, err := strconv.ParseInt(fields["p999_ns"], 10, 64)
	if err != nil {
		return LiveMetrics{}, err
	}
	tps, err := strconv.ParseFloat(fields["tps_1s"], 64)
	if err != nil {
		return LiveMetrics{}, err
	}
	errRate, err := strconv.ParseFloat(fields["error_rate"], 64)
	if err != nil {
		return LiveMetrics{}, err
	}
	updatedAt, err := strconv.ParseInt(fields["updated_at_ns"], 10, 64)
	if err != nil {
		return LiveMetrics{}, err
	}
	return LiveMetrics{
		ContestantID: contestantID,
		SessionID:    sessionID,
		WaveIndex:    waveIndex,
		P50NS:        p50,
		P99NS:        p99,
		P999NS:       p999,
		TPS1s:        tps,
		ErrorRate:    errRate,
		UpdatedAtNS:  updatedAt,
	}, nil
}
