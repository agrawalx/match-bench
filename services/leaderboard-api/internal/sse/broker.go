package sse

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
)

// heartbeatInterval is how often an SSE comment is written to each client so
// idle streams survive the ALB default 60s idle timeout.
const heartbeatInterval = 15 * time.Second

type SnapshotFunc func(context.Context) (any, error)

type Broker struct {
	mu        sync.Mutex
	clients   map[chan []byte]struct{}
	snapshot  SnapshotFunc
	heartbeat time.Duration
}

func New(snapshot SnapshotFunc) *Broker {
	return &Broker{clients: make(map[chan []byte]struct{}), snapshot: snapshot, heartbeat: heartbeatInterval}
}

func (b *Broker) Broadcast(ev topics.LeaderboardUpdateEvent) {
	payload, err := json.Marshal(ev)
	if err != nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.clients {
		select {
		case ch <- payload:
		default:
			close(ch)
			delete(b.clients, ch)
			metrics.Counter("leaderboard_api_sse_dropped_clients_total", "Leaderboard SSE clients dropped because they could not keep up.", nil, 1)
		}
	}
	metrics.Gauge("leaderboard_api_sse_clients", "Connected leaderboard SSE clients.", nil, float64(len(b.clients)))
}

func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan []byte, 16)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	metrics.Gauge("leaderboard_api_sse_clients", "Connected leaderboard SSE clients.", nil, float64(len(b.clients)))
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		if _, ok := b.clients[ch]; ok {
			delete(b.clients, ch)
			close(ch)
		}
		metrics.Gauge("leaderboard_api_sse_clients", "Connected leaderboard SSE clients.", nil, float64(len(b.clients)))
		b.mu.Unlock()
	}()

	if b.snapshot != nil {
		snap, err := b.snapshot(r.Context())
		if err != nil {
			http.Error(w, "snapshot failed", http.StatusServiceUnavailable)
			return
		}
		writeEvent(w, "snapshot", snap)
		flusher.Flush()
	}
	// Heartbeats are written by this per-client goroutine, not via the
	// broadcast channel, so they never count against the slow-client drop.
	keepalive := time.NewTicker(b.heartbeat)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
				return
			}
			flusher.Flush()
		case payload, ok := <-ch:
			if !ok {
				return
			}
			w.Write([]byte("event: update\n"))
			w.Write([]byte("data: "))
			w.Write(payload)
			w.Write([]byte("\n\n"))
			flusher.Flush()
		}
	}
}

func writeEvent(w http.ResponseWriter, name string, v any) {
	payload, _ := json.Marshal(v)
	w.Write([]byte("event: " + name + "\n"))
	w.Write([]byte("data: "))
	w.Write(payload)
	w.Write([]byte("\n\n"))
}

func (b *Broker) ClientCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.clients)
}
