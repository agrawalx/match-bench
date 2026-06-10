package sse

import (
	"bufio"
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/iicpc/schemas/topics"
)

func TestSSESendsSnapshotThenUpdate(t *testing.T) {
	b := New(func(context.Context) (any, error) {
		return map[string]any{"rows": []string{"a"}}, nil
	})
	req := httptest.NewRequest("GET", "/api/events", nil)
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		b.ServeHTTP(rec, req)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	b.Broadcast(topics.LeaderboardUpdateEvent{RunGroupID: "rg", ContestantID: "c"})
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	body := rec.Body.String()
	if !strings.Contains(body, "event: snapshot") || !strings.Contains(body, "event: update") {
		t.Fatalf("missing snapshot/update in %q", body)
	}
}

func TestHeartbeatKeepalive(t *testing.T) {
	b := New(nil)
	b.heartbeat = 10 * time.Millisecond
	req := httptest.NewRequest("GET", "/api/events", nil)
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		b.ServeHTTP(rec, req)
		close(done)
	}()
	time.Sleep(60 * time.Millisecond)
	b.Broadcast(topics.LeaderboardUpdateEvent{RunGroupID: "rg"})
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	body := rec.Body.String()
	if !strings.Contains(body, ": keepalive\n\n") {
		t.Fatalf("missing keepalive comment in %q", body)
	}
	if !strings.Contains(body, "event: update") {
		t.Fatalf("heartbeat must not break update delivery, body %q", body)
	}
}

func TestSlowClientDrop(t *testing.T) {
	b := New(nil)
	ch := make(chan []byte, 1)
	b.clients[ch] = struct{}{}
	ch <- []byte("full")
	b.Broadcast(topics.LeaderboardUpdateEvent{RunGroupID: "rg"})
	if b.ClientCount() != 0 {
		t.Fatalf("slow client was not dropped")
	}
}

func TestEventWireFormat(t *testing.T) {
	rec := httptest.NewRecorder()
	writeEvent(rec, "snapshot", map[string]string{"ok": "yes"})
	line, _ := bufio.NewReader(rec.Body).ReadString('\n')
	if line != "event: snapshot\n" {
		t.Fatalf("first line %q", line)
	}
}
