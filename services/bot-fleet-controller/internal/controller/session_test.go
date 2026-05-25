package controller

import (
	"context"
	"testing"

	"github.com/iicpc/schemas/topics"
)

func TestSessionManagerAddIsIdempotent(t *testing.T) {
	m := NewSessionManager()
	s1 := newTestSession("sess-A")
	got, existed := m.Add(s1)
	if existed || got != s1 {
		t.Fatalf("first Add: existed=%v got=%v", existed, got)
	}

	s2 := newTestSession("sess-A") // same id, different pointer
	got, existed = m.Add(s2)
	if !existed {
		t.Fatalf("expected second Add to report existed")
	}
	if got != s1 {
		t.Fatalf("expected second Add to return original session pointer")
	}
}

func TestDispatchReadyDeliversToSession(t *testing.T) {
	m := NewSessionManager()
	sess := newTestSession("sess-A")
	m.Add(sess)

	sig := topics.ReadySignal{SessionID: "sess-A", WorkerIndex: 0, WorkerID: "pod-0"}
	if !m.DispatchReady(sig) {
		t.Fatalf("DispatchReady returned false for known session")
	}

	select {
	case got := <-sess.readyCh:
		if got.WorkerID != "pod-0" {
			t.Errorf("got worker_id %s", got.WorkerID)
		}
	default:
		t.Fatalf("readyCh did not receive signal")
	}
}

func TestDispatchReadyReturnsFalseForUnknownSession(t *testing.T) {
	m := NewSessionManager()
	if m.DispatchReady(topics.ReadySignal{SessionID: "ghost"}) {
		t.Errorf("DispatchReady should return false for unknown session")
	}
}

func TestSessionManagerDrop(t *testing.T) {
	m := NewSessionManager()
	m.Add(newTestSession("sess-A"))
	m.Drop("sess-A")
	if _, ok := m.Get("sess-A"); ok {
		t.Errorf("session should be gone after Drop")
	}
}

func newTestSession(id string) *Session {
	_, cancel := context.WithCancel(context.Background())
	return &Session{
		SessionID:     id,
		WorkerCount:   1,
		ReadyReceived: map[uint32]topics.ReadySignal{},
		readyCh:       make(chan topics.ReadySignal, 4),
		cancel:        cancel,
	}
}
