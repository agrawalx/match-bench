// Package controller implements session behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"context"
	"sync"
	"time"

	"github.com/iicpc/bot-fleet-controller/internal/orchestrator"
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
)

// Session groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Session struct {
	SessionID    string
	SubmissionID string
	ContestantID string
	RunGroupID   string // parent group; included on every status update for rollup

	Status  string // mirrors topics.RunStatus*
	Message string

	SlotID   string
	Endpoint *orchestrator.Endpoint

	WorkerCount uint32

	ReadyReceived map[uint32]topics.ReadySignal

	readyCh chan topics.ReadySignal

	cancel context.CancelFunc

	CreatedAt time.Time
}

// SessionManager groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewSessionManager performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewSessionManager() *SessionManager {
	return &SessionManager{sessions: make(map[string]*Session)}
}

// Add applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *SessionManager) Add(sess *Session) (*Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.sessions[sess.SessionID]; ok {
		metrics.Counter("controller_duplicate_benchmark_requested_total", "Duplicate benchmark.requested messages ignored by controller.", nil, 1)
		return existing, true
	}
	m.sessions[sess.SessionID] = sess
	metrics.Gauge("controller_active_sessions", "Active sessions tracked by the controller.", nil, float64(len(m.sessions)))
	return sess, false
}

// Get applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *SessionManager) Get(sessionID string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[sessionID]
	return s, ok
}

// Drop applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *SessionManager) Drop(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, sessionID)
	metrics.Gauge("controller_active_sessions", "Active sessions tracked by the controller.", nil, float64(len(m.sessions)))
}

// Snapshot applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *SessionManager) Snapshot() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.sessions))
	for id := range m.sessions {
		out = append(out, id)
	}
	return out
}

// DispatchReady applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *SessionManager) DispatchReady(sig topics.ReadySignal) bool {
	m.mu.RLock()
	sess, ok := m.sessions[sig.SessionID]
	m.mu.RUnlock()
	if !ok {
		return false
	}
	select {
	case sess.readyCh <- sig:
		return true
	default:
		return false
	}
}
