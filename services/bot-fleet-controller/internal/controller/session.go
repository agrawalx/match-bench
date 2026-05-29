package controller

import (
	"context"
	"sync"
	"time"

	"github.com/iicpc/bot-fleet-controller/internal/orchestrator"
	"github.com/iicpc/schemas/topics"
)

// Session holds per-run state for one benchmark in flight.
//
// Lives in memory only — no PostgreSQL backing for session state. This is
// safe because the controller is architecturally locked at a single
// replica: if the process dies, every session in this map dies with it
// and there is no other replica that needs to learn about them. The
// startup recovery sweep on the next process re-reads the runs table,
// finds any rows still in non-terminal status, and marks them failed.
// User re-triggers via the frontend.
//
// Concurrent access is protected by sync.RWMutex on the SessionManager
// (not here on Session itself — once a Session is constructed and inserted
// into the map, its fields are only mutated by its own runner goroutine).
type Session struct {
	SessionID    string
	SubmissionID string
	ContestantID string
	RunGroupID   string // parent group; included on every status update for rollup

	Status  string // mirrors topics.RunStatus*
	Message string

	// Populated after orchestrator allocates the slot.
	SlotID   string
	Endpoint *orchestrator.Endpoint

	// Worker count for this session. Computed from the scenario's total task
	// count and the per-pod task ceiling. Not a deployment-wide setting any
	// longer — different scenarios produce different worker counts.
	WorkerCount uint32

	// Fan-in state. Keyed by worker_index so re-delivery is idempotent.
	ReadyReceived map[uint32]topics.ReadySignal

	// readyCh is fed by the bot.ready consumer; the runner goroutine drains
	// it. Buffered so a sudden burst of ready signals does not block the
	// consumer; capacity = WorkerCount * 2 is generous.
	readyCh chan topics.ReadySignal

	// cancel terminates the per-session runner goroutine.
	cancel context.CancelFunc

	CreatedAt time.Time
}

// SessionManager is a mutex-protected map of session_id → *Session.
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewSessionManager() *SessionManager {
	return &SessionManager{sessions: make(map[string]*Session)}
}

// Add registers a new session. Returns the existing *Session if one already
// exists for session_id — the caller decides what to do (the benchmark.requested
// consumer treats a repeat as idempotent and does not start a second runner).
func (m *SessionManager) Add(sess *Session) (*Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.sessions[sess.SessionID]; ok {
		return existing, true
	}
	m.sessions[sess.SessionID] = sess
	return sess, false
}

func (m *SessionManager) Get(sessionID string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[sessionID]
	return s, ok
}

func (m *SessionManager) Drop(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, sessionID)
}

// Snapshot returns a slice of currently tracked session_ids. Used by
// /healthz to surface load.
func (m *SessionManager) Snapshot() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.sessions))
	for id := range m.sessions {
		out = append(out, id)
	}
	return out
}

// DispatchReady delivers one bot.ready signal to the matching session.
// Returns false when no session is tracked (logged by the caller — usually
// means the message arrived after the session completed or before it was
// created on a controller restart).
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
		// Channel full. Should not happen with capacity = WorkerCount * 2.
		return false
	}
}
