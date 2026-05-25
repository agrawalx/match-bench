package store

import (
	"sync"
	"time"
)

// SlotState is the orchestrator's view of one sandbox slot.
// Derived from k8s Pod state on every Refresh.
type SlotState string

const (
	StateCreating    SlotState = "creating"    // Pod exists, not yet Ready
	StateReady       SlotState = "ready"       // Pod Ready condition is True
	StateFailed      SlotState = "failed"      // ImagePullBackOff, CrashLoopBackOff, or Failed phase
	StateTerminating SlotState = "terminating" // DELETE called, cleanup in progress
)

// Slot is the orchestrator's in-memory record for one slot.
// k8s is the durable source of truth — this map is rebuilt on startup
// from a Pod list with label app=algo.
type Slot struct {
	SlotID    string
	Image     string
	Port      int
	State     SlotState
	Message   string // human-readable reason for the current state
	Endpoint  Endpoint
	CreatedAt time.Time
}

type Endpoint struct {
	Host string // e.g. algo-{slot_id}.sandbox.svc.cluster.local
	Port int
}

// SlotStore is a mutex-protected map of slot_id → Slot.
type SlotStore struct {
	mu    sync.RWMutex
	slots map[string]*Slot
}

func NewSlotStore() *SlotStore {
	return &SlotStore{slots: make(map[string]*Slot)}
}

func (s *SlotStore) Get(slotID string) (*Slot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	slot, ok := s.slots[slotID]
	if !ok {
		return nil, false
	}
	// copy so the caller cannot mutate our state without going through Put.
	cp := *slot
	return &cp, true
}

func (s *SlotStore) Put(slot *Slot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *slot
	s.slots[slot.SlotID] = &cp
}

func (s *SlotStore) Delete(slotID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.slots, slotID)
}

// List returns a snapshot of all known slots.
func (s *SlotStore) List() []Slot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Slot, 0, len(s.slots))
	for _, slot := range s.slots {
		out = append(out, *slot)
	}
	return out
}
