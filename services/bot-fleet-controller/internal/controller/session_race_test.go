// Package controller defines tests for session race test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/iicpc/schemas/topics"
)

// TestSessionManagerConcurrentAccess performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestSessionManagerConcurrentAccess(t *testing.T) {
	tests := []struct {
		name        string
		sessionN    int
		dispatchers int
	}{
		{name: "balanced", sessionN: 16, dispatchers: 16},
		{name: "dispatch heavy", sessionN: 8, dispatchers: 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewSessionManager()
			for i := 0; i < tt.sessionN; i++ {
				sessionID := fmt.Sprintf("session-%d", i)
				manager.Add(&Session{
					SessionID:     sessionID,
					SubmissionID:  fmt.Sprintf("submission-%d", i),
					ReadyReceived: map[uint32]topics.ReadySignal{},
					readyCh:       make(chan topics.ReadySignal, tt.dispatchers*4),
					CreatedAt:     time.Now(),
				})
			}

			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < tt.dispatchers; i++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					<-start
					for n := 0; n < 200; n++ {
						sessionID := fmt.Sprintf("session-%d", n%tt.sessionN)
						_ = manager.DispatchReady(topics.ReadySignal{
							SessionID:   sessionID,
							WorkerID:    fmt.Sprintf("worker-%d", worker),
							WorkerIndex: uint32(worker),
						})
						_, _ = manager.Get(sessionID)
						_ = manager.Snapshot()
					}
				}(i)
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for n := 0; n < 100; n++ {
					sessionID := fmt.Sprintf("ephemeral-%d", n)
					manager.Add(&Session{
						SessionID:     sessionID,
						ReadyReceived: map[uint32]topics.ReadySignal{},
						readyCh:       make(chan topics.ReadySignal, 4),
					})
					manager.Drop(sessionID)
				}
			}()

			close(start)
			wg.Wait()
		})
	}
}
