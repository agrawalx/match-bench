package controller

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/iicpc/libs/metrics"
)

// PartitionLeaseAllocator hands out exclusive workload.assignments partitions
// to sessions so two sessions' worker specs never collide on the same
// partition (see docs/multi-contestant-audit.md §1/§2: worker_index %
// partitions let worker 0 of every session land on partition 0). It is an
// in-memory bitmap — sufficient while the controller runs at replicas=1;
// promote to a Postgres-backed table before scaling replicas.
type PartitionLeaseAllocator struct {
	mu         sync.Mutex
	total      int
	free       map[int]struct{}
	leasedBy   map[string][]int
	waitCh     chan struct{}
	metricName string
	reason     string
}

// NewPartitionLeaseAllocator builds an allocator over workload.assignments
// partitions [0, total).
func NewPartitionLeaseAllocator(total int) *PartitionLeaseAllocator {
	return newLeaseAllocator(total, "partitions", "partition_leases")
}

// NewBandLeaseAllocator builds a second, independent allocator over EXCLUSIVE
// orders.sent/orders.acked partition bands [0, total) — same generic
// bitmap-lease mechanics as PartitionLeaseAllocator (Acquire blocks until
// count free bands are available, Release returns them), just over band
// indices instead of workload.assignments partition indices. Bands are
// acquired alongside partition leases at session admission (both-or-block,
// same LEASE_ACQUIRE_TIMEOUT) so a session never runs with only one of the
// two leases held.
func NewBandLeaseAllocator(total int) *PartitionLeaseAllocator {
	return newLeaseAllocator(total, "order_bands", "order_band_leases")
}

func newLeaseAllocator(total int, metricName, reason string) *PartitionLeaseAllocator {
	free := make(map[int]struct{}, total)
	for i := 0; i < total; i++ {
		free[i] = struct{}{}
	}
	return &PartitionLeaseAllocator{
		total:      total,
		free:       free,
		leasedBy:   make(map[string][]int),
		waitCh:     make(chan struct{}),
		metricName: metricName,
		reason:     reason,
	}
}

// Acquire blocks until count free partitions are available for sessionID, or
// ctx is done. Free partitions below count is the cross-session capacity
// check validateWorkerCapacity never was: it now blocks admission instead of
// silently colliding two sessions' worker specs on the same partition.
func (a *PartitionLeaseAllocator) Acquire(ctx context.Context, sessionID string, count int) ([]int, error) {
	if count <= 0 {
		return nil, fmt.Errorf("lease count must be positive, got %d", count)
	}
	blocked := false
	for {
		a.mu.Lock()
		if count > a.total {
			a.mu.Unlock()
			return nil, fmt.Errorf("requested %d partitions exceeds workload.assignments partition count %d", count, a.total)
		}
		if _, already := a.leasedBy[sessionID]; already {
			a.mu.Unlock()
			return nil, fmt.Errorf("session %s already holds a partition lease", sessionID)
		}
		if len(a.free) >= count {
			parts := make([]int, 0, count)
			for p := range a.free {
				parts = append(parts, p)
				if len(parts) == count {
					break
				}
			}
			sort.Ints(parts)
			for _, p := range parts {
				delete(a.free, p)
			}
			a.leasedBy[sessionID] = parts
			a.reportLocked()
			a.mu.Unlock()
			return parts, nil
		}
		ch := a.waitCh
		a.mu.Unlock()

		if !blocked {
			blocked = true
			metrics.Counter("controller_admission_blocked_total", "Session admissions blocked by scarce capacity.", metrics.Labels("reason", a.reason), 1)
		}

		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Release returns sessionID's leased partitions to the free pool. Safe to
// call on a session that holds no lease (no-op).
func (a *PartitionLeaseAllocator) Release(sessionID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	parts, ok := a.leasedBy[sessionID]
	if !ok {
		return
	}
	for _, p := range parts {
		a.free[p] = struct{}{}
	}
	delete(a.leasedBy, sessionID)
	a.reportLocked()
	a.notifyLocked()
}

// LeasedCount returns the total number of partitions currently leased across
// all sessions.
func (a *PartitionLeaseAllocator) LeasedCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.total - len(a.free)
}

// reportLocked publishes the leased-partitions gauge. Callers must hold mu.
func (a *PartitionLeaseAllocator) reportLocked() {
	metrics.Gauge(
		"controller_leased_"+a.metricName,
		"Resources ("+a.metricName+") currently leased by in-flight sessions.",
		nil,
		float64(a.total-len(a.free)),
	)
}

// notifyLocked wakes every Acquire currently blocked in this allocator.
// Callers must hold mu.
func (a *PartitionLeaseAllocator) notifyLocked() {
	close(a.waitCh)
	a.waitCh = make(chan struct{})
}
