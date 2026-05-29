package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSlotStoreConcurrentAccess(t *testing.T) {
	tests := []struct {
		name    string
		readers int
		writers int
	}{
		{name: "balanced", readers: 8, writers: 8},
		{name: "read heavy", readers: 32, writers: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewSlotStore()
			var wg sync.WaitGroup
			start := make(chan struct{})

			for i := 0; i < tt.writers; i++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					<-start
					for n := 0; n < 200; n++ {
						slotID := fmt.Sprintf("slot-%d-%d", worker, n%16)
						store.Put(&Slot{
							SlotID:    slotID,
							Image:     "image",
							Port:      8080,
							State:     StateReady,
							CreatedAt: time.Now(),
						})
						if n%5 == 0 {
							store.Delete(slotID)
						}
					}
				}(i)
			}

			for i := 0; i < tt.readers; i++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					<-start
					for n := 0; n < 400; n++ {
						_, _ = store.Get(fmt.Sprintf("slot-%d-%d", worker%max(tt.writers, 1), n%16))
						_ = store.Len()
						_ = store.List()
					}
				}(i)
			}

			close(start)
			wg.Wait()
		})
	}
}

func TestSlotStoreReturnsCopies(t *testing.T) {
	store := NewSlotStore()
	store.Put(&Slot{SlotID: "slot-a", Image: "before", State: StateReady})

	slot, ok := store.Get("slot-a")
	if !ok {
		t.Fatal("slot not found")
	}
	slot.Image = "mutated"

	got, ok := store.Get("slot-a")
	if !ok {
		t.Fatal("slot not found after mutation")
	}
	if got.Image != "before" {
		t.Fatalf("store exposed mutable slot pointer, got image %q", got.Image)
	}
}
