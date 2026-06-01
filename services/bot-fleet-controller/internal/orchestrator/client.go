// Package orchestrator is the HTTP client for sandbox-orchestrator.
//
// Wire contract (defined in services/sandbox-orchestrator):
//   POST   /slots             create a slot for {slot_id, image, port}
//   GET    /slots/{slot_id}   poll current state
//   DELETE /slots/{slot_id}   release Pod + Service
//
// The controller treats the orchestrator as a black box: each slot is one
// Pod and one Service (both named algo-{slot_id}), created together on
// POST and released together on DELETE. slot_id is always the controller's
// session_id verbatim — the orchestrator does not mint its own IDs. Image
// ref is supplied by the caller (controller composes the Harbor reference
// from HARBOR_PRODUCTION_ENDPOINT and HARBOR_PROJECT env).
package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// State mirrors store.SlotState in the orchestrator. Duplicated here as a
// small wire enum so the controller does not depend on the orchestrator's
// internal store package.
type State string

const (
	StateCreating    State = "creating"
	StateReady       State = "ready"
	StateFailed      State = "failed"
	StateTerminating State = "terminating"
)

type Endpoint struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type Slot struct {
	SlotID   string   `json:"slot_id"`
	State    State    `json:"state"`
	Message  string   `json:"message"`
	Endpoint Endpoint `json:"endpoint"`
}

var ErrSlotNotFound = errors.New("slot not found")

// Client speaks to one sandbox-orchestrator instance.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient — baseURL is the orchestrator's ClusterIP service URL,
// e.g. "http://sandbox-orchestrator.sandbox.svc.cluster.local:8080".
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 15 * time.Second},
	}
}

type createSlotRequest struct {
	SlotID       string `json:"slot_id"`
	ContestantID string `json:"contestant_id"`
	Image        string `json:"image"`
	Port         int    `json:"port"`
}

// CreateSlot is POST /slots. Returns the current slot state. The endpoint
// is populated synchronously; State is whatever the orchestrator observed
// right after creation (typically "creating"). contestantID is forwarded so the
// orchestrator can stamp it onto the eBPF capture's latency events.
func (c *Client) CreateSlot(ctx context.Context, slotID, contestantID, image string, port int) (*Slot, error) {
	body, err := json.Marshal(createSlotRequest{SlotID: slotID, ContestantID: contestantID, Image: image, Port: port})
	if err != nil {
		return nil, fmt.Errorf("marshal create slot: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/slots", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.doSlot(req)
}

// GetSlot is GET /slots/{slot_id}. Returns ErrSlotNotFound on 404.
func (c *Client) GetSlot(ctx context.Context, slotID string) (*Slot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/slots/"+slotID, nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	return c.doSlot(req)
}

// DeleteSlot is DELETE /slots/{slot_id}. Idempotent on the server side, so we
// treat any 2xx (and 404) as success.
func (c *Client) DeleteSlot(ctx context.Context, slotID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/slots/"+slotID, nil)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("delete slot: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || (resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return fmt.Errorf("delete slot: %s — %s", resp.Status, string(body))
}

// WaitForReady polls GetSlot until the slot is ready, failed, or the deadline
// passes. pollInterval defaults to 500ms when zero.
func (c *Client) WaitForReady(ctx context.Context, slotID string, deadline time.Duration, pollInterval time.Duration) (*Slot, error) {
	if pollInterval == 0 {
		pollInterval = 500 * time.Millisecond
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		slot, err := c.GetSlot(deadlineCtx, slotID)
		if err != nil {
			return nil, err
		}
		switch slot.State {
		case StateReady, StateFailed:
			return slot, nil
		}
		select {
		case <-deadlineCtx.Done():
			return slot, fmt.Errorf("slot did not become ready within %s (last state: %s)", deadline, slot.State)
		case <-ticker.C:
		}
	}
}

func (c *Client) doSlot(req *http.Request) (*Slot, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("orchestrator request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrSlotNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("orchestrator returned %s — %s", resp.Status, string(body))
	}

	var slot Slot
	if err := json.NewDecoder(resp.Body).Decode(&slot); err != nil {
		return nil, fmt.Errorf("decode slot response: %w", err)
	}
	return &slot, nil
}
