package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	cerrs "github.com/iicpc/sandbox-orchestrator/internal/errors"
	"github.com/iicpc/sandbox-orchestrator/internal/k8s"
	"github.com/iicpc/sandbox-orchestrator/internal/store"
)

// createSlotRequest is the controller → orchestrator contract for POST /slots.
//
// slot_id MUST be the controller's session_id verbatim. The orchestrator
// never mints its own IDs — having a single ID for the run means the
// algo Pod, Service, and any log/metric record can be cross-referenced
// without translation. The image ref is also supplied by the caller;
// the orchestrator does not assemble Harbor refs itself.
type createSlotRequest struct {
	SlotID string `json:"slot_id"`
	Image  string `json:"image"`
	Port   int    `json:"port"`
}

// slotResponse is returned by POST /slots and GET /slots/{id}.
type slotResponse struct {
	SlotID   string       `json:"slot_id"`
	State    string       `json:"state"`
	Message  string       `json:"message"`
	Endpoint endpointJSON `json:"endpoint"`
}

type endpointJSON struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// CreateSlot is the POST /slots handler.
// Idempotent: if a slot with the same slot_id and image already exists,
// returns 200 with current state. Different image → 409 Conflict.
func CreateSlot(mgr *k8s.Manager, slots *store.SlotStore, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createSlotRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
		if req.SlotID == "" || req.Image == "" || req.Port <= 0 {
			writeError(w, http.StatusBadRequest, "slot_id, image, and port are required")
			return
		}

		ctx := r.Context()
		existing, exists := slots.Get(req.SlotID)
		if exists && existing.Image == req.Image {
			// Refresh the cached state so the controller sees current pod status.
			state, msg, err := mgr.Refresh(ctx, req.SlotID)
			if err != nil && !errors.Is(err, cerrs.ErrSlotNotFound) {
				log.ErrorContext(ctx, "refresh slot", "slot_id", req.SlotID, "error", err)
				writeError(w, http.StatusInternalServerError, "refresh slot: "+err.Error())
				return
			}
			if !errors.Is(err, cerrs.ErrSlotNotFound) {
				existing.State = state
				existing.Message = msg
				slots.Put(existing)
			}
			writeJSON(w, http.StatusOK, toResponse(existing))
			return
		}

		if err := mgr.CreateSlot(ctx, req.SlotID, req.Image, req.Port); err != nil {
			switch {
			case errors.Is(err, cerrs.ErrSlotImageMismatch):
				writeError(w, http.StatusConflict, err.Error())
			default:
				log.ErrorContext(ctx, "create slot", "slot_id", req.SlotID, "error", err)
				writeError(w, http.StatusInternalServerError, "create slot: "+err.Error())
			}
			return
		}

		state, msg, _ := mgr.Refresh(ctx, req.SlotID) // newly created pod is in Pending
		slot := &store.Slot{
			SlotID:    req.SlotID,
			Image:     req.Image,
			Port:      req.Port,
			State:     state,
			Message:   msg,
			Endpoint:  store.Endpoint{Host: k8s.ServiceFQDN(req.SlotID, mgr.Namespace()), Port: req.Port},
			CreatedAt: time.Now().UTC(),
		}
		slots.Put(slot)
		log.InfoContext(ctx, "slot created", "slot_id", req.SlotID, "image", req.Image, "port", req.Port)
		writeJSON(w, http.StatusCreated, toResponse(slot))
	}
}

// GetSlot is the GET /slots/{slot_id} handler.
// Always refreshes from k8s so the controller sees authoritative state.
func GetSlot(mgr *k8s.Manager, slots *store.SlotStore, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slotID := chi.URLParam(r, "slot_id")
		if slotID == "" {
			writeError(w, http.StatusBadRequest, "missing slot_id")
			return
		}

		ctx := r.Context()
		slot, ok := slots.Get(slotID)
		if !ok {
			writeError(w, http.StatusNotFound, "slot not found")
			return
		}

		state, msg, err := mgr.Refresh(ctx, slotID)
		if errors.Is(err, cerrs.ErrSlotNotFound) {
			// Pod was deleted externally; drop the stale entry and 404.
			slots.Delete(slotID)
			writeError(w, http.StatusNotFound, "slot not found")
			return
		}
		if err != nil {
			log.ErrorContext(ctx, "refresh slot", "slot_id", slotID, "error", err)
			writeError(w, http.StatusInternalServerError, "refresh slot: "+err.Error())
			return
		}

		slot.State = state
		slot.Message = msg
		slots.Put(slot)
		writeJSON(w, http.StatusOK, toResponse(slot))
	}
}

// DeleteSlot is the DELETE /slots/{slot_id} handler.
// Idempotent: 204 whether or not the slot existed.
func DeleteSlot(mgr *k8s.Manager, slots *store.SlotStore, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slotID := chi.URLParam(r, "slot_id")
		if slotID == "" {
			writeError(w, http.StatusBadRequest, "missing slot_id")
			return
		}

		ctx := r.Context()
		if err := mgr.DeleteSlot(ctx, slotID); err != nil {
			log.ErrorContext(ctx, "delete slot", "slot_id", slotID, "error", err)
			writeError(w, http.StatusInternalServerError, "delete slot: "+err.Error())
			return
		}
		slots.Delete(slotID)
		log.InfoContext(ctx, "slot released", "slot_id", slotID)
		w.WriteHeader(http.StatusNoContent)
	}
}

func toResponse(slot *store.Slot) slotResponse {
	return slotResponse{
		SlotID:  slot.SlotID,
		State:   string(slot.State),
		Message: slot.Message,
		Endpoint: endpointJSON{
			Host: slot.Endpoint.Host,
			Port: slot.Endpoint.Port,
		},
	}
}
