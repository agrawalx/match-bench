// Package handler defines tests for ownership test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"context"
	"errors"
	"testing"

	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/store"
)

// fakeBinder groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type fakeBinder struct {
	claimed  bool
	claimErr error

	reread    *store.SubmissionMeta
	rereadErr error

	claimCalls  int
	rereadCalls int
}

// ClaimSubmissionContestantIfEmpty applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (f *fakeBinder) ClaimSubmissionContestantIfEmpty(ctx context.Context, submissionID, contestantID string) (bool, error) {
	f.claimCalls++
	return f.claimed, f.claimErr
}

// GetByID applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (f *fakeBinder) GetByID(ctx context.Context, submissionID string) (*store.SubmissionMeta, error) {
	f.rereadCalls++
	return f.reread, f.rereadErr
}

// TestClaimOrResolveOwner performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestClaimOrResolveOwner(t *testing.T) {
	dbErr := errors.New("connection reset")

	tests := []struct {
		name        string
		sub         store.SubmissionMeta
		binder      *fakeBinder
		wantOwner   string
		wantErr     error
		wantClaims  int
		wantRereads int
	}{
		{
			name:        "already owned row is returned untouched",
			sub:         store.SubmissionMeta{SubmissionID: "s1", ContestantID: "owner"},
			binder:      &fakeBinder{},
			wantOwner:   "owner",
			wantClaims:  0,
			wantRereads: 0,
		},
		{
			name:        "claim won binds the caller",
			sub:         store.SubmissionMeta{SubmissionID: "s1"},
			binder:      &fakeBinder{claimed: true},
			wantOwner:   "me",
			wantClaims:  1,
			wantRereads: 0,
		},
		{
			name: "claim lost to another contestant resolves to the DB owner",
			sub:  store.SubmissionMeta{SubmissionID: "s1"},
			binder: &fakeBinder{
				claimed: false,
				reread:  &store.SubmissionMeta{SubmissionID: "s1", ContestantID: "rival"},
			},
			wantOwner:   "rival",
			wantClaims:  1,
			wantRereads: 1,
		},
		{
			name: "claim lost but DB owner is the caller (idempotent retry)",
			sub:  store.SubmissionMeta{SubmissionID: "s1"},
			binder: &fakeBinder{
				claimed: false,
				reread:  &store.SubmissionMeta{SubmissionID: "s1", ContestantID: "me"},
			},
			wantOwner:   "me",
			wantClaims:  1,
			wantRereads: 1,
		},
		{
			name:        "claim lost and row vanished",
			sub:         store.SubmissionMeta{SubmissionID: "s1"},
			binder:      &fakeBinder{claimed: false, reread: nil},
			wantErr:     cerrs.ErrSubmissionNotFound,
			wantClaims:  1,
			wantRereads: 1,
		},
		{
			name:       "claim error propagates",
			sub:        store.SubmissionMeta{SubmissionID: "s1"},
			binder:     &fakeBinder{claimErr: dbErr},
			wantErr:    dbErr,
			wantClaims: 1,
		},
		{
			name:        "re-read error propagates",
			sub:         store.SubmissionMeta{SubmissionID: "s1"},
			binder:      &fakeBinder{claimed: false, rereadErr: dbErr},
			wantErr:     dbErr,
			wantClaims:  1,
			wantRereads: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub := tt.sub
			resolved, err := claimOrResolveOwner(context.Background(), tt.binder, &sub, "me")
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
			} else {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				if resolved.ContestantID != tt.wantOwner {
					t.Fatalf("resolved owner = %q, want %q", resolved.ContestantID, tt.wantOwner)
				}
			}
			if tt.binder.claimCalls != tt.wantClaims {
				t.Fatalf("claim calls = %d, want %d", tt.binder.claimCalls, tt.wantClaims)
			}
			if tt.binder.rereadCalls != tt.wantRereads {
				t.Fatalf("re-read calls = %d, want %d", tt.binder.rereadCalls, tt.wantRereads)
			}
		})
	}
}
