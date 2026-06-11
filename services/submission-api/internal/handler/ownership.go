// Package handler implements ownership behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"context"
	"fmt"

	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/store"
)

// submissionBinder defines the behavior expected by this package boundary.
// Implementations should preserve the caller-visible contract.
type submissionBinder interface {
	ClaimSubmissionContestantIfEmpty(ctx context.Context, submissionID, contestantID string) (bool, error)
	GetByID(ctx context.Context, submissionID string) (*store.SubmissionMeta, error)
}

// claimOrResolveOwner performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func claimOrResolveOwner(ctx context.Context, pg submissionBinder, sub *store.SubmissionMeta, contestantID string) (*store.SubmissionMeta, error) {
	if sub.ContestantID != "" {
		return sub, nil
	}
	claimed, err := pg.ClaimSubmissionContestantIfEmpty(ctx, sub.SubmissionID, contestantID)
	if err != nil {
		return nil, fmt.Errorf("claim submission contestant: %w", err)
	}
	if claimed {
		bound := *sub
		bound.ContestantID = contestantID
		return &bound, nil
	}
	fresh, err := pg.GetByID(ctx, sub.SubmissionID)
	if err != nil {
		return nil, fmt.Errorf("re-read submission after lost claim: %w", err)
	}
	if fresh == nil {
		return nil, fmt.Errorf("%w: submission vanished after lost claim", cerrs.ErrSubmissionNotFound)
	}
	return fresh, nil
}
