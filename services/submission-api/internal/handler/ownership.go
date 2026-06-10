package handler

import (
	"context"
	"fmt"

	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/store"
)

// submissionBinder is the slice of the store the claim/ownership resolution
// needs. *store.PostgresStore satisfies it; tests fake it to script the
// lost-claim interleavings that cannot be scheduled through a live handler.
type submissionBinder interface {
	ClaimSubmissionContestantIfEmpty(ctx context.Context, submissionID, contestantID string) (bool, error)
	GetByID(ctx context.Context, submissionID string) (*store.SubmissionMeta, error)
}

// claimOrResolveOwner returns the submission with its OWNER AS THE DATABASE
// SEES IT after attempting to bind an unowned row to contestantID.
//
// Three outcomes:
//   - the row is already owned: returned unchanged — the caller compares
//     owner vs contestantID and 404s/409s on mismatch as before;
//   - the claim wins: a copy with ContestantID = contestantID is returned;
//   - the claim is LOST (another request bound the row between our read and
//     the UPDATE): the row is re-read so ownership reflects the winner, not
//     our local assumption. The pre-fix call sites set sub.ContestantID =
//     contestantID unconditionally after the claim call, so the loser of the
//     race proceeded as if it owned another contestant's submission.
//
// A vanished row on re-read maps to cerrs.ErrSubmissionNotFound.
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
