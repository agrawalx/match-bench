package errors

import "errors"

var (
	ErrDuplicateSubmission = errors.New("duplicate submission")
	ErrInvalidArtifact = errors.New("invalid artifact")
	ErrStoreUploadFailed = errors.New("failed to upload artifact to store")
	ErrStoreDatabaseFailed = errors.New("failed to save metadata to database")
	ErrSubmissionNotFound = errors.New("submission not found")
	ErrInternal = errors.New("internal server error")
	ErrValidation = errors.New("validation failed")
	ErrCorruptArchive = errors.New("file is not a valid ZIP archive")
	ErrConfigMissing = errors.New("benchmark.yaml not found at zip root")

	// ErrActiveRunExists signals that the partial unique index on runs
	// (idx_runs_one_active_per_submission, defined as UNIQUE on
	// runs(submission_id) WHERE status NOT IN ('completed','failed'))
	// rejected the INSERT because a non-terminal run already exists for
	// this submission.
	//
	// This is the integrity gate behind "one active run per submission":
	// even under racing concurrent POSTs, the database guarantees only
	// one row is in a non-terminal state. The handler uses this sentinel
	// to fall back to returning the existing run_id with HTTP 200
	// instead of 202 — making the endpoint idempotent at the database
	// level rather than at the application level.
	ErrActiveRunExists    = errors.New("active run already exists for this submission")
	ErrRunNotFound        = errors.New("run not found")
	ErrSubmissionNotReady = errors.New("submission is not in 'ready' status")
)
