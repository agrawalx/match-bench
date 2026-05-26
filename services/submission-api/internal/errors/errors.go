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

	// ErrActiveRunGroupExists signals that the partial unique index on
	// run_groups (idx_run_groups_one_active_per_submission, UNIQUE on
	// run_groups(submission_id) WHERE status NOT IN ('completed','failed'))
	// rejected the INSERT because a non-terminal run-group already exists
	// for this submission.
	//
	// This is the integrity gate behind "one active benchmark per
	// submission": even under racing concurrent POSTs, the database
	// guarantees only one run_group row is in a non-terminal state. The
	// handler uses this sentinel to fall back to returning the existing
	// run_group_id with HTTP 200 instead of 202 — making the endpoint
	// idempotent at the database level rather than at the application level.
	ErrActiveRunGroupExists = errors.New("active run-group already exists for this submission")
	ErrRunNotFound          = errors.New("run not found")
	ErrRunGroupNotFound     = errors.New("run-group not found")
	ErrScenarioNotFound     = errors.New("scenario not found")
	ErrSubmissionNotReady   = errors.New("submission is not in 'ready' status")
)
