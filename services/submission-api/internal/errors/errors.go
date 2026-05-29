package errors

import "errors"

var (
	ErrDuplicateSubmission = errors.New("duplicate submission")
	ErrInvalidArtifact     = errors.New("invalid artifact")
	ErrStoreUploadFailed   = errors.New("failed to upload artifact to store")
	ErrStoreDatabaseFailed = errors.New("failed to save metadata to database")
	ErrSubmissionNotFound  = errors.New("submission not found")
	ErrInternal            = errors.New("internal server error")
	ErrValidation          = errors.New("validation failed")

	ErrCorruptArchive  = errors.New("file is not a valid ZIP archive")
	ErrNotZip          = ErrCorruptArchive
	ErrCorruptZip      = errors.New("corrupt zip archive")
	ErrTooLarge        = errors.New("file exceeds 100MB limit")
	ErrConfigMissing   = errors.New("benchmark.yaml not found at zip root")
	ErrNoBenchmarkYAML = ErrConfigMissing
	ErrNoSrcDir        = errors.New("src/ directory not found in zip")

	ErrDuplicateRootEntry    = errors.New("duplicate root entry in zip archive")
	ErrMultipleBenchmarkYAML = errors.New("multiple benchmark.yaml files at zip root")
	ErrInvalidProtocol       = errors.New("invalid protocol: must be FIX, REST, or WS")
	ErrInvalidLanguage       = errors.New("invalid language: must be cpp, rust, or go")
	ErrMissingBuildTarget    = errors.New("build.target is required in benchmark.yaml")
	ErrInvalidPortRange      = errors.New("port out of allowed range (1024–65535)")
	ErrRootConfigTooLarge    = errors.New("root config/build file exceeds 1 MiB after decompression")
	ErrMissingCMakeLists     = errors.New("cpp project must include CMakeLists.txt at zip root")
	ErrMissingCargoToml      = errors.New("rust project must include Cargo.toml at zip root")
	ErrMissingGoMod          = errors.New("go project must include go.mod at zip root")
	ErrInvalidBuildType      = errors.New("invalid build.type for the selected language")
	ErrMissingCMakeTarget    = errors.New("CMakeLists.txt has no add_executable target")
	ErrMissingCargoBin       = errors.New("Cargo.toml has no [[bin]] with the requested name")

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
