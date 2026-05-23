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
)
