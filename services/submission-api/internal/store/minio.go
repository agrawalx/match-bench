package store

import (
	"context"
	"fmt"
	"io"
	"time"

	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	minioInitTimeout        = 5 * time.Second
	submissionsObjectPrefix = "iicpc-submissions"
	artifactObjectName      = "artifact.zip"
)

type MinioStore struct {
	client *minio.Client
	bucket string
}

type MinioStoreOptions struct {
	CreateBucketIfMissing bool
}

func NewMinioStore(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*MinioStore, error) {
	ctx, cancel := context.WithTimeout(context.Background(), minioInitTimeout)
	defer cancel()

	return NewMinioStoreWithOptions(ctx, endpoint, accessKey, secretKey, bucket, useSSL, MinioStoreOptions{})
}

func NewMinioStoreWithOptions(ctx context.Context, endpoint, accessKey, secretKey, bucket string, useSSL bool, opts MinioStoreOptions) (*MinioStore, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: minio client: %v", cerrs.ErrStoreUploadFailed, err)
	}

	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("%w: minio bucket check: %v", cerrs.ErrStoreUploadFailed, err)
	}
	if !exists {
		if !opts.CreateBucketIfMissing {
			return nil, fmt.Errorf("%w: minio bucket %q does not exist", cerrs.ErrStoreUploadFailed, bucket)
		}
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("%w: minio make bucket: %v", cerrs.ErrStoreUploadFailed, err)
		}
	}

	return &MinioStore{client: client, bucket: bucket}, nil
}

// Upload streams r into MinIO and returns the object path.
//
// The object name is intentionally fixed to artifact.zip because each
// submission_id is immutable and inserted once. Re-uploads mint a new
// submission_id rather than overwriting this object.
func (s *MinioStore) Upload(ctx context.Context, submissionID string, r io.Reader, size int64, sha256hex string) (string, error) {
	if size <= 0 {
		return "", fmt.Errorf("%w: minio put object: size must be greater than zero", cerrs.ErrStoreUploadFailed)
	}

	objectPath := fmt.Sprintf("%s/%s/%s", submissionsObjectPrefix, submissionID, artifactObjectName)

	_, err := s.client.PutObject(ctx, s.bucket, objectPath, r, size, minio.PutObjectOptions{
		ContentType: "application/zip",
		UserMetadata: map[string]string{
			"sha256": sha256hex,
		},
	})
	if err != nil {
		return "", fmt.Errorf("%w: minio put object: %v", cerrs.ErrStoreUploadFailed, err)
	}

	return objectPath, nil
}

// Close is a no-op. The MinIO Go client does not own persistent resources that
// require shutdown, but this keeps store lifecycle management uniform.
func (s *MinioStore) Close() error {
	return nil
}
