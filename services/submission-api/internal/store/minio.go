package store

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const minioObjectPrefix = "submissions"

type MinioStore struct {
	client *minio.Client
	bucket string
}

func NewMinioStore(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*MinioStore, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio client: %w", err)
	}

	ctx := context.Background()
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("minio bucket check: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("minio make bucket: %w", err)
		}
	}

	return &MinioStore{client: client, bucket: bucket}, nil
}

// Upload streams r into MinIO and returns the object path.
// objectPath format: submissions/{submissionID}/artifact.zip
func (s *MinioStore) Upload(ctx context.Context, submissionID string, r io.Reader, size int64) (string, error) {
	objectPath := fmt.Sprintf("%s/%s/artifact.zip", minioObjectPrefix, submissionID)

	_, err := s.client.PutObject(ctx, s.bucket, objectPath, r, size, minio.PutObjectOptions{
		ContentType: "application/zip",
	})
	if err != nil {
		return "", fmt.Errorf("minio put object: %w", err)
	}

	return objectPath, nil
}
