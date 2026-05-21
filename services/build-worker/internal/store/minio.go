package store

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

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

// DownloadObject fetches an object from MinIO and returns its contents.
func (s *MinioStore) DownloadObject(ctx context.Context, objectPath string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, objectPath, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("minio get object: %w", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("minio read object: %w", err)
	}
	return data, nil
}

// UploadBytes uploads raw bytes to MinIO at the given object path.
func (s *MinioStore) UploadBytes(ctx context.Context, objectPath, contentType string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, objectPath,
		bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: contentType},
	)
	if err != nil {
		return fmt.Errorf("minio put %s: %w", objectPath, err)
	}
	return nil
}
