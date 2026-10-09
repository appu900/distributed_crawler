package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/appu/leadgraph/internal/config"
)

// S3 is a Blob backed by any S3-compatible service (AWS S3, MinIO, ...).
type S3 struct {
	client *minio.Client
	bucket string
}

// NewS3 connects and makes sure the bucket exists.
func NewS3(ctx context.Context, cfg config.S3) (*S3, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	// The endpoint may still be starting (docker compose up); give it a
	// moment before giving up.
	var exists bool
	for attempt := 0; ; attempt++ {
		exists, err = client.BucketExists(ctx, cfg.Bucket)
		if err == nil {
			break
		}
		if attempt >= 15 || ctx.Err() != nil || minio.ToErrorResponse(err).Code != "" {
			return nil, fmt.Errorf("s3: checking bucket %q at %s: %w", cfg.Bucket, cfg.Endpoint, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{Region: cfg.Region}); err != nil {
			// Another worker may have created it concurrently.
			if ok, cerr := client.BucketExists(ctx, cfg.Bucket); cerr != nil || !ok {
				return nil, fmt.Errorf("s3: creating bucket %q: %w", cfg.Bucket, err)
			}
		}
	}
	return &S3{client: client, bucket: cfg.Bucket}, nil
}

func (s *S3) Put(ctx context.Context, key string, data []byte, contentType string) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: contentType})
	return err
}

func (s *S3) get(ctx context.Context, key string, opts minio.GetObjectOptions) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return data, nil
}

func (s *S3) Get(ctx context.Context, key string) ([]byte, error) {
	return s.get(ctx, key, minio.GetObjectOptions{})
}

func (s *S3) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if length <= 0 {
		return nil, fmt.Errorf("s3: invalid range length %d", length)
	}
	var opts minio.GetObjectOptions
	if err := opts.SetRange(offset, offset+length-1); err != nil {
		return nil, err
	}
	return s.get(ctx, key, opts)
}

func (s *S3) Delete(ctx context.Context, key string) error {
	return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

func (s *S3) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}
