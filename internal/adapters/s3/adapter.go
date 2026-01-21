package s3

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/SHIMA0111/s3-syncer/internal/core/domain"
	"github.com/SHIMA0111/s3-syncer/internal/core/ports"
)

type Adapter struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

// Ensure Adapter implements ports.Storage
var _ ports.Storage = (*Adapter)(nil)

// NewAdapter creates a new S3 adapter for a specific profile and bucket.
// If profile is empty, the default chain is used.
func NewAdapter(ctx context.Context, bucket, profile string, region string) (*Adapter, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}
	if profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(profile))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	return &Adapter{
		client:   client,
		uploader: manager.NewUploader(client),
		bucket:   bucket,
	}, nil
}

func (a *Adapter) List(ctx context.Context, prefix string) (<-chan domain.FileInfo, <-chan error) {
	outCh := make(chan domain.FileInfo, 100) // Buffer to reduce blocking
	errCh := make(chan error, 1)

	go func() {
		defer close(outCh)
		defer close(errCh)

		// Fix: Handle directory/file paths correctly. ensuring prefix ends with / if it's meant to be a directory is often good practice,
		// but standard ListObjectsV2 behavior is just prefix matching.
		// However, for "directories", users usually expect it to mean "contents of".

		paginator := s3.NewListObjectsV2Paginator(a.client, &s3.ListObjectsV2Input{
			Bucket: aws.String(a.bucket),
			Prefix: aws.String(prefix),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				errCh <- fmt.Errorf("failed to list objects: %w", err)
				return
			}
			for _, obj := range page.Contents {
				// obj.Size is *int64.
				size := aws.ToInt64(obj.Size)
				key := aws.ToString(obj.Key)

				// Skip "folders" themselves if they appear as 0-byte objects ending in /
				// Usually "folder/" objects have size 0.
				if key == prefix && size == 0 {
					continue
				}

				outCh <- domain.FileInfo{
					Key:          key,
					Size:         size,
					LastModified: aws.ToTime(obj.LastModified),
					ETag:         aws.ToString(obj.ETag),
				}
			}
		}
	}()

	return outCh, errCh
}

func (a *Adapter) Upload(ctx context.Context, key string, body io.Reader, size int64) error {
	// uploader.Upload handles multipart automatically
	// It's good practice to provide body with known size if possible,
	// but manager handles io.Reader too.
	_, err := a.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(a.bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return fmt.Errorf("failed to upload object %s: %w", key, err)
	}
	return nil
}

func (a *Adapter) Download(ctx context.Context, key string) (io.ReadCloser, domain.FileInfo, error) {
	// Stick to GetObject for streaming download source -> upload destination.
	out, err := a.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// You might want to check for errors.As(err, &nsk) here
		return nil, domain.FileInfo{}, fmt.Errorf("failed to download object %s: %w", key, err)
	}

	info := domain.FileInfo{
		Key:          key,
		Size:         aws.ToInt64(out.ContentLength),
		LastModified: aws.ToTime(out.LastModified),
		ETag:         aws.ToString(out.ETag),
	}

	return out.Body, info, nil
}

func (a *Adapter) Stat(ctx context.Context, key string) (domain.FileInfo, error) {
	out, err := a.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Can check for NotFound error here explicitly if needed
		return domain.FileInfo{}, fmt.Errorf("failed to stat object %s: %w", key, err)
	}

	return domain.FileInfo{
		Key:          key,
		Size:         aws.ToInt64(out.ContentLength),
		LastModified: aws.ToTime(out.LastModified),
		ETag:         aws.ToString(out.ETag),
	}, nil
}
