package ports

import (
	"context"
	"io"

	"github.com/SHIMA0111/s3-syncer/internal/core/domain"
)

// Storage defines the interface for a storage system (adapter)
type Storage interface {
	// List returns a channel of FileInfo for the given prefix.
	// We use a channel for streaming processing of large file lists.
	List(ctx context.Context, prefix string) (<-chan domain.FileInfo, <-chan error)

	// Upload uploads content to the specified key.
	Upload(ctx context.Context, key string, body io.Reader, size int64) error

	// Download retrieves the content of the specified key.
	Download(ctx context.Context, key string) (io.ReadCloser, domain.FileInfo, error)

	// Stat returns the FileInfo for the specified key.
	Stat(ctx context.Context, key string) (domain.FileInfo, error)

	// GetAccountID returns the AWS account ID.
	GetAccountID(ctx context.Context) (string, error)

	// GetBucketName returns the bucket name.
	GetBucketName() string

	// CopyFrom performs a server-side copy.
	CopyFrom(ctx context.Context, srcBucket, srcKey, dstKey string) error
}
