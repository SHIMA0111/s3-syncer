package cli

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/SHIMA0111/s3-syncer/internal/core/domain"
	"github.com/SHIMA0111/s3-syncer/internal/core/ports"
)

type MockStorage struct {
	Bucket string
}

func (m *MockStorage) List(_ context.Context, _ string) (<-chan domain.FileInfo, <-chan error) {
	outCh := make(chan domain.FileInfo)
	errCh := make(chan error, 1)
	close(outCh)
	close(errCh)
	return outCh, errCh
}
func (m *MockStorage) Upload(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return nil
}
func (m *MockStorage) Download(_ context.Context, _ string) (io.ReadCloser, domain.FileInfo, error) {
	return nil, domain.FileInfo{}, nil
}
func (m *MockStorage) Stat(_ context.Context, _ string) (domain.FileInfo, error) {
	return domain.FileInfo{}, nil
}
func (m *MockStorage) CopyFrom(_ context.Context, _, _, _ string) error {
	return nil
}
func (m *MockStorage) GetAccountID(_ context.Context) (string, error) { return "123", nil }
func (m *MockStorage) GetBucketName() string                          { return m.Bucket }

func TestRootCmd(t *testing.T) {
	// Backup and restore globals
	oldFactory := storageFactory
	oldWriter := outWriter
	defer func() {
		storageFactory = oldFactory
		outWriter = oldWriter
	}()

	outWriter = io.Discard
	rootCmd.SetOut(io.Discard)
	rootCmd.SetErr(io.Discard)
	rootCmd.SilenceErrors = true
	rootCmd.SilenceUsage = true

	t.Run("Missing buckets", func(t *testing.T) {
		srcBucket = ""
		dstBucket = ""
		rootCmd.SetArgs([]string{})
		err := rootCmd.Execute()
		if err == nil {
			t.Error("Expected error for missing buckets, got nil")
		}
	})

	t.Run("Successful execution", func(t *testing.T) {
		storageFactory = func(_ context.Context, bucket, _, _ string) (ports.Storage, error) {
			return &MockStorage{Bucket: bucket}, nil
		}
		rootCmd.SetArgs([]string{
			"--src-bucket", "src",
			"--dst-bucket", "dst",
		})
		err := rootCmd.Execute()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("Factory error", func(t *testing.T) {
		storageFactory = func(_ context.Context, _, _, _ string) (ports.Storage, error) {
			return nil, fmt.Errorf("factory error")
		}
		rootCmd.SetArgs([]string{
			"--src-bucket", "src",
			"--dst-bucket", "dst",
		})
		err := rootCmd.Execute()
		if err == nil {
			t.Error("Expected error from factory, got nil")
		}
	})
}
