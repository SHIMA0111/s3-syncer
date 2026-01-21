package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/SHIMA0111/s3-syncer/internal/core/domain"
)

type MockStorage struct {
	Files     map[string][]byte
	Shared    map[string]map[string][]byte // bucket -> key -> data
	ListFn    func(ctx context.Context, prefix string) (<-chan domain.FileInfo, <-chan error)
	AccountID string
	Bucket    string
	mu        sync.Mutex
}

func NewMockStorage(bucket string, shared map[string]map[string][]byte) *MockStorage {
	if _, ok := shared[bucket]; !ok {
		shared[bucket] = make(map[string][]byte)
	}
	return &MockStorage{
		Files:     shared[bucket],
		Shared:    shared,
		AccountID: "123456789012",
		Bucket:    bucket,
	}
}

func (m *MockStorage) List(ctx context.Context, prefix string) (<-chan domain.FileInfo, <-chan error) {
	if m.ListFn != nil {
		return m.ListFn(ctx, prefix)
	}
	// Default list implementation
	outCh := make(chan domain.FileInfo)
	errCh := make(chan error, 1) // buffered
	go func() {
		defer close(outCh)
		defer close(errCh)
		m.mu.Lock()
		defer m.mu.Unlock()
		for k, v := range m.Files {
			outCh <- domain.FileInfo{
				Key:          k,
				Size:         int64(len(v)),
				LastModified: time.Now(),
			}
		}
	}()
	return outCh, errCh
}

func (m *MockStorage) Upload(_ context.Context, key string, body io.Reader, _ int64) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.Files[key] = data
	m.mu.Unlock()
	return nil
}

func (m *MockStorage) Download(_ context.Context, key string) (io.ReadCloser, domain.FileInfo, error) {
	m.mu.Lock()
	data, ok := m.Files[key]
	m.mu.Unlock()
	if !ok {
		return nil, domain.FileInfo{}, fmt.Errorf("not found")
	}
	return io.NopCloser(bytes.NewReader(data)), domain.FileInfo{
		Key:  key,
		Size: int64(len(data)),
	}, nil
}

func (m *MockStorage) Stat(_ context.Context, _ string) (domain.FileInfo, error) {
	return domain.FileInfo{}, nil
}

func (m *MockStorage) GetAccountID(_ context.Context) (string, error) {
	return m.AccountID, nil
}

func (m *MockStorage) GetBucketName() string {
	return m.Bucket
}

func (m *MockStorage) CopyFrom(_ context.Context, srcBucket, srcKey, dstKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	bucketData, ok := m.Shared[srcBucket]
	if !ok {
		return fmt.Errorf("source bucket not found")
	}

	data, ok := bucketData[srcKey]
	if !ok {
		return fmt.Errorf("source key not found")
	}

	// In Mock, we just copy data to our own Files (which is m.Shared[m.Bucket])
	m.Files[dstKey] = data
	return nil
}

func TestCopyService_Copy(t *testing.T) {
	shared := make(map[string]map[string][]byte)

	t.Run("Same Account (Native Copy)", func(t *testing.T) {
		src := NewMockStorage("src-bucket", shared)
		src.Files["file1.txt"] = []byte("content1")
		src.Files["file2.txt"] = []byte("content2")

		dst := NewMockStorage("dst-bucket", shared)
		dst.AccountID = src.AccountID

		svc, err := NewCopyService(context.Background(), src, dst, 2)
		if err != nil {
			t.Fatalf("Failed to create service: %v", err)
		}

		if !svc.isSameAccount {
			t.Error("Expected isSameAccount to be true")
		}

		err = svc.Copy(context.Background(), "", nil)
		if err != nil {
			t.Fatalf("Copy failed: %v", err)
		}

		if len(dst.Files) != 2 {
			t.Errorf("Expected 2 files in dst, got %d", len(dst.Files))
		}
	})

	t.Run("Different Account (Download/Upload)", func(t *testing.T) {
		src := NewMockStorage("src-bucket-2", shared)
		src.Files["file1.txt"] = []byte("content1")

		dst := NewMockStorage("dst-bucket-2", shared)
		dst.AccountID = "999999999999"

		svc, err := NewCopyService(context.Background(), src, dst, 2)
		if err != nil {
			t.Fatalf("Failed to create service: %v", err)
		}

		if svc.isSameAccount {
			t.Error("Expected isSameAccount to be false")
		}

		err = svc.Copy(context.Background(), "", nil)
		if err != nil {
			t.Fatalf("Copy failed: %v", err)
		}

		if len(dst.Files) != 1 {
			t.Errorf("Expected 1 file in dst, got %d", len(dst.Files))
		}
	})
}
