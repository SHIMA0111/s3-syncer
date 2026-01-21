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
	Files  map[string][]byte
	ListFn func(ctx context.Context, prefix string) (<-chan domain.FileInfo, <-chan error)
	mu     sync.Mutex
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		Files: make(map[string][]byte),
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

func TestCopyService_Copy(t *testing.T) {
	src := NewMockStorage()
	src.Files["file1.txt"] = []byte("content1")
	src.Files["file2.txt"] = []byte("content2")

	dst := NewMockStorage()

	svc := NewCopyService(src, dst, 2)

	progressCalled := 0
	onProgress := func(found, copied int64) {
		progressCalled++
	}

	err := svc.Copy(context.Background(), "", onProgress)
	if err != nil {
		t.Fatalf("Copy failed: %v", err)
	}

	if len(dst.Files) != 2 {
		t.Errorf("Expected 2 files in dst, got %d", len(dst.Files))
	}

	if string(dst.Files["file1.txt"]) != "content1" {
		t.Errorf("Content mismatch for file1.txt")
	}
}
