package services

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/SHIMA0111/s3-syncer/internal/core/domain"
	"github.com/SHIMA0111/s3-syncer/internal/core/ports"
)

type CopyService struct {
	src     ports.Storage
	dst     ports.Storage
	workers int
}

func NewCopyService(src, dst ports.Storage, workers int) *CopyService {
	if workers <= 0 {
		workers = 1
	}
	return &CopyService{
		src:     src,
		dst:     dst,
		workers: workers,
	}
}

// ProgressCallback is called to report progress
type ProgressCallback func(found, copied int64)

// Copy performs the copy operation.
// It returns a list of errors encountered, or nil if successful.
// If multiple errors occur, they are aggregated (simplified for now).
// In a real CLI, we might want to log errors to a file immediately instead of returning them all.
func (s *CopyService) Copy(ctx context.Context, prefix string, onProgress ProgressCallback) error {
	filesCh, listErrCh := s.src.List(ctx, prefix)

	var wg sync.WaitGroup
	var foundCount, copiedCount atomic.Int64
	var errs []error
	var errMu sync.Mutex

	// Worker pool
	// We use a semaphore pattern or just spawn N workers consuming the channel.
	// Spawning N fixed workers is easier.

	for i := 0; i < s.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range filesCh {
				// Update Found Count
				currentFound := foundCount.Add(1)
				if onProgress != nil {
					onProgress(currentFound, copiedCount.Load())
				}

				// Perform Copy
				err := s.copyFile(ctx, file)
				if err != nil {
					errMu.Lock()
					errs = append(errs, fmt.Errorf("error copying %s: %w", file.Key, err))
					errMu.Unlock()
					// Continue to next file
				} else {
					currentCopied := copiedCount.Add(1)
					if onProgress != nil {
						onProgress(currentFound, currentCopied)
					}
				}
			}
		}()
	}

	// Wait for List to finish (it closes filesCh)
	// checking list error
	if err := <-listErrCh; err != nil {
		return fmt.Errorf("listing failed: %w", err)
	}

	// Wait for workers to finish
	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("encountered %d errors during copy", len(errs))
	}
	return nil
}

func (s *CopyService) copyFile(ctx context.Context, file domain.FileInfo) error {
	// 1. Download stream
	// Note: We need to import domain package properly.
	reader, info, err := s.src.Download(ctx, file.Key)
	if err != nil {
		return err
	}
	defer reader.Close()

	// 2. Upload stream
	// We use info.Size from Download (or from List) for ContentLength
	return s.dst.Upload(ctx, file.Key, reader, info.Size)
}
