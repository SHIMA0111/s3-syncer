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
	src           ports.Storage
	dst           ports.Storage
	workers       int
	isSameAccount bool
}

func NewCopyService(ctx context.Context, src, dst ports.Storage, workers int) (*CopyService, error) {
	if workers <= 0 {
		workers = 1
	}

	srcAcc, err := src.GetAccountID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get src account ID: %w", err)
	}
	dstAcc, err := dst.GetAccountID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get dst account ID: %w", err)
	}

	return &CopyService{
		src:           src,
		dst:           dst,
		workers:       workers,
		isSameAccount: srcAcc == dstAcc,
	}, nil
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
	var progressMu sync.Mutex

	safeProgress := func(found, copied int64) {
		if onProgress == nil {
			return
		}
		progressMu.Lock()
		defer progressMu.Unlock()
		onProgress(found, copied)
	}

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
				safeProgress(currentFound, copiedCount.Load())

				// Perform Copy
				err := s.copyFile(ctx, file)
				if err != nil {
					errMu.Lock()
					errs = append(errs, fmt.Errorf("error copying %s: %w", file.Key, err))
					errMu.Unlock()
					// Continue to next file
				} else {
					currentCopied := copiedCount.Add(1)
					safeProgress(currentFound, currentCopied)
				}
			}
		}()
	}

	// Wait for List to finish (it closes filesCh)
	listErr := <-listErrCh

	// Wait for workers to finish
	wg.Wait()

	if listErr != nil {
		return fmt.Errorf("failed to list files: %w", listErr)
	}

	if len(errs) > 0 {
		return fmt.Errorf("encountered %d errors during copy", len(errs))
	}
	return nil
}

func (s *CopyService) copyFile(ctx context.Context, file domain.FileInfo) error {
	if s.isSameAccount {
		// Try native S3 copy first (limited to 5GB for single CopyObject call)
		// For intra-account copy, this is much faster as it stays within AWS.
		return s.dst.CopyFrom(ctx, s.src.GetBucketName(), file.Key, file.Key)
	}

	// Cross-account or fallback: Download and Upload stream
	// 1. Download stream
	reader, info, err := s.src.Download(ctx, file.Key)
	if err != nil {
		return err
	}
	defer func() {
		_ = reader.Close()
	}()

	// 2. Upload stream
	// The adapter's uploader is configured with Chunked (Multipart) upload.
	return s.dst.Upload(ctx, file.Key, reader, info.Size)
}
