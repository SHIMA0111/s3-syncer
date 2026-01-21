package domain

import "time"

// FileInfo represents a file in a storage system
type FileInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}
