package minio

import (
	"context"
)

// IMinio defines the interface for interacting with a MinIO client.
type IMinio interface {
	// Read retrieves the content of the specified object as a slice of strings,
	// splitting the content by newline characters.
	Read(ctx context.Context, objectName string) ([]string, error)

	// Write uploads a slice of strings as a file to the specified object name in a MinIO bucket.
	// The lines are joined into a single string with newline characters before being uploaded.
	Write(ctx context.Context, objectName string, lines []string) error
}
