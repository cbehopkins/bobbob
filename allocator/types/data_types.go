package types

// Package types defines the core data types and sentinel values used throughout
// the allocator system. These types are intentionally simple and decoupled from
// any particular allocator implementation.
import (
	"github.com/cbehopkins/bobbob/internal"
)

// ObjectId is an identifier unique within the store for an object.
// A user refers to an object by its ObjectId.
type ObjectId = internal.ObjectId

// FileOffset represents a byte offset within a file.
type FileOffset = internal.FileOffset

// FileSize represents the size of data in bytes.
type FileSize int64

// Common error constants (not exhaustive; implementations may return other errors)
// These are primarily for testing and documentation purposes.
var (
	// ErrNotFound indicates an ObjectId is not allocated or has been deleted.
	ErrNotFound = &allocatorError{"object not found"}

	// ErrUnsupported indicates an operation is not supported by this allocator.
	ErrUnsupported = &allocatorError{"operation not supported"}

	// ErrAllocationFailed indicates allocation could not be completed.
	ErrAllocationFailed = &allocatorError{"allocation failed"}
)

// allocatorError is a simple error type for allocator operations.
type allocatorError struct {
	msg string
}

func (e *allocatorError) Error() string {
	return e.msg
}
