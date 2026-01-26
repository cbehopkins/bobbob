package store

import (
	"io"
	"os"

	"github.com/cbehopkins/bobbob/internal"
)

// DiskTokenManager handles acquisition and release of disk tokens for rate-limiting I/O.
// Exported for use by multistore package.
type DiskTokenManager struct {
	tokens chan struct{}
}

// NewDiskTokenManager creates a token manager with the specified capacity.
// Returns nil if maxTokens is 0 (unlimited).
// Exported for use by multistore package.
func NewDiskTokenManager(maxTokens int) *DiskTokenManager {
	if maxTokens <= 0 {
		return nil
	}
	tokens := make(chan struct{}, maxTokens)
	for range maxTokens {
		tokens <- struct{}{}
	}
	return &DiskTokenManager{tokens: tokens}
}

// Acquire obtains a disk token, blocking if necessary.
func (m *DiskTokenManager) Acquire() {
	if m != nil {
		<-m.tokens
	}
}

// Release returns a disk token to the pool.
func (m *DiskTokenManager) Release() {
	if m != nil {
		m.tokens <- struct{}{}
	}
}

// CreateSectionReader creates an io.Reader for a specific section of a file.
// This helper encapsulates the common pattern of reading object data from disk.
// Exported for use by multistore package.
func CreateSectionReader(file *os.File, offset FileOffset, size int) io.Reader {
	return io.NewSectionReader(file, int64(offset), int64(size))
}

// CreateSectionWriter creates an io.Writer for a specific section of a file.
// This helper encapsulates the common pattern of writing object data to disk.
// Exported for use by multistore package.
func CreateSectionWriter(file *os.File, offset FileOffset, size int) io.Writer {
	return NewSectionWriter(file, int64(offset), int64(size))
}

// CreateFinisherWithToken creates a Finisher that releases a disk token.
// If tokenManager is nil, returns a no-op finisher that does nothing.
// Always returns a non-nil finisher for API convenience.
// Exported for use by multistore package.
func CreateFinisherWithToken(tokenManager *DiskTokenManager) Finisher {
	if tokenManager == nil {
		return func() error { return nil }
	}
	return func() error {
		tokenManager.Release()
		return nil
	}
}

// WriteZeros writes a block of zeros to the file at the given offset.
// This is commonly used when allocating space for a new object.
// Returns the number of bytes written and any error.
// Exported for use by multistore package.
func WriteZeros(file *os.File, offset FileOffset, size int) (int, error) {
	zeros := make([]byte, size)
	return file.WriteAt(zeros, int64(offset))
}

// WriteBatchedSections writes multiple contiguous data sections to a file in a single operation.
// The sections must be ordered by offset and truly contiguous (no gaps).
// Returns the total number of bytes written and any error.
// Exported for use by multistore package.
func WriteBatchedSections(file *os.File, firstOffset FileOffset, sections [][]byte) (int, error) {
	// Concatenate all sections into a single buffer
	totalSize := 0
	for _, section := range sections {
		totalSize += len(section)
	}

	data := make([]byte, 0, totalSize)
	for _, section := range sections {
		data = append(data, section...)
	}

	return file.WriteAt(data, int64(firstOffset))
}

// ObjectInfoProvider is an interface for retrieving object location information.
// Both baseStore and multiStore delegate to their allocators to implement this.
type ObjectInfoProvider interface {
	GetObjectInfo(objId ObjectId) (ObjectInfo, bool)
}

// LateReadWithTokens is a reusable helper for implementing LateReadObj with token management.
// It handles the common pattern of:
// 1. Acquiring a disk token
// 2. Looking up object info
// 3. Creating a section reader
// 4. Returning a finisher that releases the token
//
// This eliminates duplication between baseStore and multiStore implementations.
func LateReadWithTokens(file *os.File, objId ObjectId, infoProvider ObjectInfoProvider, tokenManager *DiskTokenManager) (io.Reader, Finisher, error) {
	tokenManager.Acquire()

	obj, found := infoProvider.GetObjectInfo(objId)
	if !found {
		tokenManager.Release()
		return nil, nil, io.ErrUnexpectedEOF
	}

	reader := CreateSectionReader(file, obj.Offset, obj.Size)
	return reader, CreateFinisherWithToken(tokenManager), nil
}

// LateWriteNewWithTokens is a reusable helper for implementing LateWriteNewObj with token management.
// It handles the common pattern of:
// 1. Acquiring a disk token
// 2. Allocating a new object (via provided allocation function)
// 3. Creating a section writer
// 4. Returning a finisher that releases the token
//
// The allocateFn parameter allows baseStore and multiStore to use their own allocation strategies.
func LateWriteNewWithTokens(file *os.File, size int, allocateFn func(int) (ObjectId, FileOffset, error), tokenManager *DiskTokenManager) (ObjectId, io.Writer, Finisher, error) {
	tokenManager.Acquire()

	objId, fileOffset, err := allocateFn(size)
	if err != nil {
		tokenManager.Release()
		return internal.ObjNotAllocated, nil, nil, err
	}

	writer := CreateSectionWriter(file, fileOffset, size)
	return objId, writer, CreateFinisherWithToken(tokenManager), nil
}

// WriteToObjWithTokens is a reusable helper for implementing WriteToObj with token management.
// It handles the common pattern of:
// 1. Acquiring a disk token
// 2. Looking up object info
// 3. Creating a section writer
// 4. Returning a finisher that releases the token
//
// This eliminates duplication between baseStore and multiStore implementations.
func WriteToObjWithTokens(file *os.File, objId ObjectId, infoProvider ObjectInfoProvider, tokenManager *DiskTokenManager) (io.Writer, Finisher, error) {
	tokenManager.Acquire()

	obj, found := infoProvider.GetObjectInfo(objId)
	if !found {
		tokenManager.Release()
		return nil, nil, io.ErrUnexpectedEOF
	}

	writer := CreateSectionWriter(file, obj.Offset, obj.Size)
	return writer, CreateFinisherWithToken(tokenManager), nil
}
