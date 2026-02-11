package types

import "os"

// TopAllocator is the user-facing, top-level abstraction for allocators.
// It provides a simple API for allocation, deletion, lookup, and persistence,
// hiding all internal details and wiring. All allocators in the system should be
// usable via this interface, but TopAllocator is intended for end users who do not
// need to know about internal composition or delegation.
//
// Save and Load orchestrate persistence of the entire allocator hierarchy.
type TopAllocator interface {
	Allocator

	// Save persists the entire allocator state to the underlying file/device.
	// Returns error if persistence fails.
	Save() error

	// Load restores the allocator state from the underlying file/device.
	// Returns error if loading fails or state is inconsistent.
	Load() error
}

// Package types defines the allocator interface contracts.
//
// Core design principle: All allocators implement the same interfaces, making them
// interchangeable in tests and allowing polymorphic usage. Specific allocators may
// additionally implement optional interfaces (HierarchicalAllocator, Introspectable, etc.).
//
// Interface hierarchy:
//   - Allocator: complete interface combining all core operations
//   - BasicAllocator: core allocation, deletion, lookup
//   - ObjectReader: optional run allocation support
//   - ObjectWriter: optional callback support
//   - Persister: marshalling/unmarshalling (always required)
//   - HierarchicalAllocator: optional, for allocators with parents
//   - Introspectable: optional, for testing and compaction
//
// Usage pattern:
//   1. All allocators implement BasicAllocator + Persister (always)
//   2. Check for optional interfaces via type assertion
//   3. In tests, use the Allocator interface for polymorphic testing

// Allocator is the core interface that all allocators implement.
// It provides the fundamental operations: allocation, deletion, lookup, and persistence.
type Allocator interface {
	BasicAllocator
	ManyAllocatable
	AllocateCallbackable
	Marsheller
}

// BlockSizeConfigurable is an optional extension for allocators that can
// add new fixed block sizes after initialization.
//
// Use with a type assertion; allocators that do not support this simply
// ignore the request at higher layers.
type BlockSizeConfigurable interface {
	AddBlockSize(size int) error
}

// BasicAllocator defines core allocation and deletion operations.
// All allocator types (BasicAllocator, PoolAllocator, BlockAllocator, OmniAllocator) implement these.
type BasicAllocator interface {
	// Allocate requests a new block of the given size.
	// Returns the ObjectId, FileOffset where the block starts, and any error.
	Allocate(size int) (ObjectId, FileOffset, error)

	// DeleteObj removes the object with the given ObjectId and makes its space available for reuse.
	// Returns an error if the ObjectId is not found or already deleted.
	DeleteObj(objId ObjectId) error

	// GetObjectInfo returns the FileOffset and Size for an allocated ObjectId.
	// Returns an error if the ObjectId is not found or marked as free.
	GetObjectInfo(objId ObjectId) (FileOffset, FileSize, error)

	// GetFile returns the file handle associated with this allocator.
	// Required for allocators to perform direct I/O operations.
	GetFile() *os.File

	// ContainsObjectId returns true if this allocator (or one of its children) owns the given ObjectId.
	// Fast ownership check without loading full allocator state.
	ContainsObjectId(objId ObjectId) bool
}

// ManyAllocatable defines optional support for efficient run allocation.
// Not all allocators support AllocateRun; check for this interface with type assertion.
type ManyAllocatable interface {
	// AllocateRun allocates multiple contiguous ObjectIds efficiently.
	// Returns partial runs if contiguous space is fragmented (caller may need to call again).
	// Returns ErrUnsupported if the allocator doesn't support run allocation.
	AllocateRun(size int, count int) ([]ObjectId, []FileOffset, error)
}

// AllocateCallbackable defines optional callback support for monitoring allocations.
type AllocateCallbackable interface {
	// SetOnAllocate registers a callback to be invoked after each successful allocation.
	// Useful for external tracking (ObjectMap updates, statistics).
	// Pass nil to clear any previous callback.
	SetOnAllocate(callback func(objId ObjectId, offset FileOffset, size int))
}

// Marsheller defines the low-level marshalling/unmarshalling interface.
// High-level Save/Load operations are handled by higher-level orchestration;
// these are the primitives for converting state to/from bytes.
type Marsheller interface {
	// Marshal serializes the allocator's state to a byte slice.
	// Returns serialized bytes ready to be written to a single object in the file.
	Marshal() ([]byte, error)

	// Unmarshal deserializes allocator state from a byte slice and restores internal state.
	// Must be called on a fresh allocator (created via New*()).
	// Populates internal maps, bitmaps, freelists, etc. from the byte representation.
	Unmarshal(data []byte) error
}

// HierarchicalAllocator is an optional interface for allocators that have a parent.
// OmniAllocator, PoolAllocator, and BlockAllocator implement this.
type HierarchicalAllocator interface {
	// Parent returns the parent allocator in the hierarchy.
	// Used for provisioning new child allocators or delegating allocation requests.
	Parent() Allocator
}

// Introspectable is an optional interface for allocators that support introspection.
// Used primarily for testing and compaction logic.
type Introspectable interface {
	// GetObjectIdsInAllocator returns all ObjectIds currently allocated within a specific
	// BlockAllocator (identified by blockSize and allocatorIndex).
	// Returns an empty slice if the allocator doesn't exist or all slots are free.
	// May trigger lazy-load of the BlockAllocator if not in memory.
	GetObjectIdsInAllocator(blockSize int, allocatorIndex int) []ObjectId
}
