package store

import "time"

// Config contains tunable constants for store behavior.
// These values can be adjusted based on performance requirements and memory constraints.

const (
	// HeaderSize is the size in bytes of the store header (ObjectId offset).
	// This must match the ObjectId type size (8 bytes for int64).
	HeaderSize = 8

	// MaxPrimeObjectSize is the maximum size allowed for a prime object.
	// Prime objects store application metadata (like collection registries, type maps).
	// Default: 1 MB, which should be plenty for most metadata.
	MaxPrimeObjectSize = 1 << 20 // 1MB

	// PrimeObjectId is the reserved ObjectId for application metadata.
	// It's the first object allocated after the header.
	PrimeObjectId = HeaderSize
)

// Timing constants
const (
	// AllocatorBackgroundFlushInterval is how often the allocator flushes
	// its cache to the backing store during normal operation.
	AllocatorBackgroundFlushInterval = 5 * time.Second

	// MemoryMonitorCheckInterval is the default interval (in operations) for checking
	// if memory monitoring and flushing should be triggered.
	// This is used by memory monitoring in the Vault.
	MemoryMonitorCheckInterval = 100

	// MemoryMonitorBackgroundTickInterval is the interval at which the background
	// memory monitor checks memory usage when enabled.
	MemoryMonitorBackgroundTickInterval = 100 * time.Millisecond
)
