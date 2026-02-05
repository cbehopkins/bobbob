package store

import (
	"time"

	"github.com/cbehopkins/bobbob/allocator"
)

// Config contains tunable constants for store behavior.
// These values can be adjusted based on performance requirements and memory constraints.

const (
	// MaxPrimeObjectSize is the maximum size allowed for a prime object.
	// Prime objects store application metadata (like collection registries, type maps).
	// Default: 1 MB, which should be plenty for most metadata.
	MaxPrimeObjectSize = 1 << 20 // 1MB
)

// primeTableBootstrapSize mirrors the allocator's PrimeTable layout (Basic, Omni, Store metadata).
// It constructs a fresh PrimeTable and registers the same three slots to compute the prefix size.
func primeTableBootstrapSize() int64 {
	pt := allocator.NewPrimeTable()
	pt.Add() // BasicAllocator
	pt.Add() // OmniAllocator
	pt.Add() // Store metadata (reserved for allocator persistence)
	return pt.SizeInBytes()
}

// PrimeObjectStart returns the ObjectId offset reserved for the prime object.
// This is aligned to the PrimeTable size so application metadata starts immediately
// after the PrimeTable bootstrap region.
func PrimeObjectStart() ObjectId {
	return ObjectId(primeTableBootstrapSize())
}

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
