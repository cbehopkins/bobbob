package store

import "github.com/cbehopkins/bobbob/store/allocator"

// This file contains compile-time interface compliance checks.
// If any type doesn't properly implement its intended interface,
// compilation will fail with a clear error message.

// Verify baseStore implements BasicStorer
var _ BasicStorer = (*baseStore)(nil)

// Verify baseStore implements ObjReader
var _ ObjReader = (*baseStore)(nil)

// Verify baseStore implements ObjWriter
var _ ObjWriter = (*baseStore)(nil)

// Verify baseStore implements Storer (which embeds all the above)
var _ Storer = (*baseStore)(nil)

// Verify concurrentStore implements Storer
var _ Storer = (*concurrentStore)(nil)

// Verify BasicAllocator implements Allocator
var _ allocator.Allocator = (*allocator.BasicAllocator)(nil)
