package store

import (
	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/allocator/basic"
	"github.com/cbehopkins/bobbob/allocator/types"
)

// This file contains compile-time interface compliance checks.
// If any type doesn't properly implement its intended interface,
// compilation will fail with a clear error message.

// Verify baseStore implements BasicStorer
var _ bobbob.BasicStorer = (*baseStore)(nil)

// Verify baseStore implements ObjReader
var _ bobbob.ObjReader = (*baseStore)(nil)

// Verify baseStore implements ObjWriter
var _ bobbob.ObjWriter = (*baseStore)(nil)

// Verify baseStore implements Storer (which embeds all the above)
var _ bobbob.Storer = (*baseStore)(nil)

// Verify concurrentStore implements Storer
var _ bobbob.Storer = (*concurrentStore)(nil)

// Verify BasicAllocator implements types.Allocator
var _ types.Allocator = (*basic.BasicAllocator)(nil)
