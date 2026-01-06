# Concurrent Multi-Allocator Store Implementation

## Summary

This document describes the implementation of concurrent access to multi-allocator stores through composable layering architecture.

## Problem Statement

The original store architecture had two independent implementations:
- `baseStore`: Single BasicAllocator with ObjectMap
- `multiStore`: Multi-allocator with OmniBlockAllocator (size-based routing)

Both implemented their own concurrency control and disk I/O management, leading to code duplication (~165 lines). The goal was to enable concurrent access to multiStore while eliminating duplication through proper layering.

## Solution: Generic Concurrent Wrapper

### Phase 1: Make concurrentStore Generic

Changed `concurrentStore` from wrapping only `baseStore` to accepting any `Storer`:

**Before:**
```go
type concurrentStore struct {
    baseStore *baseStore  // Hardcoded dependency
    // ...
}
```

**After:**
```go
type concurrentStore struct {
    innerStore Storer     // Generic wrapper
    lock       sync.RWMutex
    lockMap    map[ObjectId]*sync.RWMutex
    diskTokens chan struct{}
}
```

### Key Design Decisions

#### 1. Dual Locking Strategy
- **Per-Object Locks**: Protect I/O operations on specific objects (read/write)
- **Global Lock**: Protect allocator state modifications (NewObj, PrimeObject, DeleteObj)

This is necessary because allocators like `omniBlockAllocator` maintain internal maps that aren't thread-safe.

#### 2. Optional Interface Support via Type Assertions

```go
func (s *concurrentStore) GetObjectInfo(objId ObjectId) (ObjectInfo, bool) {
    if getter, ok := s.innerStore.(interface{ GetObjectInfo(ObjectId) (ObjectInfo, bool) }); ok {
        // ... acquire per-object lock
        return getter.GetObjectInfo(objId)
    }
    return ObjectInfo{}, false
}
```

This allows wrapping stores that may or may not implement optional interfaces like `GetObjectInfo` or `AllocateRun`.

#### 3. Disk Token Pool Integration

Both `concurrentStore` and `multiStore` support optional disk token pools for rate-limiting concurrent I/O:
- Token acquisition happens in Late* methods (LateReadObj, LateWriteNewObj, WriteToObj)
- NewObj doesn't acquire tokens (no I/O)
- Tokens are released by the Finisher callback

### API

#### Direct Wrapping
```go
ms, err := multistore.NewMultiStore(path, diskTokens)
cs := store.NewConcurrentStoreWrapping(ms, diskTokens)
```

#### Convenience Constructors
```go
// Create new
cs, err := multistore.NewConcurrentMultiStore(path, diskTokens)

// Load existing
cs, err := multistore.LoadConcurrentMultiStore(path, diskTokens)
```

### Tested Examples (preferred)
- End-to-end concurrent multiStore usage, disk tokens, and load/save: [multistore/concurrent_multi_store_test.go](../multistore/concurrent_multi_store_test.go)
- Concurrent wrapper behaviors on any Storer: [store/concurrent_store_test.go](../store/concurrent_store_test.go)
- Treap batch persistence (baseStore path) for contiguous allocation: [yggdrasil/treap/persistent_treap_batch_external_test.go](../yggdrasil/treap/persistent_treap_batch_external_test.go)

### Implementation Files

#### Modified Files
1. **store/concurrent_store.go** (~236 lines)
   - Changed `baseStore` field to `innerStore Storer`
   - Added `NewConcurrentStoreWrapping(innerStore Storer, maxDiskTokens int)`
   - Added global locking to NewObj, PrimeObject, DeleteObj
   - Type assertions for optional interfaces (GetObjectInfo, AllocateRun)

2. **store/concurrent_store_test.go** (~573 lines)
   - Updated tests to check `innerStore` field
   - Added `TestNewConcurrentStoreWrapping` - comprehensive wrapping test

3. **multistore/multi_store.go** (~618 lines)
   - Re-added disk token support after Phase 2 revert
   - Added `NewConcurrentMultiStore()` convenience constructor
   - Added `LoadConcurrentMultiStore()` convenience constructor

#### New Files
1. **multistore/concurrent_multi_store_test.go** (~400 lines)
   - `TestNewConcurrentMultiStore` - tests convenience constructor
   - `TestLoadConcurrentMultiStore` - tests load convenience constructor
   - `TestConcurrentMultiStore` - 20 goroutines × 50 objects concurrent write test
   - `TestConcurrentMultiStoreReadWrite` - concurrent readers/writers test
   - `TestConcurrentMultiStoreAllocateRun` - verifies AllocateRun unsupported (expected)
   - `TestConcurrentMultiStoreGetObjectInfo` - tests GetObjectInfo through wrapper
   - `TestConcurrentMultiStoreDiskTokens` - verifies disk token limiting

2. **docs/PHASE_1_COMPLETE.md** (~320 lines)
   - Comprehensive Phase 1 documentation
   - Usage examples and patterns

3. **docs/PHASE_2_ANALYSIS.md** (~250 lines)
   - Analysis of why multiStore can't wrap baseStore
   - Alternative approaches (shared I/O helpers)

### Test Results

All tests pass:
```
ok  github.com/cbehopkins/bobbob/multistore 3.347s
ok  github.com/cbehopkins/bobbob/store      3.140s
ok  github.com/cbehopkins/bobbob/yggdrasil/vault 30.345s
```

Key test coverage:
- ✅ Concurrent writes (20 goroutines × 50 objects)
- ✅ Concurrent read/write mix
- ✅ Convenience constructor create/load cycle
- ✅ GetObjectInfo delegation through wrapper
- ✅ AllocateRun unsupported (multiStore design decision)
- ✅ Disk token rate limiting

### Architecture Notes

#### Why multiStore Doesn't Implement AllocateRun

The `RunAllocator` interface (providing `AllocateRun` for contiguous allocation) is optional. multiStore intentionally doesn't implement it because:

1. **Size-Based Routing**: OmniBlockAllocator routes objects to different block allocators based on size
2. **No Cross-Allocator Contiguity**: Objects of different sizes go to different allocators, making contiguous allocation across sizes impossible
3. **Batch Persistence**: BatchPersist feature (49-81% speedup) requires AllocateRun, so it's only available with baseStore

This is a design trade-off: multiStore optimizes for space efficiency (size-classed allocation), while baseStore + BatchPersist optimizes for write throughput.

#### Allocation Strategy Conflict (Phase 2 Failure)

We initially attempted to make multiStore wrap baseStore to eliminate code duplication, but discovered a fundamental architectural conflict:

- **baseStore**: Uses BasicAllocator + ObjectMap (ObjectId → offset lookup)
- **multiStore**: Uses OmniBlockAllocator + allocator queries (no central ObjectMap)

Mixing these on the same file causes "object not found" errors because:
1. baseStore allocates via BasicAllocator and records in ObjectMap
2. multiStore tries to query allocators for objects it didn't allocate
3. Allocators don't know about baseStore's objects → not found

This validated the current architecture: baseStore and multiStore are fundamentally different allocation strategies and should remain independent, with concurrentStore providing a common concurrency layer for both.

### Performance Characteristics

#### Locking Overhead
- **Per-Object Locking**: Allows concurrent operations on different objects with minimal contention
- **Global Allocation Lock**: Serializes NewObj/DeleteObj but these are typically fast operations
- **Disk Token Pool**: Optional rate limiting prevents I/O saturation

#### Memory Usage
- Lock map grows with number of unique objects accessed
- Lock map cleanup on DeleteObj prevents unbounded growth
- Disk token pool: constant memory (buffered channel of specified size)

### Future Work (Optional)

#### Phase 2b: Shared I/O Helpers ✅ COMPLETE
**Status: Implemented**

Extracted common I/O operations (~165 lines duplication) into shared helpers in [store/io_helpers.go](../store/io_helpers.go):

**Shared Components:**
- `DiskTokenManager`: Unified disk token pool management for rate-limiting I/O
  - `Acquire()` / `Release()` methods with nil-safe operation
  - Used by both multiStore and concurrentStore
- `CreateSectionReader/Writer`: Unified file section I/O creation
- `CreateFinisherWithToken`: Unified finisher creation (always returns non-nil)
- `WriteZeros`: Unified zero-byte initialization for new objects
- `WriteBatchedSections`: Unified batched write operations

**Benefits Achieved:**
- Eliminated ~165 lines of duplicated I/O code between baseStore and multiStore
- Consistent disk token handling across all store types
- Simplified maintenance - I/O logic centralized in one place
- Consistent API - finishers always non-nil for easier usage
- Better testability - I/O helpers can be tested independently

**Files Modified:**
- Created: [store/io_helpers.go](../store/io_helpers.go) (~97 lines)
- Updated: [store/base_store.go](../store/base_store.go) - uses shared helpers
- Updated: [multistore/multi_store.go](../multistore/multi_store.go) - uses shared helpers
- Updated: [store/store.go](../store/store.go) - nil-safe finisher handling
- Updated: [store/concurrent_store.go](../store/concurrent_store.go) - global lock for LateWriteNewObj allocation

**Key Implementation Details:**
1. All helper functions exported (capitalized) for use across packages
2. DiskTokenManager methods are nil-safe (no-op when manager is nil)
3. CreateFinisherWithToken always returns a function (never nil) for API convenience
4. LateWriteNewObj in concurrentStore uses global lock during allocation to protect allocator state
5. Both baseStore and multiStore use identical I/O patterns through helpers

Completion time: ~1.5 hours
Risk assessment: Low (pure refactoring with comprehensive test coverage)
Result: All 91 store tests + 200+ integration tests passing

## Conclusion

Phase 1 successfully achieves the original goal: **concurrent access to multi-allocator stores** through composable layering.

Key achievements:
- ✅ concurrentStore made generic (wraps any Storer)
- ✅ Convenience constructors for easy usage
- ✅ Comprehensive integration tests
- ✅ Documentation updated
- ✅ All tests passing

The architecture is now:
- **Composable**: Wrap any store with concurrency
- **Flexible**: Optional interfaces supported via type assertions
- **Tested**: Integration tests prove layered approach works
- **Documented**: Clear usage examples and architectural notes

Users can now choose:
- `baseStore` for simplicity and BatchPersist performance
- `multiStore` for space-efficient size-classed allocation
- Wrap either with `concurrentStore` for concurrent access
- Use convenience constructors for common patterns
