# Store Architecture Analysis: Layering Concurrency and Multi-Allocators

## Current State

### concurrentStore (store/concurrent_store.go)
- **Wraps**: `baseStore` (basic allocator only)
- **Responsibility**: Per-object read/write locking
- **Features**: 
  - RWMutex per ObjectId (lazy-created)
  - Disk token pool for I/O rate limiting
  - Delegates allocation to baseStore
  - Simple lock ordering for batched writes

### multiStore (multistore/multi_store.go)
- **Independent implementation** (no baseStore dependency)
- **Responsibility**: Multi-allocator strategy
- **Features**:
  - Root allocator (basic) for large/variable objects
  - Omni block allocator for fixed-size treap nodes
  - Automatic size-based routing
  - Disk token pool for I/O rate limiting
  - File I/O management (doesn't reuse baseStore)

### Problem: Duplicate Implementation
- **multiStore reimplements** file management, I/O, finishers that baseStore already handles
- **No concurrency** in multiStore (each caller must add external synchronization)
- **No composition**: Can't do "multiStore + concurrency" without rewriting

## Proposed Layered Architecture

```
┌─────────────────────────────────────────────┐
│         Application Layer                   │
│    (Treaps, Vaults, Collections)            │
└──────────────┬──────────────────────────────┘
               │
┌──────────────▼──────────────────────────────┐
│   ConcurrentStore (Locking Layer)           │
│  - Per-object RWMutex                       │
│  - Lock ordering for batched ops            │
│  - Disk token management                    │
└──────────────┬──────────────────────────────┘
               │ wraps Storer
┌──────────────▼──────────────────────────────┐
│   MultiStore (Allocation Strategy Layer)    │
│  - Root allocator                           │
│  - Block allocator routing                  │
│  - Size-aware allocation                    │
│  - RunAllocator support                     │
└──────────────┬──────────────────────────────┘
               │ wraps Storer
┌──────────────▼──────────────────────────────┐
│   BaseStore (Core I/O Layer)                │
│  - File handle management                   │
│  - Object offset tracking                   │
│  - Basic allocation (no concurrency)        │
│  - Finisher/resource cleanup                │
└─────────────────────────────────────────────┘
```

## Key Changes Required

### 1. Refactor multiStore to Wrap baseStore

**Current:**
```go
type multiStore struct {
    filePath   string
    file       *os.File              // Reimplemented file management
    allocators []allocator.Allocator
    diskTokens chan struct{}
}
```

**Proposed:**
```go
type multiStore struct {
    baseStore  *baseStore            // Reuse baseStore
    allocators []allocator.Allocator // Keep multi-allocator routing
    diskTokens chan struct{}          // Disk token pool
}
```

**Benefits:**
- Eliminates duplicate file I/O code (~150 lines)
- Shares ObjectMap tracking with baseStore
- Inherits finisher management from baseStore
- Same lifecycle guarantees as baseStore

### 2. Update multiStore Constructors

**NewMultiStore Changes:**
```go
// Create baseStore first (file management)
baseStore, err := NewBasicStore(filePath)

// Replace baseStore's basic allocator with omni allocator
// Keep root allocator for large objects

ms := &multiStore{
    baseStore: baseStore,
    allocators: [...],  // If still needed for routing
}
```

**Questions to resolve:**
- Does baseStore's allocator field need to be replaceable?
- Should we inject allocators into baseStore at construction?
- Or keep allocators internal to multiStore and proxy allocation calls?

### 3. Enable Concurrent Access to multiStore

**Create NewConcurrentMultiStore:**
```go
func NewConcurrentMultiStore(filePath string, maxDiskTokens int) (store.Storer, error) {
    // Create multiStore with multiple allocators
    ms, err := NewMultiStore(filePath, 0)  // No token limiting (concurrent adds them)
    if err != nil {
        return nil, err
    }
    
    // Wrap with concurrentStore for locking
    return NewConcurrentStore_Wrapping(ms, maxDiskTokens)
}
```

**Or make concurrentStore allocator-agnostic:**
```go
func NewConcurrentStore(storer store.Storer, maxDiskTokens int) (store.Storer, error) {
    // Works with any Storer implementation
    return &concurrentStore{
        innerStore: storer,
        // ... locking logic ...
    }
}
```

### 4. Method Delegation Pattern

Both multiStore and concurrentStore should delegate unimplemented methods:

```go
// multiStore delegates to baseStore where not overridden
func (s *multiStore) GetObjectInfo(objId ObjectId) (ObjectInfo, bool) {
    return s.baseStore.GetObjectInfo(objId)
}

func (s *multiStore) Sync() error {
    return s.baseStore.Sync()
}

// concurrentStore delegates to inner store
func (s *concurrentStore) Sync() error {
    return s.innerStore.Sync()
}
```

## Implementation Path

### Phase 1: Make concurrentStore Inner-Store Agnostic
1. Change concurrentStore to accept any `store.Storer` instead of `*baseStore`
2. Add new constructor: `func NewConcurrentStore(inner store.Storer, maxDiskTokens int)`
3. Keep old constructor for backward compatibility: wraps `NewBasicStore`
4. Update tests to use both patterns

### Phase 2: Refactor multiStore to Wrap baseStore
1. Extract file management to baseStore (already done)
2. Inject allocators into multiStore at construction
3. Replace multiStore's file handling with baseStore delegation
4. Verify all tests pass

### Phase 3: Enable Concurrent multiStore
1. Create convenience constructor for concurrent multi-allocator store
2. Document layering approach
3. Update vault and collections code to use layered approach

## Design Decisions

### Should multiStore Know About Allocators?

**Option A: Keep allocators in multiStore**
- multiStore routes by size to appropriate allocator
- baseStore has single allocator (root)
- Pro: Clear separation of concerns
- Con: multiStore still reimplements some logic

**Option B: All allocators in baseStore**
- baseStore supports allocator lists
- multiStore provides routing logic via wrapper
- Pro: True layering
- Con: baseStore becomes more complex

**Recommendation**: Option A (incremental). Keep allocators in multiStore initially, focus on reducing file I/O duplication first.

### Disk Token Pool Placement

**Current**: Each layer has its own token pool
**Better**: Single token pool at innermost layer (baseStore)

```go
// All I/O goes through baseStore file operations
// One token pool manages all disk access
type baseStore struct {
    file       *os.File
    diskTokens chan struct{}  // Moved here
    // ...
}

// multiStore just delegates to baseStore's token management
// concurrentStore just delegates to inner store's token management
```

## Concrete Steps for Implementation

### Step 1: Update concurrentStore Constructor Signature
```go
// Old: wraps baseStore directly
func NewConcurrentStore(filePath string, maxDiskTokens int) (*concurrentStore, error)

// New: wraps any Storer
type concurrentStore struct {
    innerStore store.Storer  // Any Storer, not just baseStore
    lock       sync.RWMutex
    lockMap    map[ObjectId]*sync.RWMutex
    diskTokens chan struct{} // Optional, if not delegating
}
```

**Impact**: Minimal - just change `baseStore *baseStore` to `innerStore store.Storer`

### Step 2: Extract multiStore File I/O
Copy less code from multiStore's:
- LateReadObj
- LateWriteNewObj
- WriteToObj
- WriteBatchedObjs

Use baseStore equivalents instead.

### Step 3: Create Layered Factory
```go
// Convenience constructor for full-featured concurrent store
func NewConcurrentMultiStore(path string, maxDiskTokens int) (store.Storer, error) {
    ms, err := NewMultiStore(path, 0)  // No inner token limit
    if err != nil {
        return nil, err
    }
    return NewConcurrentStore(ms, maxDiskTokens)
}
```

## Testing Strategy

1. **Existing tests**: Verify they still pass with refactored implementations
2. **Composition tests**: New tests for concurrentStore wrapping multiStore
3. **Concurrency tests**: Add tests for concurrent multiStore access (treap batching, etc.)
4. **Layer independence**: Verify each layer can be swapped (baseStore, multiStore, concurrentStore)

## Files to Modify

| File | Change | Complexity |
|------|--------|-----------|
| store/concurrent_store.go | Change inner to `store.Storer` | Low |
| multistore/multi_store.go | Wrap baseStore, delegate I/O | Medium |
| store/base_store.go | Add allocator configuration? | Low-Medium |
| Tests | Verify layering works | Medium |

## Migration Path for Users

```go
// Before:
cs, _ := store.NewConcurrentStore(path, 4)
ms, _ := collections.NewMultiStore(path, 4)  // No concurrency

// After - Option 1: Use layered factory
cms, _ := collections.NewConcurrentMultiStore(path, 4)

// After - Option 2: Manual layering (more explicit)
baseMs, _ := collections.NewMultiStore(path, 0)
cms, _ := store.NewConcurrentStore(baseMs, 4)

// Backward compat: Still works
cs, _ := store.NewConcurrentStore(path, 4)  // Wraps BasicStore
```

## Summary

The ideal architecture is:
1. **baseStore**: File I/O, basic allocation, ObjectMap tracking
2. **multiStore**: Multi-allocator routing, wraps baseStore
3. **concurrentStore**: Locking layer, wraps any Storer (baseStore, multiStore, etc.)

This enables:
- ✅ Concurrent access to multiStore
- ✅ Code reuse (no duplicate file I/O)
- ✅ Flexible composition
- ✅ Clear separation of concerns
- ✅ Backward compatibility
