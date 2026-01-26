# Store Implementation Comparison (Post-Phase 2b)

Concise differences after shared I/O helpers and generic concurrent wrapper.

## Core Differences
- **baseStore**: Single allocator (delegates location queries to allocator). Uses shared I/O helpers.
- **multiStore**: Size-based allocators (delegates location queries to allocator). Uses shared I/O helpers.
- **concurrentStore**: Per-object locks + optional disk tokens; wraps any `Storer`; global lock for allocations.

## Duplication Status
- ~165 lines of I/O duplication removed via [store/io_helpers.go](../store/io_helpers.go).
- Both stores now share section reader/writer creation, zero init, batched writes, token finisher helpers.

## Why multiStore doesn’t wrap baseStore
- Allocation strategies conflict: baseStore uses single allocator; multiStore uses size-based routing across multiple allocators.
- Both delegate location queries to their allocators; architectures stay independent.

## When to Choose Which
- **baseStore**: Simpler, works with BatchPersist contiguity (RunAllocator path); great for treap batch benchmarks.
- **multiStore**: Space-efficient, size-classed allocation for treap nodes; no AllocateRun, so BatchPersist falls back.
- Wrap either with `concurrentStore` for thread safety and disk token limiting.

## Tested Examples (source of truth)
- Concurrent multi-allocator usage and token limiting: [multistore/concurrent_multi_store_test.go](../multistore/concurrent_multi_store_test.go)
- Concurrent wrapper behaviors: [store/concurrent_store_test.go](../store/concurrent_store_test.go)
- Treap batch persist (baseStore path): [yggdrasil/treap/persistent_treap_batch_external_test.go](../yggdrasil/treap/persistent_treap_batch_external_test.go)
       root allocator

Characteristics:
- All objects in single free space
- No fragmentation optimization
- Simple allocation
```

### multiStore with concurrent wrapper (proposed)
```
Application
    ↓
ConcurrentStore (per-object locks)
    ↓ (delegates all I/O)
MultiStore (allocator routing)
    ├── RootAllocator (large objects)
    └── OmniBlockAllocator (fixed-size treap nodes)
        ├── BlockAllocator[120 bytes] (treap nodes)
        ├── BlockAllocator[160 bytes] (treap+payload)
        └── BlockAllocator[200 bytes] (payload variants)
    ↓
BaseStore (core I/O and ObjectMap)
    ↓
File: [Hdr] [BlockMeta] [Block120] [Block160] [Block200] [ObjMap]
       ^
       Metadata tracking

Characteristics:
- Optimized allocation for treap-heavy workloads
- Reduced fragmentation (block-based)
- Automatic size-based routing
- Better cache locality for similar-sized objects
```

---

## Implementation Complexity Matrix

### Phase 1: Make concurrentStore Generic

```
Effort to implement:  ████░░░░░░ (40%)
Complexity:          ██░░░░░░░░ (20%)
Risk:                ██░░░░░░░░ (20%)
Benefit:             ███░░░░░░░ (30%)
Testing needed:      ████░░░░░░ (40%)

Key changes:
- Change 1 field: baseStore → innerStore
- Update ~8 method implementations
- Add 1 new constructor
- Keep 1 old constructor for backward compat
```

### Phase 2: Refactor multiStore to Wrap baseStore

```
Effort to implement:  ██████░░░░ (60%)
Complexity:          ████░░░░░░ (40%)
Risk:                █████░░░░░ (50%)
Benefit:             █████░░░░░ (50%)
Testing needed:      ███████░░░ (70%)

Key changes:
- Change struct: add baseStore field
- Replace ~200 lines of I/O with delegation
- Update 2 constructors (new + load)
- Update ~8 method implementations
- Keep allocation routing logic
- Test all paths heavily
```

### Phase 3: Convenient Constructors

```
Effort to implement:  ██░░░░░░░░ (20%)
Complexity:          ░░░░░░░░░░ (10%)
Risk:                ░░░░░░░░░░ (10%)
Benefit:             ███░░░░░░░ (30%)
Testing needed:      ██░░░░░░░░ (20%)

Key changes:
- Add 2 convenience constructors
- Wire concurrentStore + multiStore together
- Document layering
```

---

## Before & After Code Metrics

### File Sizes
```
Before:
- store/concurrent_store.go:        208 lines
- multistore/multi_store.go:        627 lines
- Total:                            835 lines

After:
- store/concurrent_store.go:        210 lines (↑ 2, generic constructor)
- multistore/multi_store.go:        480 lines (↓ 147, removed duplication)
- Total:                            690 lines (↓ 145 lines, 17% reduction)
```

### Duplication
```
Before:
- I/O logic duplicated:             ~165 lines
- File management duplicated:       ~30 lines
- Total duplication:                ~195 lines (23% of code)

After:
- No duplication between stores:    0 lines
- Only code that can't be shared:   Allocation routing, metadata
```

### Test Coverage
```
Before:
- store tests:     ~15 files
- multistore tests: ~10 files
- concurrency test: Only in vault, not focused

After:
- store tests:     ~16 files (+ concurrency wrapping tests)
- multistore tests: ~10 files (same, should still pass)
- NEW: concurrent_multi_store_test.go (focused concurrency tests)
```

---

## Migration Guide for Users

### Old Pattern: Separate Stores
```go
// Can do this:
cs := store.NewConcurrentStore(path, 4)      // baseStore + concurrency

// Or this:
ms := multistore.NewMultiStore(path, 0)      // multiStore without concurrency

// But NOT this:
// cms := ??? (no way to get concurrent multiStore)
```

### New Pattern: Composable Stores
```go
// Old patterns still work:
cs := store.NewConcurrentStore(path, 4)      // ✅ Still works (backward compat)
ms := multistore.NewMultiStore(path, 0)      // ✅ Still works (now wraps baseStore)

// New patterns now available:
cms := multistore.NewConcurrentMultiStore(path, 4)  // ✅ New! Concurrent + multi-allocators

// Or manually compose:
base, _ := store.NewBasicStore(path)
ms, _ := store.NewConcurrentStore(base, 4)   // Generic wrapping
```

---

## Risk Assessment Summary

### Highest Risk: Phase 2 (multiStore refactoring)

**Risks:**
1. **Data corruption**: Changing how objects are stored/retrieved
2. **Persistence**: Allocator state serialization differences
3. **Performance**: Extra delegation layer

**Mitigations:**
1. Run all existing multi_store tests first
2. Run vault tests with refactored multiStore
3. Benchmark before/after
4. Incremental refactoring (one method at a time)
5. Clear commit history for easy rollback

### Medium Risk: Phase 1 (concurrentStore generalization)

**Risks:**
1. Subtle type issues with innerStore interface
2. Missing methods in wrapped stores

**Mitigations:**
1. Add interface compliance checks
2. Comprehensive wrapping tests
3. Clear error messages

### Low Risk: Phase 3 (Convenience constructors)

**Risks:**
1. Public API naming confusion
2. Documentation incomplete

**Mitigations:**
1. Clear naming (NewConcurrentMultiStore)
2. Good doc strings
3. Example code in tests

---

## Success Validation

After refactoring, you should be able to:

```go
// Scenario 1: Basic store
s, _ := store.NewBasicStore("data.bin")
s.LateWriteNewObj(100)

// Scenario 2: Concurrent basic
s, _ := store.NewConcurrentStore("data.bin", 0)
s.LateWriteNewObj(100)

// Scenario 3: Multi-allocator
s, _ := multistore.NewMultiStore("data.bin", 0)
s.LateWriteNewObj(100)  // Routed by size

// Scenario 4: Concurrent multi-allocator ← NEW!
s, _ := multistore.NewConcurrentMultiStore("data.bin", 4)
s.LateWriteNewObj(100)  // Thread-safe + size-routed

// All scenarios pass same test suite ✅
```
