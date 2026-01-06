# Batch Persistence for Treaps

## Overview

`BatchPersist()` provides optimized bulk persistence for treap nodes by leveraging contiguous disk allocation and batched writes, significantly improving performance over the standard `Persist()` method.

## Performance

Benchmarks show BatchPersist offers substantial speedups:

```
BenchmarkPersistentTreapPersist/count=50        173µs → 89µs  (49% faster)
BenchmarkPersistentTreapPersist/count=100       567µs → 107µs (81% faster)
BenchmarkPersistentTreapPersist/count=500      2028µs → 900µs (56% faster)
```

## When to Use

**Use BatchPersist when:**
- Persisting a freshly-built treap with uniform node sizes
- Working with stores that support run allocation (multiStore, block allocators)
- Performance is critical and nodes haven't been persisted yet

**Use standard Persist when:**
- Incrementally updating an existing persisted treap
- Node sizes vary (BatchPersist will auto-fallback)
- Using stores without run allocation support (BatchPersist will auto-fallback)

## How It Works

1. **Post-order collection**: Gathers all nodes ensuring children precede parents
2. **Run allocation**: Requests contiguous ObjectIds/offsets from allocator
3. **Contiguity verification**: Checks allocated offsets are actually adjacent
4. **Batch marshal**: Serializes all nodes with synchronized child ObjectIds
5. **Single write**: Calls `WriteBatchedObjs` to write all data in one operation
6. **Auto-fallback**: Falls back to `Persist()` if any step fails

## Example

```go
// Build treap
pt := treap.NewPersistentTreap[IntKey](IntLess, keyTemplate, multiStore)
for i := 0; i < 100; i++ {
    key := &IntKey{Value: i}
    pt.Insert(key)
}

// Batch persist (faster than pt.Persist())
if err := pt.BatchPersist(); err != nil {
    return err
}

// Get root for later reload
rootId, _ := pt.Root().(PersistentTreapNodeInterface).ObjectId()
```

## Implementation Details

### PersistentTreap.BatchPersist()
- Located in [yggdrasil/treap/persistent_treap.go](../yggdrasil/treap/persistent_treap.go#L911-L990)
- Requires `store.RunAllocator` interface
- Verifies offsets from `AllocateRun()` are contiguous
- Falls back to `Persist()` on any error

### PersistentPayloadTreap.BatchPersist()
- Located in [yggdrasil/treap/persistent_payload.go](../yggdrasil/treap/persistent_payload.go#L725-L852)
- Checks all nodes have uniform size before attempting batch
- Same fallback strategy as base implementation

### Store Support

**Supports run allocation:**
- `multiStore` (block allocators for fixed-size treap nodes)
- `blockAllocator`, `multiBlockAllocator`, `omniBlockAllocator`

**Does not support run allocation:**
- `baseStore` (basic allocator)
- `concurrentStore` (wraps baseStore, inherits limitations)

## Disk Token Pool

Both `concurrentStore` and `multiStore` accept optional `maxDiskTokens` parameter to limit concurrent disk I/O:

```go
// Limit to 4 concurrent disk operations
ms, err := multistore.NewMultiStore(path, 4)

// Unlimited concurrent disk operations
ms, err := multistore.NewMultiStore(path, 0)
```

Disk tokens apply to all Late* methods (`LateReadObj`, `LateWriteNewObj`, `WriteToObj`) and are automatically managed by finishers.

## Limitations

- **Contiguity requirement**: All objects must be allocated at consecutive offsets
- **Uniform size requirement** (payload treaps): All nodes must have identical size
- **Best for bulk operations**: Designed for initial persistence, not incremental updates
- **No partial success**: Either all nodes persist or operation falls back entirely

## Future Enhancements

- Grouped batching: detect and batch contiguous sub-ranges when full contiguity unavailable
- Metadata hints: pass known offsets to `WriteBatchedObjs` to avoid redundant lookups
- Adaptive strategy: automatically choose Persist vs BatchPersist based on node state
