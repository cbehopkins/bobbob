# Allocator Architecture: A Guide for New Developers

## Welcome!

This document explains how `bobbob` manages objects in persistent storage. If you're new to the codebase, start here to understand:
- **What problem the allocators solve**
- **How objects are tracked and located**
- **Why the design is organized this way**

Don't worry if you don't understand everything on first read. This is meant to be explored gradually.

## The Core Problem

Imagine you're building a database. You need to:

1. **Store objects** in a file on disk
2. **Keep track of where** each object is stored
3. **Quickly find** an object by its ID without searching the entire file
4. **Allocate and free** space as objects are created and deleted

The old approach used a single big `ObjectMap`:
```go
objectMap := map[ObjectId]AllocatedRegion{
    ObjectId(1) → {FileOffset: 0, Size: 1024},
    ObjectId(2) → {FileOffset: 1024, Size: 512},
    ObjectId(3) → {FileOffset: 1536, Size: 256},
}
```

This works, but has a problem: **the map itself uses a lot of memory** (48 bytes per entry). With millions of objects, the map becomes a memory bottleneck!

## The Solution: The Allocator Hierarchy

Instead of one big map, we use specialized allocators:

```
┌─────────────────────────────────────────────────────────┐
│              Your Code (MultiStore)                     │
│         "Where is object ID 42 stored?"                 │
└─────────────────────────────────────────────────────────┘
                            ↓
                    (ask allocators)
                            ↓
┌─────────────────────────────────────────────────────────┐
│    OmniBlockAllocator (smart router)                    │
│  "Is it a 64-byte object? 256-byte? Something else?"   │
└─────────────────────────────────────────────────────────┘
          ↙                    ↓                    ↘
  64-byte objects    256-byte objects      Irregular sizes
         ↓                    ↓                    ↓
   BlockAllocator      BlockAllocator       BasicAllocator
   (calculates)        (calculates)          (looks up)
```

**Key insight**: Most objects are fixed-size blocks. For those, we can **calculate the location** without storing anything. For irregular sizes, only then do we use the map.

This reduces memory overhead from **48 bytes per entry** to **~1 byte per entry** (when using BlockAllocators)!

## Three Types of Allocators

### 1. BlockAllocator - For Fixed-Size Objects

Think of it like a parking lot with numbered spaces. All spaces are the same size.

```go
type blockAllocator struct {
    blockSize         int        // All blocks are this size (e.g., 64 bytes)
    blockCount        int        // How many blocks available
    startingFileOffset int64     // Where the first block starts in the file
    startingObjectId  ObjectId   // ID of the first block
    allocatedList     []bool     // Which slots are occupied
}
```

**How it works**:
- When you ask "where is object ID 1005?", it calculates:
  ```
  Slot number = ObjectId - startingObjectId  // e.g., 1005 - 1000 = 5
  FileOffset = startingFileOffset + (slot * blockSize)
  ```
- This is **O(1) arithmetic** - no map lookup needed!

**Memory cost**: One boolean per slot (~1 byte). For 10,000 blocks, that's just 10 KB!

### 2. BasicAllocator - For Variable-Size Objects

This is the traditional allocator for objects of any size. It keeps a map, but only for objects that don't fit the block sizes.

```go
type BasicAllocator struct {
    allocations map[ObjectId]AllocatedRegion  // Only irregular sizes
    freeList    GapHeap                       // Track free space
}
```

**Memory cost**: ~40-50 bytes per entry, but only for non-block-aligned objects. In most cases, very few objects end up here.

### 3. OmniBlockAllocator - The Smart Router

This allocator says: "Let me route you to the right place!"

```go
type omniBlockAllocator struct {
    blockMap   map[int]*blockAllocator  // e.g., 64→BlockAllocator, 256→BlockAllocator
    blockCount int
    parent     Allocator                // Falls back to BasicAllocator
}
```

**How it works**:
```
User asks: "Where is object ID 5042?"
OmniBlockAllocator checks:
  - "Do I have a BlockAllocator for size 64?" → YES
  - "Is ID 5042 in that range?" → YES
  - "Then it's at this file offset:" (calculates)

If the ID isn't in any BlockAllocator:
  - Falls back to parent (BasicAllocator)
  - BasicAllocator.GetObjectInfo(ID) → looks in map
```

## How It All Works Together

### Example: Allocating an Object

```
User stores 50,000 64-byte objects

1. OmniBlockAllocator.Allocate(size=64)
2. → "I have a BlockAllocator for 64-byte blocks!"
3. → BlockAllocator finds a free slot
4. → Returns ObjectId = startingObjectId + slotNumber
5. → Returns FileOffset = calculated from slot number
6. User stores the ObjectId
```

### Example: Finding an Object Later

```
User says: "I need object ID 5042"

1. MultiStore calls: getObjectInfo(ObjectId=5042)
2. OmniBlockAllocator.GetObjectInfo(5042)
3. → "Is this in my 64-byte BlockAllocator?"
4. → YES! "The offset is: startingOffset + ((5042 - startingId) * 64)"
5. File is read at that offset
6. Object found!
```

If the ID was NOT in any BlockAllocator:
```
7. → Falls back to: BasicAllocator.GetObjectInfo(5042)
8. → Looks in allocations map: allocations[5042] → offset
9. Returns that offset
```

## Memory Overhead Comparison

### Old Design (Single ObjectMap)

For 100,000 objects:
```
ObjectMap:
  - Each entry: ~48 bytes (including map overhead)
  - 100,000 × 48 = 4.8 MB  ← This is the bottleneck!
```

### New Design (Allocator Hierarchy)

For 100,000 objects (assuming 90% are 64-byte blocks):
```
OmniBlockAllocator for 64-byte blocks:
  - 90,000 objects in BlockAllocator
  - 90,000 × 1 byte = 90 KB  ← Tiny!

BasicAllocator for the remaining 10%:
  - 10,000 × ~40 bytes = 400 KB

Total: ~490 KB vs 4.8 MB = **98% less memory!**
```

## Why This Design?

1. **Memory Efficiency**: Block-allocated objects cost almost nothing to track
2. **Speed**: BlockAllocators use arithmetic, not hash tables (faster)
3. **Simplicity**: No need to maintain a separate ObjectMap
4. **Scalability**: Can handle millions of objects without memory issues
5. **Cache Friendly**: Array-based tracking (allocatedList) has better cache locality

## Files to Explore

After reading this, look at:

- **[store/basic_allocator.go](store/basic_allocator.go)** - BasicAllocator implementation
- **[store/block_allocator.go](store/block_allocator.go)** - BlockAllocator & OmniBlockAllocator
- **[store/allocator_memory_test.go](store/allocator_memory_test.go)** - See the memory savings in action
- **[multistore/multi_store.go](multistore/multi_store.go)** - How MultiStore uses allocators

## Key Concepts

| Term | Meaning |
|------|---------|
| **ObjectId** | Unique identifier for an object |
| **FileOffset** | Where in the file the object is stored |
| **BlockAllocator** | Allocator for fixed-size blocks (fast, memory-efficient) |
| **BasicAllocator** | Allocator for variable-size objects (flexible, uses map) |
| **OmniBlockAllocator** | Router that delegates to the right allocator |
| **GetObjectInfo** | "Tell me where this object is stored" |

## Performance Characteristics

| Operation | Time | Why |
|-----------|------|-----|
| Find 64-byte object | O(1) arithmetic | Just calculate: offset = base + (id × size) |
| Find variable-size object | O(1) hash lookup | BasicAllocator map lookup |
| Allocate block | O(1) | Find first free slot in boolean array |
| Free block | O(1) | Mark slot as free in boolean array |

No matter how many objects you have, these operations stay fast!

## Common Questions

**Q: Why not just use one giant BlockAllocator for all sizes?**
A: Some objects have irregular sizes (not 64, 256, etc.). We'd waste space padding them. BasicAllocator handles those efficiently.

**Q: Can I customize block sizes?**
A: Yes! When creating OmniBlockAllocator, you specify which sizes to optimize for:
```go
omni := NewOmniBlockAllocator([]int{64, 256, 1024}, blockCount, parent)
```

**Q: What if I allocate a 100-byte object with OmniBlockAllocator for [64, 256, ...]?**
A: It routes to the next larger block (256), or to BasicAllocator if no block fits.

**Q: Can ObjectId change?**
A: No. Once assigned, ObjectIds are stable. They encode the allocator type and slot number.

## Next Steps

1. Read the [code](store/block_allocator.go) - comments explain implementation details
2. Run the tests - see how different sizes are allocated
3. Check [store/allocator_memory_test.go](store/allocator_memory_test.go) - watch the memory savings happen
4. Explore [multistore/multi_store.go](multistore/multi_store.go) - see it in real use

## Unified AllocatorIndex + LRU

To remove duplication and support disk-searchable lookups, the system uses a unified index called AllocatorIndex.

- Purpose: One index for both variable-sized objects and fixed-size ranges.
- Persistence: Binary format with magic "ALIX1" followed by counts, then
  - Ranges segment: entries of `[StartObjectId, EndObjectId, BlockSize, StartingFileOffset]`
  - Objects segment: entries sorted by ObjectId as `[ObjectId, FileOffset, Size]`
- Instances:
  - BasicAllocator owns an index instance primarily for per-object entries.
  - OmniBlockAllocator owns an index instance primarily for range entries.
- Lookup path:
  1) Check ranges for arithmetic offset (fast path for blocks)
  2) Check in-memory object table for explicit entries
  3) Check a small LRU cache of recent on-disk lookups
  4) Binary search the objects segment on disk; insert result into the LRU
- LRU details:
  - Capacity: 4096 recent object entries (ObjectId → FileOffset, Size)
  - Updated on Add/Delete to stay coherent
  - Reduces repeated disk reads during hotspot access patterns

Removed legacy component:
- The previous `ObjectIdLookupCache` (range-only, in-memory) is removed. Its responsibilities are covered by AllocatorIndex ranges, with `AllocatorRange` now defined alongside the index.

Where to look in code:
- AllocatorIndex: [store/allocator/allocator_index.go](store/allocator/allocator_index.go)
- Omni allocator integration: [store/allocator/block_allocators.go](store/allocator/block_allocators.go)

## Monitoring and Debugging Allocations

The allocator provides callback hooks for monitoring allocation patterns, useful for testing, profiling, and understanding system behavior.

### Setting Up Allocation Callbacks

Both `BasicAllocator` and `OmniBlockAllocator` support allocation callbacks via `SetOnAllocate`:

```go
// Configure callback on BasicAllocator
basicAlloc.SetOnAllocate(func(objId allocator.ObjectId, offset allocator.FileOffset, size int) {
    log.Printf("BasicAllocator: allocated objId=%d at offset=%d, size=%d", objId, offset, size)
})

// Configure callback on OmniBlockAllocator
omniAlloc.SetOnAllocate(func(objId allocator.ObjectId, offset allocator.FileOffset, size int) {
    log.Printf("OmniBlockAllocator: allocated objId=%d at offset=%d, size=%d", objId, offset, size)
})
```

### Accessing the Parent Allocator

When you have an `OmniBlockAllocator`, you can access and configure its parent `BasicAllocator`:

```go
// Create allocators
basicAlloc, _ := allocator.NewBasicAllocator(dataFile)
omniAlloc, _ := allocator.NewOmniBlockAllocator([]int{}, 10000, basicAlloc)

// Access parent and configure its callback
if parent := omniAlloc.Parent(); parent != nil {
    if ba, ok := parent.(*allocator.BasicAllocator); ok {
        ba.SetOnAllocate(func(objId, offset, size) {
            log.Printf("Parent BasicAllocator called: objId=%d, offset=%d, size=%d", 
                objId, offset, size)
        })
    }
}
```

### Understanding Allocation Flow

With callbacks configured on both levels, you can observe:

1. **Preallocation Phase**: When `OmniBlockAllocator` is created, it calls the parent's `Allocate()` for each block size range (7 times for default sizes: 64, 128, 256, 512, 1024, 2048, 4096 bytes). These show up in the parent callback.

2. **User Allocations**:
   - Small/medium objects (≤4096 bytes): Handled by `OmniBlockAllocator`, show up in its callback
   - Large objects (>4096 bytes): Fall through to parent `BasicAllocator`, show up in parent callback

3. **Allocation Routing**: Most user allocations are caught by block allocators, so the parent callback is rarely invoked for user data after preallocation.

### Example Use Cases

**Testing and Validation**:
```go
var allocCount int
omniAlloc.SetOnAllocate(func(objId, offset, size) {
    allocCount++
})
// ... run test ...
if allocCount != expectedCount {
    t.Errorf("Expected %d allocations, got %d", expectedCount, allocCount)
}
```

**Performance Profiling**:
```go
var (
    mu sync.Mutex
    sizeHistogram = make(map[int]int)
)
omniAlloc.SetOnAllocate(func(objId, offset, size) {
    mu.Lock()
    sizeHistogram[size]++
    mu.Unlock()
})
// Analyze which sizes are most common to optimize block sizes
```

**Debugging Allocation Patterns**:
```go
basicAlloc.SetOnAllocate(func(objId, offset, size) {
    if size > 10000 {
        log.Printf("WARNING: Large allocation %d bytes at offset %d", size, offset)
    }
})
```

### External Package Access

These features are accessible from outside the allocator package. See [store/allocator_callback_check.go](../../store/allocator_callback_check.go) for a working example of external configuration.
