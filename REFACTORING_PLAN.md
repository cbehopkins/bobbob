# ObjectMap Elimination Refactoring Plan

## Executive Summary

Your proposal to eliminate the ObjectMap by encoding metadata in the ObjectId hierarchy is sound and addresses the O(n) memory scaling problem. This plan outlines a detailed refactoring strategy that avoids circular dependencies and maintains system integrity throughout.

## Current Architecture

```
BasicAllocator (root)
    ├── Simple sequential allocation
    ├── Returns ObjectId = FileOffset
    └── Tracks free gaps

OmniBlockAllocator (middle)
    ├── Routes allocations by size to BlockAllocators
    ├── Falls back to parent for unhandled sizes
    └── blockMap: Map[size] -> BlockAllocator

BlockAllocator (leaf)
    ├── Allocates blocks of fixed size (N*1024)
    ├── Uses boolean array for slot tracking
    ├── Derived ObjectId from startingObjectId + slot index
    └── All allocated slots use parent allocator

MultiStore (consumer)
    ├── Has both allocators and ObjectMap
    ├── ObjectMap stores ObjectId -> (offset, size)
    └── ObjectMap is the O(n) memory bottleneck
```

## Problem Analysis

**Current Issue**: ObjectMap entries consume ~35-56 bytes per object. For 2.65M objects, this is ~107 MB.

**Root Cause**: ObjectMap is a `map[ObjectId]ObjectInfo` that grows unbounded.

**Key Insight**: The ObjectId already encodes enough information to reverse-lookup the offset and size!

## Proposed Solution: Object Metadata Derivation

Instead of storing metadata in a map, we derive it from the allocator hierarchy:

### ObjectId Encoding Strategy

```
For BlockAllocator-allocated objects:
┌─────────────────────────────────────────────────┐
│ ObjectId = startingObjectId + slot_index        │
│ Where:                                          │
│   - startingObjectId encodes which BlockAllocator
│   - slot_index (0 to blockCount-1) picks the slot
└─────────────────────────────────────────────────┘

Example:
  BlockAllocator for 64-byte blocks, starting at ObjectId=1000, slot 42
  → ObjectId = 1000 + 42 = 1042
```

### Reverse Lookup Process

Given an ObjectId, derive (FileOffset, Size):

1. **Query OmniBlockAllocator**: "Which BlockAllocator created this ObjectId?"
   - OmniBlockAllocator maintains a list of all BlockAllocators
   - Each BlockAllocator knows its startingObjectId range
   - Binary search or linear scan to find the owner

2. **Query BlockAllocator**: "What's the offset and size for this slot?"
   - slot_index = ObjectId - startingObjectId
   - FileOffset = startingFileOffset + (slot_index * blockSize)
   - Size = blockSize
   - Status = allocatedList[slot_index]

3. **Handle unassigned sizes**: If size wasn't found in OmniBlockAllocator
   - Fall back to BasicAllocator
   - BasicAllocator stores free gaps; doesn't track allocated regions
   - Need new responsibility for BasicAllocator

## Refactoring Plan

### Phase 1: Prepare BasicAllocator for Object Tracking

**Goal**: Enable BasicAllocator to support reverse lookups without ObjectMap.

**Challenge**: BasicAllocator currently only tracks free gaps, not allocated regions.

**Solution**: Add allocation tracking to BasicAllocator:

```go
type BasicAllocator struct {
    mu           sync.Mutex
    freeList     GapHeap
    end          int64
    allocations  map[ObjectId]AllocatedRegion  // NEW
}

type AllocatedRegion struct {
    FileOffset FileOffset
    Size       int
}
```

**Rationale**: 
- BasicAllocator is root; it's responsible for its allocations
- Size is typically small (variable-size objects are rare)
- Avoids circular dependency: allocators above don't need to know about this

**Changes**:
1. Add `allocations` field to BasicAllocator
2. Update `Allocate()` to record in allocations
3. Update `Free()` to remove from allocations
4. Add `GetObjectInfo(ObjectId) (FileOffset, int, error)` method
5. Update marshaling to persist allocations

### Phase 2: Enable OmniBlockAllocator Reverse Lookup

**Goal**: Add reverse lookup capability without changing public API.

**Changes to OmniBlockAllocator**:

```go
// New method to support reverse lookups
func (o *omniBlockAllocator) GetObjectInfo(objId ObjectId) (FileOffset, int, error) {
    // Try each BlockAllocator
    for size, allocator := range o.blockMap {
        if allocator.ContainsObjectId(objId) {
            offset, err := allocator.GetFileOffset(objId)
            return offset, size, err
        }
    }
    
    // Fall back to parent (BasicAllocator)
    return o.parent.(interface{ GetObjectInfo(ObjectId) (FileOffset, int, error) }).GetObjectInfo(objId)
}
```

**Changes to BlockAllocator**:

```go
// Check if ObjectId is in this allocator's range
func (a *blockAllocator) ContainsObjectId(objId ObjectId) bool {
    return objId >= a.startingObjectId && 
           objId < a.startingObjectId + ObjectId(a.blockCount)
}

// Get file offset for a specific ObjectId
func (a *blockAllocator) GetFileOffset(objId ObjectId) (FileOffset, error) {
    slotIndex := objId - a.startingObjectId
    if slotIndex < 0 || slotIndex >= ObjectId(len(a.allocatedList)) {
        return 0, errors.New("invalid ObjectId")
    }
    if !a.allocatedList[slotIndex] {
        return 0, errors.New("ObjectId not allocated")
    }
    return a.startingFileOffset + FileOffset(slotIndex) * FileOffset(a.blockSize), nil
}
```

### Phase 3: Update MultiStore to Use Reverse Lookups

**Goal**: Eliminate ObjectMap while maintaining functionality.

**Changes**:

```go
// Before: Store in ObjectMap
func (s *multiStore) NewObj(size int) (store.ObjectId, error) {
    objId, fileOffset, err := s.allocators[1].Allocate(size)
    if err != nil {
        return store.ObjNotAllocated, err
    }
    s.objectMap.Set(objId, store.ObjectInfo{Offset: fileOffset, Size: size})
    return objId, nil
}

// After: No ObjectMap needed
func (s *multiStore) NewObj(size int) (store.ObjectId, error) {
    objId, _, err := s.allocators[1].Allocate(size)
    if err != nil {
        return store.ObjNotAllocated, err
    }
    return objId, nil
}

// New helper to get object info on demand
func (s *multiStore) GetObjectInfo(objId store.ObjectId) (FileOffset, int, error) {
    if omniBased, ok := s.allocators[1].(interface{ GetObjectInfo(ObjectId) (FileOffset, int, error) }); ok {
        return omniBased.GetObjectInfo(objId)
    }
    return 0, 0, errors.New("allocator doesn't support reverse lookup")
}
```

**Changes to methods**:
- `LateReadObj`: Use `GetObjectInfo()` instead of `objectMap.Get()`
- `LateWriteNewObj`: Don't store in ObjectMap
- `WriteToObj`: Use `GetObjectInfo()` instead of `objectMap.Get()`
- `DeleteObj`: Don't delete from ObjectMap
- `WriteBatchedObjs`: Use `GetObjectInfo()` instead of `objectMap.Get()`

**Remove**:
- `objectMap` field
- `objectMap` marshaling/unmarshaling in `Close()`/`LoadMultiStore()`
- `s.objectMap.Set()` calls
- `GetObjectCount()` (moved to allocators if needed)

### Phase 4: Testing & Validation

**Unit Tests to Create**:
1. `TestBasicAllocatorReverseLooku()`
   - Allocate objects
   - Verify GetObjectInfo returns correct offsets/sizes
   - Verify deleted objects return errors

2. `TestBlockAllocatorContainsObjectId()`
   - Test range membership
   - Test boundary conditions

3. `TestOmniBlockAllocatorReverseLooku()`
   - Test lookup across multiple size classes
   - Test fallback to parent
   - Test error cases

4. `TestMultiStoreWithoutObjectMap()`
   - Allocate objects
   - Read/write via reverse lookup
   - Delete and verify inaccessibility

5. `TestMultiStoreMemoryUsageWithouObjectMap()`
   - Same test as before but verify ObjectMap is gone
   - Expect heap usage to drop dramatically

**Modify Existing Tests**:
- Remove assertions about ObjectMap size
- Remove ObjectMap access in test setup
- Update multi_store_memory_test.go to verify memory reduction

## Dependency Analysis

### No Circular Dependencies Risk Because:

1. **BasicAllocator** (root):
   - Adds own allocation tracking
   - No dependency on other allocators
   - ✓ Safe

2. **OmniBlockAllocator** (middle):
   - Already depends on BlockAllocators
   - Needs to support reverse lookup
   - Falls back to parent (BasicAllocator)
   - ✓ Safe (parent already exists)

3. **BlockAllocator** (leaf):
   - Only needs to know its own boundaries
   - No new dependencies
   - ✓ Safe

4. **MultiStore** (consumer):
   - Uses only public allocator interface
   - Calls GetObjectInfo() which may exist
   - ✓ Safe (interface discovery pattern)

## Implementation Order

1. **Phase 1**: Add allocation tracking to BasicAllocator
   - Minimal risk, isolated change
   - Add GetObjectInfo method
   - Update tests

2. **Phase 2**: Add reverse lookup to OmniBlockAllocator
   - Adds new methods, doesn't change existing behavior
   - Add ContainsObjectId, GetFileOffset to BlockAllocator
   - Add GetObjectInfo to OmniBlockAllocator
   - Update tests

3. **Phase 3**: Update MultiStore
   - Remove ObjectMap field
   - Update all read/write methods to use reverse lookup
   - Remove marshaling of ObjectMap
   - Delete test assertions about ObjectMap

4. **Phase 4**: Run full test suite
   - Ensure no regressions
   - Verify memory improvement
   - Profile before/after

## Expected Benefits

- **Memory**: Eliminate ~100+ MB for large datasets (2.65M objects → 0 ObjectMap overhead)
- **Consistency**: Metadata is derived, not cached → no sync issues
- **Scalability**: O(1) lookup for OmniBlockAllocator after binary search
- **Simplicity**: Fewer moving parts, clearer ownership

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Lookup performance regression | Profile before/after; binary search optimization available |
| Deleted object queries | Return clear error from allocatedList check |
| Marshaling complexity | Already handling it in allocators |
| Test coverage gaps | Add explicit reverse-lookup tests before removing ObjectMap |

## Questions for Clarification

1. Should BasicAllocator's allocation tracking persist to disk or just track live allocations?
2. How should we handle ObjectIds for objects allocated by parent Allocator in OmniBlockAllocator's fallback case?
3. Should GetObjectInfo return an error for deleted/never-allocated IDs, or should allocation state be tracked separately?
