package allocator

import (
    "container/heap"
    "errors"
    "os"
    "testing"
)

// helper to create a basic allocator with a temp file
func newTestBasicAllocator(t *testing.T) (*BasicAllocator, *os.File) {
    t.Helper()
    file, err := os.CreateTemp("", "alloc_pool_test_*.dat")
    if err != nil {
        t.Fatalf("temp file: %v", err)
    }
    t.Cleanup(func() { _ = os.Remove(file.Name()) })
    alloc, err := NewBasicAllocator(file)
    if err != nil {
        file.Close()
        t.Fatalf("NewBasicAllocator: %v", err)
    }
    return alloc, file
}

func TestAllocatorRefEnsureLoadedErrors(t *testing.T) {
    // Unpersisted ref should error
    ref := &allocatorRef{fileOff: 0, blockSize: 64, blockCount: 4}
    if err := ref.ensureLoaded(); err == nil {
        t.Fatalf("expected error for unpersisted ref")
    }

    // Missing file handle should error
    ref = &allocatorRef{fileOff: 128, blockSize: 64, blockCount: 4, file: nil}
    if err := ref.ensureLoaded(); err == nil {
        t.Fatalf("expected error for ref without file")
    }
}

func TestAllocatorSliceUnmarshalInvalidData(t *testing.T) {
    slice := allocatorSlice{}
    if _, err := slice.Unmarshal([]byte{0x00, 0x01}, 1024); err == nil {
        t.Fatalf("expected error on truncated data")
    }
}

func TestAllocatorPoolFreeFromPoolNotFound(t *testing.T) {
    parent, file := newTestBasicAllocator(t)
    pool := NewAllocatorPool(64, 4, parent, file)

    if err := pool.freeFromPool(999, 64, nil); err == nil {
        t.Fatalf("expected error when freeing non-existent block")
    }
}

func TestAllocatorPoolFreeMovesFullToAvailable(t *testing.T) {
    parent, file := newTestBasicAllocator(t)
    pool := NewAllocatorPool(8, 1, parent, file)

    // First allocation creates an allocator in available
    _, firstOffset, _, err := pool.Allocate()
    if err != nil {
        t.Fatalf("Allocate: %v", err)
    }

    // Second allocation exhausts first allocator and moves it to full, provisioning a new one
    if _, _, _, err := pool.Allocate(); err != nil {
        t.Fatalf("second Allocate: %v", err)
    }
    if len(pool.full) != 1 {
        t.Fatalf("expected one full allocator, got %d", len(pool.full))
    }

    // Free the first block; allocator should move back to available and requestedSize cleared
    if err := pool.freeFromPool(firstOffset, 8, nil); err != nil {
        t.Fatalf("freeFromPool: %v", err)
    }
    if len(pool.full) != 0 {
        t.Fatalf("expected full to be empty after free")
    }
    if len(pool.available) < 2 {
        t.Fatalf("expected freed allocator returned to available")
    }
    var freedRef *allocatorRef
    for _, r := range pool.available {
        if r != nil && r.allocator != nil && r.allocator.startingFileOffset == firstOffset {
            freedRef = r
            break
        }
    }
    if freedRef == nil {
        t.Fatalf("freed allocator not found in available list")
    }
    if err := freedRef.ensureLoaded(); err != nil {
        t.Fatalf("ensureLoaded: %v", err)
    }
    if got, ok := freedRef.allocator.requestedSize(freedRef.allocator.startingObjectId); ok && got != 0 {
        t.Fatalf("expected requestedSize cleared, got %d", got)
    }
}

func TestAllocatorPoolAllocateRunPartial(t *testing.T) {
    parent, file := newTestBasicAllocator(t)
    pool := NewAllocatorPool(8, 2, parent, file)

    objIds, offsets, newRef, err := pool.AllocateRun(3)
    if err != nil {
        t.Fatalf("AllocateRun: %v", err)
    }
    if len(objIds) != 2 || len(offsets) != 2 {
        t.Fatalf("expected partial run of 2, got %d", len(objIds))
    }
    if newRef == nil {
        t.Fatalf("expected new allocator ref to be returned")
    }
}

func TestSetRequestedSizeNoAllocators(t *testing.T) {
    parent, file := newTestBasicAllocator(t)
    pool := NewAllocatorPool(64, 4, parent, file)

    // Should be a no-op and not panic
    pool.SetRequestedSize(123, 10)
}

func TestBlockAllocatorAllAllocated(t *testing.T) {
    alloc := NewBlockAllocator(64, 2, 0, 0, nil)
    if _, _, err := alloc.Allocate(64); err != nil {
        t.Fatalf("first allocate: %v", err)
    }
    if _, _, err := alloc.Allocate(64); err != nil {
        t.Fatalf("second allocate: %v", err)
    }
    if _, _, err := alloc.Allocate(64); !errors.Is(err, AllAllocated) {
        t.Fatalf("expected AllAllocated, got %v", err)
    }
}

func TestGapHeapOrdering(t *testing.T) {
    h := &gapHeap{}
    heap.Init(h)
    heap.Push(h, Gap{Start: 0, End: 50})  // size 50
    heap.Push(h, Gap{Start: 0, End: 20})  // size 20 (smallest)
    heap.Push(h, Gap{Start: 0, End: 30})  // size 30

    gap := heap.Pop(h).(Gap)
    if gap.End-gap.Start != 20 {
        t.Fatalf("expected smallest gap size 20, got %d", gap.End-gap.Start)
    }
}
