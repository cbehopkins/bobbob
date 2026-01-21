package allocator

import (
	"container/heap"
	"os"
	"path/filepath"
	"testing"
)

// Helpers
func newTempBasicAllocator(t *testing.T) (*BasicAllocator, *os.File) {
	t.Helper()
	f, err := os.CreateTemp("", "alloc_cov_*.dat")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(f.Name()) })
	alloc, err := NewBasicAllocator(f)
	if err != nil {
		f.Close()
		t.Fatalf("NewBasicAllocator: %v", err)
	}
	return alloc, f
}

// AllocationStore coverage (in-memory)
func TestInMemoryAllocationStoreCRUD(t *testing.T) {
	s := NewInMemoryAllocationStore()

	if err := s.Add(1, 10, 5); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if off, sz, ok, err := s.Get(1); err != nil || !ok || off != 10 || sz != 5 {
		t.Fatalf("Get mismatch: off=%d sz=%d ok=%v err=%v", off, sz, ok, err)
	}

	if err := s.Add(2, 20, 3); err != nil {
		t.Fatalf("Add second: %v", err)
	}
	if err := s.Add(3, 30, 7); err != nil {
		t.Fatalf("Add third: %v", err)
	}

	collected := make(map[ObjectId]AllocationRecord)
	_ = s.IterateAll(func(id ObjectId, off FileOffset, sz int) bool {
		collected[id] = AllocationRecord{ID: id, Offset: off, Size: sz}
		return true
	})
	if len(collected) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(collected))
	}

	if err := s.Delete(2); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	replace := []AllocationRecord{{ID: 5, Offset: 50, Size: 9}}
	if err := s.ReplaceAll(replace); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	if _, _, ok, _ := s.Get(1); ok {
		t.Fatalf("expected old entry removed")
	}
	if off, sz, ok, _ := s.Get(5); !ok || off != 50 || sz != 9 {
		t.Fatalf("replace entry missing")
	}
}

// AllocationStore coverage (file-backed persistence)
func TestFileAllocationStorePersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "alloc_file_store.bin")

	s := NewFileAllocationStore(path)
	if err := s.Add(1, 100, 4); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Reload via ensureLoaded path
	s2 := NewFileAllocationStore(path)
	if err := s2.ensureLoaded(); err != nil {
		t.Fatalf("ensureLoaded: %v", err)
	}
	if off, sz, ok, err := s2.Get(1); err != nil || !ok || off != 100 || sz != 4 {
		t.Fatalf("persisted read mismatch: off=%d sz=%d ok=%v err=%v", off, sz, ok, err)
	}
}

// allocatorRef methods
func TestAllocatorRefOps(t *testing.T) {
	ba := NewBlockAllocator(8, 3, 0, 0, nil)
	ref := &allocatorRef{allocator: ba}
	obj, off, err := ref.Allocate(8)
	if err != nil || obj != 0 || off != 0 {
		t.Fatalf("Allocate got obj=%d off=%d err=%v", obj, off, err)
	}
	objs, offs, err := ref.AllocateRun(8, 2)
	if err != nil || len(objs) != 2 || offs[0] != 8 {
		t.Fatalf("AllocateRun unexpected: objs=%v offs=%v err=%v", objs, offs, err)
	}
	if gotOff, err := ref.GetFileOffset(objs[0]); err != nil || gotOff != 8 {
		t.Fatalf("GetFileOffset: %v off=%d", err, gotOff)
	}
	if err := ref.Free(off, 8); err != nil {
		t.Fatalf("Free: %v", err)
	}
}

// allocatorSlice Marshal/Unmarshal
func TestAllocatorSliceMarshalRoundTrip(t *testing.T) {
	ref1 := &allocatorRef{allocator: NewBlockAllocator(8, 1, 0, 0, nil)}
	ref2 := &allocatorRef{allocator: NewBlockAllocator(8, 1, 8, 1, nil)}
	slice := allocatorSlice{ref1, ref2}
	data, err := slice.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out allocatorSlice
	if _, err := out.Unmarshal(data, 1); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 allocators, got %d", len(out))
	}
}

// BlockAllocator run allocation and size
func TestBlockAllocatorAllocateRunAndSize(t *testing.T) {
	ba := NewBlockAllocator(16, 2, 0, 0, nil)
	objs, offs, err := ba.AllocateRun(16, 3) // request more than available
	if err != nil || len(objs) != 2 || offs[1] != 16 {
		t.Fatalf("AllocateRun unexpected: objs=%v offs=%v err=%v", objs, offs, err)
	}
	expected := 8 + ((ba.blockCount + 7) / 8) + 2*ba.blockCount
	if ba.SizeInBytes() != expected {
		t.Fatalf("SizeInBytes expected %d, got %d", expected, ba.SizeInBytes())
	}
}

// allocatorPool AllocateRun (new allocator path)
func TestAllocatorPoolAllocateRunNewAllocator(t *testing.T) {
	parent, file := newTempBasicAllocator(t)

	// Use builder pattern to register callback for tracking new allocators
	newAllocatorCalled := false
	pool := NewAllocatorPoolBuilder(8, 1, parent, file).
		OnNewAllocator(func(ref *allocatorRef) error {
			newAllocatorCalled = true
			return nil
		}).
		Build()

	pool.available = nil // force new allocator path
	objs, offs, err := pool.AllocateRun(1)
	if err != nil || len(objs) != 1 || len(offs) != 1 {
		t.Fatalf("AllocateRun new allocator unexpected: objs=%v offs=%v err=%v", objs, offs, err)
	}
	if !newAllocatorCalled {
		t.Fatalf("onNewAllocator callback was not invoked")
	}
}

// OmniBlockAllocator coverage: Parent, AllocateRun, Free, Delete
func TestOmniBlockAllocatorRunAndFree(t *testing.T) {
	parent, file := newTempBasicAllocator(t)
	omni, err := NewOmniBlockAllocator([]int{8}, 4, parent, file, WithoutPreallocation())
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator: %v", err)
	}
	if omni.Parent() != parent {
		t.Fatalf("Parent mismatch")
	}
	objs, offs, err := omni.AllocateRun(8, 2)
	if err != nil || len(objs) != 2 || offs[1] != offs[0]+8 {
		t.Fatalf("AllocateRun unexpected: objs=%v offs=%v err=%v", objs, offs, err)
	}
	if err := omni.Free(offs[0], 8); err != nil {
		t.Fatalf("Free: %v", err)
	}
	if err := omni.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

// Gap heap operations already partially covered; ensure Push/Pop ordering via heap interface
func TestGapHeapHeapInterface(t *testing.T) {
	h := &gapHeap{}
	heap.Init(h)
	heap.Push(h, Gap{Start: 0, End: 5}) // size 5
	heap.Push(h, Gap{Start: 0, End: 2}) // size 2 (smallest)
	heap.Push(h, Gap{Start: 0, End: 3}) // size 3

	if h.Len() != 3 {
		t.Fatalf("Len mismatch: %d", h.Len())
	}
	first := heap.Pop(h).(Gap)
	if first.End-first.Start != 2 {
		t.Fatalf("expected smallest gap size 2, got %d", first.End-first.Start)
	}
}
