package allocator

import (
	"os"
	"testing"
	"time"
)

func countUnsynced(pool *allocatorPool) int {
	count := 0
	for _, ref := range pool.available {
		if ref != nil && !ref.synced {
			count++
		}
	}
	for _, ref := range pool.full {
		if ref != nil && !ref.synced {
			count++
		}
	}
	return count
}

func TestOmniBlockAllocatorBackgroundPersist(t *testing.T) {
	file, err := os.CreateTemp("", "omni_persist_*.dat")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Remove(file.Name())
		_ = file.Close()
	})

	parent, err := NewBasicAllocator(file)
	if err != nil {
		t.Fatalf("basic allocator: %v", err)
	}

	omni, err := NewOmniBlockAllocator(nil, 4, parent, file)
	if err != nil {
		t.Fatalf("omni allocator: %v", err)
	}

	// Create a few allocations then persist once via Marshal to assign fileOff.
	for i := 0; i < 6; i++ {
		if _, _, err := omni.Allocate(64); err != nil {
			t.Fatalf("allocate %d: %v", i, err)
		}
	}
	if _, err := omni.Marshal(); err != nil {
		t.Fatalf("initial marshal: %v", err)
	}

	pool := omni.blockMap[64]
	if pool == nil {
		t.Fatalf("expected 64-byte pool to exist")
	}

	// Mutate pool to mark allocator unsynced.
	if _, _, err := omni.Allocate(64); err != nil {
		t.Fatalf("post-marshal allocate: %v", err)
	}
	if count := countUnsynced(pool); count == 0 {
		t.Fatalf("expected unsynced allocators after mutation")
	}

	omni.StartBackgroundPersist(10 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	omni.StopBackgroundPersist()

	if count := countUnsynced(pool); count != 0 {
		t.Fatalf("expected background persist to sync allocators, still have %d", count)
	}

	// MarshalMultiple should now only require the LUT write (no allocator rewrites).
	sizes, err := pool.PreMarshal()
	if err != nil {
		t.Fatalf("PreMarshal: %v", err)
	}
	objIds := make([]ObjectId, len(sizes))
	for i, size := range sizes {
		objId, _, err := parent.Allocate(size)
		if err != nil {
			t.Fatalf("alloc objId: %v", err)
		}
		objIds[i] = objId
	}
	_, objs, err := pool.MarshalMultiple(objIds)
	if err != nil {
		t.Fatalf("MarshalMultiple: %v", err)
	}
	if len(objs) != 1 {
		t.Fatalf("expected only LUT write after background persist, got %d", len(objs))
	}
}
