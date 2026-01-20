package allocator

import (
	"os"
	"testing"
)

// helper to force marshal of pool and write to disk
func persistPoolOnce(t *testing.T, pool *allocatorPool, parent Allocator, file *os.File) {
	t.Helper()
	sizes, err := pool.PreMarshal()
	if err != nil {
		t.Fatalf("PreMarshal failed: %v", err)
	}
	objIds := make([]ObjectId, len(sizes))
	for i, size := range sizes {
		objId, _, err := parent.Allocate(size)
		if err != nil {
			t.Fatalf("Allocating objId failed: %v", err)
		}
		objIds[i] = objId
	}
	_, objs, err := pool.MarshalMultiple(objIds)
	if err != nil {
		t.Fatalf("MarshalMultiple failed: %v", err)
	}
	for _, obj := range objs {
		data, err := obj.ByteFunc()
		if err != nil {
			t.Fatalf("ByteFunc failed: %v", err)
		}
		_, offset, err := parent.(interface {
			GetObjectInfo(ObjectId) (FileOffset, int, error)
		}).GetObjectInfo(obj.ObjectId)
		if err != nil {
			t.Fatalf("GetObjectInfo failed: %v", err)
		}
		if _, err := file.WriteAt(data, int64(offset)); err != nil {
			t.Fatalf("WriteAt failed: %v", err)
		}
	}
}

func TestAllocatorPoolPersistUnsynced(t *testing.T) {
	file, err := os.CreateTemp("", "pool_persist_*.dat")
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

	blockSize := 64
	blockCount := 4
	pool := NewAllocatorPool(blockSize, blockCount, parent, file)

	// Create initial allocator and persist once to assign fileOff and mark synced.
	for i := 0; i < 5; i++ {
		if _, _, _, err := pool.Allocate(); err != nil {
			t.Fatalf("allocate %d: %v", i, err)
		}
	}
	persistPoolOnce(t, pool, parent, file)

	// Mutate state to make allocator unsynced (same underlying allocator).
	if _, _, _, err := pool.Allocate(); err != nil {
		t.Fatalf("post-persist allocate: %v", err)
	}

	persisted, err := pool.PersistUnsynced()
	if err != nil {
		t.Fatalf("PersistUnsynced failed: %v", err)
	}
	if persisted == 0 {
		t.Fatalf("expected at least one allocator persisted")
	}

	// After background-style persist, MarshalMultiple should only emit the LUT.
	sizes, err := pool.PreMarshal()
	if err != nil {
		t.Fatalf("PreMarshal failed: %v", err)
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
		t.Fatalf("MarshalMultiple after persist failed: %v", err)
	}
	if len(objs) != 1 {
		t.Fatalf("expected only LUT write after background persist, got %d", len(objs))
	}
}
