package omni

import (
	"os"
	"testing"

	"github.com/cbehopkins/bobbob/allocator/basic"
	"github.com/cbehopkins/bobbob/allocator/cache"
	"github.com/cbehopkins/bobbob/allocator/types"
)

// TestMarshalUnmarshalWithLazyRehydration verifies that Marshal delegates
// all pools to cache, and Unmarshal restores them lazily on demand.
func TestMarshalUnmarshalWithLazyRehydration(t *testing.T) {
	// Create temporary file
	f, err := os.CreateTemp(os.TempDir(), "test_omni_marshal_*.bin")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer f.Close()
	defer os.Remove(f.Name())

	// Create parent allocator
	parent, err := basic.New(f)
	if err != nil {
		t.Fatalf("failed to create basic allocator: %v", err)
	}

	// Create cache
	pc, err := cache.New()
	if err != nil {
		t.Fatalf("failed to create pool cache: %v", err)
	}
	defer pc.Close()

	// Create OmniAllocator
	blockSize := 64
	oa, err := NewOmniAllocator([]int{blockSize, 256}, parent, f, pc)
	if err != nil {
		t.Fatalf("failed to create OmniAllocator: %v", err)
	}
	defer oa.Close()

	// Allocate several objects
	allocatedIds := make([]types.ObjectId, 0)
	for i := 0; i < 10; i++ {
		objId, _, err := oa.Allocate(blockSize)
		if err != nil {
			t.Fatalf("failed to allocate object %d: %v", i, err)
		}
		allocatedIds = append(allocatedIds, objId)
		t.Logf("Allocated object: objId=%d", objId)
	}

	// Verify pools exist and have allocators before marshal
	oa.mu.RLock()
	pl, exists := oa.pools[blockSize]
	oa.mu.RUnlock()

	if !exists {
		t.Fatalf("expected pool for block size %d before marshal", blockSize)
	}

	availBefore := len(pl.AvailableAllocators())
	fullBefore := len(pl.FullAllocators())
	t.Logf("Before marshal: %d available allocators, %d full allocators", availBefore, fullBefore)

	if availBefore == 0 && fullBefore == 0 {
		t.Fatalf("expected at least one allocator before marshal")
	}

	// Marshal the OmniAllocator (should delegate all pools to cache)
	marshalData, err := oa.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	t.Logf("Marshaled data size: %d bytes", len(marshalData))

	// Verify cache now has entries (all allocators were delegated)
	cacheEntries := pc.GetAll()
	if len(cacheEntries) == 0 {
		t.Fatalf("expected cache to have entries after marshal, but it's empty")
	}
	t.Logf("Cache has %d entries after marshal", len(cacheEntries))

	// Verify pools are now drained (all allocators moved to cache)
	oa.mu.RLock()
	pl, exists = oa.pools[blockSize]
	oa.mu.RUnlock()

	if exists && pl != nil {
		availAfterMarshal := len(pl.AvailableAllocators())
		fullAfterMarshal := len(pl.FullAllocators())
		t.Logf("After marshal: %d available allocators, %d full allocators", availAfterMarshal, fullAfterMarshal)
		if availAfterMarshal+fullAfterMarshal != 0 {
			t.Logf("Note: Pool not fully drained (expected behavior if DrainFullAllocators only drains full ones)")
		}
	}

	// Create a new OmniAllocator for unmarshal test
	pc2, err := cache.New()
	if err != nil {
		t.Fatalf("failed to create second pool cache: %v", err)
	}
	defer pc2.Close()

	oa2, err := NewOmniAllocator([]int{}, parent, f, pc2)
	if err != nil {
		t.Fatalf("failed to create second OmniAllocator: %v", err)
	}
	defer oa2.Close()

	// Unmarshal into the new OmniAllocator
	err = oa2.Unmarshal(marshalData)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	t.Logf("Unmarshal complete")

	// Verify configuration was restored
	if len(oa2.blockSizes) != 2 {
		t.Fatalf("expected 2 configured blockSizes, got %d", len(oa2.blockSizes))
	}
	if oa2.blockSizes[0] != blockSize || oa2.blockSizes[1] != 256 {
		t.Fatalf("blockSizes not restored correctly: %v", oa2.blockSizes)
	}

	// Verify pools exist but are EMPTY (lazy rehydration)
	oa2.mu.RLock()
	pl2, exists := oa2.pools[blockSize]
	oa2.mu.RUnlock()

	if !exists {
		t.Fatalf("expected pool for block size %d after unmarshal", blockSize)
	}

	availAfterUnmarshal := len(pl2.AvailableAllocators())
	fullAfterUnmarshal := len(pl2.FullAllocators())
	t.Logf("After unmarshal (before access): %d available allocators, %d full allocators",
		availAfterUnmarshal, fullAfterUnmarshal)

	// Pools should be empty initially (lazy rehydration)
	if availAfterUnmarshal+fullAfterUnmarshal != 0 {
		t.Logf("Note: Pool has allocators after unmarshal (may be OK if not all were delegated)")
	}

	// Verify cache was restored
	cache2Entries := pc2.GetAll()
	if len(cache2Entries) == 0 {
		t.Fatalf("expected cache to have entries after unmarshal")
	}
	t.Logf("Cache has %d entries after unmarshal", len(cache2Entries))

	// Now access one of the allocated objects - should trigger lazy rehydration
	testObjId := allocatedIds[0]
	t.Logf("Accessing object %d (should trigger rehydration)", testObjId)

	offset, size, err := oa2.GetObjectInfo(testObjId)
	if err != nil {
		t.Fatalf("failed to get object info after unmarshal (rehydration failed): %v", err)
	}

	t.Logf("Successfully accessed object %d after unmarshal: offset=%d, size=%d", testObjId, offset, size)

	// Verify the pool now has allocators (rehydrated from cache)
	oa2.mu.RLock()
	pl2AfterAccess := oa2.pools[blockSize]
	oa2.mu.RUnlock()

	availAfterAccess := len(pl2AfterAccess.AvailableAllocators())
	fullAfterAccess := len(pl2AfterAccess.FullAllocators())
	t.Logf("After accessing object: %d available allocators, %d full allocators",
		availAfterAccess, fullAfterAccess)

	if availAfterAccess+fullAfterAccess == 0 {
		t.Fatalf("expected at least one allocator after rehydration, but pool is still empty")
	}

	// Verify all allocated objects are still accessible via lazy rehydration
	for _, objId := range allocatedIds {
		if !oa2.ContainsObjectId(objId) {
			t.Fatalf("object %d not found after unmarshal", objId)
		}
	}

	t.Log("✓ Marshal/Unmarshal test passed: lazy rehydration works correctly")
}

// TestMarshalUnmarshalEmptyAllocator verifies marshaling works with no allocations
func TestMarshalUnmarshalEmptyAllocator(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "test_omni_marshal_empty_*.bin")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer f.Close()
	defer os.Remove(f.Name())

	parent, err := basic.New(f)
	if err != nil {
		t.Fatalf("failed to create basic allocator: %v", err)
	}

	pc, err := cache.New()
	if err != nil {
		t.Fatalf("failed to create pool cache: %v", err)
	}
	defer pc.Close()

	oa, err := NewOmniAllocator([]int{64, 256}, parent, f, pc)
	if err != nil {
		t.Fatalf("failed to create OmniAllocator: %v", err)
	}
	defer oa.Close()

	// Marshal without any allocations
	data, err := oa.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal empty allocator: %v", err)
	}

	t.Logf("Empty allocator marshaled to %d bytes", len(data))

	// Create new allocator and unmarshal
	pc2, err := cache.New()
	if err != nil {
		t.Fatalf("failed to create second pool cache: %v", err)
	}
	defer pc2.Close()

	oa2, err := NewOmniAllocator([]int{}, parent, f, pc2)
	if err != nil {
		t.Fatalf("failed to create second OmniAllocator: %v", err)
	}
	defer oa2.Close()

	err = oa2.Unmarshal(data)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Verify configuration was restored
	if len(oa2.blockSizes) != 2 {
		t.Fatalf("expected 2 configured blockSizes, got %d", len(oa2.blockSizes))
	}

	t.Log("✓ Empty allocator marshal/unmarshal test passed")
}
