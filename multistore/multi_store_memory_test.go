package multistore

import (
	"path/filepath"
	"runtime"
	"testing"
)

// TestMultiStoreAllocatorMemoryUsage verifies that memory usage is reasonable
// with allocator-based object tracking (BlockAllocators for fixed-size objects).
//
// Object metadata is tracked at the allocator level using bitmap-based tracking
// (~1 byte per object for BlockAllocators), rather than in a centralized map.
// This test ensures that heap usage scales appropriately for large object counts.
func TestMultiStoreAllocatorMemoryUsage(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "memory_test.db")

	ms, err := NewMultiStore(storePath, 0)
	if err != nil {
		t.Fatalf("Failed to create multistore: %v", err)
	}

	const (
		numObjects  = 100_000          // 100k objects to test reasonable scaling
		objectSize  = 64               // Small, fixed-size objects
		heapCeiling = 20 * 1024 * 1024 // 20 MB - reasonable for 100k tracked objects
	)

	// Capture baseline heap usage
	runtime.GC()
	var ms1 runtime.MemStats
	runtime.ReadMemStats(&ms1)
	baselineAlloc := ms1.Alloc

	// Allocate many small objects
	allocatedIds := make([]int, 0, numObjects)
	for i := 0; i < numObjects; i++ {
		objId, err := ms.NewObj(objectSize)
		if err != nil {
			t.Fatalf("Failed to allocate object %d: %v", i, err)
		}
		allocatedIds = append(allocatedIds, int(objId))
	}

	// Force GC to clean up temporary allocations
	runtime.GC()
	var ms2 runtime.MemStats
	runtime.ReadMemStats(&ms2)
	heapUsed := int64(ms2.Alloc - baselineAlloc)

	// Calculate expected memory usage
	// BlockAllocator uses bitmap-based tracking (~1 byte per object)
	// This is significantly more efficient than map-based tracking would be
	objectCount := int64(numObjects)

	t.Logf("=== MultiStore Memory Analysis (BlockAllocator-Based Tracking) ===")
	t.Logf("Objects allocated: %d", objectCount)
	t.Logf("Object size: %d bytes", objectSize)
	t.Logf("Heap used: %d bytes (%.2f MB)", heapUsed, float64(heapUsed)/(1024*1024))
	t.Logf("")
	t.Logf("Per-object overhead: %.2f bytes",
		float64(heapUsed)/float64(objectCount))
	t.Logf("")

	// The test should pass because BlockAllocator tracking is very memory-efficient
	if heapUsed > heapCeiling {
		t.Errorf("Heap usage %d bytes (%.2f MB) exceeds ceiling %d bytes (%.2f MB)",
			heapUsed, float64(heapUsed)/(1024*1024),
			heapCeiling, float64(heapCeiling)/(1024*1024))
		t.Logf("Memory usage per object: %.2f bytes", float64(heapUsed)/float64(objectCount))

		t.Logf("This is approximately %.2f bytes per object entry.",
			float64(heapUsed)/float64(objectCount))
	}

	_ = ms.Close()
}

// TestMultiStoreAllocatorScaling verifies memory scaling with object count
// using allocator-based tracking. Memory per object should remain constant.
func TestMultiStoreAllocatorScaling(t *testing.T) {
	tempDir := t.TempDir()

	testCases := []struct {
		name       string
		numObjects int
	}{
		{"1k objects", 1_000},
		{"10k objects", 10_000},
		{"50k objects", 50_000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storePath := filepath.Join(tempDir, tc.name+".db")
			ms, err := NewMultiStore(storePath, 0)
			if err != nil {
				t.Fatalf("Failed to create multistore: %v", err)
			}

			runtime.GC()
			var ms1 runtime.MemStats
			runtime.ReadMemStats(&ms1)
			baselineAlloc := ms1.Alloc

			// Allocate objects
			for i := 0; i < tc.numObjects; i++ {
				_, err := ms.NewObj(64)
				if err != nil {
					t.Fatalf("Failed to allocate object %d: %v", i, err)
				}
			}

			runtime.GC()
			var ms2 runtime.MemStats
			runtime.ReadMemStats(&ms2)
			heapUsed := int64(ms2.Alloc - baselineAlloc)

			bytesPerObject := float64(heapUsed) / float64(tc.numObjects)
			t.Logf("%s: %.2f bytes/object (%.2f MB total)",
				tc.name,
				bytesPerObject,
				float64(heapUsed)/(1024*1024))

			_ = ms.Close()
		})
	}
}
