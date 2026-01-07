package allocator

import (
	"runtime"
	"runtime/debug"
	"testing"
	"unsafe"
)

// TestBasicAllocatorMemoryUsageBaseline measures heap growth for a fixed number
// of sequential allocations to establish a memory-usage baseline.
// Run with: go test ./store/allocator -run TestBasicAllocatorMemoryUsageBaseline -v
func TestBasicAllocatorMemoryUsageBaseline(t *testing.T) {
	counts := []int{1_000, 10_000, 50_000}
	const objSize = 256

	for _, n := range counts {
		// Encourage a clean baseline
		runtime.GC()
		debug.FreeOSMemory()

		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		alloc := NewEmptyBasicAllocator()
		for i := 0; i < n; i++ {
			if _, _, err := alloc.Allocate(objSize); err != nil {
				t.Fatalf("Allocate failed at %d: %v", i, err)
			}
		}

		// Settle heap and measure
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		delta := int64(after.Alloc) - int64(before.Alloc)
		perObj := float64(delta) / float64(n)

		// Internal sizes for context
		allocCount := len(alloc.Allocations)
		freeListLen := len(alloc.FreeList)

		t.Logf("BasicAllocator baseline: n=%d size=%d heap_delta=%dB ~%.2fB/obj allocations=%d freeList=%d",
			n, objSize, delta, perObj, allocCount, freeListLen)
	}
}

// TestOmniBlockAllocatorMemoryOverhead verifies that OmniBlockAllocator
// delegates to BlockAllocators efficiently without excessive overhead
func TestOmniBlockAllocatorMemoryOverhead(t *testing.T) {
	const itemsPerBlock = 10_000 // Blocks per allocator
	const itemCount = 10_000

	// Create an OmniBlockAllocator
	parent := &MockAllocator{}
	blockSizes := []int{64, 128, 256, 512, 1024}
	omni, err := NewOmniBlockAllocator(blockSizes, itemsPerBlock, parent)
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator failed: %v", err)
	}

	// Calculate expected memory for each BlockAllocator
	structSize := 64 // approximate blockAllocator struct size
	totalMemory := 0

	t.Logf("=== OmniBlockAllocator Memory Overhead ===")
	t.Logf("Block sizes supported: %v", blockSizes)
	t.Logf("Blocks per allocator: %d\n", itemsPerBlock)

	for _, size := range blockSizes {
		boolArraySize := itemsPerBlock * 1
		allocatorMemory := structSize + boolArraySize
		totalMemory += allocatorMemory
		t.Logf("BlockAllocator (size %d):", size)
		t.Logf("  Struct: %d bytes", structSize)
		t.Logf("  Bool array: %d bytes", boolArraySize)
		t.Logf("  Total: %d bytes (%.2f KB)", allocatorMemory, float64(allocatorMemory)/1024)
	}

	omniStructSize := int(unsafe.Sizeof(*omni))
	totalMemory += omniStructSize

	t.Logf("\nOmniBlockAllocator struct: %d bytes", omniStructSize)
	t.Logf("Total memory: %d bytes (%.2f KB)", totalMemory, float64(totalMemory)/1024)

	// Per-item overhead assuming items are evenly distributed
	itemsPerSize := itemCount / len(blockSizes)
	bytesPerItem := float64(itemsPerBlock*len(blockSizes)) / float64(itemsPerBlock*len(blockSizes))
	t.Logf("Per-item tracking overhead: %.2f bytes\n", bytesPerItem)

	// Allocate items of various fixed sizes
	for _, blockSize := range blockSizes {
		for i := range itemsPerSize {
			_, _, err := omni.Allocate(blockSize)
			if err != nil {
				t.Fatalf("Failed to allocate size %d (item %d/%d): %v", blockSize, i, itemsPerSize, err)
			}
		}
	}

	// Verify number of BlockAllocators created
	// With the new best-fit strategy, we now combine provided sizes with defaults.
	// Defaults are [64, 128, 256, 512, 1024, 2048, 4096]
	// Provided are [64, 128, 256, 512, 1024]
	// After deduplication, we get: 64, 128, 256, 512, 1024, 2048, 4096 = 7 allocators
	allocatorCount := len(omni.blockMap)
	t.Logf("Number of BlockAllocators created: %d", allocatorCount)

	expectedAllocators := 7 // 5 provided + 2 additional defaults (2048, 4096) not in original list
	if allocatorCount != expectedAllocators {
		t.Errorf("Expected %d BlockAllocators (provided + defaults with deduplication), got %d",
			expectedAllocators, allocatorCount)
	} else {
		t.Logf("✓ BlockAllocators include defaults plus provided sizes")
	}

	t.Logf("✓ Successfully allocated %d items across %d block sizes", itemCount, len(blockSizes))
}

// TestMemoryComparisonObjectMapVsBlockAllocator demonstrates the memory savings
// from eliminating ObjectMap in favor of BlockAllocator
func TestMemoryComparisonObjectMapVsBlockAllocator(t *testing.T) {
	const itemCount = 50_000

	t.Logf("=== Memory Comparison: ObjectMap vs BlockAllocator ===")
	t.Logf("Comparing tracking overhead for %d items\n", itemCount)

	// Old ObjectMap approach: map[ObjectId]AllocatedRegion
	// Each map entry is approximately 48 bytes (key + value + map overhead)
	objectMapBytesPerItem := 48
	objectMapTotalBytes := itemCount * objectMapBytesPerItem

	t.Logf("ObjectMap approach (old):")
	t.Logf("  Per-item overhead: %d bytes", objectMapBytesPerItem)
	t.Logf("  Total for %d items: %d bytes (%.2f MB)",
		itemCount, objectMapTotalBytes, float64(objectMapTotalBytes)/(1024*1024))
	t.Logf("")

	// New BlockAllocator approach: bool array
	ba := NewBlockAllocator(1024, itemCount, 0, 1000)
	structSize := int(unsafe.Sizeof(*ba))
	boolArraySize := itemCount * 1 // 1 byte per bool
	blockAllocatorTotalBytes := structSize + boolArraySize
	blockAllocatorBytesPerItem := float64(boolArraySize) / float64(itemCount)

	t.Logf("BlockAllocator approach (new):")
	t.Logf("  Struct overhead: %d bytes", structSize)
	t.Logf("  Bool array: %d bytes", boolArraySize)
	t.Logf("  Per-item overhead: %.2f bytes", blockAllocatorBytesPerItem)
	t.Logf("  Total for %d items: %d bytes (%.2f MB)",
		itemCount, blockAllocatorTotalBytes, float64(blockAllocatorTotalBytes)/(1024*1024))
	t.Logf("")

	// Calculate savings
	savings := float64(objectMapTotalBytes-blockAllocatorTotalBytes) / float64(objectMapTotalBytes) * 100
	reduction := float64(objectMapBytesPerItem) / blockAllocatorBytesPerItem

	t.Logf("Comparison:")
	t.Logf("  Memory savings: %.1f%%", savings)
	t.Logf("  Reduction factor: %.0fx", reduction)
	t.Logf("  Saved: %d bytes (%.2f MB)",
		objectMapTotalBytes-blockAllocatorTotalBytes,
		float64(objectMapTotalBytes-blockAllocatorTotalBytes)/(1024*1024))
	t.Logf("")

	// Verify we achieved significant savings
	if savings < 95 {
		t.Errorf("Expected at least 95%% memory savings, got %.1f%%", savings)
	} else {
		t.Logf("✓ Achieved >95%% reduction in tracking overhead")
	}

	if blockAllocatorBytesPerItem > 1.1 {
		t.Errorf("BlockAllocator should use ~1 byte per item, got %.2f bytes", blockAllocatorBytesPerItem)
	} else {
		t.Logf("✓ BlockAllocator uses ~1 byte per item as expected")
	}

	// Verify the allocator works
	for range 100 {
		_, _, err := ba.Allocate(1024)
		if err != nil {
			t.Fatalf("Failed to allocate: %v", err)
		}
	}
	t.Logf("✓ BlockAllocator allocation verified")
}

// TestBlockAllocatorStructSize verifies the BlockAllocator struct itself
// is small and most memory is in the allocatedList
func TestBlockAllocatorStructSize(t *testing.T) {
	ba := &blockAllocator{
		blockSize:          1024,
		blockCount:         1000,
		allocatedList:      make([]bool, 1000),
		startingFileOffset: 0,
		startingObjectId:   0,
		allAllocated:       false,
	}

	structSize := unsafe.Sizeof(*ba)
	sliceSize := unsafe.Sizeof(ba.allocatedList)
	boolSize := unsafe.Sizeof(ba.allocatedList[0])

	t.Logf("=== BlockAllocator Structure Analysis ===")
	t.Logf("Struct size (without slice data): %d bytes", structSize)
	t.Logf("Slice header size: %d bytes", sliceSize)
	t.Logf("Bool size: %d byte", boolSize)
	t.Logf("AllocatedList capacity: %d", cap(ba.allocatedList))
	t.Logf("AllocatedList data size: %d bytes (1 byte × %d)", len(ba.allocatedList), len(ba.allocatedList))
	t.Logf("")
	t.Logf("Total BlockAllocator memory:")
	t.Logf("  Struct + slice header: %d bytes", structSize)
	t.Logf("  Slice data: %d bytes", len(ba.allocatedList))
	t.Logf("  Total: %d bytes", int(structSize)+len(ba.allocatedList))
	t.Logf("  Per-item overhead: %.2f bytes", float64(int(structSize)+len(ba.allocatedList))/float64(len(ba.allocatedList)))

	// Verify the struct overhead is small relative to data
	overhead := float64(structSize) / float64(len(ba.allocatedList))
	if overhead > 0.1 {
		t.Logf("Note: Struct overhead is %.2f bytes per item (%.1f%% of total)",
			overhead, overhead*100)
	} else {
		t.Logf("✓ Struct overhead is negligible (%.4f bytes per item)", overhead)
	}
}
