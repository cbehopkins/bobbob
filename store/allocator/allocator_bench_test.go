package allocator

import (
	"testing"
)

// BenchmarkAllocatorAllocation benchmarks the allocation performance
// across different allocator types
func BenchmarkAllocatorAllocation(b *testing.B) {
	b.Run("BasicAllocator/Sequential", func(b *testing.B) {
		alloc := NewEmptyBasicAllocator()
		b.ResetTimer()
		for range b.N {
			alloc.Allocate(256)
		}
	})

	b.Run("BlockAllocator/FixedSize64", func(b *testing.B) {
		alloc := NewBlockAllocator(64, 10000, 0, ObjectId(1), nil)
		b.ResetTimer()
		for range b.N {
			alloc.Allocate(64)
		}
	})

	b.Run("BlockAllocator/FixedSize256", func(b *testing.B) {
		alloc := NewBlockAllocator(256, 10000, 0, ObjectId(1), nil)
		b.ResetTimer()
		for range b.N {
			alloc.Allocate(256)
		}
	})
}

// BenchmarkAllocatorGetObjectInfo benchmarks reverse lookup performance
// This is critical for the ObjectMap elimination strategy
func BenchmarkAllocatorGetObjectInfo(b *testing.B) {
	b.Run("BasicAllocator/GetObjectInfo", func(b *testing.B) {
		alloc := NewEmptyBasicAllocator()
		// Pre-allocate some objects
		var lastObjId ObjectId
		for range 100 {
			objId, _, _ := alloc.Allocate(256)
			lastObjId = objId
		}

		b.ResetTimer()
		for range b.N {
			alloc.GetObjectInfo(lastObjId)
		}
	})

	b.Run("BlockAllocator/GetFileOffset", func(b *testing.B) {
		blockAlloc := NewBlockAllocator(256, 10000, 0, ObjectId(1), nil)
		// Pre-allocate some objects
		var lastObjId ObjectId
		for range 100 {
			objId, _, _ := blockAlloc.Allocate(256)
			lastObjId = objId
		}

		b.ResetTimer()
		for range b.N {
			blockAlloc.GetFileOffset(lastObjId)
		}
	})
}

// BenchmarkAllocatorFree benchmarks the free/deallocation performance
func BenchmarkAllocatorFree(b *testing.B) {
	b.Run("BasicAllocator/Free/100Items", func(b *testing.B) {
		// Pre-allocate objects to free
		allocations := make([]struct {
			offset FileOffset
			size   int
		}, 100)

		for i := range 100 {
			alloc := NewEmptyBasicAllocator()
			_, offset, _ := alloc.Allocate(256)
			allocations[i] = struct {
				offset FileOffset
				size   int
			}{offset, 256}
		}

		b.ResetTimer()
		for range b.N {
			alloc := NewEmptyBasicAllocator()
			for _, alloc_item := range allocations {
				alloc.Free(alloc_item.offset, alloc_item.size)
			}
		}
	})

	b.Run("BlockAllocator/Free/100Items", func(b *testing.B) {
		blockAlloc := NewBlockAllocator(256, 1000, 0, ObjectId(1), nil)
		// Pre-allocate objects to free
		allocations := make([]struct {
			offset FileOffset
			size   int
		}, 100)

		for i := range 100 {
			_, offset, _ := blockAlloc.Allocate(256)
			allocations[i] = struct {
				offset FileOffset
				size   int
			}{offset, 256}
		}

		b.ResetTimer()
		for range b.N {
			for _, alloc_item := range allocations {
				blockAlloc.Free(alloc_item.offset, alloc_item.size)
			}
		}
	})
}

// BenchmarkAllocatorMarshal benchmarks serialization performance
func BenchmarkAllocatorMarshal(b *testing.B) {
	b.Run("BasicAllocator/Marshal", func(b *testing.B) {
		alloc := NewEmptyBasicAllocator()
		for range 1000 {
			alloc.Allocate(256)
		}

		b.ResetTimer()
		for range b.N {
			alloc.Marshal()
		}
	})

	b.Run("BlockAllocator/Marshal", func(b *testing.B) {
		blockAlloc := NewBlockAllocator(256, 1000, 0, ObjectId(1), nil)
		for range 500 {
			blockAlloc.Allocate(256)
		}

		b.ResetTimer()
		for range b.N {
			blockAlloc.Marshal()
		}
	})
}

// BenchmarkAllocatorScaling measures performance degradation
// as allocation count increases (tests for memory efficiency)
func BenchmarkAllocatorScaling(b *testing.B) {
	b.Run("BasicAllocator/ScalingLookup/1k", func(b *testing.B) {
		alloc := NewEmptyBasicAllocator()
		allocations := make([]ObjectId, 1000)
		for i := range 1000 {
			objId, _, _ := alloc.Allocate(256)
			allocations[i] = objId
		}

		b.ResetTimer()
		for i := range b.N {
			idx := i % len(allocations)
			alloc.GetObjectInfo(allocations[idx])
		}
	})

	b.Run("BasicAllocator/ScalingLookup/10k", func(b *testing.B) {
		alloc := NewEmptyBasicAllocator()
		allocations := make([]ObjectId, 10000)
		for i := range 10000 {
			objId, _, _ := alloc.Allocate(256)
			allocations[i] = objId
		}

		b.ResetTimer()
		for i := range b.N {
			idx := i % len(allocations)
			alloc.GetObjectInfo(allocations[idx])
		}
	})

	b.Run("BlockAllocator/ScalingLookup/1k", func(b *testing.B) {
		blockAlloc := NewBlockAllocator(256, 10000, 0, ObjectId(1), nil)
		allocations := make([]ObjectId, 1000)
		for i := range 1000 {
			objId, _, _ := blockAlloc.Allocate(256)
			allocations[i] = objId
		}

		b.ResetTimer()
		for i := range b.N {
			idx := i % len(allocations)
			blockAlloc.GetFileOffset(allocations[idx])
		}
	})
}

// BenchmarkAllocatorMemoryOverhead measures the memory overhead of different strategies.
// This is critical for understanding when to use BasicAllocator vs BlockAllocators.
// BasicAllocator: 48 bytes per entry (map[ObjectId]ObjectInfo overhead)
// BlockAllocator: ~1 byte per entry (bitfield + minimal tracking)
func BenchmarkAllocatorMemoryOverhead(b *testing.B) {
	// Note: This benchmark measures allocation patterns, not actual memory usage.
	// Real memory overhead is measured via benchmark output analysis.

	b.Run("BasicAllocator/VariableSizes", func(b *testing.B) {
		alloc := NewEmptyBasicAllocator()
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			// Realistic variable sizes: metadata, small nodes, large objects
			sizes := []int{256, 512, 1024, 2048, 4096}
			alloc.Allocate(sizes[i%len(sizes)])
		}
	})

	b.Run("BlockAllocator/TreepNodes256", func(b *testing.B) {
		alloc := NewBlockAllocator(256, 100000, 0, ObjectId(1), nil)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			alloc.Allocate(256)
		}
	})

	b.Run("BlockAllocator/TreepNodes512", func(b *testing.B) {
		alloc := NewBlockAllocator(512, 100000, 0, ObjectId(1), nil)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			alloc.Allocate(512)
		}
	})
}

// BenchmarkAllocatorRoutingDecision measures OmniBlockAllocator routing overhead.
// The OmniBlockAllocator must decide which sub-allocator to use for each allocation.
// For treap-heavy workloads, this routing should be highly optimized.
func BenchmarkAllocatorRoutingDecision(b *testing.B) {
	b.Run("OmniBlockAllocator/RouteSmallObject", func(b *testing.B) {
		parent := NewEmptyBasicAllocator()
		omni, _ := NewOmniBlockAllocator([]int{256, 512}, 1000, parent, nil)
		b.ResetTimer()
		for range b.N {
			// Small objects should route to BasicAllocator (root)
			omni.Allocate(100)
		}
	})

	b.Run("OmniBlockAllocator/RouteTreepNode256", func(b *testing.B) {
		parent := NewEmptyBasicAllocator()
		omni, _ := NewOmniBlockAllocator([]int{256, 512}, 1000, parent, nil)
		b.ResetTimer()
		for range b.N {
			// Treap nodes typically ~256 bytes
			omni.Allocate(256)
		}
	})

	b.Run("OmniBlockAllocator/RouteTreepNode512", func(b *testing.B) {
		parent := NewEmptyBasicAllocator()
		omni, _ := NewOmniBlockAllocator([]int{256, 512}, 1000, parent, nil)
		b.ResetTimer()
		for range b.N {
			// Larger treap nodes ~512 bytes
			omni.Allocate(512)
		}
	})

	b.Run("OmniBlockAllocator/RouteLargeObject", func(b *testing.B) {
		parent := NewEmptyBasicAllocator()
		omni, _ := NewOmniBlockAllocator([]int{256, 512}, 1000, parent, nil)
		b.ResetTimer()
		for range b.N {
			// Large objects route to root allocator
			omni.Allocate(4096)
		}
	})
}

// BenchmarkAllocatorLookupCacheHitRate measures LRU cache effectiveness
// in the allocator index. High hit rates indicate good locality.
func BenchmarkAllocatorLookupCacheHitRate(b *testing.B) {
	b.Run("LocalitySequential", func(b *testing.B) {
		alloc := NewEmptyBasicAllocator()
		allocations := make([]ObjectId, 100)
		for i := range 100 {
			objId, _, _ := alloc.Allocate(256)
			allocations[i] = objId
		}

		b.ResetTimer()
		for i := range b.N {
			// Sequential access pattern: high cache locality
			idx := (i / 10) % len(allocations)
			alloc.GetObjectInfo(allocations[idx])
		}
	})

	b.Run("LocalityRandom", func(b *testing.B) {
		alloc := NewEmptyBasicAllocator()
		allocations := make([]ObjectId, 100)
		for i := range 100 {
			objId, _, _ := alloc.Allocate(256)
			allocations[i] = objId
		}

		// Pseudo-random access pattern
		seed := uint32(12345)
		b.ResetTimer()
		for range b.N {
			seed = seed*1103515245 + 12345
			idx := (int(seed) / 65536) % len(allocations)
			alloc.GetObjectInfo(allocations[idx])
		}
	})
}

// BenchmarkAllocatorAllocationRun measures throughput for realistic allocation patterns.
// This simulates the work done during tree construction and maintenance.
func BenchmarkAllocatorAllocationRun(b *testing.B) {
	b.Run("BasicAllocator/AllocateRun", func(b *testing.B) {
		alloc := NewEmptyBasicAllocator()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for i := range 1000 {
				// Mix of object sizes: metadata, treap nodes, payloads
				size := 256 + (i % 256)
				alloc.Allocate(size)
			}
		}
	})

	b.Run("BlockAllocator256/AllocateRun", func(b *testing.B) {
		alloc := NewBlockAllocator(256, 100000, 0, ObjectId(1), nil)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for range 1000 {
				alloc.Allocate(256)
			}
		}
	})

	b.Run("BlockAllocator512/AllocateRun", func(b *testing.B) {
		alloc := NewBlockAllocator(512, 100000, 0, ObjectId(1), nil)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for range 1000 {
				alloc.Allocate(512)
			}
		}
	})
}
