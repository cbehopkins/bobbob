package store

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
		alloc := NewBlockAllocator(64, 10000, 0, ObjectId(1))
		b.ResetTimer()
		for range b.N {
			alloc.Allocate(64)
		}
	})

	b.Run("BlockAllocator/FixedSize256", func(b *testing.B) {
		alloc := NewBlockAllocator(256, 10000, 0, ObjectId(1))
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
		blockAlloc := NewBlockAllocator(256, 10000, 0, ObjectId(1))
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
		blockAlloc := NewBlockAllocator(256, 1000, 0, ObjectId(1))
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
		blockAlloc := NewBlockAllocator(256, 1000, 0, ObjectId(1))
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
		blockAlloc := NewBlockAllocator(256, 10000, 0, ObjectId(1))
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
