package treap_test

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	collections "github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// BenchmarkPersistAfterFlush benchmarks the scenario where a tree is persisted,
// then some nodes are flushed from memory, and then re-persisted. This tests
// the AllocatorIndex.Get() -> searchDisk() path which is suspected to be a bottleneck.
//
// Scenario:
// 1. Create and persist a large tree
// 2. Flush nodes to disk (simulating memory pressure)
// 3. Make modifications that require re-persisting
// 4. Persist again - this should trigger lookups of existing object IDs
//
// This benchmark should expose the searchDisk bottleneck if it exists.
func BenchmarkPersistAfterFlush(b *testing.B) {
	nodeCounts := []int{
		1000,
		5000,
		10000,
	}

	for _, nodeCount := range nodeCounts {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			ms, err := collections.NewMultiStore(filepath.Join(b.TempDir(), "persist_after_flush.bin"), 0)
			if err != nil {
				b.Fatalf("failed to create multistore: %v", err)
			}
			defer ms.Close()

			var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				// Create and initially persist tree
				pt := treap.NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms)
				for j := 0; j < nodeCount; j++ {
					key := (*types.IntKey)(new(int32))
					*key = types.IntKey(j)
					pt.Insert(key)
				}

				// First persist
				if err := pt.Persist(); err != nil {
					b.Fatalf("Initial Persist failed: %v", err)
				}

				// Get root objectId for later reload
				rootNode, ok := pt.Root().(treap.PersistentTreapNodeInterface[types.IntKey])
				if !ok {
					b.Fatalf("root is not a PersistentTreapNodeInterface")
				}
				rootObjId, err := rootNode.ObjectId()
				if err != nil {
					b.Fatalf("failed to get root ObjectId: %v", err)
				}

				// Flush all nodes to disk (simulating memory pressure)
				if err := rootNode.Flush(); err != nil {
					b.Fatalf("Flush failed: %v", err)
				}

				// Force GC to clear any lingering references
				runtime.GC()

				// Now reload the tree from disk
				pt2 := treap.NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms)
				if err := pt2.Load(rootObjId); err != nil {
					b.Fatalf("Load failed: %v", err)
				}

				// Modify the tree (this will require nodes to be loaded from disk)
				newKey := (*types.IntKey)(new(int32))
				*newKey = types.IntKey(nodeCount + i)
				pt2.Insert(newKey)

				b.StartTimer()
				// Re-persist - this should trigger Get() calls for existing nodes
				// If searchDisk is a bottleneck, it will show up here
				if err := pt2.Persist(); err != nil {
					b.Fatalf("Second Persist failed: %v", err)
				}
				b.StopTimer()
			}
		})
	}
}

// BenchmarkPersistWithExistingObjects benchmarks persisting when the allocator
// already has many allocated objects (simulating a long-running system).
// This creates pressure on the AllocatorIndex to track many objects.
func BenchmarkPersistWithExistingObjects(b *testing.B) {
	b.Run("with_10k_existing_objects", func(b *testing.B) {
		ms, err := collections.NewMultiStore(filepath.Join(b.TempDir(), "persist_with_existing.bin"), 0)
		if err != nil {
			b.Fatalf("failed to create multistore: %v", err)
		}
		defer ms.Close()

		var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

		// Pre-populate with 10k objects to create allocator pressure
		pt0 := treap.NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms)
		for j := 0; j < 10000; j++ {
			key := (*types.IntKey)(new(int32))
			*key = types.IntKey(j)
			pt0.Insert(key)
		}
		if err := pt0.Persist(); err != nil {
			b.Fatalf("Pre-populate Persist failed: %v", err)
		}

		// Now benchmark persisting new trees in this "dirty" environment
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			pt := treap.NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms)
			for j := 0; j < 1000; j++ {
				key := (*types.IntKey)(new(int32))
				*key = types.IntKey(10000 + i*1000 + j)
				pt.Insert(key)
			}
			b.StartTimer()

			if err := pt.Persist(); err != nil {
				b.Fatalf("Persist failed: %v", err)
			}
		}
	})
}

// BenchmarkPersistWithBasicStore tests the same scenario with BasicStore instead
// of MultiStore to see if the allocator strategy affects the bottleneck.
func BenchmarkPersistWithBasicStore(b *testing.B) {
	nodeCounts := []int{1000, 10000}

	for _, nodeCount := range nodeCounts {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			tempFile := filepath.Join(b.TempDir(), "basic_store.bin")
			bs, err := store.NewBasicStore(tempFile)
			if err != nil {
				b.Fatalf("failed to create basicstore: %v", err)
			}
			defer bs.Close()

			var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				pt := treap.NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, bs)
				for j := 0; j < nodeCount; j++ {
					key := (*types.IntKey)(new(int32))
					*key = types.IntKey(j)
					pt.Insert(key)
				}

				b.StartTimer()

				if err := pt.Persist(); err != nil {
					b.Fatalf("Persist failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkRepeatPersist benchmarks repeatedly persisting the same tree after modifications.
// This simulates a write-heavy workload where persist is called frequently.
func BenchmarkRepeatPersist(b *testing.B) {
	ms, err := collections.NewMultiStore(filepath.Join(b.TempDir(), "repeat_persist.bin"), 0)
	if err != nil {
		b.Fatalf("failed to create multistore: %v", err)
	}
	defer ms.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

	// Create initial tree
	pt := treap.NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms)
	for j := 0; j < 1000; j++ {
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(j)
		pt.Insert(key)
	}

	// Initial persist
	if err := pt.Persist(); err != nil {
		b.Fatalf("Initial Persist failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Modify tree slightly
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(1000 + i)
		pt.Insert(key)

		// Persist again - subsequent persists might trigger different code paths
		if err := pt.Persist(); err != nil {
			b.Fatalf("Persist %d failed: %v", i, err)
		}
	}
}
