package treap_test

import (
	"path/filepath"
	"testing"

	collections "github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestBatchPersistPartialAllocation verifies that BatchPersist correctly handles
// partial allocations from the allocator (e.g., when requesting 10,000 nodes but
// only 400 slots are available in the current block).
func TestBatchPersistPartialAllocation(t *testing.T) {
	ms, err := collections.NewMultiStore(filepath.Join(t.TempDir(), "partial_alloc.bin"), 0)
	if err != nil {
		t.Fatalf("failed to create multistore: %v", err)
	}
	defer ms.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

	// Create a treap with fixed-size nodes
	tr := treap.NewPersistentPayloadTreap[types.IntKey, fixedPayload](
		types.IntLess,
		keyTemplate,
		ms,
	)

	// Insert nodes that will require multiple batches
	// With block size 1024, a tree with 2500 nodes should require multiple batches
	const nodeCount = 2500
	for i := 0; i < nodeCount; i++ {
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(i)
		payload := fixedPayload{A: uint64(i), B: uint64(i * 10)}
		tr.Insert(key, payload)
	}

	// Use BatchPersist which should now handle partial allocations
	err = tr.BatchPersist()
	if err != nil {
		t.Fatalf("BatchPersist failed: %v", err)
	}

	// Verify all nodes were persisted by checking the root has a valid ObjectId
	rootId, err := tr.GetRootObjectId()
	if err != nil {
		t.Fatalf("Failed to get root object ID: %v", err)
	}
	if rootId <= 0 {
		t.Errorf("Root was not persisted, got ObjectId: %d", rootId)
	}

	// Verify we can search for all inserted nodes
	for i := 0; i < nodeCount; i++ {
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(i)
		node := tr.Search(key)
		if node == nil {
			t.Errorf("Failed to find node with key %d after BatchPersist", i)
			continue
		}
		payload := node.GetPayload()
		expectedValue := uint64(i * 10)
		if payload.B != expectedValue {
			t.Errorf("Node %d: expected payload.B %d, got %d", i, expectedValue, payload.B)
		}
	}
}

// TestBatchPersistFragmentedAllocator tests BatchPersist behavior when the allocator
// has fragmented free space (some blocks partially full).
func TestBatchPersistFragmentedAllocator(t *testing.T) {
	ms, err := collections.NewMultiStore(filepath.Join(t.TempDir(), "fragmented.bin"), 0)
	if err != nil {
		t.Fatalf("failed to create multistore: %v", err)
	}
	defer ms.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

	// Create and persist a first treap to partially fill allocators
	treap1 := treap.NewPersistentPayloadTreap[types.IntKey, fixedPayload](
		types.IntLess,
		keyTemplate,
		ms,
	)

	// Insert and persist some nodes to fragment the allocator
	for i := 0; i < 500; i++ {
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(i)
		payload := fixedPayload{A: uint64(i), B: uint64(i * 2)}
		treap1.Insert(key, payload)
	}

	err = treap1.Persist()
	if err != nil {
		t.Fatalf("First treap Persist failed: %v", err)
	}

	// Now create a second treap that will need to work with partially-filled allocators
	treap2 := treap.NewPersistentPayloadTreap[types.IntKey, fixedPayload](
		types.IntLess,
		keyTemplate,
		ms,
	)

	// Insert more nodes
	for i := 1000; i < 2000; i++ {
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(i)
		payload := fixedPayload{A: uint64(i), B: uint64(i * 3)}
		treap2.Insert(key, payload)
	}

	// BatchPersist should handle this gracefully with partial allocations
	err = treap2.BatchPersist()
	if err != nil {
		t.Fatalf("Second treap BatchPersist failed: %v", err)
	}

	// Verify all nodes are accessible
	for i := 1000; i < 2000; i++ {
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(i)
		node := treap2.Search(key)
		if node == nil {
			t.Errorf("Failed to find node with key %d in second treap", i)
			continue
		}
		payload := node.GetPayload()
		expectedValue := uint64(i * 3)
		if payload.B != expectedValue {
			t.Errorf("Node %d: expected payload.B %d, got %d", i, expectedValue, payload.B)
		}
	}
}

// TestBatchPersistLargeTree tests BatchPersist with a tree large enough to require
// multiple allocation batches even without fragmentation.
func TestBatchPersistLargeTree(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large tree test in short mode")
	}

	ms, err := collections.NewMultiStore(filepath.Join(t.TempDir(), "large_tree.bin"), 0)
	if err != nil {
		t.Fatalf("failed to create multistore: %v", err)
	}
	defer ms.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

	tr := treap.NewPersistentPayloadTreap[types.IntKey, fixedPayload](
		types.IntLess,
		keyTemplate,
		ms,
	)

	// Insert a large number of nodes (more than typical block capacity)
	const nodeCount = 10000
	for i := 0; i < nodeCount; i++ {
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(i)
		payload := fixedPayload{A: uint64(i), B: uint64(i * 5)}
		tr.Insert(key, payload)
	}

	// BatchPersist should iterate through multiple batches
	err = tr.BatchPersist()
	if err != nil {
		t.Fatalf("BatchPersist on large tree failed: %v", err)
	}

	// Verify persistence worked
	rootId, err := tr.GetRootObjectId()
	if err != nil {
		t.Fatalf("Failed to get root object ID: %v", err)
	}
	if rootId <= 0 {
		t.Errorf("Root was not persisted, got ObjectId: %d", rootId)
	}

	// Spot-check some nodes
	checkIndices := []int{0, 1000, 5000, 9999}
	for _, i := range checkIndices {
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(i)
		node := tr.Search(key)
		if node == nil {
			t.Errorf("Failed to find node with key %d", i)
			continue
		}
		payload := node.GetPayload()
		expectedValue := uint64(i * 5)
		if payload.B != expectedValue {
			t.Errorf("Node %d: expected payload.B %d, got %d", i, expectedValue, payload.B)
		}
	}
}

// BenchmarkBatchPersistIterativeAllocation benchmarks the new iterative allocation
// approach with various tree sizes.
func BenchmarkBatchPersistIterativeAllocation(b *testing.B) {
	sizes := []struct {
		count int
		name  string
	}{
		{1000, "1k"},
		{5000, "5k"},
		{10000, "10k"},
	}

	for _, size := range sizes {
		b.Run("nodes="+size.name, func(b *testing.B) {
			ms, err := collections.NewMultiStore(filepath.Join(b.TempDir(), "bench.bin"), 0)
			if err != nil {
				b.Fatalf("failed to create multistore: %v", err)
			}
			defer ms.Close()

			var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tr := treap.NewPersistentPayloadTreap[types.IntKey, fixedPayload](
					types.IntLess,
					keyTemplate,
					ms,
				)

				for j := 0; j < size.count; j++ {
					key := (*types.IntKey)(new(int32))
					*key = types.IntKey(i*size.count + j)
					payload := fixedPayload{A: uint64(j), B: uint64(j * 2)}
					tr.Insert(key, payload)
				}
				b.StartTimer()

				err := tr.BatchPersist()
				if err != nil {
					b.Fatalf("BatchPersist failed: %v", err)
				}
			}
		})
	}
}
