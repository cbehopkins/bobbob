package treap

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestSimplifiedInsertIterateBaseline - minimal test without concurrent readers or background flushes
// Just insert, delete, and iterate - no duplicates allowed
func TestSimplifiedInsertIterateBaseline(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	const (
		cycles    = 50
		batchSize = 10
	)

	expected := make(map[int]bool)
	keys := make([]int, 0)
	rng := rand.New(rand.NewSource(42))

	for cycle := range cycles {
		// Insert - check expected map first to prevent duplicates
		startKey := cycle * batchSize
		for i := range batchSize {
			keyVal := startKey + i
			if !expected[keyVal] {
				key := types.IntKey(keyVal)
				payload := MockPayload{Data: fmt.Sprintf("item_%d", keyVal)}
				treap.Insert(&key, payload)
				expected[keyVal] = true
				keys = append(keys, keyVal)
			}
		}

		// Random deletes
		deleteCount := rng.Intn(batchSize / 2)
		for i := 0; i < deleteCount && len(keys) > 0; i++ {
			idx := rng.Intn(len(keys))
			keyVal := keys[idx]
			key := types.IntKey(keyVal)
			treap.Delete(&key)
			delete(expected, keyVal)
			last := len(keys) - 1
			keys[idx] = keys[last]
			keys = keys[:last]
		}

		// Iterate and verify
		seen := make(map[int]bool)
		err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
			pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
			k := int(*pNode.GetKey().(*types.IntKey))
			if !expected[k] {
				return fmt.Errorf("unexpected key %d in treap", k)
			}
			seen[k] = true
			return nil
		})
		if err != nil {
			t.Fatalf("cycle %d: InOrderVisit failed: %v", cycle, err)
		}

		if len(seen) != len(expected) {
			t.Fatalf("cycle %d: saw %d items but expected %d", cycle, len(seen), len(expected))
		}

		t.Logf("cycle %d: OK", cycle)
	}

	t.Logf("Test passed")
}

// TestSimplifiedInsertIterateConcurrentReaders - add concurrent readers to the mix
// Still no background flushes, but now we have race conditions between writers and readers
func TestSimplifiedInsertIterateConcurrentReaders(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	const (
		cycles    = 50
		batchSize = 10
	)

	expected := make(map[int]bool)
	keys := make([]int, 0)
	rng := rand.New(rand.NewSource(42))

	done := make(chan struct{})
	var wg sync.WaitGroup

	// Spawn 4 concurrent readers
	for reader := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			iterCount := 0
			for {
				select {
				case <-done:
					t.Logf("Reader %d: completed %d iterations", id, iterCount)
					return
				default:
				}

				seenCount := 0
				err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
					_ = node
					seenCount++
					return nil
				})
				if err != nil {
					t.Errorf("Reader %d: InOrderVisit failed: %v", id, err)
					return
				}
				iterCount++
			}
		}(reader)
	}

	// Inserter/Deleter (main thread)
	for cycle := range cycles {
		// Insert a batch
		startKey := cycle * batchSize
		for i := range batchSize {
			keyVal := startKey + i
			if !expected[keyVal] {
				key := types.IntKey(keyVal)
				if err := treap.Insert(&key); err != nil {
					t.Fatalf("cycle %d: Insert failed: %v", cycle, err)
				}
				expected[keyVal] = true
				keys = append(keys, keyVal)
			}
		}

		// Random deletes
		deleteCount := rng.Intn(batchSize / 2)
		for i := 0; i < deleteCount && len(keys) > 0; i++ {
			idx := rng.Intn(len(keys))
			keyVal := keys[idx]
			key := types.IntKey(keyVal)
			if err := treap.Delete(&key); err != nil {
				t.Fatalf("cycle %d: Delete failed: %v", cycle, err)
			}
			delete(expected, keyVal)
			last := len(keys) - 1
			keys[idx] = keys[last]
			keys = keys[:last]
		}

		t.Logf("cycle %d: OK", cycle)
	}

	close(done)
	wg.Wait()

	t.Logf("Concurrent reader test passed")
}

// TestFlushThenDeleteChild - tests the specific race: flush parent, then delete child
// This should reproduce the bug where parent still references deleted child
func TestFlushThenDeleteChild(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	// Create a balanced tree: insert 0, 2, 4, 6, 8, 10 etc (10 items)
	// This creates a reasonably balanced structure
	keys := []int{5, 2, 8, 1, 3, 7, 9, 0, 4, 6}
	for _, k := range keys {
		key := types.IntKey(k)
		if err := treap.Insert(&key); err != nil {
			t.Fatalf("Insert %d failed: %v", k, err)
		}
	}

	// Flush 50% of the nodes (oldest ones)
	numFlushed, err := treap.FlushOldestPercentile(50)
	if err != nil {
		t.Fatalf("FlushOldestPercentile failed: %v", err)
	}
	t.Logf("Flushed %d nodes", numFlushed)

	// Now delete some leaf nodes and some that might have parents still in memory
	// Try deleting in an order that might leave stale references
	delKeys := []int{0, 1, 9}
	for _, k := range delKeys {
		key := types.IntKey(k)
		if err := treap.Delete(&key); err != nil {
			t.Fatalf("Delete %d failed: %v", k, err)
		}
	}

	// Now try to iterate - this should work without crashes
	seen := 0
	err = treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		seen++
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit failed after delete: %v", err)
	}

	expected := len(keys) - len(delKeys) // 10 - 3 = 7
	if seen != expected {
		t.Fatalf("Expected %d nodes, saw %d", expected, seen)
	}

	t.Logf("Test passed: flushed, deleted, iterated successfully")
}

// TestFlushInLoopWithPayloads - use PayloadTreap with frequent manual flushes
// Reproduces the original failure with PayloadTreap + flushes, but without background goroutines
func TestFlushInLoopWithPayloads(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	const (
		cycles    = 100
		batchSize = 20
	)

	expected := make(map[int]bool)
	keys := make([]int, 0)
	rng := rand.New(rand.NewSource(42))

	for cycle := range cycles {
		// Insert batch
		startKey := cycle * batchSize
		for i := range batchSize {
			keyVal := startKey + i
			if !expected[keyVal] {
				key := types.IntKey(keyVal)
				payload := MockPayload{Data: fmt.Sprintf("item_%d", keyVal)}
				treap.Insert(&key, payload)
				expected[keyVal] = true
				keys = append(keys, keyVal)
			}
		}

		// Delete some
		deleteCount := rng.Intn(batchSize / 2)
		for i := 0; i < deleteCount && len(keys) > 0; i++ {
			idx := rng.Intn(len(keys))
			keyVal := keys[idx]
			key := types.IntKey(keyVal)
			treap.Delete(&key)
			delete(expected, keyVal)
			last := len(keys) - 1
			keys[idx] = keys[last]
			keys = keys[:last]
		}

		// Every 10 cycles, flush oldest 50%
		if cycle%10 == 0 && cycle > 0 {
			numFlushed, err := treap.FlushOldestPercentile(50)
			if err != nil {
				t.Fatalf("cycle %d: FlushOldestPercentile failed: %v", cycle, err)
			}
			_ = numFlushed
		}

		// Check integrity
		seen := make(map[int]bool)
		err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
			pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
			k := int(*pNode.GetKey().(*types.IntKey))
			if !expected[k] {
				return fmt.Errorf("unexpected key %d", k)
			}
			seen[k] = true
			return nil
		})
		if err != nil {
			t.Fatalf("cycle %d: InOrderVisit failed: %v", cycle, err)
		}

		if len(seen) != len(expected) {
			t.Fatalf("cycle %d: saw %d items but expected %d", cycle, len(seen), len(expected))
		}
	}

	t.Logf("Test passed successfully")
}
