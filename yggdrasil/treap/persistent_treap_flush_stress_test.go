package treap

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestPersistentTreapFlushInsertInterleaved tests interleaving Flush and Insert operations
// to catch potential issues with state inconsistency when nodes are flushed and then
// the tree is modified.
func TestPersistentTreapFlushInsertInterleaved(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Phase 1: Build initial tree
	const initialSize = 100
	keys := make([]int, initialSize)
	for i := range initialSize {
		keys[i] = i * 10
		keyObj := types.IntKey(keys[i])
		treap.Insert(&keyObj)
	}

	// Persist the tree
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist initial tree: %v", err)
	}

	// Phase 2: Interleave flush and insert operations
	const numRounds = 50
	for round := range numRounds {
		// Flush the tree
		rootNode, ok := treap.root.(*PersistentTreapNode[types.IntKey])
		if !ok {
			t.Fatalf("Round %d: root is not a PersistentTreapNode", round)
		}
		if err := rootNode.Flush(); err != nil {
			t.Logf("Round %d: Flush returned error (may be partial): %v", round, err)
		}

		// Verify nodes were flushed (children should be nil)
		if rootNode.TreapNode.left != nil || rootNode.TreapNode.right != nil {
			t.Errorf("Round %d: Root node has non-nil children after flush", round)
		}

		// Insert new keys
		newKey := initialSize*10 + round*2
		keyObj := types.IntKey(newKey)
		treap.Insert(&keyObj)
		keys = append(keys, newKey)

		// Persist after insert
		if err := treap.Persist(); err != nil {
			t.Fatalf("Round %d: Failed to persist after insert: %v", round, err)
		}

		// Search for a random existing key (will trigger load from disk)
		searchIdx := round % len(keys)
		searchKey := types.IntKey(keys[searchIdx])
		result := treap.Search(&searchKey)
		if result == nil || result.IsNil() {
			t.Errorf("Round %d: Failed to find key %d after flush+insert", round, keys[searchIdx])
		}

		// Verify the newly inserted key
		result = treap.Search(&keyObj)
		if result == nil || result.IsNil() {
			t.Errorf("Round %d: Failed to find newly inserted key %d", round, newKey)
		}
	}

	// Phase 3: Final validation - ensure all keys are present
	for i, k := range keys {
		searchKey := types.IntKey(k)
		result := treap.Search(&searchKey)
		if result == nil || result.IsNil() {
			t.Errorf("Final validation failed for key %d (index %d)", k, i)
		}
	}
}

// TestPersistentTreapFlushInsertConcurrent tests concurrent flush and insert operations
// This is a more aggressive test that attempts to trigger race conditions.
func TestPersistentTreapFlushInsertConcurrent(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Build initial tree
	const initialSize = 50
	for i := 0; i < initialSize; i++ {
		keyObj := types.IntKey(i * 10)
		treap.Insert(&keyObj)
	}

	// Persist the tree
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist initial tree: %v", err)
	}

	// Concurrent operations
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// Goroutine 1: Insert new keys
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 50 {
			keyObj := types.IntKey(1000 + i)
			treap.Insert(&keyObj)
			if i%10 == 0 {
				if err := treap.Persist(); err != nil {
					errChan <- fmt.Errorf("insert goroutine persist failed: %w", err)
					return
				}
			}
		}
	}()

	// Goroutine 2: Search for keys (triggers loads after flush)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			keyObj := types.IntKey((i % initialSize) * 10)
			result := treap.Search(&keyObj)
			if result == nil || result.IsNil() {
				errChan <- fmt.Errorf("search goroutine failed to find key %d", (i%initialSize)*10)
				return
			}
		}
	}()

	// Goroutine 3: Periodically flush oldest nodes
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			// Flush oldest 20%
			_, err := treap.FlushOldestPercentile(20)
			if err != nil {
				errChan <- fmt.Errorf("flush goroutine failed: %w", err)
				return
			}
		}
	}()

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		t.Error(err)
	}

	// Final validation
	for i := 0; i < initialSize; i++ {
		keyObj := types.IntKey(i * 10)
		result := treap.Search(&keyObj)
		if result == nil || result.IsNil() {
			t.Errorf("Final validation: failed to find initial key %d", i*10)
		}
	}
}

// TestPersistentTreapFlushDeepTraversal tests that flushed nodes can be reloaded
// during deep tree traversals (e.g., searches that traverse multiple levels).
func TestPersistentTreapFlushDeepTraversal(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Build a tree with specific structure to force deep traversals
	// Insert keys in random order to avoid a degenerate (linear) tree
	rng := rand.New(rand.NewSource(42))
	const treeSize = 200
	keys := make([]int, treeSize)
	for i := 0; i < treeSize; i++ {
		keys[i] = i
	}
	// Shuffle to get random insertion order
	rng.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for _, k := range keys {
		keyObj := types.IntKey(k)
		treap.Insert(&keyObj)
	}

	// Persist
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	// Flush all nodes
	rootNode, ok := treap.root.(*PersistentTreapNode[types.IntKey])
	if !ok {
		t.Fatal("root is not a PersistentTreapNode")
	}

	// Recursive flush of entire tree
	var flushRecursive func(node *PersistentTreapNode[types.IntKey]) error
	flushRecursive = func(node *PersistentTreapNode[types.IntKey]) error {
		if node == nil || node.IsNil() {
			return nil
		}
		// Flush children first
		if node.TreapNode.left != nil {
			if leftNode, ok := node.TreapNode.left.(*PersistentTreapNode[types.IntKey]); ok {
				if err := flushRecursive(leftNode); err != nil {
					return err
				}
			}
		}
		if node.TreapNode.right != nil {
			if rightNode, ok := node.TreapNode.right.(*PersistentTreapNode[types.IntKey]); ok {
				if err := flushRecursive(rightNode); err != nil {
					return err
				}
			}
		}
		// Then flush this node
		return node.Flush()
	}

	if err := flushRecursive(rootNode); err != nil {
		t.Logf("Flush returned error (may be partial): %v", err)
	}

	// Now search for all keys - each search should trigger loads from disk
	for i := 0; i < treeSize; i++ {
		searchKey := types.IntKey(i)
		result := treap.Search(&searchKey)
		if result == nil || result.IsNil() {
			t.Errorf("Failed to find key %d after full flush", i)
		}
	}

	// Insert new keys after flush
	for i := 0; i < 50; i++ {
		keyObj := types.IntKey(treeSize + i)
		treap.Insert(&keyObj)
	}

	// Persist again
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist after post-flush inserts: %v", err)
	}

	// Validate all keys (original + new)
	for i := 0; i < treeSize+50; i++ {
		searchKey := types.IntKey(i)
		result := treap.Search(&searchKey)
		if result == nil || result.IsNil() {
			t.Errorf("Final validation: failed to find key %d", i)
		}
	}
}

// TestPersistentTreapFlushInsertPattern tests specific patterns that might expose bugs
func TestPersistentTreapFlushInsertPattern(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	_ = stre // Used in subtests

	testPatterns := []struct {
		name string
		fn   func(t *testing.T, treap *PersistentTreap[types.IntKey])
	}{
		{
			name: "flush-insert-at-root",
			fn: func(t *testing.T, treap *PersistentTreap[types.IntKey]) {
				// Insert root
				rootKey := types.IntKey(50)
				treap.Insert(&rootKey)
				if err := treap.Persist(); err != nil {
					t.Fatalf("Failed to persist root: %v", err)
				}

				// Flush root's children
				rootNode, ok := treap.root.(*PersistentTreapNode[types.IntKey])
				if !ok {
					t.Fatal("root is not a PersistentTreapNode")
				}
				if err := rootNode.Flush(); err != nil {
					t.Logf("Flush error (may be partial): %v", err)
				}

				// Insert left child
				leftKey := types.IntKey(25)
				treap.Insert(&leftKey)

				// Insert right child
				rightKey := types.IntKey(75)
				treap.Insert(&rightKey)

				// Verify all keys
				for _, k := range []int{25, 50, 75} {
					searchKey := types.IntKey(k)
					if result := treap.Search(&searchKey); result == nil || result.IsNil() {
						t.Errorf("Failed to find key %d", k)
					}
				}
			},
		},
		{
			name: "flush-insert-rebalance",
			fn: func(t *testing.T, treap *PersistentTreap[types.IntKey]) {
				// Create a small tree
				for i := 0; i < 10; i++ {
					keyObj := types.IntKey(i * 10)
					treap.Insert(&keyObj)
				}
				if err := treap.Persist(); err != nil {
					t.Fatalf("Failed to persist: %v", err)
				}

				// Flush everything
				if _, err := treap.FlushOldestPercentile(100); err != nil {
					t.Logf("Flush error (may be partial): %v", err)
				}

				// Insert many new keys that might trigger rotations
				for i := 0; i < 20; i++ {
					keyObj := types.IntKey(100 + i)
					treap.Insert(&keyObj)
				}

				// Persist
				if err := treap.Persist(); err != nil {
					t.Fatalf("Failed to persist after rebalance: %v", err)
				}

				// Validate all keys
				for i := 0; i < 10; i++ {
					searchKey := types.IntKey(i * 10)
					if result := treap.Search(&searchKey); result == nil || result.IsNil() {
						t.Errorf("Failed to find original key %d", i*10)
					}
				}
				for i := 0; i < 20; i++ {
					searchKey := types.IntKey(100 + i)
					if result := treap.Search(&searchKey); result == nil || result.IsNil() {
						t.Errorf("Failed to find new key %d", 100+i)
					}
				}
			},
		},
		{
			name: "alternating-flush-insert",
			fn: func(t *testing.T, treap *PersistentTreap[types.IntKey]) {
				// Alternate between insert and flush many times
				for i := 0; i < 50; i++ {
					// Insert
					keyObj := types.IntKey(i)
					treap.Insert(&keyObj)

					// Persist
					if err := treap.Persist(); err != nil {
						t.Fatalf("Iteration %d: Failed to persist: %v", i, err)
					}

					// Flush some nodes
					if i > 0 && i%5 == 0 {
						if _, err := treap.FlushOldestPercentile(30); err != nil {
							t.Logf("Iteration %d: Flush error (may be partial): %v", i, err)
						}
					}
				}

				// Final validation
				for i := 0; i < 50; i++ {
					searchKey := types.IntKey(i)
					if result := treap.Search(&searchKey); result == nil || result.IsNil() {
						t.Errorf("Failed to find key %d", i)
					}
				}
			},
		},
		{
			name: "flush-insert-validate-cycle",
			fn: func(t *testing.T, treap *PersistentTreap[types.IntKey]) {
				// Build tree
				for i := 0; i < 30; i++ {
					keyObj := types.IntKey(i * 5)
					treap.Insert(&keyObj)
				}

				// Multiple cycles of persist->validate->flush->insert
				for cycle := 0; cycle < 10; cycle++ {
					// Persist
					if err := treap.Persist(); err != nil {
						t.Fatalf("Cycle %d: Persist failed: %v", cycle, err)
					}

					// Validate
					errors := treap.ValidateAgainstDisk()
					if len(errors) > 0 {
						t.Errorf("Cycle %d: Validation failed with %d errors:", cycle, len(errors))
						for _, e := range errors {
							t.Errorf("  - %s", e)
						}
					}

					// Flush
					if _, err := treap.FlushOldestPercentile(50); err != nil {
						t.Logf("Cycle %d: Flush error (may be partial): %v", cycle, err)
					}

					// Insert new keys
					for i := 0; i < 3; i++ {
						newKey := types.IntKey(1000 + cycle*10 + i)
						treap.Insert(&newKey)
					}
				}

				// Final persist and validate
				if err := treap.Persist(); err != nil {
					t.Fatalf("Final persist failed: %v", err)
				}
				errors := treap.ValidateAgainstDisk()
				if len(errors) > 0 {
					t.Errorf("Final validation failed with %d errors:", len(errors))
					for _, e := range errors {
						t.Errorf("  - %s", e)
					}
				}
			},
		},
	}

	for _, pattern := range testPatterns {
		t.Run(pattern.name, func(t *testing.T) {
			// Create fresh treap for each pattern
			freshTreap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)
			pattern.fn(t, freshTreap)
		})
	}
}

// TestPersistentTreapFlushInsertObjectIdConsistency tests that after flush+insert+persist,
// all nodes have valid objectIds and the tree structure is consistent with disk.
// Note: objectIds may change when tree rotations occur during inserts (expected behavior).
func TestPersistentTreapFlushInsertObjectIdConsistency(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Build tree
	const size = 50
	for i := 0; i < size; i++ {
		keyObj := types.IntKey(i)
		treap.Insert(&keyObj)
	}

	// Persist
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist: %v", err)
	}

	// Validate before flush
	errors := treap.ValidateAgainstDisk()
	if len(errors) > 0 {
		t.Errorf("Validation before flush failed with %d errors:", len(errors))
		for _, e := range errors {
			t.Errorf("  - %s", e)
		}
	}

	// Flush nodes
	if _, err := treap.FlushOldestPercentile(80); err != nil {
		t.Logf("Flush error (may be partial): %v", err)
	}

	// Insert new keys (may trigger rotations, which invalidate some objectIds)
	for i := 0; i < 20; i++ {
		keyObj := types.IntKey(size + i)
		treap.Insert(&keyObj)
	}

	// Persist again
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist after inserts: %v", err)
	}

	// Verify all nodes can be found and have valid objectIds
	for i := 0; i < size+20; i++ {
		searchKey := types.IntKey(i)
		result := treap.Search(&searchKey)
		if result == nil || result.IsNil() {
			t.Errorf("Failed to find key %d after flush+insert", i)
			continue
		}
		pNode, ok := result.(*PersistentTreapNode[types.IntKey])
		if !ok {
			t.Errorf("Node for key %d is not a PersistentTreapNode", i)
			continue
		}
		objId, err := pNode.ObjectId()
		if err != nil {
			t.Errorf("Failed to get objectId for key %d: %v", i, err)
			continue
		}
		if objId < 0 {
			t.Errorf("Key %d has invalid objectId %d after persist", i, objId)
		}
	}

	// Validate against disk - this is the critical test
	errors = treap.ValidateAgainstDisk()
	if len(errors) > 0 {
		t.Errorf("Validation after flush+insert+persist failed with %d errors:", len(errors))
		for _, e := range errors {
			t.Errorf("  - %s", e)
		}
	}
}

// TestPersistentTreapFlushInsertHeavyStress is an aggressive stress test that
// interleaves many operations to find edge cases
// KNOWN ISSUE: This test currently fails due to data corruption when flush+insert
// operations interleave without intermediate persist calls. Investigation needed.
func TestPersistentTreapFlushInsertHeavyStress(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Track all keys we've inserted
	allKeys := make(map[int]bool)
	rng := rand.New(rand.NewSource(98765))

	const (
		numRounds = 100
		keysPerOp = 10
		maxKeyVal = 5000
	)

	for round := 0; round < numRounds; round++ {
		// Operation mix for this round
		op := rng.Intn(100)

		switch {
		case op < 40: // 40% insert
			for i := 0; i < keysPerOp; i++ {
				key := rng.Intn(maxKeyVal)
				keyObj := types.IntKey(key)
				treap.Insert(&keyObj)
				allKeys[key] = true
			}

		case op < 60: // 20% persist
			if err := treap.Persist(); err != nil {
				t.Fatalf("Round %d: Persist failed: %v", round, err)
			}

		case op < 75: // 15% flush some nodes
			if round > 10 { // Only flush after we have some data persisted
				flushed, err := treap.FlushOldestPercentile(30)
				if err != nil {
					t.Logf("Round %d: Flush error (may be partial): %v", round, err)
				}
				t.Logf("Round %d: Flushed %d nodes", round, flushed)
			}

		case op < 90: // 15% search random keys
			for i := 0; i < 5; i++ {
				key := rng.Intn(maxKeyVal)
				keyObj := types.IntKey(key)
				result := treap.Search(&keyObj)
				shouldExist := allKeys[key]
				exists := result != nil && !result.IsNil()
				if shouldExist && !exists {
					t.Errorf("Round %d: Key %d should exist but not found", round, key)
				}
			}

		default: // 10% validate
			errors := treap.ValidateAgainstDisk()
			if len(errors) > 0 {
				t.Errorf("Round %d: Validation failed with %d errors:", round, len(errors))
				for idx, e := range errors {
					if idx < 10 { // Limit error output
						t.Errorf("  - %s", e)
					}
				}
				if len(errors) > 10 {
					t.Errorf("  ... and %d more errors", len(errors)-10)
				}
			}
		}

		// Periodic full validation
		if round > 0 && round%20 == 0 {
			if err := treap.Persist(); err != nil {
				t.Fatalf("Round %d: Periodic persist failed: %v", round, err)
			}
			errors := treap.ValidateAgainstDisk()
			if len(errors) > 0 {
				t.Errorf("Round %d: Periodic validation failed with %d errors", round, len(errors))
			}
		}
	}

	// Final persist and validate
	if err := treap.Persist(); err != nil {
		t.Fatalf("Final persist failed: %v", err)
	}

	errors := treap.ValidateAgainstDisk()
	if len(errors) > 0 {
		t.Errorf("Final validation failed with %d errors:", len(errors))
		for idx, e := range errors {
			if idx < 20 {
				t.Errorf("  - %s", e)
			}
		}
		if len(errors) > 20 {
			t.Errorf("  ... and %d more errors", len(errors)-20)
		}
	}

	// Verify all inserted keys are present
	missingCount := 0
	for key := range allKeys {
		keyObj := types.IntKey(key)
		result := treap.Search(&keyObj)
		if result == nil || result.IsNil() {
			missingCount++
			if missingCount <= 10 {
				t.Errorf("Final check: Key %d missing", key)
			}
		}
	}
	if missingCount > 0 {
		t.Errorf("Final check: %d keys missing out of %d inserted", missingCount, len(allKeys))
	}

	t.Logf("Test complete: inserted %d unique keys across %d rounds", len(allKeys), numRounds)
}

// TestPersistentTreapFlushInsertMemoryPressure tests that repeated flush+insert
// cycles don't cause unbounded memory growth
// KNOWN ISSUE: FlushOldestPercentile doesn't actually reduce memory as designed
// because Flush() clears a node's CHILDREN, not the node itself from memory.
// To actually evict nodes, parent nodes need to call Flush() on their children.
// This test documents the current behavior for future fixing.
func TestPersistentTreapFlushInsertMemoryPressure(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Insert initial dataset
	const initialSize = 1000
	for i := 0; i < initialSize; i++ {
		keyObj := types.IntKey(i)
		treap.Insert(&keyObj)
	}

	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist initial tree: %v", err)
	}

	// Record baseline memory
	baselineCount := treap.CountInMemoryNodes()
	t.Logf("Baseline: %d nodes in memory", baselineCount)

	// Perform many flush+insert cycles
	const numCycles = 50
	for cycle := 0; cycle < numCycles; cycle++ {
		// Flush most nodes
		flushed, err := treap.FlushOldestPercentile(80)
		if err != nil {
			t.Logf("Cycle %d: Flush error (may be partial): %v", cycle, err)
		}

		afterFlushCount := treap.CountInMemoryNodes()
		t.Logf("Cycle %d: After flush: %d nodes in memory (flushed %d)", cycle, afterFlushCount, flushed)

		// Insert a few new keys (this will load some nodes from disk during traversal)
		for i := 0; i < 5; i++ {
			keyObj := types.IntKey(initialSize + cycle*5 + i)
			treap.Insert(&keyObj)
		}

		if err := treap.Persist(); err != nil {
			t.Fatalf("Cycle %d: Persist failed: %v", cycle, err)
		}

		// Check memory doesn't grow unbounded
		currentCount := treap.CountInMemoryNodes()
		t.Logf("Cycle %d: After insert+persist: %d nodes in memory", cycle, currentCount)

		// Allow some growth but not linear with cycle count
		// We expect ~20 nodes in memory max (after flush removes 80%)
		// Plus some recently accessed during inserts
		maxExpected := baselineCount / 2 // Allow up to half of baseline
		if currentCount > maxExpected {
			t.Errorf("Cycle %d: Memory growth concern: %d nodes (expected < %d)",
				cycle, currentCount, maxExpected)
		}
	}

	// Final validation
	errors := treap.ValidateAgainstDisk()
	if len(errors) > 0 {
		t.Errorf("Final validation failed with %d errors", len(errors))
	}

	t.Logf("Memory pressure test complete: %d cycles", numCycles)
}
