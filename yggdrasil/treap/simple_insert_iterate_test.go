package treap

import (
	"fmt"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestSimpleInsertDeleteIterate is a simplified version without concurrent flush
// to isolate the insert/delete/iterate behavior.
// Uses deterministic deletions for repeatability.
func TestSimpleInsertDeleteIterate(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	const (
		cycles       = 100 // Increase to original
		batchSize    = 1000 // Increase to original
		maxNodes     = 10000
		deleteEveryN = 4 // Delete every 4th key to reduce tree size
	)

	expected := make(map[int]MockPayload)
	keys := make([]int, 0, maxNodes)
	
	insertLog := make([]int, 0, 1000) // Track last 1000 inserts
	deleteLog := make([]int, 0, 1000) // Track last 1000 deletes

	// Helper to verify tree structure via InOrderVisit
	verifyTreeStructure := func(operation string, cycleNum int, operationNum int) {
		seenKeys := make(map[int]bool)
		err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
			pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
			k := int(*pNode.GetKey().(*types.IntKey))
			seenKeys[k] = true
			return nil
		})
		if err != nil {
			t.Fatalf("%s cycle %d op %d: InOrderVisit failed: %v", operation, cycleNum, operationNum, err)
		}
		if len(seenKeys) != len(expected) {
			// Find missing keys
			missing := []int{}
			for k := range expected {
				if !seenKeys[k] {
					missing = append(missing, k)
				}
			}
			// Find unexpected keys
			unexpected := []int{}
			for k := range seenKeys {
				if _, ok := expected[k]; !ok {
					unexpected = append(unexpected, k)
				}
			}
			t.Logf("%s cycle %d op %d: Tree has %d nodes but expected has %d", 
				operation, cycleNum, operationNum, len(seenKeys), len(expected))
			if len(missing) > 0 {
				t.Logf("  Missing: %v", missing)
			}
			if len(unexpected) > 0 {
				t.Logf("  Unexpected: %v", unexpected)
			}
			t.Fatalf("%s cycle %d op %d: CORRUPTION DETECTED!", operation, cycleNum, operationNum)
		}
	}

	deleteFirst := func() {
		if len(keys) == 0 {
			return
		}
		keyVal := keys[0]
		key := types.IntKey(keyVal)
		treap.Delete(&key)
		delete(expected, keyVal)
		keys = keys[1:]
		deleteLog = append(deleteLog, keyVal)
	}

	// Inserter/Checker worker
	for cycle := 0; cycle < cycles; cycle++ {
		keysBeforeInsert := len(expected)
		
		// Insert a batch
		startKey := cycle * batchSize
		for i := 0; i < batchSize; i++ {
			keyVal := startKey + i
			key := types.IntKey(keyVal)
			payload := MockPayload{Data: fmt.Sprintf("item_%d", keyVal)}
			
			treap.Insert(&key, payload)
			expected[keyVal] = payload
			keys = append(keys, keyVal)
			insertLog = append(insertLog, keyVal)
			
			// Verify immediately after insert
			afterSearch := treap.Search(&key)
			if afterSearch == nil {
				t.Fatalf("cycle %d insert %d: Key %d not found immediately after Insert!", cycle, i, keyVal)
			}
			
			
			// Verify tree structure after EVERY insert (granular)
			verifyTreeStructure("INSERT", cycle, i)
		}
		
		keysAfterInsert := len(expected)
		t.Logf("cycle %d: inserted %d keys (expected before=%d, after=%d)", 
			cycle, keysAfterInsert-keysBeforeInsert, keysBeforeInsert, keysAfterInsert)

		// Verify tree structure after all inserts
		verifyTreeStructure("POST-INSERT-BATCH", cycle, batchSize)

		// Deterministic deletions: delete every Nth key to reduce tree size
		deleteCount := batchSize / deleteEveryN
		keysDeletedThisCycle := 0
		deletedThisCycle := []int{} // Track what we actually delete
		for i := 0; i < deleteCount && len(keys) > 0; i++ {
			if i < 5 {
				deletedThisCycle = append(deletedThisCycle, keys[0])
			}
			
			deleteFirst()
			keysDeletedThisCycle++
			
			// Verify tree structure after each delete
			if i % 25 == 0 {  // Check every 25 deletes to avoid slowness
				verifyTreeStructure("DELETE", cycle, i)
			}
		}

		// Cap total nodes by deleting oldest keys
		deleteCapIteration := 0
		for len(keys) > maxNodes {
			deleteFirst()
			keysDeletedThisCycle++
			deleteCapIteration++
			
			if deleteCapIteration % 25 == 0 {
				verifyTreeStructure("DELETE-CAP", cycle, deleteCapIteration)
			}
		}
		
		if keysDeletedThisCycle > 0 {
			t.Logf("cycle %d: deleted %d keys (expected now=%d). First 5 deleted: %v", 
				cycle, keysDeletedThisCycle, len(expected), deletedThisCycle)
		}

		// Verify tree structure after all deletes
		verifyTreeStructure("POST-DELETE-BATCH", cycle, deleteCount)

		// Final validation for the cycle
		t.Logf("cycle %d: PASSED (tree has %d nodes)", cycle, len(expected))
	}

	t.Logf("Successfully completed %d cycles with insert/delete/iterate", cycles)
}

// TestSimpleInsertDeleteIterate_Treap tests the base in-memory Treap (no persistence)
// to isolate whether the bug is in Treap core or in persistent layers
func TestSimpleInsertDeleteIterate_Treap(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	const (
		cycles       = 100
		batchSize    = 1000
		maxNodes     = 10000
		deleteEveryN = 4
	)

	expected := make(map[int]bool)
	keys := make([]int, 0, maxNodes)

	deleteFirst := func() {
		if len(keys) == 0 {
			return
		}
		keyVal := keys[0]
		key := types.IntKey(keyVal)
		treap.Delete(key)
		delete(expected, keyVal)
		keys = keys[1:]
	}

	for cycle := 0; cycle < cycles; cycle++ {
		// Insert a batch
		startKey := cycle * batchSize
		for i := 0; i < batchSize; i++ {
			keyVal := startKey + i
			key := types.IntKey(keyVal)

			treap.Insert(key)
			expected[keyVal] = true
			keys = append(keys, keyVal)
		}

		// Deterministic deletions
		deleteCount := batchSize / deleteEveryN
		for i := 0; i < deleteCount && len(keys) > 0; i++ {
			deleteFirst()
		}

		// Cap total nodes
		for len(keys) > maxNodes {
			deleteFirst()
		}

		// Verify all expected keys exist via Search
		for k := range expected {
			key := types.IntKey(k)
			if treap.Search(key) == nil {
				t.Fatalf("cycle %d: Key %d missing from tree (expected but Search returned nil)", cycle, k)
			}
		}

		// Verify via iteration
		seen := make(map[int]bool)
		for node := range treap.Iter() {
			k := int(node.GetKey().Value())
			seen[k] = true
		}

		if len(seen) != len(expected) {
			missing := []int{}
			for k := range expected {
				if !seen[k] {
					missing = append(missing, k)
					if len(missing) >= 10 {
						break
					}
				}
			}
			t.Fatalf("cycle %d: Expected %d keys, iteration saw %d. Missing: %v", 
				cycle, len(expected), len(seen), missing)
		}

		t.Logf("cycle %d: OK (tree has %d nodes)", cycle, len(expected))
	}

	t.Logf("TestSimpleInsertDeleteIterate_Treap: Successfully completed %d cycles", cycles)
}

// TestSimpleInsertDeleteIterate_PersistentTreap tests PersistentTreap (no payloads)
// to isolate whether the bug is in PersistentPayloadTreap or in PersistentTreap core
func TestSimpleInsertDeleteIterate_PersistentTreap(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	const (
		cycles       = 100
		batchSize    = 1000
		maxNodes     = 10000
		deleteEveryN = 4
	)

	expected := make(map[int]bool)
	keys := make([]int, 0, maxNodes)

	deleteFirst := func() {
		if len(keys) == 0 {
			return
		}
		keyVal := keys[0]
		key := types.IntKey(keyVal)
		treap.Delete(&key)
		delete(expected, keyVal)
		keys = keys[1:]
	}

	for cycle := 0; cycle < cycles; cycle++ {
		// Insert a batch
		startKey := cycle * batchSize
		for i := 0; i < batchSize; i++ {
			keyVal := startKey + i
			key := types.IntKey(keyVal)

			treap.Insert(&key)
			expected[keyVal] = true
			keys = append(keys, keyVal)
		}

		// Deterministic deletions
		deleteCount := batchSize / deleteEveryN
		for i := 0; i < deleteCount && len(keys) > 0; i++ {
			deleteFirst()
		}

		// Cap total nodes
		for len(keys) > maxNodes {
			deleteFirst()
		}

		// Verify all expected keys exist via Search
		for k := range expected {
			key := types.IntKey(k)
			if treap.Search(&key) == nil {
				t.Fatalf("cycle %d: Key %d missing from tree (expected but Search returned nil)", cycle, k)
			}
		}

		// Verify via iteration
		seen := make(map[int]bool)
		err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
			k := int(*node.GetKey().(*types.IntKey))
			seen[k] = true
			return nil
		})
		if err != nil {
			t.Fatalf("cycle %d: InOrderVisit failed: %v", cycle, err)
		}

		if len(seen) != len(expected) {
			missing := []int{}
			for k := range expected {
				if !seen[k] {
					missing = append(missing, k)
					if len(missing) >= 10 {
						break
					}
				}
			}
			t.Fatalf("cycle %d: Expected %d keys, iteration saw %d. Missing: %v", 
				cycle, len(expected), len(seen), missing)
		}

		t.Logf("cycle %d: OK (tree has %d nodes)", cycle, len(expected))
	}

	t.Logf("TestSimpleInsertDeleteIterate_PersistentTreap: Successfully completed %d cycles", cycles)
}
