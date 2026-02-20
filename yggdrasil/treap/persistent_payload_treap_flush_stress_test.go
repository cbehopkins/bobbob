package treap

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestPersistentPayloadTreapFlushInsertInterleaved tests interleaving Flush and Insert operations
// for payload treaps to catch potential issues with state inconsistency when nodes are flushed and then
// the tree is modified.
func TestPersistentPayloadTreapFlushInsertInterleaved(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](types.IntLess, keyTemplate, stre)

	// Phase 1: Build initial tree
	const initialSize = 100
	keys := make([]int, initialSize)
	for i := range initialSize {
		keys[i] = i * 10
		keyObj := types.IntKey(keys[i])
		payload := MockPayload{Data: fmt.Sprintf("%d", keys[i]*100)}
		treap.Insert(&keyObj, payload)
	}

	// Persist the tree
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist initial tree: %v", err)
	}

	// Phase 2: Interleave flush and insert operations
	const numRounds = 50
	for round := range numRounds {
		// Flush the tree
		rootNode, ok := treap.root.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
		if !ok {
			t.Fatalf("Round %d: root is not a PersistentPayloadTreapNode", round)
		}
		if err := rootNode.Flush(); err != nil {
			t.Logf("Round %d: Flush returned error (may be partial): %v", round, err)
		}

		// Verify nodes were flushed (children should be nil)
		if rootNode.TreapNode.left != nil || rootNode.TreapNode.right != nil {
			t.Errorf("Round %d: Root node has non-nil children after flush", round)
		}

		// Insert new keys with payloads
		newKey := initialSize*10 + round*2
		keyObj := types.IntKey(newKey)
		payload := MockPayload{Data: fmt.Sprintf("%d", newKey*100)}
		treap.Insert(&keyObj, payload)
		keys = append(keys, newKey)

		// Persist after insert
		if err := treap.Persist(); err != nil {
			t.Fatalf("Round %d: Failed to persist after insert: %v", round, err)
		}

		// Search for a random existing key (will trigger load from disk)
		searchIdx := round % len(keys)
		searchKey := types.IntKey(keys[searchIdx])
		var foundPayload MockPayload
		result, err := treap.SearchComplex(&searchKey, func(node TreapNodeInterface[types.IntKey]) error {
			foundPayload = node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload]).GetPayload()
			return nil
		})
		if err != nil {
			t.Errorf("Round %d: Search failed for key %d: %v", round, keys[searchIdx], err)
		} else if result == nil || result.IsNil() {
			t.Errorf("Round %d: Failed to find key %d after flush+insert", round, keys[searchIdx])
		} else {
			// Verify payload
			expectedData := fmt.Sprintf("%d", keys[searchIdx]*100)
			if foundPayload.Data != expectedData {
				t.Errorf("Round %d: Payload mismatch for key %d: got %q, expected %q", round, keys[searchIdx], foundPayload.Data, expectedData)
			}
		}

		// Verify the newly inserted key
		var insertedPayload MockPayload
		result, err = treap.SearchComplex(&keyObj, func(node TreapNodeInterface[types.IntKey]) error {
			insertedPayload = node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload]).GetPayload()
			return nil
		})
		if err != nil {
			t.Errorf("Round %d: Search failed for newly inserted key %d: %v", round, newKey, err)
		} else if result == nil || result.IsNil() {
			t.Errorf("Round %d: Failed to find newly inserted key %d", round, newKey)
		} else {
			expectedData := fmt.Sprintf("%d", newKey*100)
			if insertedPayload.Data != expectedData {
				t.Errorf("Round %d: Payload mismatch for newly inserted key %d: got %q, expected %q", round, newKey, insertedPayload.Data, expectedData)
			}
		}
	}

	// Phase 3: Final validation - ensure all keys are present with correct payloads
	for i, k := range keys {
		searchKey := types.IntKey(k)
		var payload MockPayload
		result, err := treap.SearchComplex(&searchKey, func(node TreapNodeInterface[types.IntKey]) error {
			payload = node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload]).GetPayload()
			return nil
		})
		if err != nil {
			t.Errorf("Final validation failed for key %d (index %d): %v", k, i, err)
		} else if result == nil || result.IsNil() {
			t.Errorf("Final validation failed for key %d (index %d)", k, i)
		} else {
			expectedData := fmt.Sprintf("%d", k*100)
			if payload.Data != expectedData {
				t.Errorf("Final validation: Payload mismatch for key %d: got %q, expected %q", k, payload.Data, expectedData)
			}
		}
	}
}

// TestPersistentPayloadTreapFlushInsertConcurrent tests concurrent flush and insert operations
// This is a more aggressive test that attempts to trigger race conditions with payload treaps.
func TestPersistentPayloadTreapFlushInsertConcurrent(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](types.IntLess, keyTemplate, stre)

	// Build initial tree
	const initialSize = 50
	for i := 0; i < initialSize; i++ {
		keyObj := types.IntKey(i * 10)
		payload := MockPayload{Data: fmt.Sprintf("%d", i*1000)}
		treap.Insert(&keyObj, payload)
	}

	// Persist the tree
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist initial tree: %v", err)
	}

	// Concurrent operations
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// Goroutine 1: Insert new keys with payloads
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			keyObj := types.IntKey(1000 + i)
			payload := MockPayload{Data: fmt.Sprintf("%d", (1000+i)*100)}
			treap.Insert(&keyObj, payload)

			// Occasionally persist
			if i%10 == 0 {
				if err := treap.Persist(); err != nil {
					errChan <- fmt.Errorf("Insert goroutine persist failed: %w", err)
					return
				}
			}
		}
	}()

	// Goroutine 2: Flush periodically
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			treap.mu.Lock()
			rootNode, ok := treap.root.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
			if !ok {
				treap.mu.Unlock()
				continue
			}
			if err := rootNode.Flush(); err != nil {
				t.Logf("Flush goroutine encountered error (may be partial): %v", err)
			}
			treap.mu.Unlock()
		}
	}()

	// Goroutine 3: Search for existing keys (triggers lazy loading)
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < 100; i++ {
			searchKey := types.IntKey(rng.Intn(initialSize) * 10)
			var payload MockPayload
			result, err := treap.SearchComplex(&searchKey, func(node TreapNodeInterface[types.IntKey]) error {
				payload = node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload]).GetPayload()
				return nil
			})
			if err != nil {
				errChan <- fmt.Errorf("Search goroutine: search failed for key %d: %v", searchKey, err)
				return
			}
			if result == nil || result.IsNil() {
				errChan <- fmt.Errorf("Search goroutine: key %d not found", searchKey)
				return
			}
			// Verify payload
			keyInt := int(searchKey)
			expectedData := fmt.Sprintf("%d", (keyInt/10)*1000)
			if payload.Data != expectedData {
				errChan <- fmt.Errorf("Search goroutine: Payload mismatch for key %d: got %q, expected %q", keyInt, payload.Data, expectedData)
				return
			}
		}
	}()

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		t.Errorf("Concurrent test error: %v", err)
	}

	// Final validation
	for i := 0; i < initialSize; i++ {
		keyObj := types.IntKey(i * 10)
		var payload MockPayload
		result, err := treap.SearchComplex(&keyObj, func(node TreapNodeInterface[types.IntKey]) error {
			payload = node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload]).GetPayload()
			return nil
		})
		if err != nil {
			t.Errorf("Final validation search failed for key %d: %v", i*10, err)
		} else if result == nil || result.IsNil() {
			t.Errorf("Final validation failed for key %d", i*10)
		} else {
			expectedData := fmt.Sprintf("%d", i*1000)
			if payload.Data != expectedData {
				t.Errorf("Final validation: Payload mismatch for key %d: got %q, expected %q", i*10, payload.Data, expectedData)
			}
		}
	}
}
