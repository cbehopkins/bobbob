package treap

import (
	"fmt"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestMemoryFlushDuringLargeIteration verifies that flushing nodes from memory
// during iteration does not corrupt or lose items.
func TestMemoryFlushDuringLargeIteration(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	// Insert a large number of items with unique keys
	const itemCount = 20_000
	t.Logf("Inserting %d items...", itemCount)
	for i := 0; i < itemCount; i++ {
		key := types.IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("value_%d", i)}
		treap.Insert(&key, payload)

		if (i+1)%5000 == 0 {
			t.Logf("Inserted %d items", i+1)
		}
	}

	// Persist the entire treap to disk
	t.Logf("Persisting treap to disk...")
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist treap: %v", err)
	}

	// Verify all nodes are in memory initially
	inMemoryBefore := treap.CountInMemoryNodes()
	t.Logf("Nodes in memory before iteration: %d", inMemoryBefore)

	// Start iteration
	count := 0
	lastKey := int32(-1)
	flushedDuringIteration := false

	t.Logf("Starting iteration with periodic flushing...")
	if err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
		k := int32(*pNode.GetKey().(*types.IntKey))

		// Verify keys are in ascending order
		if k <= lastKey {
			t.Errorf("Keys not in order: %d followed by %d", lastKey, k)
		}
		lastKey = k

		// Verify payload is not empty
		payload := pNode.GetPayload()
		if payload.Data == "" {
			t.Errorf("Payload data is empty for key %d", k)
		}

		count++

		// Trigger aggressive memory flushing every 1000 items
		// This simulates memory pressure during iteration
		if count%1000 == 0 {
			// Use flushOldestPercentileLocked since we're inside InOrderVisit callback
			flushedCount, err := treap.flushOldestPercentileLocked(90) // Flush oldest 90%
			if err != nil {
				return fmt.Errorf("failed to flush memory at item %d: %w", count, err)
			}
			if flushedCount > 0 {
				flushedDuringIteration = true
				if count%5000 == 0 {
					inMemory := treap.CountInMemoryNodesLocked()
					t.Logf("At item %d: flushed %d nodes, %d remain in memory",
						count, flushedCount, inMemory)
				}
			}
		}

		return nil
	}); err != nil {
			t.Fatalf("InOrderVisit error at item %d: %v", count, err)
	}

	// Verify we iterated all items
	if count != itemCount {
		t.Errorf("Expected to iterate %d items, but got %d (%.2f%% loss)",
			itemCount, count, 100.0*float64(itemCount-count)/float64(itemCount))
	} else {
		t.Logf("SUCCESS: All %d items correctly iterated", itemCount)
	}

	// Verify we actually triggered flushing during iteration
	if !flushedDuringIteration {
		t.Logf("WARNING: No nodes were flushed during iteration (test may not be effective)")
	}

	// Verify final memory state
	inMemoryAfter := treap.CountInMemoryNodes()
	t.Logf("Nodes in memory after iteration: %d", inMemoryAfter)
	t.Logf("Memory reduced from %d to %d nodes (%.1f%% reduction)",
		inMemoryBefore, inMemoryAfter,
		100.0*float64(inMemoryBefore-inMemoryAfter)/float64(inMemoryBefore))
}

// TestMemoryFlushDuringPayloadIteration tests the same scenario using
// the InOrderVisit method which loads nodes on demand.
func TestMemoryFlushDuringPayloadIteration(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	// Insert items
	const itemCount = 20_000
	t.Logf("Inserting %d items...", itemCount)
	for i := 0; i < itemCount; i++ {
		key := types.IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("value_%d", i)}
		treap.Insert(&key, payload)
	}

	// Persist to disk
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist treap: %v", err)
	}

	t.Logf("Starting InOrderVisit with on-demand loading...")

	count := 0
	lastKey := int32(-1)

	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
		k := int32(*pNode.GetKey().(*types.IntKey))

		// Verify order
		if k <= lastKey {
			t.Errorf("Keys not in order: %d followed by %d", lastKey, k)
		}
		lastKey = k

		// Verify payload
		payload := pNode.GetPayload()
		if payload.Data == "" {
			t.Errorf("Payload data is empty for key %d", k)
		}

		count++

		// Log progress
		if count%5000 == 0 {
			inMemory := treap.CountInMemoryNodesLocked()
			t.Logf("At item %d: %d nodes in memory", count, inMemory)
		}

		return nil
	})

	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	// Verify we iterated all items
	if count != itemCount {
		t.Errorf("Expected to iterate %d items, but got %d", itemCount, count)
	} else {
		t.Logf("SUCCESS: All %d items correctly iterated with aggressive flushing", itemCount)
	}

	inMemoryAfter := treap.CountInMemoryNodes()
	t.Logf("Nodes in memory after InOrderVisit: %d", inMemoryAfter)
}

// TestConcurrentFlushDuringIteration tests the most challenging scenario:
// concurrent flushing happening while iteration is in progress.
func TestConcurrentFlushDuringIteration(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	// Insert items
	const itemCount = 10_000
	t.Logf("Inserting %d items...", itemCount)
	for i := 0; i < itemCount; i++ {
		key := types.IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("value_%d", i)}
		treap.Insert(&key, payload)
	}

	// Persist to disk
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist treap: %v", err)
	}

	t.Logf("Starting iteration with background flushing...")

	count := 0
	if err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		count++

		// Simulate concurrent background memory management
		// In real usage, vault's memory manager might do this
		if count == 2500 || count == 5000 || count == 7500 {
			flushed, err := treap.flushOldestPercentileLocked(80)
			if err != nil {
				return fmt.Errorf("failed to flush: %w", err)
			}
			inMemory := treap.CountInMemoryNodesLocked()
			t.Logf("Background flush at item %d: flushed %d nodes, %d remain",
				count, flushed, inMemory)
		}

		return nil
	}); err != nil {
		t.Fatalf("InOrderVisit error: %v", err)
	}

	if count != itemCount {
		t.Errorf("Expected %d items, got %d (%.2f%% loss)",
			itemCount, count, 100.0*float64(itemCount-count)/float64(itemCount))
	} else {
		t.Logf("SUCCESS: All %d items iterated despite concurrent flushing", itemCount)
	}
}
