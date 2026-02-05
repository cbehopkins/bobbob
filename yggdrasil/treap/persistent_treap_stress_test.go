package treap

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestPersistentTreapStressRandom performs random operations (Insert/Walk/Persist/Flush)
// and validates that all inserted items can be retrieved
func TestPersistentTreapStressRandom(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Reference map to track what should be in the treap
	reference := make(map[int]bool)

	// Use fixed seed for reproducibility
	rng := rand.New(rand.NewSource(12345))

	const (
		numOperations = 1000
		maxKey        = 500 // Key space
		insertWeight  = 50  // % of operations that are inserts
		walkWeight    = 25  // % that are walks
		persistWeight = 15  // % that are persists
		flushWeight   = 10  // % that are flush operations
	)

	insertCount := 0
	walkCount := 0
	persistCount := 0
	flushCount := 0

	for i := 0; i < numOperations; i++ {
		op := rng.Intn(100)

		switch {
		case op < insertWeight:
			// Insert a random key
			key := rng.Intn(maxKey)
			keyObj := types.IntKey(key)
			treap.Insert(&keyObj)
			reference[key] = true
			insertCount++

			if insertCount%100 == 0 {
				t.Logf("After %d inserts: Reference has %d keys", insertCount, len(reference))
			}

		case op < insertWeight+walkWeight:
			// Walk and verify all reference keys are present
			walked := make(map[int]bool)

			err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
				key, ok := node.GetKey().(*types.IntKey)
				if ok {
					walked[int(*key)] = true
				}
				return nil
			})

			if err != nil {
				t.Fatalf("Walk failed after %d ops: %v", i, err)
			}

			// Verify all reference keys were walked
			for key := range reference {
				if !walked[key] {
					t.Errorf("Walk %d: Key %d in reference but not walked (walked %d/%d keys)",
						walkCount, key, len(walked), len(reference))
					t.FailNow()
				}
			}

			// Verify no extra keys were walked
			for key := range walked {
				if !reference[key] {
					t.Errorf("Walk %d: Key %d walked but not in reference", walkCount, key)
					t.FailNow()
				}
			}

			walkCount++

		case op < insertWeight+walkWeight+persistWeight:
			// Persist the tree
			err := treap.Persist()
			if err != nil {
				t.Fatalf("Persist failed after %d ops: %v", i, err)
			}
			persistCount++

			if persistCount%10 == 0 {
				t.Logf("Persist %d complete", persistCount)
			}

		default:
			// Flush oldest 20% of nodes
			if treap.root != nil {
				nodes := treap.GetInMemoryNodes()
				if len(nodes) > 0 {
					numToFlush := len(nodes) / 5 // 20%
					if numToFlush == 0 {
						numToFlush = 1
					}

					// Sort by access time and flush oldest
					// For simplicity, just flush first N
					flushed := 0
					for i := 0; i < numToFlush && i < len(nodes); i++ {
						err := nodes[i].Node.Flush()
						if err == nil {
							flushed++
						}
					}

					if flushed > 0 {
						t.Logf("Flush %d: Flushed %d/%d nodes", flushCount, flushed, numToFlush)
					}
				}
			}
			flushCount++
		}
	}

	// Final validation: walk entire tree
	t.Logf("Final validation: %d inserts, %d walks, %d persists, %d flushes",
		insertCount, walkCount, persistCount, flushCount)
	t.Logf("Reference has %d keys", len(reference))

	// Count in-memory vs flushed
	inMemory := 0
	flushed := 0
	if treap.root != nil {
		nodes := treap.GetInMemoryNodes()
		inMemory = len(nodes)
		// Estimate flushed as reference - inMemory (not exact but close)
		flushed = len(reference) - inMemory
	}
	t.Logf("Tree state: %d in-memory, ~%d flushed", inMemory, flushed)

	// Final walk to force full traversal
	walked := make(map[int]bool)

	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		key, ok := node.GetKey().(*types.IntKey)
		if ok {
			walked[int(*key)] = true
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Final walk failed: %v", err)
	}

	t.Logf("Final walk yielded %d keys", len(walked))

	// Verify completeness
	for key := range reference {
		if !walked[key] {
			t.Errorf("Final: Key %d in reference but not walked", key)
		}
	}

	if len(walked) != len(reference) {
		t.Errorf("Final: Walked %d keys but reference has %d", len(walked), len(reference))
		t.FailNow()
	}

	t.Logf("✓ All %d keys successfully walked", len(walked))
}

type postOrderChildPresence struct {
	left  bool
	right bool
}

func collectPostOrderAndPresence(root TreapNodeInterface[types.IntKey]) ([]int, map[int]postOrderChildPresence) {
	order := make([]int, 0)
	presence := make(map[int]postOrderChildPresence)

	var walk func(node TreapNodeInterface[types.IntKey])
	walk = func(node TreapNodeInterface[types.IntKey]) {
		if node == nil || node.IsNil() {
			return
		}
		pnode, ok := node.(*PersistentTreapNode[types.IntKey])
		if !ok {
			return
		}
		left := pnode.TreapNode.left
		right := pnode.TreapNode.right
		if left != nil && !left.IsNil() {
			walk(left)
		}
		if right != nil && !right.IsNil() {
			walk(right)
		}
		keyPtr, ok := pnode.GetKey().(*types.IntKey)
		if !ok || keyPtr == nil {
			return
		}
		key := int(*keyPtr)
		order = append(order, key)
		presence[key] = postOrderChildPresence{
			left:  left != nil && !left.IsNil(),
			right: right != nil && !right.IsNil(),
		}
	}

	walk(root)
	return order, presence
}

func TestPersistentTreapRangeOverTreapPostOrder(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	tr := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	const count = 250
	for i := 0; i < count; i++ {
		key := types.IntKey(i)
		tr.Insert(&key)
	}

	expectedOrder, _ := collectPostOrderAndPresence(tr.root)
	if len(expectedOrder) != count {
		t.Fatalf("expected %d nodes in post-order, got %d", count, len(expectedOrder))
	}

	actualOrder := make([]int, 0, count)
	_, err := tr.RangeOverTreapPostOrder(func(node *PersistentTreapNode[types.IntKey]) error {
		keyPtr, ok := node.GetKey().(*types.IntKey)
		if !ok || keyPtr == nil {
			return fmt.Errorf("node key is not *types.IntKey")
		}
		actualOrder = append(actualOrder, int(*keyPtr))
		return nil
	})
	if err != nil {
		t.Fatalf("RangeOverTreapPostOrder failed: %v", err)
	}

	if len(actualOrder) != len(expectedOrder) {
		t.Fatalf("post-order length mismatch: expected %d, got %d", len(expectedOrder), len(actualOrder))
	}
	for i := range expectedOrder {
		if expectedOrder[i] != actualOrder[i] {
			t.Fatalf("post-order mismatch at %d: expected %d, got %d", i, expectedOrder[i], actualOrder[i])
		}
	}
}

func TestPersistentTreapRangeOverTreapPostOrderAfterPersist(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	tr := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	const count = 250
	for i := 0; i < count; i++ {
		key := types.IntKey(i)
		tr.Insert(&key)
	}

	expectedOrder, _ := collectPostOrderAndPresence(tr.root)
	if len(expectedOrder) != count {
		t.Fatalf("expected %d nodes in post-order, got %d", count, len(expectedOrder))
	}

	if err := tr.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	actualOrder := make([]int, 0, count)
	_, err := tr.RangeOverTreapPostOrder(func(node *PersistentTreapNode[types.IntKey]) error {
		keyPtr, ok := node.GetKey().(*types.IntKey)
		if !ok || keyPtr == nil {
			return fmt.Errorf("node key is not *types.IntKey")
		}
		actualOrder = append(actualOrder, int(*keyPtr))
		return nil
	})
	if err != nil {
		t.Fatalf("RangeOverTreapPostOrder failed: %v", err)
	}

	if len(actualOrder) != len(expectedOrder) {
		t.Fatalf("post-order length mismatch: expected %d, got %d", len(expectedOrder), len(actualOrder))
	}
	for i := range expectedOrder {
		if expectedOrder[i] != actualOrder[i] {
			t.Fatalf("post-order mismatch at %d: expected %d, got %d", i, expectedOrder[i], actualOrder[i])
		}
	}
}

func TestPersistentTreapPersistValidateFlushRangeOverPostOrder(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	tr := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	const count = 250
	for i := 0; i < count; i++ {
		key := types.IntKey(i)
		tr.Insert(&key)
	}

	_, presence := collectPostOrderAndPresence(tr.root)

	if err := tr.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	if diskErrors := tr.ValidateAgainstDisk(); len(diskErrors) > 0 {
		t.Fatalf("ValidateAgainstDisk found %d errors: %v", len(diskErrors), diskErrors)
	}

	rootNode, ok := tr.root.(*PersistentTreapNode[types.IntKey])
	if !ok {
		t.Fatalf("root is not a PersistentTreapNode")
	}
	if err := rootNode.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	visited := 0
	_, err := tr.RangeOverTreapPostOrder(func(node *PersistentTreapNode[types.IntKey]) error {
		keyPtr, ok := node.GetKey().(*types.IntKey)
		if !ok || keyPtr == nil {
			return fmt.Errorf("node key is not *types.IntKey")
		}
		key := int(*keyPtr)
		visited++

		if !store.IsValidObjectId(node.objectId) {
			return fmt.Errorf("node %d has invalid objectId", key)
		}
		if node.TreapNode.left != nil || node.TreapNode.right != nil {
			return fmt.Errorf("node %d has non-nil child pointers after Flush", key)
		}

		exp, ok := presence[key]
		if !ok {
			return fmt.Errorf("node %d missing from expected presence map", key)
		}
		if exp.left && !store.IsValidObjectId(node.leftObjectId) {
			return fmt.Errorf("node %d expected valid leftObjectId", key)
		}
		if !exp.left && store.IsValidObjectId(node.leftObjectId) {
			return fmt.Errorf("node %d expected invalid leftObjectId", key)
		}
		if exp.right && !store.IsValidObjectId(node.rightObjectId) {
			return fmt.Errorf("node %d expected valid rightObjectId", key)
		}
		if !exp.right && store.IsValidObjectId(node.rightObjectId) {
			return fmt.Errorf("node %d expected invalid rightObjectId", key)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RangeOverTreapPostOrder failed: %v", err)
	}

	if visited != count {
		t.Fatalf("expected to visit %d nodes, got %d", count, visited)
	}
}

// TestPersistentTreapStressDeterministic recreates a specific failure sequence
// Use this to debug specific failure patterns found by the random test
func TestPersistentTreapStressDeterministic(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)
	reference := make(map[int]bool)

	// Helper functions for common operations
	insert := func(key int) {
		t.Logf("Insert key=%d", key)
		keyObj := types.IntKey(key)
		treap.Insert(&keyObj)
		reference[key] = true
	}

	persist := func() {
		t.Logf("Persist (in-memory: %d)", len(treap.GetInMemoryNodes()))
		err := treap.Persist()
		if err != nil {
			t.Fatalf("Persist failed: %v", err)
		}

		// Validate after persist
		if validationErrors := treap.ValidateAgainstDisk(); len(validationErrors) > 0 {
			t.Errorf("Validation errors after persist:")
			for _, err := range validationErrors {
				t.Errorf("  - %s", err)
			}
			t.FailNow()
		}
		t.Logf("  ✓ Persist validation passed")
	}

	flush := func(percent int) {
		if treap.root == nil {
			return
		}
		nodes := treap.GetInMemoryNodes()
		numToFlush := (len(nodes) * percent) / 100
		if numToFlush == 0 && len(nodes) > 0 {
			numToFlush = 1
		}

		flushed := 0
		for i := 0; i < numToFlush && i < len(nodes); i++ {
			err := nodes[i].Node.Flush()
			if err == nil {
				flushed++
			}
		}
		t.Logf("Flush %d%%: flushed %d/%d nodes (remaining in memory: %d)",
			percent, flushed, numToFlush, len(treap.GetInMemoryNodes()))

		// Validate after flush
		if validationErrors := treap.ValidateAgainstDisk(); len(validationErrors) > 0 {
			t.Errorf("Validation errors after flush:")
			for _, err := range validationErrors {
				t.Errorf("  - %s", err)
			}
			t.FailNow()
		}
		t.Logf("  ✓ Flush validation passed")
	}

	walk := func() {
		walked := make(map[int]bool)

		err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
			key, ok := node.GetKey().(*types.IntKey)
			if ok {
				walked[int(*key)] = true
			}
			return nil
		})

		if err != nil {
			t.Fatalf("Walk failed: %v", err)
		}

		t.Logf("Walk: yielded %d/%d keys", len(walked), len(reference))

		// Verify
		for key := range reference {
			if !walked[key] {
				t.Errorf("Key %d in reference but not walked", key)
			}
		}

		if len(walked) != len(reference) {
			t.Errorf("Walked %d keys but expected %d", len(walked), len(reference))

			// Show first few missing keys
			missing := []int{}
			for key := range reference {
				if !walked[key] {
					missing = append(missing, key)
					if len(missing) >= 10 {
						break
					}
				}
			}
			t.Errorf("Missing keys (first 10): %v", missing)
			t.FailNow()
		}
	}

	// Reproduce a specific failure scenario
	// Example: Insert several keys, persist, flush some, insert more, walk

	// Phase 1: Initial inserts
	for i := 0; i < 20; i++ {
		insert(i)
	}

	walk() // Should see all 20

	// Phase 2: Persist and flush half
	persist()
	flush(50)
	walk() // Should still see all 20

	// Phase 3: More inserts (will cause rotations)
	for i := 20; i < 40; i++ {
		insert(i)
	}
	walk() // Should see all 40

	// Phase 4: Persist and flush aggressively
	persist()
	flush(80)
	walk() // Should see all 40

	// Phase 5: More inserts and walks
	for i := 40; i < 60; i++ {
		insert(i)
		if i%5 == 0 {
			persist()
			flush(50)
		}
	}
	walk() // Should see all 60

	t.Logf("✓ All phases completed successfully")
}

// TestPersistentTreapBasicPersistFlush tests the most basic persist/flush cycle
func TestPersistentTreapBasicPersistFlush(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	// First, test if basic store write/read works
	testData := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	objId, err := store.WriteNewObjFromBytes(stre, testData)
	if err != nil {
		t.Fatalf("WriteNewObjFromBytes failed: %v", err)
	}
	t.Logf("Wrote test data to objectId %d", objId)

	readData, err := store.ReadBytesFromObj(stre, objId)
	if err != nil {
		t.Fatalf("ReadBytesFromObj failed: %v", err)
	}
	t.Logf("Read back %d bytes", len(readData))

	if len(readData) != len(testData) {
		t.Fatalf("Length mismatch: wrote %d bytes, read %d bytes", len(testData), len(readData))
	}

	for i := 0; i < len(testData); i++ {
		if testData[i] != readData[i] {
			t.Fatalf("Data mismatch at byte %d: wrote %d, read %d", i, testData[i], readData[i])
		}
	}

	t.Logf("✓ Basic store write/read works correctly")

	// Test writing 64 bytes (same size as treap nodes)
	testData64 := make([]byte, 64)
	for i := 0; i < 64; i++ {
		testData64[i] = byte(i + 1)
	}
	objId64, err := store.WriteNewObjFromBytes(stre, testData64)
	if err != nil {
		t.Fatalf("WriteNewObjFromBytes (64 bytes) failed: %v", err)
	}
	t.Logf("Wrote 64-byte test data to objectId %d", objId64)

	readData64, err := store.ReadBytesFromObj(stre, objId64)
	if err != nil {
		t.Fatalf("ReadBytesFromObj (64 bytes) failed: %v", err)
	}
	t.Logf("Read back %d bytes", len(readData64))

	if len(readData64) != 64 {
		t.Fatalf("Length mismatch: wrote 64 bytes, read %d bytes", len(readData64))
	}

	for i := 0; i < 64; i++ {
		if testData64[i] != readData64[i] {
			t.Fatalf("Data mismatch at byte %d: wrote %d, read %d (first 8 bytes: %v)",
				i, testData64[i], readData64[i], readData64[0:8])
		}
	}

	t.Logf("✓ 64-byte store write/read works correctly")

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Create ONE node manually and test its marshal/persist cycle
	testKey := types.IntKey(42)
	testNode := NewPersistentTreapNode[types.IntKey](&testKey, 12345, stre, treap)

	// Marshal the node
	marshaledData, err := testNode.Marshal()
	if err != nil {
		t.Fatalf("Manual marshal failed: %v", err)
	}
	t.Logf("Manually marshaled node: %d bytes, priority bytes [8-12]: %v",
		len(marshaledData), marshaledData[8:12])

	// Write it directly
	manualObjId, err := store.WriteNewObjFromBytes(stre, marshaledData)
	if err != nil {
		t.Fatalf("Manual write failed: %v", err)
	}
	t.Logf("Manually wrote to objectId %d", manualObjId)

	// Read it back
	readBackData, err := store.ReadBytesFromObj(stre, manualObjId)
	if err != nil {
		t.Fatalf("Manual read back failed: %v", err)
	}
	t.Logf("Manually read back: %d bytes, priority bytes [8-12]: %v",
		len(readBackData), readBackData[8:12])

	// Compare
	if len(marshaledData) != len(readBackData) {
		t.Fatalf("Manual: length mismatch")
	}
	for i := 0; i < len(marshaledData); i++ {
		if marshaledData[i] != readBackData[i] {
			t.Fatalf("Manual: byte %d mismatch: wrote %d, read %d", i, marshaledData[i], readBackData[i])
		}
	}
	t.Logf("✓ Manual marshal/write/read cycle works correctly")

	// Now test persist() on that same node
	err = testNode.persist()
	if err != nil {
		t.Fatalf("Node persist() failed: %v", err)
	}
	persistedObjId, _ := testNode.ObjectId()
	t.Logf("Node persist() wrote to objectId %d", persistedObjId)

	// Read it back
	persistReadData, err := store.ReadBytesFromObj(stre, persistedObjId)
	if err != nil {
		t.Fatalf("Persist read back failed: %v", err)
	}
	t.Logf("Persist read back: %d bytes, priority bytes [8-12]: %v",
		len(persistReadData), persistReadData[8:12])

	t.Logf("========")
	t.Logf("This is where the bug should be revealed!")
	t.Logf("========")

	// Insert 10 keys
	keys := make([]*types.IntKey, 10)
	for i := 0; i < 10; i++ {
		key := types.IntKey(i)
		keys[i] = &key
		treap.Insert(&key)
	}

	t.Logf("Inserted keys: %v", keys)

	// Verify all are in memory
	nodes := treap.GetInMemoryNodes()
	t.Logf("After insert: %d nodes in memory", len(nodes))
	if len(nodes) != 10 {
		t.Fatalf("Expected 10 in-memory nodes, got %d", len(nodes))
	}

	// Persist all nodes
	err = treap.Persist()
	if err != nil {
		t.Fatalf("Persist failed: %v", err)
	}
	t.Logf("After persist: all nodes should have valid objectIds")

	// Validate in-memory nodes against disk
	if validationErrors := treap.ValidateAgainstDisk(); len(validationErrors) > 0 {
		t.Errorf("Validation errors after persist:")
		for _, err := range validationErrors {
			t.Errorf("  - %s", err)
		}
		t.FailNow()
	}
	t.Logf("✓ Validation passed: in-memory data matches disk")

	// Verify nodes have valid objectIds
	nodes = treap.GetInMemoryNodes()
	for _, n := range nodes {
		objId, err := n.Node.ObjectId()
		if err != nil || !store.IsValidObjectId(objId) {
			t.Errorf("Node has invalid objectId after persist: %v (err: %v)", objId, err)
		}
	}

	// Flush 5 nodes (50%)
	nodes = treap.GetInMemoryNodes()
	flushed := 0
	for i := 0; i < 5 && i < len(nodes); i++ {
		err := nodes[i].Node.Flush()
		if err == nil {
			flushed++
		} else {
			t.Logf("Failed to flush node %d: %v", i, err)
		}
	}
	t.Logf("Flushed %d nodes", flushed)

	// Validate remaining in-memory nodes against disk after flush
	if validationErrors := treap.ValidateAgainstDisk(); len(validationErrors) > 0 {
		t.Errorf("Validation errors after flush:")
		for _, err := range validationErrors {
			t.Errorf("  - %s", err)
		}
		t.FailNow()
	}
	t.Logf("✓ Validation passed after flush")

	// Count remaining in memory
	remaining := len(treap.GetInMemoryNodes())
	t.Logf("After flush: %d nodes remain in memory", remaining)

	// Walk tree - should see all 10 keys
	walked := 0

	t.Logf("Starting walk... Tree structure:")
	if treap.root != nil {
		rootObjId, _ := treap.root.(*PersistentTreapNode[types.IntKey]).ObjectId()
		t.Logf("  Root objectId: %v", rootObjId)
	}

	err = treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		walked++
		key, ok := node.GetKey().(*types.IntKey)
		if ok {
			t.Logf("  Walked key=%d", int(*key))
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Walk failed: %v", err)
	}

	t.Logf("Walk yielded %d keys (expected 10)", walked)

	if walked != 10 {
		t.Errorf("Expected to walk 10 keys, but got %d", walked)
		t.Errorf("Flushed %d nodes, %d remain in memory, walked %d", flushed, remaining, walked)
		t.FailNow()
	}

	t.Logf("✓ Basic persist/flush/walk cycle works correctly")
}
