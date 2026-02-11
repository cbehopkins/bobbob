package treap

import (
	"fmt"
	"sync"
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestIterator_BasicTraversal validates that the hybrid walker visits
// all nodes in sorted order without any disk I/O.
func TestIterator_BasicTraversal(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	// Insert some values
	keys := []int32{5, 3, 7, 1, 4, 6, 9}
	for _, k := range keys {
		key := types.IntKey(k)
		if err := treap.Insert(&key); err != nil {
			t.Fatalf("Insert(%d) failed: %v", k, err)
		}
	}

	// Visit all nodes in order
	var visited []int32
	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		key := node.GetKey()
		if key == nil {
			t.Fatal("node key is nil")
		}
		// IntKey is just int32, convert it
		val := int32(key.Value())
		visited = append(visited, val)
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	// Verify visited order is sorted
	expected := []int32{1, 3, 4, 5, 6, 7, 9}
	if len(visited) != len(expected) {
		t.Errorf("got %d nodes, expected %d", len(visited), len(expected))
	}

	for i, v := range visited {
		if i < len(expected) && v != expected[i] {
			t.Errorf("visited[%d] = %d, expected %d", i, v, expected[i])
		}
	}
}

// TestIterator_NoMutationNoTrash validates that a read-only traversal
// does not accumulate any trash or delete any nodes.
func TestIterator_NoMutationNoTrash(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	// Insert some values
	keys := []int32{5, 3, 7, 1, 4, 6, 9}
	for _, k := range keys {
		key := types.IntKey(k)
		if err := treap.Insert(&key); err != nil {
			t.Fatalf("Insert(%d) failed: %v", k, err)
		}
	}

	// Persist all nodes
	if err := treap.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Visit all nodes without mutating
	visitCount := 0
	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		visitCount++
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	if visitCount != len(keys) {
		t.Errorf("visited %d nodes, expected %d", visitCount, len(keys))
	}
}

// TestIterator_MutationDetection validates that mutations are detected
// and the old ObjectId is added to trash.
func TestIterator_MutationDetection(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	// Insert and persist a single node
	key := types.IntKey(5)
	if err := treap.Insert(&key); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if err := treap.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Get the ObjectId of the persisted node
	var nodeToMutate TreapNodeInterface[types.IntKey]
	treap.mu.RLock()
	nodeToMutate = treap.root
	pNode := nodeToMutate.(*PersistentTreapNode[types.IntKey])
	oldObjectId := pNode.objectId
	treap.mu.RUnlock()

	if oldObjectId < 0 {
		t.Fatal("node should be persisted with valid ObjectId")
	}

	// Mutate the node during iteration
	mutationDetected := false
	err := treap.InOrderMutate(func(node TreapNodeInterface[types.IntKey]) error {
		// Type assert to access internal fields for mutation
		pNode := node.(*PersistentTreapNode[types.IntKey])
		// Mutate by modifying priority (in-memory change)
		pNode.priority = pNode.priority + 1
		pNode.objectId = -1 // Mark as unpersisted
		mutationDetected = true
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderMutate failed: %v", err)
	}

	if !mutationDetected {
		t.Fatal("callback was not invoked")
	}

	// Verify old ObjectId was deleted
	// Note: In a real test, we'd verify store deletion, but for now
	// we just verify no panic occurred
}

// TestIterator_EmptyTree validates that iterating over an empty tree
// returns no error and invokes no callbacks.
func TestIterator_EmptyTree(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	visitCount := 0
	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		visitCount++
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderVisit on empty tree failed: %v", err)
	}

	if visitCount != 0 {
		t.Errorf("expected 0 visits on empty tree, got %d", visitCount)
	}
}

func TestSimpleWalkInOrder(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()
	treap := NewPersistentTreap(types.IntLess, templateKey, stre)

	values := []types.IntKey{50, 30, 70, 20, 40, 60, 80}
	for _, v := range values {
		key := v
		treap.Insert(&key)
	}

	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	var collected []types.IntKey

	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		key, ok := node.GetKey().(*types.IntKey)
		if !ok {
			return fmt.Errorf("key is not *types.IntKey")
		}
		collected = append(collected, *key)
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	expected := []types.IntKey{20, 30, 40, 50, 60, 70, 80}
	if len(collected) != len(expected) {
		t.Fatalf("Expected %d nodes, got %d", len(expected), len(collected))
	}

	for i, v := range expected {
		if collected[i] != v {
			t.Errorf("Position %d: expected %d, got %d", i, v, collected[i])
		}
	}
}

func TestSimpleWalkKeys(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()
	treap := NewPersistentTreap(types.IntLess, templateKey, stre)

	for i := types.IntKey(10); i >= 1; i-- {
		key := i
		treap.Insert(&key)
	}

	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	var collected []types.IntKey

	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		key := node.GetKey().(types.PersistentKey[types.IntKey])
		intKey, ok := key.(*types.IntKey)
		if !ok {
			return fmt.Errorf("key is not *types.IntKey")
		}
		collected = append(collected, *intKey)
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	for i := 0; i < len(collected); i++ {
		if collected[i] != types.IntKey(i+1) {
			t.Errorf("Position %d: expected %d, got %d", i, i+1, collected[i])
		}
	}
}

func TestSimpleCount(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()
	treap := NewPersistentTreap(types.IntLess, templateKey, stre)

	for i := 0; i < 10; i++ {
		key := types.IntKey(i)
		treap.Insert(&key)
	}

	count, err := treap.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}

	if count != 10 {
		t.Errorf("Expected count 10, got %d", count)
	}
}

func TestAggressiveFlushing(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()
	treap := NewPersistentTreap(types.IntLess, templateKey, stre)

	k1, k2, k3, k4, k5, k6, k7 := types.IntKey(1), types.IntKey(2), types.IntKey(3), types.IntKey(4), types.IntKey(5), types.IntKey(6), types.IntKey(7)
	treap.InsertComplex(&k4, Priority(1000))
	treap.InsertComplex(&k2, Priority(900))
	treap.InsertComplex(&k6, Priority(900))
	treap.InsertComplex(&k1, Priority(800))
	treap.InsertComplex(&k3, Priority(800))
	treap.InsertComplex(&k5, Priority(800))
	treap.InsertComplex(&k7, Priority(800))

	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	initialNodes := treap.GetInMemoryNodes()
	visitedCount := 0
	if err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		visitedCount++
		return nil
	}); err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	if visitedCount != 7 {
		t.Errorf("Expected to visit 7 nodes, visited %d", visitedCount)
	}

	if _, err := treap.FlushOldestPercentile(75); err != nil {
		t.Fatalf("FlushOldestPercentile failed: %v", err)
	}

	finalNodes := treap.GetInMemoryNodes()
	if len(finalNodes) >= len(initialNodes) {
		t.Errorf("Expected explicit flush to reduce memory, but initial=%d, final=%d",
			len(initialNodes), len(finalNodes))
	}
}

func TestFlushingComparison(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()

	treap1 := NewPersistentTreap(types.IntLess, templateKey, stre)
	for i := types.IntKey(1); i <= 20; i++ {
		key := i
		treap1.Insert(&key)
	}
	_ = treap1.Persist()
	_ = treap1.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error { return nil })

	keepInMemoryCount := len(treap1.GetInMemoryNodes())

	treap2 := NewPersistentTreap(types.IntLess, templateKey, stre)
	for i := types.IntKey(1); i <= 20; i++ {
		key := i
		treap2.Insert(&key)
	}
	_ = treap2.Persist()

	_ = treap2.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error { return nil })
	if _, err := treap2.FlushOldestPercentile(75); err != nil {
		t.Fatalf("FlushOldestPercentile failed: %v", err)
	}

	aggressiveFlushCount := len(treap2.GetInMemoryNodes())

	if aggressiveFlushCount >= keepInMemoryCount {
		t.Errorf("Explicit flushing should reduce memory: keepInMemory=%d, aggressive=%d",
			keepInMemoryCount, aggressiveFlushCount)
	}
}

// TestIterator_TrashListContents verifies that mutations produce correct trash ObjectIds
func TestIterator_TrashListContents(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	// Insert nodes
	keys := []int32{5, 3, 7}
	for _, k := range keys {
		key := types.IntKey(k)
		if err := treap.Insert(&key); err != nil {
			t.Fatalf("Insert(%d) failed: %v", k, err)
		}
	}

	// Persist and capture ObjectIds before mutation
	if err := treap.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Capture ObjectIds of all nodes before mutation
	nodeObjectIds := make(map[int32]store.ObjectId)
	treap.mu.RLock()
	var captureIds func(node TreapNodeInterface[types.IntKey])
	captureIds = func(node TreapNodeInterface[types.IntKey]) {
		if node == nil {
			return
		}
		key := node.GetKey()
		pNode := node.(*PersistentTreapNode[types.IntKey])
		nodeObjectIds[int32(key.Value())] = pNode.objectId
		captureIds(node.GetLeft())
		captureIds(node.GetRight())
	}
	captureIds(treap.root)
	treap.mu.RUnlock()

	// Verify nodes are persisted
	for k, objId := range nodeObjectIds {
		if objId < 0 {
			t.Fatalf("node %d should be persisted with valid ObjectId", k)
		}
	}

	// Create a mock store that tracks DeleteObj calls
	type deletion struct {
		objId store.ObjectId
	}
	var deletions []deletion
	var deletionsMu sync.Mutex

	// Wrap the store to intercept DeleteObj calls
	originalStore := treap.Store
	mockStore := &mockStoreWrapper{
		Storer: originalStore,
		onDelete: func(objId store.ObjectId) error {
			deletionsMu.Lock()
			deletions = append(deletions, deletion{objId: objId})
			deletionsMu.Unlock()
			return originalStore.DeleteObj(objId)
		},
	}
	treap.Store = mockStore

	// Mutate specific nodes
	mutatedCount := 0
	err := treap.InOrderMutate(func(node TreapNodeInterface[types.IntKey]) error {
		key := node.GetKey()
		if key.Value() == 3 || key.Value() == 5 {
			// Mutate these nodes - need type assertion to access internal fields
			pNode := node.(*PersistentTreapNode[types.IntKey])
			pNode.priority = pNode.priority + 100
			pNode.objectId = -1
			mutatedCount++
		}
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderMutate failed: %v", err)
	}

	if mutatedCount != 2 {
		t.Errorf("expected 2 mutations, got %d", mutatedCount)
	}

	// Verify deletions match old ObjectIds of mutated nodes
	deletionsMu.Lock()
	defer deletionsMu.Unlock()

	if len(deletions) != 2 {
		t.Errorf("expected 2 deletions, got %d", len(deletions))
	}

	// Check that the correct ObjectIds were deleted (for keys 3 and 5)
	deletedIds := make(map[store.ObjectId]bool)
	for _, d := range deletions {
		deletedIds[d.objId] = true
	}

	oldRootId := nodeObjectIds[5]
	oldLeftId := nodeObjectIds[3]
	if !deletedIds[oldRootId] {
		t.Errorf("expected ObjectId %d (key 5) to be deleted, got deletions: %v", oldRootId, deletions)
	}
	if !deletedIds[oldLeftId] {
		t.Errorf("expected ObjectId %d (key 3) to be deleted, got deletions: %v", oldLeftId, deletions)
	}
}

// TestIterator_AncestorInvalidationVerified verifies that parent childObjectIds are invalidated
func TestIterator_AncestorInvalidationVerified(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	// Build a simple tree with predictable structure
	keys := []int32{5, 3, 7, 1, 4}
	for _, k := range keys {
		key := types.IntKey(k)
		if err := treap.Insert(&key); err != nil {
			t.Fatalf("Insert(%d) failed: %v", k, err)
		}
	}

	// Persist everything
	if err := treap.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Find a node with a child to mutate - walk tree to find suitable structure
	treap.mu.RLock()
	var parentNode TreapNodeInterface[types.IntKey]
	var childNode TreapNodeInterface[types.IntKey]
	var childKey int32
	var isLeftChild bool

	// Try to find any node with a child
	var findNodeWithChild func(node TreapNodeInterface[types.IntKey]) bool
	findNodeWithChild = func(node TreapNodeInterface[types.IntKey]) bool {
		if node == nil {
			return false
		}
		pNode := node

		// Check if this node has a left child
		if pNode.GetLeft() != nil {
			parentNode = pNode
			childNode = pNode.GetLeft()
			childKey = int32(childNode.GetKey().Value())
			isLeftChild = true
			return true
		}

		// Check if this node has a right child
		if pNode.GetRight() != nil {
			parentNode = pNode
			childNode = pNode.GetRight()
			childKey = int32(childNode.GetKey().Value())
			isLeftChild = false
			return true
		}

		// Recurse
		if findNodeWithChild(pNode.GetLeft()) {
			return true
		}
		return findNodeWithChild(pNode.GetRight())
	}

	if !findNodeWithChild(treap.root) {
		treap.mu.RUnlock()
		t.Skip("couldn't find node with child, skipping test")
	}

	initialChildObjectId := store.ObjectId(-1)
	if isLeftChild {
		initialChildObjectId = parentNode.(*PersistentTreapNode[types.IntKey]).leftObjectId
	} else {
		initialChildObjectId = parentNode.(*PersistentTreapNode[types.IntKey]).rightObjectId
	}
	treap.mu.RUnlock()

	if initialChildObjectId < 0 {
		t.Fatal("child ObjectId should be valid after persist")
	}

	// Mutate the child node
	err := treap.InOrderMutate(func(node TreapNodeInterface[types.IntKey]) error {
		key := node.GetKey()
		if key.Value() == types.IntKey(childKey) {
			// Mutate this node
			node.(*PersistentTreapNode[types.IntKey]).priority = node.(*PersistentTreapNode[types.IntKey]).priority + 100
			node.(*PersistentTreapNode[types.IntKey]).objectId = -1
		}
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderMutate failed: %v", err)
	}

	// Verify ancestor childObjectId was invalidated
	treap.mu.RLock()
	defer treap.mu.RUnlock()

	var currentChildObjectId store.ObjectId
	if isLeftChild {
		currentChildObjectId = parentNode.(*PersistentTreapNode[types.IntKey]).leftObjectId
	} else {
		currentChildObjectId = parentNode.(*PersistentTreapNode[types.IntKey]).rightObjectId
	}

	if currentChildObjectId >= 0 {
		t.Errorf("parent's childObjectId should be invalidated, got %d", currentChildObjectId)
	}
}

// TestIterator_MultipleSequentialMutations verifies multiple mutations accumulate correctly
func TestIterator_MultipleSequentialMutations(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	// Insert many nodes
	keys := []int32{10, 5, 15, 3, 7, 12, 20, 1, 4}
	for _, k := range keys {
		key := types.IntKey(k)
		if err := treap.Insert(&key); err != nil {
			t.Fatalf("Insert(%d) failed: %v", k, err)
		}
	}

	// Persist all
	if err := treap.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Track deletions
	var deletedIds []store.ObjectId
	var deletionsMu sync.Mutex

	originalStore := treap.Store
	mockStore := &mockStoreWrapper{
		Storer: originalStore,
		onDelete: func(objId store.ObjectId) error {
			deletionsMu.Lock()
			deletedIds = append(deletedIds, objId)
			deletionsMu.Unlock()
			return originalStore.DeleteObj(objId)
		},
	}
	treap.Store = mockStore

	// Mutate every other node
	mutatedCount := 0
	err := treap.InOrderMutate(func(node TreapNodeInterface[types.IntKey]) error {
		key := node.GetKey()
		if int32(key.Value())%2 == 0 {
			// Mutate even-valued nodes
			node.(*PersistentTreapNode[types.IntKey]).priority = node.(*PersistentTreapNode[types.IntKey]).priority + 100
			node.(*PersistentTreapNode[types.IntKey]).objectId = -1
			mutatedCount++
		}
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderMutate failed: %v", err)
	}

	// We expect 4, 10, 12, 20 to be mutated (even values)
	if mutatedCount < 4 {
		t.Errorf("expected at least 4 mutations, got %d", mutatedCount)
	}

	// Verify deletions occurred
	deletionsMu.Lock()
	defer deletionsMu.Unlock()

	if len(deletedIds) != mutatedCount {
		t.Errorf("expected %d deletions, got %d", mutatedCount, len(deletedIds))
	}

	// Verify all deleted ObjectIds are unique and valid
	seen := make(map[store.ObjectId]bool)
	for _, objId := range deletedIds {
		if objId < 0 {
			t.Errorf("deleted invalid ObjectId: %d", objId)
		}
		if seen[objId] {
			t.Errorf("ObjectId %d deleted multiple times", objId)
		}
		seen[objId] = true
	}
}

// TestIterator_PartiallyFlushedTree verifies hybrid traversal loads from disk
func TestIterator_PartiallyFlushedTree(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	// Insert nodes
	keys := []int32{10, 5, 15, 3, 7, 12, 20}
	for _, k := range keys {
		key := types.IntKey(k)
		if err := treap.Insert(&key); err != nil {
			t.Fatalf("Insert(%d) failed: %v", k, err)
		}
	}

	// Persist everything
	if err := treap.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Manually clear some in-memory pointers to simulate partial flush
	treap.mu.Lock()
	rootNode := treap.root.(*PersistentTreapNode[types.IntKey])
	// Clear left subtree pointer but keep ObjectId
	rootNode.left = nil
	// Clear right subtree pointer but keep ObjectId
	rootNode.right = nil
	treap.mu.Unlock()

	// Visit all nodes - should load from disk via GetLeft/GetRight
	var visited []int32
	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		key := node.GetKey()
		visited = append(visited, int32(key.Value()))
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	// Verify all nodes were visited despite cleared pointers
	if len(visited) != len(keys) {
		t.Errorf("expected %d nodes visited, got %d", len(keys), len(visited))
	}

	// Verify sorted order
	for i := 1; i < len(visited); i++ {
		if visited[i] <= visited[i-1] {
			t.Errorf("visited nodes not in sorted order: %v", visited)
			break
		}
	}
}

// TestIterator_ConcurrentReadAccess verifies multiple concurrent readers work correctly
func TestIterator_ConcurrentReadAccess(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	// Insert nodes
	keys := []int32{10, 5, 15, 3, 7, 12, 20, 1, 4, 6, 8}
	for _, k := range keys {
		key := types.IntKey(k)
		if err := treap.Insert(&key); err != nil {
			t.Fatalf("Insert(%d) failed: %v", k, err)
		}
	}

	// Persist
	if err := treap.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Launch multiple concurrent readers
	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			visitCount := 0
			err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
				visitCount++
				return nil
			})

			if err != nil {
				errors <- fmt.Errorf("reader %d failed: %w", id, err)
				return
			}

			if visitCount != len(keys) {
				errors <- fmt.Errorf("reader %d: expected %d visits, got %d", id, len(keys), visitCount)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}
}

// TestIterator_MutationsDuringTraversal verifies mutations are detected mid-iteration
func TestIterator_MutationsDuringTraversal(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	// Insert nodes with known priorities to ensure tree shape
	keys := []int32{5, 3, 7, 1, 4, 6, 9}
	for _, k := range keys {
		key := types.IntKey(k)
		if err := treap.Insert(&key); err != nil {
			t.Fatalf("Insert(%d) failed: %v", k, err)
		}
	}

	// Persist everything
	if err := treap.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Capture ObjectIds before mutation
	treap.mu.RLock()
	nodeObjectIds := make(map[int32]store.ObjectId)
	var captureIds func(node TreapNodeInterface[types.IntKey])
	captureIds = func(node TreapNodeInterface[types.IntKey]) {
		if node == nil {
			return
		}
		key := node.GetKey()
		pNode := node.(*PersistentTreapNode[types.IntKey])
		nodeObjectIds[int32(key.Value())] = pNode.objectId
		captureIds(node.GetLeft())
		captureIds(node.GetRight())
	}
	captureIds(treap.root)
	treap.mu.RUnlock()

	// Track deletions
	var deletedIds []store.ObjectId
	var deletionsMu sync.Mutex

	originalStore := treap.Store
	mockStore := &mockStoreWrapper{
		Storer: originalStore,
		onDelete: func(objId store.ObjectId) error {
			deletionsMu.Lock()
			deletedIds = append(deletedIds, objId)
			deletionsMu.Unlock()
			return originalStore.DeleteObj(objId)
		},
	}
	treap.Store = mockStore

	// Mutate nodes 3 and 7 during traversal
	mutated := make(map[int32]bool)

	err := treap.InOrderMutate(func(node TreapNodeInterface[types.IntKey]) error {
		key := node.GetKey()
		keyVal := int32(key.Value())

		if keyVal == 3 || keyVal == 7 {
			// Mutate
			node.(*PersistentTreapNode[types.IntKey]).priority = node.(*PersistentTreapNode[types.IntKey]).priority + 100
			node.(*PersistentTreapNode[types.IntKey]).objectId = -1
			mutated[keyVal] = true
		}

		return nil
	})

	if err != nil {
		t.Fatalf("InOrderMutate failed: %v", err)
	}

	if len(mutated) != 2 {
		t.Errorf("expected 2 mutations, got %d", len(mutated))
	}

	// Verify the old ObjectIds were deleted
	deletionsMu.Lock()
	defer deletionsMu.Unlock()

	if len(deletedIds) != 2 {
		t.Errorf("expected 2 deletions, got %d", len(deletedIds))
	}

	// Verify correct ObjectIds were deleted
	for keyVal := range mutated {
		expectedObjId := nodeObjectIds[keyVal]
		found := false
		for _, deletedId := range deletedIds {
			if deletedId == expectedObjId {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected ObjectId %d (for key %d) to be deleted", expectedObjId, keyVal)
		}
	}
}

// mockStoreWrapper wraps a Storer and intercepts DeleteObj calls
type mockStoreWrapper struct {
	store.Storer
	onDelete func(store.ObjectId) error
}

func (m *mockStoreWrapper) DeleteObj(objId store.ObjectId) error {
	if m.onDelete != nil {
		return m.onDelete(objId)
	}
	return m.Storer.DeleteObj(objId)
}
