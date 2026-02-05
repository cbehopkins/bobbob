package treap

import (
	"fmt"
	"sync"
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

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
