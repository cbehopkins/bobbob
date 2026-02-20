package treap

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// exampleCustomKey is a custom test key type used only in treap tests
type exampleCustomKey struct {
	ID   int
	Name string
}

func (k exampleCustomKey) Value() exampleCustomKey { return k }
func (k exampleCustomKey) SizeInBytes() int        { return 8 }

func (k exampleCustomKey) Marshal() ([]byte, error) {
	return json.Marshal(k)
}

func (k *exampleCustomKey) Unmarshal(data []byte) error {
	return json.Unmarshal(data, k)
}

func (k exampleCustomKey) Equals(other exampleCustomKey) bool {
	return k.ID == other.ID && k.Name == other.Name
}

func customKeyLess(a, b exampleCustomKey) bool {
	if a.ID == b.ID {
		return a.Name < b.Name
	}
	return a.ID < b.ID
}

func countTreapNodes[T any](t *Treap[T]) int {
	count := 0
	t.Walk(func(node TreapNodeInterface[T]) {
		if node != nil && !node.IsNil() {
			count++
		}
	})
	return count
}

// TestTreap verifies basic treap operations: inserting keys, searching for existing
// and non-existent keys, updating priorities, and using SearchComplex with callbacks.
func TestTreap(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	keys := []types.IntKey{10, 20, 15, 5, 30}

	for _, key := range keys {
		treap.Insert(key)
	}

	for _, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %d in the treap, but it was not found", key)
		} else if node.GetKey().(types.IntKey) != key {
			t.Errorf("Expected to find key %d, but found key %d instead", key, node.GetKey().(types.IntKey))
		}
	}

	nonExistentKeys := []types.IntKey{1, 3, 6}

	for _, key := range nonExistentKeys {
		node := treap.Search(key)
		if node != nil && !node.IsNil() {
			t.Errorf("Expected not to find key %d in the treap, but it was found", key)
		}
	}

	// Test UpdatePriority
	keyToUpdate := keys[2]
	newPriority := Priority(200)
	if err := treap.UpdatePriority(keyToUpdate, newPriority); err != nil {
		t.Fatalf("UpdatePriority returned error: %v", err)
	}
	updatedNode := treap.Search(keyToUpdate)
	if updatedNode == nil {
		t.Errorf("Expected to find key %d in the treap after updating priority, but it was not found", keyToUpdate)
	} else if updatedNode.GetPriority() != newPriority {
		t.Errorf("Expected priority %d, but got %d", newPriority, updatedNode.GetPriority())
	}

	// Test SearchComplex with callback
	var accessedNodes []types.IntKey
	callback := func(node TreapNodeInterface[types.IntKey]) error {
		if node != nil && !node.IsNil() {
			accessedNodes = append(accessedNodes, node.GetKey().(types.IntKey))
		}
		return nil
	}

	searchKey := types.IntKey(15)
	foundNode, err := treap.SearchComplex(searchKey, callback)
	if err != nil {
		t.Errorf("Unexpected error from SearchComplex: %v", err)
	}
	if foundNode == nil {
		t.Errorf("Expected to find key %d in the treap using SearchComplex", searchKey)
	}
	if len(accessedNodes) == 0 {
		t.Errorf("Expected callback to be called at least once during SearchComplex")
	}
	// Verify the callback was called with the searched key
	found := false
	for _, k := range accessedNodes {
		if k == searchKey {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected callback to be called with key %d, but it was not in the accessed nodes: %v", searchKey, accessedNodes)
	}
}

// TestTreapSearchComplexWithError verifies that SearchComplex properly propagates
// errors returned from the callback function, allowing searches to be aborted.
func TestTreapSearchComplexWithError(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	keys := []types.IntKey{10, 20, 15, 5, 30}
	for _, key := range keys {
		treap.Insert(key)
	}

	// Test that callback error aborts the search
	var accessedCount int
	expectedError := errors.New("custom error from callback")
	callback := func(node TreapNodeInterface[types.IntKey]) error {
		accessedCount++
		// Always return error to test error handling
		return expectedError
	}

	searchKey := types.IntKey(5) // Search for a key that exists
	foundNode, err := treap.SearchComplex(searchKey, callback)

	// Should get the error we returned
	if err == nil {
		t.Errorf("Expected error from callback, but got nil")
	}
	if err != expectedError {
		t.Errorf("Expected error %v, but got %v", expectedError, err)
	}

	// Should not have found the node due to error
	if foundNode != nil {
		t.Errorf("Expected nil node when callback returns error, but got node with key %v", foundNode.GetKey())
	}

	// Should have accessed at least 1 node before error (the root)
	if accessedCount < 1 {
		t.Errorf("Expected at least 1 node access, but got %d", accessedCount)
	}
}

// TestPayloadTreap verifies that a treap can store key-value pairs (payloads),
// allowing insertion and retrieval of both keys and associated data.
func TestPayloadTreap(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	keys := []types.IntKey{10, 20, 15, 5, 30}

	payloads := []string{"ten", "twenty", "fifteen", "five", "thirty"}
	for i, key := range keys {
		treap.Insert(key, payloads[i])
	}

	for i, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %d in the treap, but it was not found", key)
		} else if node.GetKey().(types.IntKey) != key {
			t.Errorf("Expected to find key %d, but found key %d instead", key, node.GetKey().(types.IntKey))
		} else if node.(*PayloadTreapNode[types.IntKey, string]).payload != payloads[i] {
			t.Errorf("Expected to find payload %s, but found payload %s instead", payloads[i], node.(*PayloadTreapNode[types.IntKey, string]).payload)
		}
	}

	nonExistentKeys := []types.IntKey{1, 25, 35}

	for _, key := range nonExistentKeys {
		node := treap.Search(key)
		if node != nil && !node.IsNil() {
			t.Errorf("Expected not to find key %d in the treap, but it was found", key)
		}
	}
}

// TestPayloadTreapUpdateOnDuplicate verifies that inserting an existing key updates its payload.
func TestPayloadTreapUpdateOnDuplicate(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)
	key := types.IntKey(42)

	treap.Insert(key, "first")
	treap.Insert(key, "second")

	node := treap.Search(key)
	if node == nil || node.IsNil() {
		t.Fatalf("Expected to find key %d in the treap", key)
	}
	if payloadNode, ok := node.(*PayloadTreapNode[types.IntKey, string]); ok {
		if payloadNode.GetPayload() != "second" {
			t.Fatalf("Expected payload to be updated to 'second', got '%s'", payloadNode.GetPayload())
		}
	} else {
		t.Fatalf("Expected PayloadTreapNode, got %T", node)
	}
}

// TestPayloadTreapDelete verifies that deleting keys removes nodes from the treap.
func TestPayloadTreapDelete(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)
	keys := []types.IntKey{1, 2, 3, 4, 5}
	for _, key := range keys {
		treap.Insert(key, "payload")
	}

	treap.Delete(types.IntKey(3))
	if node := treap.Search(types.IntKey(3)); node != nil && !node.IsNil() {
		t.Fatalf("Expected key 3 to be deleted, but it was found")
	}

	if node := treap.Search(types.IntKey(2)); node == nil || node.IsNil() {
		t.Fatalf("Expected key 2 to remain after delete")
	}
}

// TestPayloadTreapUpdatePayload verifies that UpdatePayload changes the stored payload.
func TestPayloadTreapUpdatePayload(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)
	key := types.IntKey(7)

	treap.Insert(key, "before")
	if err := treap.UpdatePayload(key, "after"); err != nil {
		t.Fatalf("Expected UpdatePayload to succeed, got error: %v", err)
	}

	node := treap.Search(key)
	if node == nil || node.IsNil() {
		t.Fatalf("Expected to find key %d in the treap", key)
	}
	if payloadNode, ok := node.(*PayloadTreapNode[types.IntKey, string]); ok {
		if payloadNode.GetPayload() != "after" {
			t.Fatalf("Expected payload to be updated to 'after', got '%s'", payloadNode.GetPayload())
		}
	} else {
		t.Fatalf("Expected PayloadTreapNode, got %T", node)
	}
}

// TestStringKeyTreap verifies that treaps work correctly with string keys,
// properly maintaining order and allowing search operations.
func TestStringKeyTreap(t *testing.T) {
	treap := NewPayloadTreap[types.StringKey, int](types.StringLess)

	keys := []types.StringKey{"apple", "banana", "cherry", "date", "elderberry"}

	payloads := []int{1, 2, 3, 4, 5}
	for i, key := range keys {
		treap.Insert(key, payloads[i])
	}

	for i, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %s in the treap, but it was not found", key)
		} else if node.GetKey().(types.StringKey) != key {
			t.Errorf("Expected to find key %s, but found key %s instead", key, node.GetKey().(types.StringKey))
		} else if node.(*PayloadTreapNode[types.StringKey, int]).payload != payloads[i] {
			t.Errorf("Expected to find payload %d, but found payload %d instead", payloads[i], node.(*PayloadTreapNode[types.StringKey, int]).payload)
		}
	}

	nonExistentKeys := []types.StringKey{"fig", "grape", "honeydew"}

	for _, key := range nonExistentKeys {
		node := treap.Search(key)
		if node != nil && !node.IsNil() {
			t.Errorf("Expected not to find key %s in the treap, but it was found", key)
		}
	}
}

// TestCustomKeyTreap verifies that treaps work with custom types (ShortUIntKey),
// demonstrating the generic key type capability.
func TestCustomKeyTreap(t *testing.T) {
	treap := NewPayloadTreap[exampleCustomKey, string](customKeyLess)

	keys := []exampleCustomKey{
		{ID: 1, Name: "one"},
		{ID: 2, Name: "two"},
		{ID: 3, Name: "three"},
		{ID: 4, Name: "four"},
		{ID: 5, Name: "five"},
	}
	payloads := []string{"payload1", "payload2", "payload3", "payload4", "payload5"}
	for i, key := range keys {
		treap.Insert(key, payloads[i])
	}

	for i, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %+v in the treap, but it was not found", key)
		} else if node.GetKey().(exampleCustomKey) != key {
			t.Errorf("Expected to find key %+v, but found key %+v instead", key, node.GetKey().(exampleCustomKey))
		} else if node.(*PayloadTreapNode[exampleCustomKey, string]).payload != payloads[i] {
			t.Errorf("Expected to find payload %s, but found payload %s instead", payloads[i], node.(*PayloadTreapNode[exampleCustomKey, string]).payload)
		}
	}

	nonExistentKeys := []exampleCustomKey{
		{ID: 6, Name: "six"},
		{ID: 7, Name: "seven"},
		{ID: 8, Name: "eight"},
	}
	for _, key := range nonExistentKeys {
		node := treap.Search(key)
		if node != nil && !node.IsNil() {
			t.Errorf("Expected not to find key %+v in the treap, but it was found", key)
		}
	}
}

// TestTreapWalk verifies that walking a treap visits all nodes in ascending order
// (in-order traversal) and that the callback is called for each node.
func TestTreapWalk(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	keys := []types.IntKey{10, 20, 15, 5, 30}

	for _, key := range keys {
		treap.Insert(key)
	}

	var walkedKeys []types.IntKey
	treap.Walk(func(node TreapNodeInterface[types.IntKey]) {
		walkedKeys = append(walkedKeys, node.GetKey().(types.IntKey))
	})

	expectedKeys := []types.IntKey{5, 10, 15, 20, 30}
	for i, key := range expectedKeys {
		if walkedKeys[i] != key {
			t.Errorf("Expected key %d at position %d, but got %d", key, i, walkedKeys[i])
		}
	}
}

// TestTreapWalkReverse verifies that WalkReverse visits all nodes in descending order
// (reverse in-order traversal).
func TestTreapWalkReverse(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	keys := []types.IntKey{10, 20, 15, 5, 30}

	for _, key := range keys {
		treap.Insert(key)
	}

	var walkedKeys []types.IntKey
	treap.WalkReverse(func(node TreapNodeInterface[types.IntKey]) {
		walkedKeys = append(walkedKeys, node.GetKey().(types.IntKey))
	})

	expectedKeys := []types.IntKey{30, 20, 15, 10, 5}
	for i, key := range expectedKeys {
		if walkedKeys[i] != key {
			t.Errorf("Expected key %d at position %d, but got %d", key, i, walkedKeys[i])
		}
	}
}

// TestPointerKeyEquality verifies that pointer-based keys (like *types.IntKey) work correctly,
// with equality based on value not pointer identity.
func TestPointerKeyEquality(t *testing.T) {
	treap := NewTreap(types.IntLess)

	key1 := types.IntKey(10)
	key2 := types.IntKey(10)

	// Insert key1 into the treap
	treap.Insert(key1)

	// Search for key2 in the treap
	node := treap.Search(key2)

	if node == nil || node.IsNil() {
		t.Errorf("Expected to find key %d in the treap, but it was not found", key2)
	} else if node.GetKey().(types.IntKey) != key1 {
		t.Errorf("Expected to find key %d, but found key %d instead", key1, node.GetKey().(types.IntKey))
	}
}

func TestTreapInsertSingle(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	key := types.IntKey(42)
	treap.Insert(key)

	found := treap.Search(key)
	if found == nil || found.IsNil() {
		t.Fatal("expected to find inserted key")
	}

	if count := countTreapNodes(treap); count != 1 {
		t.Fatalf("expected 1 node, got %d", count)
	}
}

func TestTreapInsertDuplicateNoDup(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	key := types.IntKey(7)
	treap.Insert(key)
	treap.Insert(key)

	if count := countTreapNodes(treap); count != 1 {
		t.Fatalf("expected 1 node after duplicate insert, got %d", count)
	}
}

func TestTreapDeleteNonExistent(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	for i := 0; i < 5; i++ {
		key := types.IntKey(i)
		treap.Insert(key)
	}

	before := countTreapNodes(treap)
	treap.Delete(types.IntKey(999))
	after := countTreapNodes(treap)

	if before != after {
		t.Fatalf("expected node count unchanged, before=%d after=%d", before, after)
	}
}

func TestTreapDeleteSingleElement(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	key := types.IntKey(99)
	treap.Insert(key)

	treap.Delete(key)
	if node := treap.Search(key); node != nil && !node.IsNil() {
		t.Fatal("expected key to be deleted from single-element treap")
	}

	if count := countTreapNodes(treap); count != 0 {
		t.Fatalf("expected 0 nodes after delete, got %d", count)
	}
}

func TestTreapSearchEmpty(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	found := treap.Search(types.IntKey(1))
	if found != nil && !found.IsNil() {
		t.Fatal("expected nil result when searching empty treap")
	}
}

func TestTreapSearchExistingAndMissing(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	keys := []types.IntKey{10, 20, 15, 5, 30}
	for _, key := range keys {
		treap.Insert(key)
	}

	found := treap.Search(types.IntKey(15))
	if found == nil || found.IsNil() {
		t.Fatal("expected to find existing key")
	}

	missing := treap.Search(types.IntKey(999))
	if missing != nil && !missing.IsNil() {
		t.Fatal("expected nil result for missing key")
	}
}

func TestTreapDeleteLeafAndRoot(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	keys := []types.IntKey{10, 20, 15, 5, 30}
	for _, key := range keys {
		treap.Insert(key)
	}

	// Delete a leaf (likely 5 or 30 depending on rotations)
	treap.Delete(types.IntKey(5))
	if node := treap.Search(types.IntKey(5)); node != nil && !node.IsNil() {
		t.Fatal("expected leaf key 5 to be deleted")
	}

	// Delete root key (not guaranteed to be actual root, but should be removed)
	treap.Delete(types.IntKey(10))
	if node := treap.Search(types.IntKey(10)); node != nil && !node.IsNil() {
		t.Fatal("expected key 10 to be deleted")
	}
}

func TestTreapDeleteInternalNode(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	keys := []types.IntKey{10, 20, 15, 5, 30, 12, 18}
	for _, key := range keys {
		treap.Insert(key)
	}

	// Delete an internal node (15 should have children in this setup)
	treap.Delete(types.IntKey(15))
	if node := treap.Search(types.IntKey(15)); node != nil && !node.IsNil() {
		t.Fatal("expected internal key 15 to be deleted")
	}
}

func TestTreapInsertMultipleOrders(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	for i := 1; i <= 50; i++ {
		treap.Insert(types.IntKey(i))
	}

	for i := 100; i >= 51; i-- {
		treap.Insert(types.IntKey(i))
	}

	if count := countTreapNodes(treap); count != 100 {
		t.Fatalf("expected 100 nodes after inserts, got %d", count)
	}

	for _, key := range []types.IntKey{1, 25, 50, 75, 100} {
		if node := treap.Search(key); node == nil || node.IsNil() {
			t.Fatalf("expected to find key %d after inserts", key)
		}
	}
}

func TestTreapUpdatePriorityKeepsKey(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	keys := []types.IntKey{10, 20, 15, 5, 30}
	for _, key := range keys {
		treap.Insert(key)
	}

	// Update priority for an existing key
	if err := treap.UpdatePriority(types.IntKey(15), Priority(999)); err != nil {
		t.Fatalf("UpdatePriority returned error: %v", err)
	}

	if node := treap.Search(types.IntKey(15)); node == nil || node.IsNil() {
		t.Fatal("expected key 15 to remain after UpdatePriority")
	}
}

func TestTreapIterSorted(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	keys := rand.New(rand.NewSource(424242)).Perm(200)
	for _, k := range keys {
		key := types.IntKey(k)
		treap.Insert(key)
	}

	var prev *types.IntKey
	count := 0
	for node := range treap.Iter() {
		key := node.GetKey().Value()
		if prev != nil && key < *prev {
			t.Fatalf("iterator not sorted: %d < %d", key, *prev)
		}
		copyKey := key
		prev = &copyKey
		count++
	}

	if count != 200 {
		t.Fatalf("expected 200 nodes from iterator, got %d", count)
	}
}

func TestPayloadTreapInsertAndSearchPayload(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	treap.Insert(types.IntKey(1), "one")
	treap.Insert(types.IntKey(2), "two")
	treap.Insert(types.IntKey(3), "three")

	cases := []struct {
		key     types.IntKey
		payload string
	}{
		{1, "one"},
		{2, "two"},
		{3, "three"},
	}

	for _, c := range cases {
		node := treap.Search(c.key)
		if node == nil || node.IsNil() {
			t.Fatalf("expected to find key %d", c.key)
		}
		payloadNode, ok := node.(*PayloadTreapNode[types.IntKey, string])
		if !ok {
			t.Fatalf("expected PayloadTreapNode for key %d", c.key)
		}
		if payloadNode.GetPayload() != c.payload {
			t.Fatalf("expected payload %q for key %d, got %q", c.payload, c.key, payloadNode.GetPayload())
		}
	}
}

func TestPayloadTreapUpdatePayloadMethod(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	key := types.IntKey(7)
	treap.Insert(key, "before")

	if err := treap.UpdatePayload(key, "after"); err != nil {
		t.Fatalf("UpdatePayload failed: %v", err)
	}

	node := treap.Search(key)
	if node == nil || node.IsNil() {
		t.Fatal("expected to find key after UpdatePayload")
	}
	payloadNode := node.(*PayloadTreapNode[types.IntKey, string])
	if payloadNode.GetPayload() != "after" {
		t.Fatalf("expected payload 'after', got %q", payloadNode.GetPayload())
	}
}

func TestPayloadTreapSearchComplexCallback(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	keys := []types.IntKey{10, 20, 15, 5, 30}
	for _, key := range keys {
		treap.Insert(key, "payload")
	}

	var accessed []types.IntKey
	callback := func(node TreapNodeInterface[types.IntKey]) error {
		if node != nil && !node.IsNil() {
			accessed = append(accessed, node.GetKey().Value())
		}
		return nil
	}

	found, err := treap.SearchComplex(types.IntKey(15), callback)
	if err != nil {
		t.Fatalf("SearchComplex returned error: %v", err)
	}
	if found == nil || found.IsNil() {
		t.Fatal("expected to find key 15")
	}
	if len(accessed) == 0 {
		t.Fatal("expected callback to be invoked at least once")
	}
}

func TestPayloadTreapSearchComplexError(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	keys := []types.IntKey{10, 20, 15, 5, 30}
	for _, key := range keys {
		treap.Insert(key, "payload")
	}

	boom := errors.New("callback error")
	callback := func(node TreapNodeInterface[types.IntKey]) error {
		_ = node
		return boom
	}

	found, err := treap.SearchComplex(types.IntKey(15), callback)
	if err == nil {
		t.Fatal("expected error from SearchComplex")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected error %q, got %v", boom, err)
	}
	if found != nil {
		t.Fatalf("expected nil node when callback errors, got %v", found)
	}
}

func TestPayloadTreapUpdatePayloadMissingKey(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	if err := treap.UpdatePayload(types.IntKey(404), "missing"); err == nil {
		t.Fatal("expected error when updating missing key")
	}
}

func TestPayloadTreapWalkInOrderSorted(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	keys := []types.IntKey{42, 7, 19, 3, 88, 55, 1}
	expected := make(map[types.IntKey]string, len(keys))
	for _, key := range keys {
		payload := fmt.Sprintf("p%d", key)
		expected[key] = payload
		treap.Insert(key, payload)
	}

	var walked []types.IntKey
	treap.Walk(func(node TreapNodeInterface[types.IntKey]) {
		payloadNode := node.(*PayloadTreapNode[types.IntKey, string])
		walked = append(walked, payloadNode.GetKey().Value())
		if expected[payloadNode.GetKey().Value()] != payloadNode.GetPayload() {
			t.Fatalf("payload mismatch for key %d", payloadNode.GetKey().Value())
		}
	})

	if len(walked) != len(keys) {
		t.Fatalf("expected %d keys from Walk, got %d", len(keys), len(walked))
	}
	for i := 1; i < len(walked); i++ {
		if walked[i-1] > walked[i] {
			t.Fatalf("payload treap walk not sorted at index %d: %d > %d", i, walked[i-1], walked[i])
		}
	}
}
