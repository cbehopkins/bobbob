package treap

import (
	"fmt"
	"testing"

	"github.com/cbehopkins/bobbob/store"
)

// TestTreapCompareBasic tests basic comparison of two in-memory treaps
func TestTreapCompareBasic(t *testing.T) {
	// Create two treaps
	treapA := NewTreap(IntLess)
	treapB := NewTreap(IntLess)

	// Populate treapA with keys 1, 2, 3, 4, 5
	for i := 1; i <= 5; i++ {
		treapA.Insert(IntKey(i))
	}

	// Populate treapB with keys 3, 4, 5, 6, 7
	for i := 3; i <= 7; i++ {
		treapB.Insert(IntKey(i))
	}

	var onlyInA []int
	var inBoth []int
	var onlyInB []int

	err := treapA.Compare(treapB,
		func(node TreapNodeInterface[IntKey]) error {
			key := node.GetKey().Value()
			onlyInA = append(onlyInA, int(key))
			return nil
		},
		func(nodeA, nodeB TreapNodeInterface[IntKey]) error {
			key := nodeA.GetKey().Value()
			inBoth = append(inBoth, int(key))
			return nil
		},
		func(node TreapNodeInterface[IntKey]) error {
			key := node.GetKey().Value()
			onlyInB = append(onlyInB, int(key))
			return nil
		},
	)
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	// Verify results
	expectedOnlyInA := []int{1, 2}
	expectedInBoth := []int{3, 4, 5}
	expectedOnlyInB := []int{6, 7}

	if !intSliceEqual(onlyInA, expectedOnlyInA) {
		t.Errorf("onlyInA: expected %v, got %v", expectedOnlyInA, onlyInA)
	}
	if !intSliceEqual(inBoth, expectedInBoth) {
		t.Errorf("inBoth: expected %v, got %v", expectedInBoth, inBoth)
	}
	if !intSliceEqual(onlyInB, expectedOnlyInB) {
		t.Errorf("onlyInB: expected %v, got %v", expectedOnlyInB, onlyInB)
	}
}

// TestTreapCompareEmpty tests comparison when one or both treaps are empty
func TestTreapCompareEmpty(t *testing.T) {
	treapA := NewTreap(IntLess)
	treapB := NewTreap(IntLess)

	// Populate only treapB
	for i := 1; i <= 3; i++ {
		treapB.Insert(IntKey(i))
	}

	var onlyInA []int
	var inBoth []int
	var onlyInB []int

	err := treapA.Compare(treapB,
		func(node TreapNodeInterface[IntKey]) error {
			onlyInA = append(onlyInA, int(node.GetKey().Value()))
			return nil
		},
		func(nodeA, nodeB TreapNodeInterface[IntKey]) error {
			inBoth = append(inBoth, int(nodeA.GetKey().Value()))
			return nil
		},
		func(node TreapNodeInterface[IntKey]) error {
			onlyInB = append(onlyInB, int(node.GetKey().Value()))
			return nil
		},
	)
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	if len(onlyInA) != 0 {
		t.Errorf("onlyInA should be empty, got %v", onlyInA)
	}
	if len(inBoth) != 0 {
		t.Errorf("inBoth should be empty, got %v", inBoth)
	}
	if len(onlyInB) != 3 {
		t.Errorf("onlyInB should have 3 elements, got %v", onlyInB)
	}
}

// TestTreapCompareIdentical tests comparison of identical treaps
func TestTreapCompareIdentical(t *testing.T) {
	treapA := NewTreap(IntLess)
	treapB := NewTreap(IntLess)

	// Populate both with same keys
	for i := 1; i <= 5; i++ {
		treapA.Insert(IntKey(i))
		treapB.Insert(IntKey(i))
	}

	var onlyInA []int
	var inBoth []int
	var onlyInB []int

	err := treapA.Compare(treapB,
		func(node TreapNodeInterface[IntKey]) error {
			onlyInA = append(onlyInA, int(node.GetKey().Value()))
			return nil
		},
		func(nodeA, nodeB TreapNodeInterface[IntKey]) error {
			inBoth = append(inBoth, int(nodeA.GetKey().Value()))
			return nil
		},
		func(node TreapNodeInterface[IntKey]) error {
			onlyInB = append(onlyInB, int(node.GetKey().Value()))
			return nil
		},
	)
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	if len(onlyInA) != 0 {
		t.Errorf("onlyInA should be empty, got %v", onlyInA)
	}
	if len(onlyInB) != 0 {
		t.Errorf("onlyInB should be empty, got %v", onlyInB)
	}
	if len(inBoth) != 5 {
		t.Errorf("inBoth should have 5 elements, got %v", inBoth)
	}
}

// TestTreapCompareNilCallbacks tests that nil callbacks are handled properly
func TestTreapCompareNilCallbacks(t *testing.T) {
	treapA := NewTreap(IntLess)
	treapB := NewTreap(IntLess)

	treapA.Insert(IntKey(1))
	treapB.Insert(IntKey(2))

	// Should not panic with nil callbacks
	err := treapA.Compare(treapB, nil, nil, nil)
	if err != nil {
		t.Fatalf("Compare with nil callbacks failed: %v", err)
	}
}

// TestTreapCompareErrorPropagation tests that errors from callbacks are propagated
func TestTreapCompareErrorPropagation(t *testing.T) {
	treapA := NewTreap(IntLess)
	treapB := NewTreap(IntLess)

	treapA.Insert(IntKey(1))
	treapB.Insert(IntKey(1))

	expectedErr := fmt.Errorf("test error")

	err := treapA.Compare(treapB,
		nil,
		func(nodeA, nodeB TreapNodeInterface[IntKey]) error {
			return expectedErr
		},
		nil,
	)

	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

// TestPersistentTreapCompare tests comparison of persistent treaps
func TestPersistentTreapCompare(t *testing.T) {
	tempDir := t.TempDir()
	storeA, err := store.NewBasicStore(tempDir + "/storeA.db")
	if err != nil {
		t.Fatalf("Failed to create store A: %v", err)
	}
	defer storeA.Close()

	storeB, err := store.NewBasicStore(tempDir + "/storeB.db")
	if err != nil {
		t.Fatalf("Failed to create store B: %v", err)
	}
	defer storeB.Close()

	treapA := NewPersistentTreap[IntKey](IntLess, (*IntKey)(new(int32)), storeA)
	treapB := NewPersistentTreap[IntKey](IntLess, (*IntKey)(new(int32)), storeB)

	// Populate treapA with keys 1, 2, 3
	for i := 1; i <= 3; i++ {
		key := IntKey(i)
		treapA.Insert(&key)
	}

	// Populate treapB with keys 2, 3, 4
	for i := 2; i <= 4; i++ {
		key := IntKey(i)
		treapB.Insert(&key)
	}

	var onlyInA []int
	var inBoth []int
	var onlyInB []int

	err = treapA.Compare(treapB,
		func(node TreapNodeInterface[IntKey]) error {
			onlyInA = append(onlyInA, int(node.GetKey().Value()))
			return nil
		},
		func(nodeA, nodeB TreapNodeInterface[IntKey]) error {
			inBoth = append(inBoth, int(nodeA.GetKey().Value()))
			return nil
		},
		func(node TreapNodeInterface[IntKey]) error {
			onlyInB = append(onlyInB, int(node.GetKey().Value()))
			return nil
		},
	)
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	expectedOnlyInA := []int{1}
	expectedInBoth := []int{2, 3}
	expectedOnlyInB := []int{4}

	if !intSliceEqual(onlyInA, expectedOnlyInA) {
		t.Errorf("onlyInA: expected %v, got %v", expectedOnlyInA, onlyInA)
	}
	if !intSliceEqual(inBoth, expectedInBoth) {
		t.Errorf("inBoth: expected %v, got %v", expectedInBoth, inBoth)
	}
	if !intSliceEqual(onlyInB, expectedOnlyInB) {
		t.Errorf("onlyInB: expected %v, got %v", expectedOnlyInB, onlyInB)
	}
}

// TestPersistentPayloadTreapCompare tests comparison of persistent payload treaps
func TestPersistentPayloadTreapCompare(t *testing.T) {
	tempDir := t.TempDir()
	storeA, err := store.NewBasicStore(tempDir + "/storeA.db")
	if err != nil {
		t.Fatalf("Failed to create store A: %v", err)
	}
	defer storeA.Close()

	storeB, err := store.NewBasicStore(tempDir + "/storeB.db")
	if err != nil {
		t.Fatalf("Failed to create store B: %v", err)
	}
	defer storeB.Close()

	treapA := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), storeA)
	treapB := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), storeB)

	// Populate treapA with keys 1, 2, 3 and payloads
	for i := 1; i <= 3; i++ {
		key := IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("valueA-%d", i)}
		treapA.Insert(&key, payload)
	}

	// Populate treapB with keys 2, 3, 4 and payloads
	for i := 2; i <= 4; i++ {
		key := IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("valueB-%d", i)}
		treapB.Insert(&key, payload)
	}

	var onlyInA []int
	var inBoth []int
	var onlyInB []int
	var payloadsInBoth []string

	err = treapA.Compare(treapB,
		func(node TreapNodeInterface[IntKey]) error {
			onlyInA = append(onlyInA, int(node.GetKey().Value()))
			// Access payload
			payloadNode := node.(PersistentPayloadNodeInterface[IntKey, MockPayload])
			payload := payloadNode.GetPayload()
			t.Logf("Only in A: key=%d, payload=%v", node.GetKey().Value(), payload.Data)
			return nil
		},
		func(nodeA, nodeB TreapNodeInterface[IntKey]) error {
			inBoth = append(inBoth, int(nodeA.GetKey().Value()))
			// Access both payloads
			payloadA := nodeA.(PersistentPayloadNodeInterface[IntKey, MockPayload]).GetPayload()
			payloadB := nodeB.(PersistentPayloadNodeInterface[IntKey, MockPayload]).GetPayload()
			payloadsInBoth = append(payloadsInBoth, fmt.Sprintf("A:%s,B:%s", payloadA.Data, payloadB.Data))
			return nil
		},
		func(node TreapNodeInterface[IntKey]) error {
			onlyInB = append(onlyInB, int(node.GetKey().Value()))
			return nil
		},
	)
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	expectedOnlyInA := []int{1}
	expectedInBoth := []int{2, 3}
	expectedOnlyInB := []int{4}

	if !intSliceEqual(onlyInA, expectedOnlyInA) {
		t.Errorf("onlyInA: expected %v, got %v", expectedOnlyInA, onlyInA)
	}
	if !intSliceEqual(inBoth, expectedInBoth) {
		t.Errorf("inBoth: expected %v, got %v", expectedInBoth, inBoth)
	}
	if !intSliceEqual(onlyInB, expectedOnlyInB) {
		t.Errorf("onlyInB: expected %v, got %v", expectedOnlyInB, onlyInB)
	}

	// Verify we could access payloads in the callbacks
	if len(payloadsInBoth) != 2 {
		t.Errorf("Expected 2 payload comparisons, got %d", len(payloadsInBoth))
	}
}

// TestPersistentPayloadTreapCompareDisjoint tests comparison of completely disjoint sets
func TestPersistentPayloadTreapCompareDisjoint(t *testing.T) {
	tempDir := t.TempDir()
	storeA, err := store.NewBasicStore(tempDir + "/storeA.db")
	if err != nil {
		t.Fatalf("Failed to create store A: %v", err)
	}
	defer storeA.Close()

	storeB, err := store.NewBasicStore(tempDir + "/storeB.db")
	if err != nil {
		t.Fatalf("Failed to create store B: %v", err)
	}
	defer storeB.Close()

	treapA := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), storeA)
	treapB := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), storeB)

	// Populate treapA with keys 1-5
	for i := 1; i <= 5; i++ {
		key := IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("A-%d", i)}
		treapA.Insert(&key, payload)
	}

	// Populate treapB with keys 6-10
	for i := 6; i <= 10; i++ {
		key := IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("B-%d", i)}
		treapB.Insert(&key, payload)
	}

	var onlyInA []int
	var inBoth []int
	var onlyInB []int

	err = treapA.Compare(treapB,
		func(node TreapNodeInterface[IntKey]) error {
			onlyInA = append(onlyInA, int(node.GetKey().Value()))
			return nil
		},
		func(nodeA, nodeB TreapNodeInterface[IntKey]) error {
			inBoth = append(inBoth, int(nodeA.GetKey().Value()))
			return nil
		},
		func(node TreapNodeInterface[IntKey]) error {
			onlyInB = append(onlyInB, int(node.GetKey().Value()))
			return nil
		},
	)
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	if len(onlyInA) != 5 {
		t.Errorf("Expected 5 keys only in A, got %d: %v", len(onlyInA), onlyInA)
	}
	if len(inBoth) != 0 {
		t.Errorf("Expected 0 keys in both, got %d: %v", len(inBoth), inBoth)
	}
	if len(onlyInB) != 5 {
		t.Errorf("Expected 5 keys only in B, got %d: %v", len(onlyInB), onlyInB)
	}
}

// Helper function to compare int slices
func intSliceEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
