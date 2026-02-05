package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestPersistentTreapRotationCorruption demonstrates the bug where rotations
// invalidate objectIds but parent references remain pointing to deleted objects.
func TestPersistentTreapRotationCorruption(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Phase 1: Build initial tree with specific priorities to control structure
	// We'll insert nodes that will definitely cause a rotation later

	// Insert root with high priority (stays at root)
	key1 := types.IntKey(50)
	treap.InsertComplex(&key1, Priority(1000))

	// Insert left child with medium priority
	key2 := types.IntKey(25)
	treap.InsertComplex(&key2, Priority(500))

	// Insert right child with medium priority
	key3 := types.IntKey(75)
	treap.InsertComplex(&key3, Priority(500))

	t.Logf("Initial tree structure created")

	// Phase 2: Persist the tree - all nodes get valid objectIds
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist initial tree: %v", err)
	}
	t.Logf("Tree persisted - all nodes have objectIds on disk")

	// Validate before rotation
	errors := treap.ValidateAgainstDisk()
	if len(errors) > 0 {
		t.Errorf("Validation BEFORE rotation failed with %d errors:", len(errors))
		for _, e := range errors {
			t.Errorf("  - %s", e)
		}
	} else {
		t.Logf("Validation before rotation: PASS")
	}

	// Phase 3: Insert a node with very high priority that will cause rotation
	// This will be inserted under node 25 but its high priority will cause it to rotate up
	key4 := types.IntKey(20)
	treap.InsertComplex(&key4, Priority(2000)) // Higher priority than root!

	t.Logf("Inserted node 20 with priority 2000 (should cause rotations)")

	// Phase 4: Validate against disk WITHOUT persisting first
	// This is where the bug should show up
	errors = treap.ValidateAgainstDisk()
	if len(errors) > 0 {
		t.Logf("Validation AFTER rotation failed with %d errors (BUG REPRODUCED):", len(errors))
		for _, e := range errors {
			t.Logf("  - %s", e)
		}
		t.Fatalf("BUG: Rotation caused disk corruption - nodes reference deleted objectIds")
	} else {
		t.Logf("Validation after rotation: PASS (bug not reproduced)")
	}
}

// TestPersistentTreapMinimalRotationBug is an even simpler test with just 2 nodes
func TestPersistentTreapMinimalRotationBug(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Insert root
	key1 := types.IntKey(10)
	treap.InsertComplex(&key1, Priority(100))

	// Check objectId before persist
	root1 := treap.Root().(*PersistentTreapNode[types.IntKey])
	t.Logf("Before persist: node 10 has objectId=%d", root1.objectId)

	// Persist
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist: %v", err)
	}

	// Check objectId after persist
	root1 = treap.Root().(*PersistentTreapNode[types.IntKey])
	t.Logf("After persist: node 10 has objectId=%d", root1.objectId)

	// Insert child with higher priority (forces rotation)
	key2 := types.IntKey(5)
	treap.InsertComplex(&key2, Priority(200))

	t.Logf("After insert before validation:")
	root2 := treap.Root().(*PersistentTreapNode[types.IntKey])
	t.Logf("  Root: key=%d, objectId=%d, leftObjId=%d, rightObjId=%d",
		root2.GetKey().Value(), root2.objectId, root2.leftObjectId, root2.rightObjectId)
	if root2.TreapNode.right != nil {
		rightNode := root2.TreapNode.right.(*PersistentTreapNode[types.IntKey])
		t.Logf("  Right: key=%d, objectId=%d, leftObjId=%d, rightObjId=%d",
			rightNode.GetKey().Value(), rightNode.objectId, rightNode.leftObjectId, rightNode.rightObjectId)
	}

	t.Logf("Tree structure after rotation:")
	t.Logf("  Expected: node 5 (pri 200) is root, node 10 (pri 100) is right child")

	// Validate - should fail if bug exists
	errors := treap.ValidateAgainstDisk()
	if len(errors) > 0 {
		t.Logf("BUG REPRODUCED with minimal case:")
		for _, e := range errors {
			t.Logf("  - %s", e)
		}

		// Let's also check what objectIds are involved
		root := treap.Root()
		if root != nil {
			rootNode, ok := root.(*PersistentTreapNode[types.IntKey])
			if ok {
				t.Logf("Root node key=%d, objectId=%d, leftObjectId=%d, rightObjectId=%d",
					rootNode.GetKey().Value(), rootNode.objectId, rootNode.leftObjectId, rootNode.rightObjectId)

				if rootNode.TreapNode.right != nil {
					rightNode, ok := rootNode.TreapNode.right.(*PersistentTreapNode[types.IntKey])
					if ok {
						t.Logf("Right child key=%d, objectId=%d, leftObjectId=%d, rightObjectId=%d",
							rightNode.GetKey().Value(), rightNode.objectId, rightNode.leftObjectId, rightNode.rightObjectId)
					}
				}
			}
		}

		t.Fatalf("BUG: Rotation invalidated objectIds without updating references")
	} else {
		t.Logf("No corruption detected (bug not reproduced)")
	}
}
