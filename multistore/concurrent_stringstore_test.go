package multistore

import (
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob"
)

// TestConcurrentStoreStringStorer verifies that ConcurrentStore properly exposes
// the StringStorer interface when wrapping a MultiStore.
func TestConcurrentStoreStringStorer(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "test.db")

	// Create a concurrent multistore (which wraps MultiStore in ConcurrentStore)
	cs, err := NewConcurrentMultiStore(storePath, 0)
	if err != nil {
		t.Fatalf("NewConcurrentMultiStore failed: %v", err)
	}
	defer cs.Close()

	// Verify the wrapped store implements StringStorer
	stringer, ok := cs.(bobbob.StringStorer)
	if !ok {
		t.Fatal("ConcurrentStore wrapping MultiStore should implement StringStorer")
	}

	// Test NewStringObj
	testStrings := []string{"hello", "world", "test"}
	var objIds []bobbob.ObjectId

	for _, str := range testStrings {
		objId, err := stringer.NewStringObj(str)
		if err != nil {
			t.Fatalf("NewStringObj(%q) failed: %v", str, err)
		}
		objIds = append(objIds, objId)
		t.Logf("NewStringObj(%q) = %d", str, objId)
	}

	// Test StringFromObjId
	for i, objId := range objIds {
		retrieved, err := stringer.StringFromObjId(objId)
		if err != nil {
			t.Fatalf("StringFromObjId(%d) failed: %v", objId, err)
		}
		if retrieved != testStrings[i] {
			t.Fatalf("String mismatch: got %q, want %q", retrieved, testStrings[i])
		}
		t.Logf("StringFromObjId(%d) = %q (correct)", objId, retrieved)
	}

	// Test HasStringObj
	for i, objId := range objIds {
		has := stringer.HasStringObj(objId)
		if !has {
			t.Fatalf("HasStringObj(%d) should return true, got false", objId)
		}
		t.Logf("HasStringObj(str%d=%d) = true (correct)", i, objId)
	}

	t.Log("SUCCESS: ConcurrentStore properly exposes StringStorer interface")
}
