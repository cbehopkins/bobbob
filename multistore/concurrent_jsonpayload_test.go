package multistore

import (
	"testing"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestConcurrentMultiStore_JsonPayload_UsesStringStorer verifies that JsonPayload
// works correctly with ConcurrentMultiStore and uses the StringStorer interface.
func TestConcurrentMultiStore_JsonPayload_UsesStringStorer(t *testing.T) {
	tmpFile := t.TempDir() + "/jsonpayload_concurrent_stringstore_test.db"

	// Create ConcurrentMultiStore (wraps MultiStore in ConcurrentStore)
	cms, err := NewConcurrentMultiStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("Failed to create concurrent multistore: %v", err)
	}
	defer cms.Close()

	type TestStruct struct {
		Name  string
		Count int
		Tags  []string
	}

	// Create a JsonPayload
	original := types.JsonPayload[TestStruct]{
		Value: TestStruct{
			Name:  "concurrent-test-item",
			Count: 99,
			Tags:  []string{"concurrent", "tag1", "tag2"},
		},
	}

	// Use LateMarshal (should use StringStorer fast path through ConcurrentStore wrapper)
	objId, size, finisher := original.LateMarshal(cms)
	if err := finisher(); err != nil {
		t.Fatalf("LateMarshal finisher failed: %v", err)
	}

	if objId == bobbob.ObjNotAllocated {
		t.Fatal("LateMarshal returned ObjNotAllocated")
	}

	if size <= 0 {
		t.Errorf("LateMarshal returned invalid size: %d", size)
	}

	// Verify it's recognized as a string object through ConcurrentStore wrapper
	// This tests that type assertion works through the wrapper layer
	stringer, ok := cms.(bobbob.StringStorer)
	if !ok {
		t.Fatal("ConcurrentMultiStore does not implement StringStorer interface!")
	}

	if !stringer.HasStringObj(objId) {
		t.Error("JsonPayload object not recognized by HasStringObj - StringStorer interface chain broken!")
	}

	// Use LateUnmarshal to verify the data round-trips correctly
	var restored types.JsonPayload[TestStruct]
	finisher2 := restored.LateUnmarshal(objId, size, cms)
	if err := finisher2(); err != nil {
		t.Fatalf("LateUnmarshal finisher failed: %v", err)
	}

	// Verify the restored value matches
	if restored.Value.Name != original.Value.Name {
		t.Errorf("Name mismatch: got %s, expected %s", restored.Value.Name, original.Value.Name)
	}
	if restored.Value.Count != original.Value.Count {
		t.Errorf("Count mismatch: got %d, expected %d", restored.Value.Count, original.Value.Count)
	}
	if len(restored.Value.Tags) != len(original.Value.Tags) {
		t.Errorf("Tags length mismatch: got %d, expected %d", len(restored.Value.Tags), len(original.Value.Tags))
	}

	t.Log("SUCCESS: JsonPayload works correctly with ConcurrentMultiStore through StringStorer interface")
}
