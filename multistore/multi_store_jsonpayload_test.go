package multistore

import (
	"testing"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestMultiStore_JsonPayload_UsesStringStorer verifies that JsonPayload
// uses the StringStorer interface for better performance.
func TestMultiStore_JsonPayload_UsesStringStorer(t *testing.T) {
	tmpFile := t.TempDir() + "/jsonpayload_stringstore_test.db"

	ms, err := NewMultiStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("Failed to create multistore: %v", err)
	}
	defer ms.Close()

	type TestStruct struct {
		Name  string
		Count int
		Tags  []string
	}

	// Create a JsonPayload
	original := types.JsonPayload[TestStruct]{
		Value: TestStruct{
			Name:  "test-item",
			Count: 42,
			Tags:  []string{"tag1", "tag2", "tag3"},
		},
	}

	// Use LateMarshal (should use StringStorer fast path)
	objId, size, finisher := original.LateMarshal(ms)
	if err := finisher(); err != nil {
		t.Fatalf("LateMarshal finisher failed: %v", err)
	}

	if objId == bobbob.ObjNotAllocated {
		t.Fatal("LateMarshal returned ObjNotAllocated")
	}

	if size <= 0 {
		t.Errorf("LateMarshal returned invalid size: %d", size)
	}

	// Verify it's recognized as a string object (proves StringStorer was used)
	if !ms.HasStringObj(objId) {
		t.Error("JsonPayload object not recognized by HasStringObj - StringStorer not used!")
	}

	// Use LateUnmarshal (should use StringStorer fast path)
	var restored types.JsonPayload[TestStruct]
	finisher2 := restored.LateUnmarshal(objId, size, ms)
	if err := finisher2(); err != nil {
		t.Fatalf("LateUnmarshal finisher failed: %v", err)
	}

	// Verify data integrity
	if restored.Value.Name != original.Value.Name {
		t.Errorf("Name mismatch: got %q, want %q", restored.Value.Name, original.Value.Name)
	}
	if restored.Value.Count != original.Value.Count {
		t.Errorf("Count mismatch: got %d, want %d", restored.Value.Count, original.Value.Count)
	}
	if len(restored.Value.Tags) != len(original.Value.Tags) {
		t.Errorf("Tags length mismatch: got %d, want %d", len(restored.Value.Tags), len(original.Value.Tags))
	}
}

// TestMultiStore_JsonPayload_MultipleObjects verifies multiple JsonPayload objects.
func TestMultiStore_JsonPayload_MultipleObjects(t *testing.T) {
	tmpFile := t.TempDir() + "/jsonpayload_multiple_test.db"

	ms, err := NewMultiStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("Failed to create multistore: %v", err)
	}
	defer ms.Close()

	type Person struct {
		Name string
		Age  int
	}

	people := []Person{
		{Name: "Alice", Age: 30},
		{Name: "Bob", Age: 25},
		{Name: "Charlie", Age: 35},
		{Name: "Diana", Age: 28},
	}

	objIds := make([]bobbob.ObjectId, len(people))

	// Store all
	for i, person := range people {
		payload := types.JsonPayload[Person]{Value: person}
		objId, _, finisher := payload.LateMarshal(ms)
		if err := finisher(); err != nil {
			t.Fatalf("LateMarshal failed for person %d: %v", i, err)
		}
		objIds[i] = objId

		// Verify StringStorer was used
		if !ms.HasStringObj(objId) {
			t.Errorf("Object %d not in StringStore", i)
		}
	}

	// Retrieve all and verify
	for i, objId := range objIds {
		var restored types.JsonPayload[Person]
		finisher := restored.LateUnmarshal(objId, 0, ms)
		if err := finisher(); err != nil {
			t.Fatalf("LateUnmarshal failed for person %d: %v", i, err)
		}

		if restored.Value.Name != people[i].Name {
			t.Errorf("Person %d name mismatch: got %q, want %q", i, restored.Value.Name, people[i].Name)
		}
		if restored.Value.Age != people[i].Age {
			t.Errorf("Person %d age mismatch: got %d, want %d", i, restored.Value.Age, people[i].Age)
		}
	}

	// Delete all
	for i, objId := range objIds {
		if err := ms.DeleteObj(objId); err != nil {
			t.Fatalf("DeleteObj failed for person %d: %v", i, err)
		}
	}

	// Wait for async worker to process deletes
	waitForWrites()

	// Verify all deleted
	for i, objId := range objIds {
		if ms.HasStringObj(objId) {
			t.Errorf("Object %d still exists after deletion", i)
		}
	}
}
