package multistore

import (
	"testing"
	"time"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// waitForWrites allows the async write worker to process pending writes.
// Default flush interval is 10ms, so 20ms ensures at least one flush cycle.
func waitForWrites() {
	time.Sleep(20 * time.Millisecond)
}

// TestMultiStore_StringStorer_BasicUsage verifies that MultiStore implements
// the StringStorer interface and can store/retrieve strings.
func TestMultiStore_StringStorer_BasicUsage(t *testing.T) {
	tmpFile := t.TempDir() + "/stringstore_test.db"

	// Create multistore
	ms, err := NewMultiStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("Failed to create multistore: %v", err)
	}
	defer ms.Close()

	// Verify it implements StringStorer
	var _ bobbob.StringStorer = ms

	// Test NewStringObj
	testString := "Hello, World!"
	objId, err := ms.NewStringObj(testString)
	if err != nil {
		t.Fatalf("NewStringObj failed: %v", err)
	}

	if objId == bobbob.ObjNotAllocated {
		t.Fatal("NewStringObj returned ObjNotAllocated")
	}

	// Test StringFromObjId
	retrieved, err := ms.StringFromObjId(objId)
	if err != nil {
		t.Fatalf("StringFromObjId failed: %v", err)
	}

	if retrieved != testString {
		t.Errorf("Retrieved string mismatch: got %q, want %q", retrieved, testString)
	}

	// Test HasStringObj
	if !ms.HasStringObj(objId) {
		t.Error("HasStringObj returned false for valid string object")
	}

	// Test HasStringObj for non-existent object
	if ms.HasStringObj(999999) {
		t.Error("HasStringObj returned true for non-existent object")
	}

	// Test DeleteObj routing
	err = ms.DeleteObj(objId)
	if err != nil {
		t.Fatalf("DeleteObj failed: %v", err)
	}

	// Wait for async worker to process delete
	waitForWrites()

	// Verify deletion
	if ms.HasStringObj(objId) {
		t.Error("HasStringObj returned true after deletion")
	}
}

// TestMultiStore_StringPayload_Integration verifies that StringPayload
// uses the StringStorer interface when available.
func TestMultiStore_StringPayload_Integration(t *testing.T) {
	tmpFile := t.TempDir() + "/stringpayload_test.db"

	ms, err := NewMultiStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("Failed to create multistore: %v", err)
	}
	defer ms.Close()

	// Create a StringPayload
	payload := types.StringPayload("Test string payload")

	// Use LateMarshal (should use StringStorer fast path)
	objId, size, finisher := payload.LateMarshal(ms)
	if err := finisher(); err != nil {
		t.Fatalf("LateMarshal finisher failed: %v", err)
	}

	if objId == bobbob.ObjNotAllocated {
		t.Fatal("LateMarshal returned ObjNotAllocated")
	}

	if size != len(payload) {
		t.Errorf("Size mismatch: got %d, want %d", size, len(payload))
	}

	// Verify it's recognized as a string object
	if !ms.HasStringObj(objId) {
		t.Error("StringPayload object not recognized by HasStringObj")
	}

	// Use LateUnmarshal (should use StringStorer fast path)
	var loaded types.StringPayload
	finisher2 := loaded.LateUnmarshal(objId, size, ms)
	if err := finisher2(); err != nil {
		t.Fatalf("LateUnmarshal finisher failed: %v", err)
	}

	if string(loaded) != string(payload) {
		t.Errorf("Loaded payload mismatch: got %q, want %q", loaded, payload)
	}
}

// TestMultiStore_MultipleStrings verifies handling of multiple string objects.
func TestMultiStore_MultipleStrings(t *testing.T) {
	tmpFile := t.TempDir() + "/multistrings_test.db"

	ms, err := NewMultiStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("Failed to create multistore: %v", err)
	}
	defer ms.Close()

	// Store multiple strings
	testStrings := []string{
		"First string",
		"Second string with more content",
		"Third",
		"Fourth string is here",
	}

	objIds := make([]bobbob.ObjectId, len(testStrings))
	for i, str := range testStrings {
		objId, err := ms.NewStringObj(str)
		if err != nil {
			t.Fatalf("NewStringObj failed for string %d: %v", i, err)
		}
		objIds[i] = objId
	}

	// Verify all can be retrieved
	for i, objId := range objIds {
		retrieved, err := ms.StringFromObjId(objId)
		if err != nil {
			t.Fatalf("StringFromObjId failed for object %d: %v", i, err)
		}
		if retrieved != testStrings[i] {
			t.Errorf("String %d mismatch: got %q, want %q", i, retrieved, testStrings[i])
		}
	}

	// Delete all
	for i, objId := range objIds {
		if err := ms.DeleteObj(objId); err != nil {
			t.Fatalf("DeleteObj failed for object %d: %v", i, err)
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
