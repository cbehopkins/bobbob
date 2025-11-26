package testutil_test

import (
	"testing"

	"bobbob/internal/testutil"
)

// Example test demonstrating testutil usage
func TestExampleUsage(t *testing.T) {
	// Setup store with automatic cleanup
	_, s, cleanup := testutil.SetupTestStore(t)
	defer cleanup()

	// Generate test data
	data := testutil.RandomBytesSeeded(100, 42)

	// Write an object
	objId := testutil.WriteObject(t, s, data)

	// Verify the object
	testutil.VerifyObject(t, s, objId, data)

	// Update the object
	newData := testutil.RandomBytesSeeded(100, 43)
	testutil.UpdateObject(t, s, objId, newData)

	// Verify the update
	testutil.VerifyObject(t, s, objId, newData)

	// Delete the object
	testutil.DeleteObject(t, s, objId)
}

func TestRandomDataGeneration(t *testing.T) {
	// Test random bytes
	data1 := testutil.RandomBytes(t, 50)
	if len(data1) != 50 {
		t.Errorf("expected 50 bytes, got %d", len(data1))
	}

	// Test seeded random bytes (deterministic)
	seeded1 := testutil.RandomBytesSeeded(50, 1)
	seeded2 := testutil.RandomBytesSeeded(50, 1)

	if len(seeded1) != 50 {
		t.Errorf("expected 50 bytes, got %d", len(seeded1))
	}

	// Seeded bytes should be identical
	for i := range seeded1 {
		if seeded1[i] != seeded2[i] {
			t.Errorf("seeded bytes differ at index %d: %d != %d", i, seeded1[i], seeded2[i])
		}
	}
}
