package testutil

import (
	"testing"
)

// TestExampleUsage demonstrates usage of testutil helpers
func TestExampleUsage(t *testing.T) {
	// Setup store with automatic cleanup
	_, s, cleanup := SetupTestStore(t)
	defer cleanup()

	// Generate test data
	data := RandomBytesSeeded(100, 42)

	// Write an object
	objId := WriteObject(t, s, data)

	// Verify the object
	VerifyObject(t, s, objId, data)

	// Update the object
	newData := RandomBytesSeeded(100, 43)
	UpdateObject(t, s, objId, newData)

	// Verify the update
	VerifyObject(t, s, objId, newData)

	// Delete the object
	DeleteObject(t, s, objId)
}

// TestRandomDataGeneration tests random byte generation
func TestRandomDataGeneration(t *testing.T) {
	// Test random bytes
	data1 := RandomBytes(t, 50)
	if len(data1) != 50 {
		t.Errorf("expected 50 bytes, got %d", len(data1))
	}

	// Test seeded random bytes (deterministic)
	seeded1 := RandomBytesSeeded(50, 1)
	seeded2 := RandomBytesSeeded(50, 1)

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
