package allocator

import (
	"os"
	"testing"
)

// TestBasicAllocatorAppendToEnd verifies that BasicAllocator appends its own
// marshaled data directly at the End position without allocating an object for it.
//
// This is a subtle but important behavior:
// - BasicAllocator doesn't allocate space for its own persistence
// - It simply appends its data at the current End position
// - The returned offset/ObjectId is NOT an actual allocation, just the file position
// - This works because BasicAllocator is the last allocator to persist,
//   so we know no more objects will be created after it
func TestBasicAllocatorAppendToEnd(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "test_allocator_append")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create a BasicAllocator
	allocator, err := NewBasicAllocator(tmpFile)
	if err != nil {
		t.Fatalf("failed to create BasicAllocator: %v", err)
	}

	// Allocate some space for test objects
	_, _, err = allocator.Allocate(100)
	if err != nil {
		t.Fatalf("failed to allocate object 1: %v", err)
	}

	_, _, err = allocator.Allocate(200)
	if err != nil {
		t.Fatalf("failed to allocate object 2: %v", err)
	}

	// Remember the current End position before marshaling BasicAllocator
	endBeforeMarshal := allocator.End

	// Marshal the BasicAllocator
	data, err := allocator.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	// NOW: In the real save flow, BasicAllocator's data should be written at
	// position endBeforeMarshal, not via Allocate().
	// This test verifies that the data is intended to be appended at that position.

	// Write the marshaled data at the End position
	_, err = tmpFile.WriteAt(data, endBeforeMarshal)
	if err != nil {
		t.Fatalf("failed to write data: %v", err)
	}

	// Verify the data was written at the correct position
	fileInfo, err := tmpFile.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	expectedFileSize := endBeforeMarshal + int64(len(data))
	if fileInfo.Size() != expectedFileSize {
		t.Errorf("file size mismatch: got %d, want %d", fileInfo.Size(), expectedFileSize)
	}

	// Load the allocator back from the file and verify it has the same state
	tmpFile2, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to open file for reading: %v", err)
	}
	defer tmpFile2.Close()

	allocator2, err := NewBasicAllocator(tmpFile2)
	if err != nil {
		t.Fatalf("failed to create new BasicAllocator: %v", err)
	}

	// Unmarshal the persisted data
	if err := allocator2.Unmarshal(data); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Verify the loaded allocator has the same End position
	if allocator2.End != endBeforeMarshal {
		t.Errorf("loaded allocator End mismatch: got %d, want %d", allocator2.End, endBeforeMarshal)
	}
}

// TestBasicAllocatorNotAllocatedForSelf verifies that BasicAllocator doesn't
// allocate an object for its own marshaled data - it just appends to the file.
func TestBasicAllocatorNotAllocatedForSelf(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_allocator_not_allocated")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	allocator, err := NewBasicAllocator(tmpFile)
	if err != nil {
		t.Fatalf("failed to create BasicAllocator: %v", err)
	}

	// Allocate one object
	_, _, err = allocator.Allocate(100)
	if err != nil {
		t.Fatalf("failed to allocate: %v", err)
	}

	endBeforeMarshal := allocator.End

	// Marshal the allocator
	data, err := allocator.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	// The key: we write data at endBeforeMarshal without calling Allocate()
	// This means the BasicAllocator's own data should NOT be in the allocations map
	// and should NOT have consumed an allocated object ID/position.

	// Write data at the End position
	_, err = tmpFile.WriteAt(data, endBeforeMarshal)
	if err != nil {
		t.Fatalf("failed to write data: %v", err)
	}

	// Create a new allocator by loading from file
	tmpFile2, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer tmpFile2.Close()

	allocator2, err := NewBasicAllocator(tmpFile2)
	if err != nil {
		t.Fatalf("failed to create new allocator: %v", err)
	}

	// Unmarshal the data
	if err := allocator2.Unmarshal(data); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// After loading, the End should be the same as before (just unmarshaled state)
	if allocator2.End != endBeforeMarshal {
		t.Errorf("loaded allocator End: got %d, want %d", allocator2.End, endBeforeMarshal)
	}

	// The next allocation should start at the End position
	nextObjId, nextOffset, err := allocator2.Allocate(50)
	if err != nil {
		t.Fatalf("failed to allocate after reload: %v", err)
	}

	// The next allocation should come AFTER the current End
	if nextOffset != FileOffset(endBeforeMarshal) {
		t.Errorf("next allocation offset: got %d, want %d", nextOffset, endBeforeMarshal)
	}

	// And the next object ID should match
	if nextObjId != ObjectId(endBeforeMarshal) {
		t.Errorf("next allocation objId: got %d, want %d", nextObjId, endBeforeMarshal)
	}
}
