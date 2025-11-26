package collections

import (
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"bobbob/internal/store"
	"bobbob/internal/yggdrasil"
)

func TestMultiStoreDeleteObjFreesAllocation(t *testing.T) {
	// Create a temporary store
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_multi_store.bin")

	ms, err := NewMultiStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create multi store: %v", err)
	}
	defer ms.Close()

	// Determine the block size used by the allocator
	// PersistentTreapObjectSizes returns the sizes used
	blockSizes := yggdrasil.PersistentTreapObjectSizes()
	if len(blockSizes) == 0 {
		t.Fatal("No block sizes configured")
	}
	// Use the second block size if available (first might be 8 bytes which maps to ObjectId 0)
	testSize := blockSizes[len(blockSizes)-1] // Use the last block size
	t.Logf("Using block size: %d from sizes: %v", testSize, blockSizes)

	// Allocate an object
	objId, err := ms.NewObj(testSize)
	if err != nil {
		t.Fatalf("Failed to allocate object: %v", err)
	}

	t.Logf("Allocated object with ID: %d", objId)

	// Verify the object was allocated
	if !store.IsValidObjectId(objId) {
		t.Fatalf("Expected valid ObjectId, got: %v", objId)
	}

	// Verify the object exists in the object map
	objInfo, found := ms.objectMap.Get(objId)
	if !found {
		t.Fatalf("Expected object %v to exist in object map", objId)
	}
	t.Logf("Object info: offset=%d, size=%d", objInfo.Offset, objInfo.Size)

	// Now delete the object
	err = ms.DeleteObj(objId)
	if err != nil {
		t.Fatalf("Failed to delete object: %v", err)
	}

	// Verify the object is no longer in the object map
	_, found = ms.objectMap.Get(objId)
	if found {
		t.Errorf("Expected object %v to be deleted from object map, but it still exists", objId)
	}

	// Note: The current implementation removes from the object map
	// To fully test free list integration, we need to enhance the design to:
	// 1. Store ObjectInfo (with FileOffset and Size) as payload in the treap
	// 2. Call allocator.Free() with the correct FileOffset and size in DeleteObj
	//
	// For now, this test verifies that:
	// - Objects can be allocated via NewObj
	// - Objects appear in the object map after allocation
	// - DeleteObj removes objects from the object map
}

func TestMultiStoreAllocateAndDelete(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_multi_store2.bin")

	ms, err := NewMultiStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create multi store: %v", err)
	}
	defer ms.Close()

	blockSizes := yggdrasil.PersistentTreapObjectSizes()
	if len(blockSizes) == 0 {
		t.Fatal("No block sizes configured")
	}
	testSize := blockSizes[0]

	// Allocate multiple objects
	const numObjects = 10
	objectIds := make([]store.ObjectId, numObjects)

	for i := 0; i < numObjects; i++ {
		objId, err := ms.NewObj(testSize)
		if err != nil {
			t.Fatalf("Failed to allocate object %d: %v", i, err)
		}
		objectIds[i] = objId
	}

	// Verify all objects exist in the object map
	for i, objId := range objectIds {
		_, found := ms.objectMap.Get(objId)
		if !found {
			t.Errorf("Object %d (id=%v) not found in object map", i, objId)
		}
	}

	// Delete half of the objects
	for i := 0; i < numObjects/2; i++ {
		err := ms.DeleteObj(objectIds[i])
		if err != nil {
			t.Fatalf("Failed to delete object %d: %v", i, err)
		}
	}

	// Verify deleted objects are gone
	for i := 0; i < numObjects/2; i++ {
		_, found := ms.objectMap.Get(objectIds[i])
		if found {
			t.Errorf("Object %d (id=%v) should be deleted but still exists", i, objectIds[i])
		}
	}

	// Verify remaining objects still exist
	for i := numObjects / 2; i < numObjects; i++ {
		_, found := ms.objectMap.Get(objectIds[i])
		if !found {
			t.Errorf("Object %d (id=%v) should still exist but was not found", i, objectIds[i])
		}
	}
}

// TestMultiStoreDeleteAddsToFreeList verifies that deleting an object
// actually adds it to the allocator's free list by checking if the space
// can be reused for a subsequent allocation
func TestMultiStoreDeleteAddsToFreeList(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_multi_store_freelist.bin")

	ms, err := NewMultiStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create multi store: %v", err)
	}
	defer ms.Close()

	blockSizes := yggdrasil.PersistentTreapObjectSizes()
	testSize := blockSizes[len(blockSizes)-1] // Use a fixed block size

	// Allocate first object
	objId1, err := ms.NewObj(testSize)
	if err != nil {
		t.Fatalf("Failed to allocate first object: %v", err)
	}
	t.Logf("Allocated first object with ID: %d", objId1)

	// Get the ObjectInfo for the first object
	objInfo1, found := ms.objectMap.Get(objId1)
	if !found {
		t.Fatalf("Expected to find first object in map")
	}
	firstOffset := objInfo1.Offset
	t.Logf("First object at offset: %d, size: %d", firstOffset, objInfo1.Size)

	// Allocate second object to ensure we're not at the beginning
	objId2, err := ms.NewObj(testSize)
	if err != nil {
		t.Fatalf("Failed to allocate second object: %v", err)
	}
	t.Logf("Allocated second object with ID: %d", objId2)

	// Delete the first object
	err = ms.DeleteObj(objId1)
	if err != nil {
		t.Fatalf("Failed to delete first object: %v", err)
	}
	t.Logf("Deleted first object")

	// Verify it's gone from the map
	_, found = ms.objectMap.Get(objId1)
	if found {
		t.Errorf("Expected first object to be deleted from map")
	}

	// Allocate a third object of the same size
	// It should reuse the space from the first object
	objId3, err := ms.NewObj(testSize)
	if err != nil {
		t.Fatalf("Failed to allocate third object: %v", err)
	}
	t.Logf("Allocated third object with ID: %d", objId3)

	// Get the ObjectInfo for the third object
	objInfo3, found := ms.objectMap.Get(objId3)
	if !found {
		t.Fatalf("Expected to find third object in map")
	}
	t.Logf("Third object at offset: %d, size: %d", objInfo3.Offset, objInfo3.Size)

	// Verify that the third object reused the first object's space
	// This proves the free list is working
	if objInfo3.Offset != firstOffset {
		t.Errorf("Expected third object to reuse first object's space at offset %d, but got offset %d",
			firstOffset, objInfo3.Offset)
		t.Logf("This indicates the allocator's free list is not being updated properly by DeleteObj")
	} else {
		t.Logf("SUCCESS: Third object reused the freed space from first object - free list is working!")
	}
}

// TestMultiStoreLateWriteAndRead tests writing and reading objects using Late methods
func TestMultiStoreLateWriteAndRead(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_late_io.bin")

	ms, err := NewMultiStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create multi store: %v", err)
	}
	defer ms.Close()

	// Test data
	testData := []byte("Hello, World! This is test data for Late I/O operations.")
	size := len(testData)

	// Write using LateWriteNewObj
	objId, writer, finisher, err := ms.LateWriteNewObj(size)
	if err != nil {
		t.Fatalf("LateWriteNewObj failed: %v", err)
	}
	t.Logf("Allocated object ID: %d (valid: %v)", objId, store.IsValidObjectId(objId))
	if finisher != nil {
		defer finisher()
	}

	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != size {
		t.Errorf("Expected to write %d bytes, wrote %d", size, n)
	}
	t.Logf("Wrote %d bytes to object %d", n, objId)

	// Read back using LateReadObj
	reader, finisher2, err := ms.LateReadObj(objId)
	if err != nil {
		t.Fatalf("LateReadObj failed: %v", err)
	}
	if finisher2 != nil {
		defer finisher2()
	}

	readData := make([]byte, size)
	n, err = reader.Read(readData)
	if err != nil && err != io.EOF {
		t.Fatalf("Read failed: %v", err)
	}
	if n != size {
		t.Errorf("Expected to read %d bytes, read %d", size, n)
	}

	// Verify data matches
	if string(readData) != string(testData) {
		t.Errorf("Data mismatch. Expected %q, got %q", testData, readData)
	} else {
		t.Logf("SUCCESS: Read data matches written data")
	}
}

// TestMultiStoreWriteToObj tests writing to an existing object
func TestMultiStoreWriteToObj(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_write_to_obj.bin")

	ms, err := NewMultiStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create multi store: %v", err)
	}
	defer ms.Close()

	// Create an initial object with some data
	initialData := []byte("Initial data content")
	size := len(initialData)

	objId, writer, finisher, err := ms.LateWriteNewObj(size)
	if err != nil {
		t.Fatalf("LateWriteNewObj failed: %v", err)
	}
	if finisher != nil {
		defer finisher()
	}

	_, err = writer.Write(initialData)
	if err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}
	t.Logf("Created object %d with initial data", objId)

	// Now overwrite using WriteToObj
	updatedData := []byte("Updated data content") // Exactly same length
	writer2, finisher2, err := ms.WriteToObj(objId)
	if err != nil {
		t.Fatalf("WriteToObj failed: %v", err)
	}
	if finisher2 != nil {
		defer finisher2()
	}

	_, err = writer2.Write(updatedData)
	if err != nil {
		t.Fatalf("Update write failed: %v", err)
	}
	t.Logf("Updated object %d with new data", objId)

	// Read back to verify
	reader, finisher3, err := ms.LateReadObj(objId)
	if err != nil {
		t.Fatalf("LateReadObj failed: %v", err)
	}
	if finisher3 != nil {
		defer finisher3()
	}

	readData := make([]byte, size)
	n, err := reader.Read(readData)
	if err != nil && err != io.EOF {
		t.Fatalf("Read failed: %v", err)
	}

	if string(readData) != string(updatedData) {
		t.Errorf("Data mismatch after update. Expected %q, got %q", updatedData, readData[:n])
	} else {
		t.Logf("SUCCESS: Object data was successfully updated")
	}
}

// TestMultiStoreMultipleObjects tests writing and reading multiple objects
func TestMultiStoreMultipleObjects(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_multiple_objects.bin")

	ms, err := NewMultiStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create multi store: %v", err)
	}
	defer ms.Close()

	// Create multiple objects with different data
	const numObjects = 5
	objects := make([]struct {
		id   store.ObjectId
		data []byte
	}, numObjects)

	for i := 0; i < numObjects; i++ {
		data := []byte(fmt.Sprintf("Object %d: This is test data for object number %d", i, i))
		objects[i].data = data

		objId, writer, finisher, err := ms.LateWriteNewObj(len(data))
		if err != nil {
			t.Fatalf("Failed to write object %d: %v", i, err)
		}
		if finisher != nil {
			defer finisher()
		}

		_, err = writer.Write(data)
		if err != nil {
			t.Fatalf("Failed to write data for object %d: %v", i, err)
		}

		objects[i].id = objId
		t.Logf("Created object %d with ID %d", i, objId)
	}

	// Read back all objects and verify
	for i := 0; i < numObjects; i++ {
		reader, finisher, err := ms.LateReadObj(objects[i].id)
		if err != nil {
			t.Fatalf("Failed to read object %d: %v", i, err)
		}
		if finisher != nil {
			defer finisher()
		}

		readData := make([]byte, len(objects[i].data))
		n, err := reader.Read(readData)
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read data for object %d: %v", i, err)
		}

		if string(readData[:n]) != string(objects[i].data) {
			t.Errorf("Object %d data mismatch. Expected %q, got %q",
				i, objects[i].data, readData[:n])
		}
	}

	t.Logf("SUCCESS: All %d objects verified correctly", numObjects)
}

// TestMultiStoreLateReadNonExistent tests reading a non-existent object
func TestMultiStoreLateReadNonExistent(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_read_nonexistent.bin")

	ms, err := NewMultiStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create multi store: %v", err)
	}
	defer ms.Close()

	// Try to read an object that doesn't exist
	nonExistentId := store.ObjectId(9999)
	_, _, err = ms.LateReadObj(nonExistentId)
	if err == nil {
		t.Errorf("Expected error when reading non-existent object, got nil")
	} else {
		t.Logf("SUCCESS: Got expected error for non-existent object: %v", err)
	}
}
