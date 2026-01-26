package multistore

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
)

func TestMultiStoreDeleteObjFreesAllocation(t *testing.T) {
	// Create a temporary store
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_multi_store.bin")

	ms, err := NewMultiStore(tempFile, 0)
	if err != nil {
		t.Fatalf("Failed to create multi store: %v", err)
	}
	defer ms.Close()

	// Determine the block size used by the allocator
	// PersistentTreapObjectSizes returns the sizes used
	blockSizes := treap.PersistentTreapObjectSizes()
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

	// Verify the object exists using GetObjectInfo
	objInfo, found := ms.GetObjectInfo(objId)
	if !found {
		t.Fatalf("Expected object %v to exist in allocator", objId)
	}
	t.Logf("Object info: offset=%d, size=%d", objInfo.Offset, objInfo.Size)

	// Now delete the object
	err = ms.DeleteObj(objId)
	if err != nil {
		t.Fatalf("Failed to delete object: %v", err)
	}

	// Wait for async deletion to complete
	ms.flushDeletes()

	// Verify the object is no longer in the allocator
	_, found = ms.GetObjectInfo(objId)
	if found {
		t.Errorf("Expected object %v to be deleted from allocator, but it still exists", objId)
	}

	// Note: Objects are tracked by the allocator
	// This test verifies that:
	// - Objects can be allocated via NewObj
	// - Objects appear in the allocator after allocation
	// - DeleteObj removes objects from the allocator
}

func TestMultiStoreAllocateAndDelete(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_multi_store2.bin")

	ms, err := NewMultiStore(tempFile, 0)
	if err != nil {
		t.Fatalf("Failed to create multi store: %v", err)
	}
	defer ms.Close()

	blockSizes := treap.PersistentTreapObjectSizes()
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

	// Verify all objects exist in the allocator
	for i, objId := range objectIds {
		_, found := ms.GetObjectInfo(objId)
		if !found {
			t.Errorf("Object %d (id=%v) not found in allocator", i, objId)
		}
	}

	// Delete half of the objects
	for i := 0; i < numObjects/2; i++ {
		err := ms.DeleteObj(objectIds[i])
		if err != nil {
			t.Fatalf("Failed to delete object %d: %v", i, err)
		}
	}

	// Wait for async deletions to complete
	ms.flushDeletes()

	// Verify deleted objects are gone
	for i := 0; i < numObjects/2; i++ {
		_, found := ms.GetObjectInfo(objectIds[i])
		if found {
			t.Errorf("Object %d (id=%v) should be deleted but still exists", i, objectIds[i])
		}
	}

	// Verify remaining objects still exist
	for i := numObjects / 2; i < numObjects; i++ {
		_, found := ms.GetObjectInfo(objectIds[i])
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

	ms, err := NewMultiStore(tempFile, 0)
	if err != nil {
		t.Fatalf("Failed to create multi store: %v", err)
	}
	defer ms.Close()

	blockSizes := treap.PersistentTreapObjectSizes()
	testSize := blockSizes[len(blockSizes)-1] // Use a fixed block size

	// Allocate first object
	objId1, err := ms.NewObj(testSize)
	if err != nil {
		t.Fatalf("Failed to allocate first object: %v", err)
	}
	t.Logf("Allocated first object with ID: %d", objId1)

	// Get the ObjectInfo for the first object
	objInfo1, found := ms.GetObjectInfo(objId1)
	if !found {
		t.Fatalf("Expected to find first object in allocator")
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

	// Wait for async deletion to complete
	ms.flushDeletes()

	// Verify it's gone from the allocator
	_, found = ms.GetObjectInfo(objId1)
	if found {
		t.Errorf("Expected first object to be deleted from allocator")
	}

	// Allocate a third object of the same size
	// It should reuse the space from the first object
	objId3, err := ms.NewObj(testSize)
	if err != nil {
		t.Fatalf("Failed to allocate third object: %v", err)
	}
	t.Logf("Allocated third object with ID: %d", objId3)

	// Get the ObjectInfo for the third object
	objInfo3, found := ms.GetObjectInfo(objId3)
	if !found {
		t.Fatalf("Expected to find third object in allocator")
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

	ms, err := NewMultiStore(tempFile, 0)
	if err != nil {
		t.Fatalf("Failed to create multi store: %v", err)
	}
	defer ms.Close()

	// Test data
	testData := []byte("Hello, World! This is test data for Late I/O operations.")

	// Write using helper
	objId := testutil.WriteObject(t, ms, testData)
	t.Logf("Allocated object ID: %d (valid: %v)", objId, store.IsValidObjectId(objId))

	// Verify using helper
	testutil.VerifyObject(t, ms, objId, testData)
	t.Logf("SUCCESS: Read data matches written data")
}

// TestMultiStoreWriteToObj tests writing to an existing object
func TestMultiStoreWriteToObj(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_write_to_obj.bin")

	ms, err := NewMultiStore(tempFile, 0)
	if err != nil {
		t.Fatalf("Failed to create multi store: %v", err)
	}
	defer ms.Close()

	// Create an initial object with some data
	initialData := []byte("Initial data content")

	objId := testutil.WriteObject(t, ms, initialData)
	t.Logf("Created object %d with initial data", objId)

	// Now overwrite using helper
	updatedData := []byte("Updated data content") // Exactly same length
	testutil.UpdateObject(t, ms, objId, updatedData)
	t.Logf("Updated object %d with new data", objId)

	// Verify using helper
	testutil.VerifyObject(t, ms, objId, updatedData)
	t.Logf("SUCCESS: Object data was successfully updated")
}

// TestMultiStoreMultipleObjects tests writing and reading multiple objects
func TestMultiStoreMultipleObjects(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_multiple_objects.bin")

	ms, err := NewMultiStore(tempFile, 0)
	if err != nil {
		t.Fatalf("Failed to create multi store: %v", err)
	}
	defer ms.Close()

	// Create multiple objects with different data
	// Use sizes larger than the largest block size (4096) to avoid block allocator padding
	const numObjects = 5
	objects := make([]struct {
		id   store.ObjectId
		data []byte
	}, numObjects)

	for i := 0; i < numObjects; i++ {
		// Create large enough data to exceed block allocator sizes
		data := []byte(fmt.Sprintf("Object %d: This is test data for object number %d with additional padding to exceed block sizes: %s",
			i, i, string(make([]byte, 5000))))
		objects[i].data = data
		objects[i].id = testutil.WriteObject(t, ms, data)
		t.Logf("Created object %d with ID %d", i, objects[i].id)
	}

	// Verify all objects
	for i := 0; i < numObjects; i++ {
		testutil.VerifyObject(t, ms, objects[i].id, objects[i].data)
	}

	t.Logf("SUCCESS: All %d objects verified correctly", numObjects)
}

// TestMultiStoreLateReadNonExistent tests reading a non-existent object
func TestMultiStoreLateReadNonExistent(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_read_nonexistent.bin")

	ms, err := NewMultiStore(tempFile, 0)
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
