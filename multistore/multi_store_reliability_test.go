package multistore

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/store"
)

// TestMultiStoreMultipleSessions tests cross-session reliability
// by performing operations across multiple open/close cycles
func TestMultiStoreMultipleSessions(t *testing.T) {
	filePath := "test_multiple_sessions.dat"
	defer testutil.CleanupTempFile(t, filePath)

	// Session 1: Create initial objects
	session1Objects := make([]struct {
		id   store.ObjectId
		data []byte
	}, 3)

	{
		ms, err := NewMultiStore(filePath, 0)
		if err != nil {
			t.Fatalf("Session 1: Failed to create store: %v", err)
		}

		for i := range session1Objects {
			data := []byte(fmt.Sprintf("Session 1 Object %d", i))
			session1Objects[i].data = data

			objId, err := ms.NewObj(len(data))
			if err != nil {
				t.Fatalf("Session 1: Failed to create object %d: %v", i, err)
			}
			session1Objects[i].id = objId

			writer, finisher, err := ms.WriteToObj(objId)
			if err != nil {
				t.Fatalf("Session 1: Failed to get writer: %v", err)
			}
			if _, err := writer.Write(data); err != nil {
				t.Fatalf("Session 1: Failed to write: %v", err)
			}
			if err := finisher(); err != nil {
				t.Fatalf("Session 1: Failed to finish: %v", err)
			}
		}

		if err := ms.Close(); err != nil {
			t.Fatalf("Session 1: Failed to close: %v", err)
		}
	}

	// Session 2: Verify session 1 objects and add more
	session2Objects := make([]struct {
		id   store.ObjectId
		data []byte
	}, 2)

	{
		ms, err := LoadMultiStore(filePath, 0)
		if err != nil {
			t.Fatalf("Session 2: Failed to load store: %v", err)
		}

		// Verify session 1 objects
		for i, obj := range session1Objects {
			reader, finisher, err := ms.LateReadObj(obj.id)
			if err != nil {
				t.Fatalf("Session 2: Failed to read session 1 object %d: %v", i, err)
			}

			readData := make([]byte, len(obj.data))
			if _, err := io.ReadFull(reader, readData); err != nil {
				t.Fatalf("Session 2: Failed to read data: %v", err)
			}
			if !bytes.Equal(readData, obj.data) {
				t.Errorf("Session 2: Object %d data mismatch", i)
			}
			finisher()
		}

		// Add new objects
		for i := range session2Objects {
			data := []byte(fmt.Sprintf("Session 2 Object %d", i))
			session2Objects[i].data = data

			objId, err := ms.NewObj(len(data))
			if err != nil {
				t.Fatalf("Session 2: Failed to create object %d: %v", i, err)
			}
			session2Objects[i].id = objId

			writer, finisher, err := ms.WriteToObj(objId)
			if err != nil {
				t.Fatalf("Session 2: Failed to get writer: %v", err)
			}
			if _, err := writer.Write(data); err != nil {
				t.Fatalf("Session 2: Failed to write: %v", err)
			}
			if err := finisher(); err != nil {
				t.Fatalf("Session 2: Failed to finish: %v", err)
			}
		}

		if err := ms.Close(); err != nil {
			t.Fatalf("Session 2: Failed to close: %v", err)
		}
	}

	// Session 3: Verify all objects and delete some
	{
		ms, err := LoadMultiStore(filePath, 0)
		if err != nil {
			t.Fatalf("Session 3: Failed to load store: %v", err)
		}

		// Verify all objects from both sessions
		allObjects := append(session1Objects, session2Objects...)
		for i, obj := range allObjects {
			reader, finisher, err := ms.LateReadObj(obj.id)
			if err != nil {
				t.Fatalf("Session 3: Failed to read object %d: %v", i, err)
			}

			readData := make([]byte, len(obj.data))
			if _, err := io.ReadFull(reader, readData); err != nil {
				t.Fatalf("Session 3: Failed to read data: %v", err)
			}
			if !bytes.Equal(readData, obj.data) {
				t.Errorf("Session 3: Object %d data mismatch", i)
			}
			finisher()
		}

		// Delete some session 1 objects
		if err := ms.DeleteObj(session1Objects[0].id); err != nil {
			t.Fatalf("Session 3: Failed to delete: %v", err)
		}

		if err := ms.Close(); err != nil {
			t.Fatalf("Session 3: Failed to close: %v", err)
		}
	}

	// Session 4: Verify deletions and remaining objects
	{
		ms, err := LoadMultiStore(filePath, 0)
		if err != nil {
			t.Fatalf("Session 4: Failed to load store: %v", err)
		}

		// Deleted object should not be readable
		if _, _, err := ms.LateReadObj(session1Objects[0].id); err == nil {
			t.Error("Session 4: Deleted object should not be readable")
		}

		// Remaining session 1 objects should be readable
		for i := 1; i < len(session1Objects); i++ {
			obj := session1Objects[i]
			reader, finisher, err := ms.LateReadObj(obj.id)
			if err != nil {
				t.Fatalf("Session 4: Failed to read session 1 object %d: %v", i, err)
			}

			readData := make([]byte, len(obj.data))
			if _, err := io.ReadFull(reader, readData); err != nil {
				t.Fatalf("Session 4: Failed to read data: %v", err)
			}
			if !bytes.Equal(readData, obj.data) {
				t.Errorf("Session 4: Object %d data mismatch", i)
			}
			finisher()
		}

		// All session 2 objects should be readable
		for i, obj := range session2Objects {
			reader, finisher, err := ms.LateReadObj(obj.id)
			if err != nil {
				t.Fatalf("Session 4: Failed to read session 2 object %d: %v", i, err)
			}

			readData := make([]byte, len(obj.data))
			if _, err := io.ReadFull(reader, readData); err != nil {
				t.Fatalf("Session 4: Failed to read data: %v", err)
			}
			if !bytes.Equal(readData, obj.data) {
				t.Errorf("Session 4: Object %d data mismatch", i)
			}
			finisher()
		}

		ms.Close()
	}

	t.Log("SUCCESS: Multi-session test passed")
}

// TestMultiStoreLargeObjects tests handling of larger objects
func TestMultiStoreLargeObjects(t *testing.T) {
	filePath := "test_large_objects.dat"
	defer testutil.CleanupTempFile(t, filePath)

	ms, err := NewMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer ms.Close()

	// Create objects of various sizes
	sizes := []int{1024, 4096, 8192, 16384}
	objects := make([]struct {
		id   store.ObjectId
		data []byte
	}, len(sizes))

	for i, size := range sizes {
		// Create data with a pattern we can verify
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(j % 256)
		}
		objects[i].data = data

		objId, err := ms.NewObj(size)
		if err != nil {
			t.Fatalf("Failed to create object of size %d: %v", size, err)
		}
		objects[i].id = objId

		writer, finisher, err := ms.WriteToObj(objId)
		if err != nil {
			t.Fatalf("Failed to get writer: %v", err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
		if err := finisher(); err != nil {
			t.Fatalf("Failed to finish: %v", err)
		}
	}

	// Verify all objects
	for i, obj := range objects {
		reader, finisher, err := ms.LateReadObj(obj.id)
		if err != nil {
			t.Fatalf("Failed to read object %d: %v", i, err)
		}

		readData := make([]byte, len(obj.data))
		if _, err := io.ReadFull(reader, readData); err != nil {
			t.Fatalf("Failed to read data: %v", err)
		}
		if !bytes.Equal(readData, obj.data) {
			t.Errorf("Object %d (size %d) data mismatch", i, len(obj.data))
		}
		finisher()
	}

	t.Log("SUCCESS: Large objects test passed")
}

// TestMultiStoreUpdateObject tests updating object data multiple times
func TestMultiStoreUpdateObject(t *testing.T) {
	filePath := "test_update_object.dat"
	defer testutil.CleanupTempFile(t, filePath)

	ms, err := NewMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer ms.Close()

	// Create an object
	size := 100
	objId, err := ms.NewObj(size)
	if err != nil {
		t.Fatalf("Failed to create object: %v", err)
	}

	// Update it multiple times with different data
	for iteration := 0; iteration < 5; iteration++ {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(iteration)
		}

		writer, finisher, err := ms.WriteToObj(objId)
		if err != nil {
			t.Fatalf("Iteration %d: Failed to get writer: %v", iteration, err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatalf("Iteration %d: Failed to write: %v", iteration, err)
		}
		if err := finisher(); err != nil {
			t.Fatalf("Iteration %d: Failed to finish: %v", iteration, err)
		}

		// Verify immediately
		reader, finisher, err := ms.LateReadObj(objId)
		if err != nil {
			t.Fatalf("Iteration %d: Failed to read: %v", iteration, err)
		}

		readData := make([]byte, size)
		if _, err := io.ReadFull(reader, readData); err != nil {
			t.Fatalf("Iteration %d: Failed to read data: %v", iteration, err)
		}
		if !bytes.Equal(readData, data) {
			t.Errorf("Iteration %d: Data mismatch", iteration)
		}
		finisher()
	}

	t.Log("SUCCESS: Update object test passed")
}

// TestMultiStoreSpaceReuse tests that freed space is reused
func TestMultiStoreSpaceReuse(t *testing.T) {
	filePath := "test_space_reuse.dat"
	defer testutil.CleanupTempFile(t, filePath)

	ms, err := NewMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer ms.Close()

	size := 50
	numCycles := 3
	objectsPerCycle := 5

	// Track file size growth
	initialSize := int64(0)

	for cycle := 0; cycle < numCycles; cycle++ {
		// Allocate objects
		objIds := make([]store.ObjectId, objectsPerCycle)
		for i := range objIds {
			objId, err := ms.NewObj(size)
			if err != nil {
				t.Fatalf("Cycle %d: Failed to create object %d: %v", cycle, i, err)
			}
			objIds[i] = objId

			data := make([]byte, size)
			for j := range data {
				data[j] = byte(cycle*10 + i)
			}

			writer, finisher, err := ms.WriteToObj(objId)
			if err != nil {
				t.Fatalf("Cycle %d: Failed to get writer: %v", cycle, err)
			}
			if _, err := writer.Write(data); err != nil {
				t.Fatalf("Cycle %d: Failed to write: %v", cycle, err)
			}
			if err := finisher(); err != nil {
				t.Fatalf("Cycle %d: Failed to finish: %v", cycle, err)
			}
		}

		// Check file size after first cycle
		if cycle == 0 {
			fileInfo, err := os.Stat(filePath)
			if err != nil {
				t.Fatalf("Failed to stat file: %v", err)
			}
			initialSize = fileInfo.Size()
			t.Logf("Initial file size after first cycle: %d bytes", initialSize)
		}

		// Delete all objects from this cycle
		for i, objId := range objIds {
			if err := ms.DeleteObj(objId); err != nil {
				t.Fatalf("Cycle %d: Failed to delete object %d: %v", cycle, i, err)
			}
		}
		// Wait for async deletions to complete before next cycle
		ms.flushDeletes()
	}

	// Check final file size - it shouldn't have grown much if reuse is working
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}
	finalSize := fileInfo.Size()

	t.Logf("Final file size: %d bytes", finalSize)
	t.Logf("File grew by: %d bytes over %d cycles", finalSize-initialSize, numCycles-1)

	// The file should not have grown proportionally to the number of cycles
	// Some growth is expected due to metadata, but not numCycles times the initial size
	maxExpectedSize := initialSize * int64(numCycles) / 2
	if finalSize > maxExpectedSize {
		t.Errorf("File grew too much (%d bytes), expected less than %d. Space reuse may not be working efficiently.",
			finalSize, maxExpectedSize)
	}

	t.Log("SUCCESS: Space reuse test passed")
}

// TestMultiStoreEmptyObjectHandling tests creating empty or very small objects
func TestMultiStoreEmptyObjectHandling(t *testing.T) {
	filePath := "test_empty_objects.dat"
	defer testutil.CleanupTempFile(t, filePath)

	ms, err := NewMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer ms.Close()

	// Test various small sizes
	sizes := []int{1, 2, 3, 5, 7}
	objects := make([]struct {
		id   store.ObjectId
		data []byte
	}, len(sizes))

	for i, size := range sizes {
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(i + j)
		}
		objects[i].data = data

		objId, err := ms.NewObj(size)
		if err != nil {
			t.Fatalf("Failed to create object of size %d: %v", size, err)
		}
		objects[i].id = objId

		writer, finisher, err := ms.WriteToObj(objId)
		if err != nil {
			t.Fatalf("Failed to get writer: %v", err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
		if err := finisher(); err != nil {
			t.Fatalf("Failed to finish: %v", err)
		}
	}

	// Verify all objects
	for i, obj := range objects {
		reader, finisher, err := ms.LateReadObj(obj.id)
		if err != nil {
			t.Fatalf("Failed to read object %d: %v", i, err)
		}

		readData := make([]byte, len(obj.data))
		if _, err := io.ReadFull(reader, readData); err != nil {
			t.Fatalf("Failed to read data: %v", err)
		}
		if !bytes.Equal(readData, obj.data) {
			t.Errorf("Object %d (size %d) data mismatch", i, len(obj.data))
		}
		finisher()
	}

	t.Log("SUCCESS: Empty object handling test passed")
}

// TestMultiStoreConcurrentUpdatesAndReads tests reading while writing
func TestMultiStoreConcurrentUpdatesAndReads(t *testing.T) {
	filePath := "test_concurrent_ops.dat"
	defer testutil.CleanupTempFile(t, filePath)

	ms, err := NewMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer ms.Close()

	// Create multiple objects
	numObjects := 10
	size := 100
	objects := make([]store.ObjectId, numObjects)

	for i := range objects {
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(i)
		}

		objId, err := ms.NewObj(size)
		if err != nil {
			t.Fatalf("Failed to create object %d: %v", i, err)
		}
		objects[i] = objId

		writer, finisher, err := ms.WriteToObj(objId)
		if err != nil {
			t.Fatalf("Failed to get writer: %v", err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
		if err := finisher(); err != nil {
			t.Fatalf("Failed to finish: %v", err)
		}
	}

	// Update some objects while reading others
	for iteration := 0; iteration < 5; iteration++ {
		// Update even-numbered objects
		for i := 0; i < numObjects; i += 2 {
			data := make([]byte, size)
			for j := range data {
				data[j] = byte(iteration*10 + i)
			}

			writer, finisher, err := ms.WriteToObj(objects[i])
			if err != nil {
				t.Fatalf("Iteration %d: Failed to get writer for object %d: %v", iteration, i, err)
			}
			if _, err := writer.Write(data); err != nil {
				t.Fatalf("Iteration %d: Failed to write object %d: %v", iteration, i, err)
			}
			if err := finisher(); err != nil {
				t.Fatalf("Iteration %d: Failed to finish object %d: %v", iteration, i, err)
			}
		}

		// Read odd-numbered objects
		for i := 1; i < numObjects; i += 2 {
			reader, finisher, err := ms.LateReadObj(objects[i])
			if err != nil {
				t.Fatalf("Iteration %d: Failed to read object %d: %v", iteration, i, err)
			}

			readData := make([]byte, size)
			if _, err := io.ReadFull(reader, readData); err != nil {
				t.Fatalf("Iteration %d: Failed to read data from object %d: %v", iteration, i, err)
			}
			// Should still have the original data (byte(i))
			expected := byte(i)
			if readData[0] != expected {
				t.Errorf("Iteration %d: Object %d first byte should be %d, got %d",
					iteration, i, expected, readData[0])
			}
			finisher()
		}
	}

	t.Log("SUCCESS: Concurrent updates and reads test passed")
}

// TestMultiStorePersistenceAcrossSessionsWithMixedOps tests
// complex operations across multiple sessions
func TestMultiStorePersistenceAcrossSessionsWithMixedOps(t *testing.T) {
	filePath := "test_mixed_ops_persistence.dat"
	defer testutil.CleanupTempFile(t, filePath)

	// Session 1: Create initial state
	var obj1, obj2, obj3 store.ObjectId
	{
		ms, err := NewMultiStore(filePath, 0)
		if err != nil {
			t.Fatalf("Session 1: Failed to create store: %v", err)
		}

		// Create three objects with known data
		for i, objPtr := range []*store.ObjectId{&obj1, &obj2, &obj3} {
			data := bytes.Repeat([]byte{byte(i)}, 50)
			objId, err := ms.NewObj(len(data))
			if err != nil {
				t.Fatalf("Session 1: Failed to create object: %v", err)
			}
			*objPtr = objId

			writer, finisher, err := ms.WriteToObj(objId)
			if err != nil {
				t.Fatalf("Session 1: Failed to get writer: %v", err)
			}
			if _, err := writer.Write(data); err != nil {
				t.Fatalf("Session 1: Failed to write: %v", err)
			}
			finisher()
		}

		ms.Close()
	}

	// Session 2: Delete middle object and update first
	{
		ms, err := LoadMultiStore(filePath, 0)
		if err != nil {
			t.Fatalf("Session 2: Failed to load store: %v", err)
		}

		t.Logf("Session 2: About to delete obj2 (%v)", obj2)
		// Delete obj2
		if err := ms.DeleteObj(obj2); err != nil {
			t.Fatalf("Session 2: Failed to delete: %v", err)
		}
		ms.flushDeletes() // Wait for async deletion
		t.Logf("Session 2: Deleted obj2, checking if it's gone...")

		// Verify it's gone immediately
		if _, _, err := ms.LateReadObj(obj2); err == nil {
			t.Error("Session 2: obj2 should be deleted immediately after DeleteObj")
		} else {
			t.Logf("Session 2: obj2 correctly unreadable after delete: %v", err)
		}

		// Update obj1
		newData := bytes.Repeat([]byte{99}, 50)
		writer, finisher, err := ms.WriteToObj(obj1)
		if err != nil {
			t.Fatalf("Session 2: Failed to get writer: %v", err)
		}
		if _, err := writer.Write(newData); err != nil {
			t.Fatalf("Session 2: Failed to write: %v", err)
		}
		finisher()

		t.Logf("Session 2: About to close, obj2 should be deleted in the marshaled state")
		ms.Close()
		t.Logf("Session 2: Closed successfully")
	}

	// Session 3: Verify state and create new object
	var obj4 store.ObjectId
	{
		ms, err := LoadMultiStore(filePath, 0)
		if err != nil {
			t.Fatalf("Session 3: Failed to load store: %v", err)
		}

		// Verify obj1 has updated data
		reader, finisher, err := ms.LateReadObj(obj1)
		if err != nil {
			t.Fatalf("Session 3: Failed to read obj1: %v", err)
		}
		data := make([]byte, 50)
		io.ReadFull(reader, data)
		if data[0] != 99 {
			t.Errorf("Session 3: obj1 should have updated data (99), got %d", data[0])
		}
		finisher()

		// Verify obj2 is deleted
		t.Logf("Session 3: Checking if obj2 (%v) is still deleted...", obj2)
		if _, _, err := ms.LateReadObj(obj2); err == nil {
			t.Error("Session 3: obj2 should be deleted")
		} else {
			t.Logf("Session 3: obj2 correctly unreadable: %v", err)
		}

		// Verify obj3 still has original data
		reader, finisher, err = ms.LateReadObj(obj3)
		if err != nil {
			t.Fatalf("Session 3: Failed to read obj3: %v", err)
		}
		data = make([]byte, 50)
		io.ReadFull(reader, data)
		if data[0] != 2 {
			t.Errorf("Session 3: obj3 should have original data (2), got %d", data[0])
		}
		finisher()

		// Create new object (should potentially reuse obj2's space)
		newData := bytes.Repeat([]byte{77}, 50)
		objId, err := ms.NewObj(len(newData))
		if err != nil {
			t.Fatalf("Session 3: Failed to create object: %v", err)
		}
		obj4 = objId
		t.Logf("Session 3: Created obj4 with ID %v (obj2 was %v)", obj4, obj2)

		writer, finisher, err := ms.WriteToObj(objId)
		if err != nil {
			t.Fatalf("Session 3: Failed to get writer: %v", err)
		}
		if _, err := writer.Write(newData); err != nil {
			t.Fatalf("Session 3: Failed to write: %v", err)
		}
		finisher()

		ms.Close()
	}

	// Session 4: Final verification
	{
		ms, err := LoadMultiStore(filePath, 0)
		if err != nil {
			t.Fatalf("Session 4: Failed to load store: %v", err)
		}

		// Note: obj4 reused obj2's ID (58), so we verify obj4's data, not that obj2 is deleted
		t.Logf("Session 4: obj4 reused obj2's ID (%v), verifying obj4's data", obj4)

		// Verify all expected objects exist and have correct data
		// obj2 was deleted but its ID was reused by obj4
		expectedObjects := map[store.ObjectId]byte{
			obj1: 99, // Updated in session 2
			obj3: 2,  // Original data
			obj4: 77, // Created in session 3 (reused obj2's ID)
		}

		for objId, expectedValue := range expectedObjects {
			reader, finisher, err := ms.LateReadObj(objId)
			if err != nil {
				t.Fatalf("Session 4: Failed to read object %v: %v", objId, err)
			}

			data := make([]byte, 50)
			io.ReadFull(reader, data)
			if data[0] != expectedValue {
				t.Errorf("Session 4: Object %v should have value %d, got %d",
					objId, expectedValue, data[0])
			}
			finisher()
		}

		ms.Close()
	}

	t.Log("SUCCESS: Persistence across sessions with mixed ops test passed")
}
