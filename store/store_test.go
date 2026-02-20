package store

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func setupStore(tb testing.TB, prefix string) (string, *baseStore) {
	dir, err := os.MkdirTemp("", prefix)
	if err != nil {
		tb.Fatalf("expected no error, got %v", err)
	}

	filePath := filepath.Join(dir, "testfile.bin")
	store, err := NewBasicStore(filePath)
	if err != nil {
		tb.Fatalf("expected no error, got %v", err)
	}

	return dir, store
}

func setupTestStore(t *testing.T) (string, *baseStore) {
	return setupStore(t, "store_test")
}

func setupBenchmarkStore(b *testing.B) (string, *baseStore) {
	return setupStore(b, "store_benchmark")
}

func createObject(t *testing.T, store *baseStore, data []byte) ObjectId {
	objId, err := store.NewObj(len(data))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	writer, closer, err := store.WriteToObj(objId)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if _, err := writer.Write(data); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}
	if err := closer(); err != nil {
		t.Fatalf("expected no error closing writer, got %v", err)
	}
	return ObjectId(objId)
}

func TestNewBob(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	if _, err := os.Stat(store.filePath); os.IsNotExist(err) {
		t.Fatalf("expected file to be created, but it does not exist")
	}

	// Prime object should allocate at the PrimeTable boundary
	expectedPrime := ObjectId(PrimeObjectStart())
	primeId, err := store.PrimeObject(32)
	if err != nil {
		t.Fatalf("expected no error priming store, got %v", err)
	}
	if primeId != expectedPrime {
		t.Fatalf("expected prime object id %d, got %d", expectedPrime, primeId)
	}
}

func TestWriteNewObj(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	objectId, err := store.NewObj(10)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	writer, closer, err := store.WriteToObj(objectId)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	data := []byte("testdata")
	if _, err := writer.Write(data); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}
	if err := closer(); err != nil {
		t.Fatalf("expected no error closing writer, got %v", err)
	}

	expectedOffset := ObjectId(PrimeObjectStart())
	if objectId != expectedOffset {
		t.Fatalf("expected offset to be %d, got %d", expectedOffset, objectId)
	}

	obj, found := store.GetObjectInfo(ObjectId(objectId))
	if !found {
		t.Fatalf("expected object to be found")
	}

	if int64(obj.Offset) != int64(objectId) {
		t.Fatalf("expected object offset to be %d, got %d", objectId, obj.Offset)
	}

	// Allocator may round up to block size, so allocated size >= requested size
	if obj.Size < 10 {
		t.Fatalf("expected object size >= 10, got %d", obj.Size)
	}
}

func TestReadObj(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	data := []byte("testdata")
	offset, writer, writeFinisher, err := store.LateWriteNewObj(len(data))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if writeFinisher != nil {
		defer writeFinisher()
	}

	if _, err := writer.Write(data); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	reader, readFinisher, err := store.LateReadObj(ObjectId(offset))
	if err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}
	if readFinisher != nil {
		defer readFinisher()
	}

	readData := make([]byte, len(data))
	if _, err := io.ReadFull(reader, readData); err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}

	if !bytes.Equal(data, readData) {
		t.Fatalf("expected read data to be %v, got %v", data, readData)
	}
}

func TestWriteToObj(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	data := []byte("testdata")
	objId, writer, finisher, err := store.LateWriteNewObj(len(data))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if finisher != nil {
		defer finisher()
	}

	if _, err := writer.Write(data); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	// Write to the existing object
	newData := []byte("newdata")
	writer, closer, err := store.WriteToObj(ObjectId(objId))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if _, err := writer.Write(newData); err != nil {
		t.Fatalf("expected no error writing new data, got %v", err)
	}
	err = closer()
	if err != nil {
		t.Fatalf("expected no error closing writer, got %v", err)
	}
	// Read back the updated object
	reader, finisher, err := store.LateReadObj(ObjectId(objId))
	if err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}
	defer finisher()
	readData := make([]byte, len(data))
	if _, err := io.ReadFull(reader, readData); err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}

	if !bytes.Equal(newData, readData[:len(newData)]) {
		t.Fatalf("expected read data to be %v, got %v", newData, readData[:len(newData)])
	}
}

func TestWriteToObjAndVerify(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Write the first object
	data1 := []byte("object1")
	objId1, writer1, finisher1, err := store.LateWriteNewObj(len(data1))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if finisher1 != nil {
		defer finisher1()
	}

	if _, err := writer1.Write(data1); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	// Write the second object
	data2 := []byte("object2")
	objId2, writer2, finisher2, err := store.LateWriteNewObj(len(data2))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if finisher2 != nil {
		defer finisher2()
	}

	if _, err := writer2.Write(data2); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}
	// Modify the first object using WriteToObj
	newData1 := []byte("newobj1") // Ensure new data is not larger than the old data
	if len(newData1) > len(data1) {
		t.Fatalf("new data is larger than the old data")
	}
	writer1, closer1, err := store.WriteToObj(ObjectId(objId1))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if _, err := writer1.Write(newData1); err != nil {
		t.Fatalf("expected no error writing new data, got %v", err)
	}
	err = closer1()
	if err != nil {
		t.Fatalf("expected no error closing writer, got %v", err)
	}

	// Read back the first object and verify the data
	reader1, finisher1, err := store.LateReadObj(ObjectId(objId1))
	if err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}
	if finisher1 != nil {
		defer finisher1()
	}
	readData1 := make([]byte, len(data1))
	if _, err := io.ReadFull(reader1, readData1); err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}
	if !bytes.Equal(newData1, readData1[:len(newData1)]) {
		t.Fatalf("expected read data to be %v, got %v", newData1, readData1[:len(newData1)])
	}

	// Read back the second object and verify the data
	reader2, finisher2, err := store.LateReadObj(ObjectId(objId2))
	if err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}
	if finisher2 != nil {
		defer finisher2()
	}
	readData2 := make([]byte, len(data2))
	if _, err := io.ReadFull(reader2, readData2); err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}
	if !bytes.Equal(data2, readData2) {
		t.Fatalf("expected read data to be %v, got %v", data2, readData2)
	}
}

func TestWriteToObjExceedLimit(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Write the first object
	data1 := []byte("object1")
	objId1, writer1, finisher, err := store.LateWriteNewObj(len(data1))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if finisher != nil {
		defer finisher()
	}

	if _, err := writer1.Write(data1); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	// Get the actual allocated size (may be larger than requested due to block allocation)
	obj, found := store.GetObjectInfo(ObjectId(objId1))
	if !found {
		t.Fatalf("object not found")
	}
	allocatedSize := obj.Size

	// Attempt to modify the object with data larger than the allocated size
	newData1 := make([]byte, allocatedSize+10) // Exceed allocated size
	writer1, closer1, err := store.WriteToObj(ObjectId(objId1))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	_, err = writer1.Write(newData1)
	if err != ErrWriteLimitExceeded {
		t.Fatalf("expected ErrWriteLimitExceeded, got %v", err)
	}
	err = closer1()
	if err != nil {
		t.Fatalf("expected no error closing writer, got %v", err)
	}

	// Read back the first object and verify the data has not changed
	reader1, finisher1, err := store.LateReadObj(ObjectId(objId1))
	if err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}
	if finisher1 != nil {
		defer finisher1()
	}
	readData1 := make([]byte, len(data1))
	if _, err := io.ReadFull(reader1, readData1); err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}
	if !bytes.Equal(data1, readData1) {
		t.Fatalf("expected read data to be %v, got %v", data1, readData1)
	}
}

// TestWriteReadWriteSequence tests that reading objects doesn't interfere
// with subsequent writes, and that the allocator state remains consistent.
func TestWriteReadWriteSequence(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Step 1: Write several objects with known data
	numInitialObjects := 10
	initialObjects := make(map[ObjectId][]byte)

	for i := 0; i < numInitialObjects; i++ {
		data := []byte(fmt.Sprintf("object_%d_data", i))
		objId := createObject(t, store, data)
		initialObjects[objId] = data
	}

	// Step 2: Read some of the middle objects to verify they're intact
	// Extract object IDs and sort them to find middle ones
	var objIds []ObjectId
	for objId := range initialObjects {
		objIds = append(objIds, objId)
	}

	// Read middle objects (indices 3, 4, 5)
	middleIndices := []int{3, 4, 5}
	for _, idx := range middleIndices {
		if idx >= len(objIds) {
			continue
		}
		objId := objIds[idx]
		expectedData := initialObjects[objId]

		reader, finisher, err := store.LateReadObj(objId)
		if err != nil {
			t.Fatalf("failed to read middle object %d: %v", objId, err)
		}

		readData, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("failed to read data from object %d: %v", objId, err)
		}

		if finisher != nil {
			finisher()
		}

		// Allocated size may be larger than written size, compare only written data
		if len(readData) < len(expectedData) || !bytes.Equal(expectedData, readData[:len(expectedData)]) {
			t.Errorf("middle object %d data mismatch: expected %v, got %v",
				objId, expectedData, readData[:min(len(readData), len(expectedData))])
		}
	}

	// Step 3: Write a new object after reading middle objects
	newObjectData := []byte("new_object_after_reads")
	newObjId := createObject(t, store, newObjectData)
	initialObjects[newObjId] = newObjectData

	// Step 4: Verify ALL objects (initial + new) exist with correct data
	for objId, expectedData := range initialObjects {
		reader, finisher, err := store.LateReadObj(objId)
		if err != nil {
			t.Errorf("failed to read object %d in final verification: %v", objId, err)
			continue
		}

		readData, err := io.ReadAll(reader)
		if err != nil {
			t.Errorf("failed to read data from object %d in final verification: %v", objId, err)
			if finisher != nil {
				finisher()
			}
			continue
		}

		if finisher != nil {
			finisher()
		}

		// Allocated size may be larger than written size, compare only written data
		if len(readData) < len(expectedData) || !bytes.Equal(expectedData, readData[:len(expectedData)]) {
			t.Errorf("final verification failed for object %d: expected %v, got %v",
				objId, expectedData, readData[:min(len(readData), len(expectedData))])
		}
	}
}

type TstObject struct {
	id    ObjectId
	size  int
	mutex *sync.Mutex
}

func TestConcurrentWriteToObj(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Define a test set of objects with random lengths up to 1KB
	numTestObjects := 1024
	testObjects := make([][]byte, numTestObjects)
	for i := 0; i < numTestObjects; i++ {
		length := rand.Intn(1024) + 1 // Length between 1 and 1024 bytes
		testObjects[i] = make([]byte, length)
		rand.Read(testObjects[i]) // Fill with random data
	}

	// Create a dict to track the test objects
	testObjectDict := make(map[int]*TstObject)

	// Create the test objects in the store and update the dict with the object ids
	for i, data := range testObjects {
		objId, writer, finisher, err := store.LateWriteNewObj(len(data))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if finisher != nil {
			defer finisher()
		}

		if _, err := writer.Write(data); err != nil {
			t.Fatalf("expected no error writing data, got %v", err)
		}

		testObjectDict[i] = &TstObject{id: ObjectId(objId), size: len(data), mutex: &sync.Mutex{}}
	}

	numGoroutines := 100
	// Channel to collect errors from goroutines
	errChan := make(chan error, numGoroutines*10)
	// Start a number of goroutines that mimic random actors accessing the objects
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				// Select a random test object
				objIndex := rand.Intn(len(testObjects))
				testObj := testObjectDict[objIndex]

				err := mutateOneObject(testObj, store)
				if err != nil {
					errChan <- err
					return
				}

			}
		}()
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(errChan)

	// Collect errors from the channel
	for err := range errChan {
		t.Error(err)
	}
}

func mutateOneObject(testObj *TstObject, store *baseStore) error {
	// We own the lock on this object for the duration of this function
	testObj.mutex.Lock()
	defer testObj.mutex.Unlock()

	// Write some arbitrary data to the object
	newData := make([]byte, rand.Intn(testObj.size)+1) // Ensure new data is not larger than the old data
	rand.Read(newData)
	writer, closer, err := store.WriteToObj(testObj.id)
	if err != nil {
		return fmt.Errorf("expected no error, got %v", err)
	}
	if _, err := writer.Write(newData); err != nil {
		return fmt.Errorf("Write error, got %v", err)
	}
	err = closer()
	if err != nil {
		return fmt.Errorf("expected no error closing writer, got %v", err)
	}
	reader, finisher, err := store.LateReadObj(testObj.id)
	if err != nil {
		return fmt.Errorf("expected no error reading data, got %v", err)
	}
	defer finisher()
	readData := make([]byte, len(newData))
	if _, err := io.ReadFull(reader, readData); err != nil {
		return fmt.Errorf("expected no error reading data, got %v", err)
	}
	if !bytes.Equal(newData, readData) {
		return fmt.Errorf("expected read data to be %v, got %v", newData, readData)
	}
	return nil
}

func TestLoadStore(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	data := []byte("testdata")
	objId, writer, finisher, err := store.LateWriteNewObj(len(data))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if finisher != nil {
		defer finisher()
	}

	if _, err := writer.Write(data); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	store.Close()

	loadedStore, err := LoadBaseStore(store.filePath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer loadedStore.Close()

	obj, found := loadedStore.GetObjectInfo(ObjectId(objId))
	if !found {
		t.Fatalf("expected object to be found")
	}

	if int64(obj.Offset) != int64(objId) {
		t.Fatalf("expected object offset to be %d, got %d", objId, obj.Offset)
	}

	// Allocator may round up to block size
	if obj.Size < len(data) {
		t.Fatalf("expected object size >= %d, got %d", len(data), obj.Size)
	}
}

func TestDeleteObj(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Write a new object
	data := []byte("testdata")
	objId, writer, finisher, err := store.LateWriteNewObj(len(data))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if finisher != nil {
		defer finisher()
	}

	if _, err := writer.Write(data); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	// Delete the object
	if !IsValidObjectId(ObjectId(objId)) {
		t.Fatalf("expected valid objectId, got %d", objId)
	}
	err = store.DeleteObj(ObjectId(objId))
	if err != nil {
		t.Fatalf("expected no error deleting object, got %v", err)
	}

	// Attempt to read the deleted object
	_, finisher, err = store.LateReadObj(ObjectId(objId))
	if err == nil {
		t.Fatalf("expected error reading deleted object, got nil")
	}
	if finisher != nil {
		err = finisher()
		if err == nil {
			t.Fatalf("expected error closing reader for deleted object, got nil")
		}
	}

	// Verify the object is removed
	if _, found := store.GetObjectInfo(ObjectId(objId)); found {
		t.Fatalf("expected object to be deleted")
	}
}

func TestDeleteNonExistentObj(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Attempt to delete a non-existent object
	err := store.DeleteObj(ObjectId(999))
	if err == nil {
		t.Fatalf("expected error deleting non-existent object, got nil")
	}
}

// TestSync tests the Sync method to ensure data is flushed to disk
func TestSync(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Write some data
	data := []byte("test data for sync")
	objId := createObject(t, store, data)

	// Call Sync to flush to disk
	err := store.Sync()
	if err != nil {
		t.Fatalf("expected no error during Sync, got %v", err)
	}

	// Verify the object still exists after sync
	_, found := store.GetObjectInfo(objId)
	if !found {
		t.Fatal("expected object to still exist after Sync")
	}
}

// Example demonstrates basic store usage: create, write, and read an object.
func Example() {
	// Create a temporary file for the store
	tmpFile := filepath.Join(os.TempDir(), "example_store.bin")
	defer os.Remove(tmpFile)

	// Create a new store
	s, err := NewBasicStore(tmpFile)
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Write data to a new object using the convenience function
	data := []byte("Hello, Store!")
	objId, _ := WriteNewObjFromBytes(s, data)

	// Read the data back - allocated size may be larger than written size
	readData, _ := ReadBytesFromObj(s, objId)

	// Only print the amount we originally wrote
	fmt.Printf("%s\n", string(readData[:len(data)]))
	// Output: Hello, Store!
}

// ExampleStorer_lateWriteNewObj demonstrates the Late method pattern for streaming writes.
func ExampleStorer_lateWriteNewObj() {
	tmpFile := filepath.Join(os.TempDir(), "example_late_write.bin")
	defer os.Remove(tmpFile)

	s, _ := NewBasicStore(tmpFile)
	defer s.Close()

	// Use the Late method for fine-grained control over writing
	data := []byte("Streaming data")
	objId, writer, finisher, _ := s.LateWriteNewObj(len(data))
	if finisher != nil {
		defer finisher()
	}

	// Write the data in chunks if needed
	writer.Write(data)

	// Read it back
	readData, _ := ReadBytesFromObj(s, objId)
	fmt.Printf("%s\n", string(readData))
	// Output: Streaming data
}

// ExampleStorer_writeToObj demonstrates updating an existing object.
func ExampleStorer_writeToObj() {
	tmpFile := filepath.Join(os.TempDir(), "example_update.bin")
	defer os.Remove(tmpFile)

	s, _ := NewBasicStore(tmpFile)
	defer s.Close()

	// Create an object with initial data
	initialData := []byte("Initial")
	objId, _ := WriteNewObjFromBytes(s, initialData)

	// Update the object with new data (must not exceed original size)
	newData := []byte("Updated")
	WriteBytesToObj(s, newData, objId)

	// Read the updated data
	readData, _ := ReadBytesFromObj(s, objId)
	fmt.Printf("%s\n", string(readData))
	// Output: Updated
}

// ExampleStorer_deleteObj demonstrates deleting objects and reclaiming space.
func ExampleStorer_deleteObj() {
	tmpFile := filepath.Join(os.TempDir(), "example_delete.bin")
	defer os.Remove(tmpFile)

	s, _ := NewBasicStore(tmpFile)
	defer s.Close()

	// Create two objects
	obj1, _ := WriteNewObjFromBytes(s, []byte("Object 1"))
	obj2, _ := WriteNewObjFromBytes(s, []byte("Object 2"))

	// Delete the first object
	s.DeleteObj(obj1)

	// The second object is still accessible
	obj2Data := []byte("Object 2")
	data, _ := ReadBytesFromObj(s, obj2)
	// Allocated size may be larger than written size
	fmt.Printf("%s\n", string(data[:len(obj2Data)]))
	// Output: Object 2
}

// ExampleLoadBaseStore demonstrates persisting and loading a store.
func ExampleLoadBaseStore() {
	tmpFile := filepath.Join(os.TempDir(), "example_persist.bin")
	defer os.Remove(tmpFile)

	// Create and populate a store
	s, err := NewBasicStore(tmpFile)
	if err != nil {
		panic(err)
	}
	objId, err := WriteNewObjFromBytes(s, []byte("Persistent data"))
	if err != nil {
		panic(err)
	}
	if err := s.Close(); err != nil {
		panic(err)
	}

	// Load the store from disk
	loadedStore, err := LoadBaseStore(tmpFile)
	if err != nil {
		panic(err)
	}
	defer loadedStore.Close()

	// Read the persisted object
	originalData := []byte("Persistent data")
	data, err := ReadBytesFromObj(loadedStore, objId)
	if err != nil {
		panic(err)
	}
	// Allocated size may be larger than written size, only print written data
	fmt.Printf("%s\n", string(data[:len(originalData)]))
	// Output: Persistent data
}

// These benchmarks seem to show that we should (unsurprisingly)
// * Write the largest possible payload in a single call
// * Have approx the name number of writers as CPUs
func BenchmarkWriteAt(b *testing.B) {
	dir, store := setupBenchmarkStore(b)
	defer os.RemoveAll(dir)
	defer store.Close()

	payloadSizes := []int{64, 256, 1024, 4096, 16384} // Different payload sizes
	concurrencyLevels := []int{1, 10, 100}            // Different concurrency levels

	for _, payloadSize := range payloadSizes {
		for _, concurrency := range concurrencyLevels {
			b.Run(fmt.Sprintf("PayloadSize=%d_Concurrency=%d", payloadSize, concurrency), func(b *testing.B) {
				data := make([]byte, payloadSize)
				rand.Read(data) // Fill with random data

				b.ResetTimer()
				var wg sync.WaitGroup
				for i := 0; i < concurrency; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for j := 0; j < b.N/concurrency; j++ {
							_, writer, finisher, err := store.LateWriteNewObj(len(data))
							if err != nil {
								b.Errorf("expected no error, got %v", err)
								return
							}
							if finisher != nil {
								defer finisher()
							}

							if _, err := writer.Write(data); err != nil {
								b.Errorf("expected no error writing data, got %v", err)
								return
							}
						}
					}()
				}
				wg.Wait()

				// Calculate and report bytes per operation and bytes per second
				bytesPerOp := float64(payloadSize)
				bytesPerSec := bytesPerOp / b.Elapsed().Seconds()
				b.ReportMetric(bytesPerOp, "bytes/op")
				b.ReportMetric(bytesPerSec, "bytes/sec")
			})
		}
	}
}

func BenchmarkWriteAtSingleCall(b *testing.B) {
	dir, store := setupBenchmarkStore(b)
	defer os.RemoveAll(dir)
	defer store.Close()

	payloadSizes := []int{64, 256, 1024, 4096, 16384} // Different payload sizes
	concurrencyLevels := []int{1, 10, 100}            // Different concurrency levels

	for _, payloadSize := range payloadSizes {
		for _, concurrency := range concurrencyLevels {
			b.Run(fmt.Sprintf("PayloadSize=%d_Concurrency=%d", payloadSize, concurrency), func(b *testing.B) {
				data := make([]byte, payloadSize)
				rand.Read(data) // Fill with random data

				b.ResetTimer()
				var wg sync.WaitGroup
				for i := 0; i < concurrency; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for j := 0; j < b.N/concurrency; j++ {
							_, writer, finisher, err := store.LateWriteNewObj(len(data))
							if err != nil {
								b.Errorf("expected no error, got %v", err)
								return
							}
							if finisher != nil {
								defer finisher()
							}

							if _, err := writer.Write(data); err != nil {
								b.Errorf("expected no error writing data, got %v", err)
								return
							}
						}
					}()
				}
				wg.Wait()

				// Calculate and report bytes per operation and bytes per second
				bytesPerOp := float64(payloadSize)
				bytesPerSec := bytesPerOp / b.Elapsed().Seconds()
				b.ReportMetric(bytesPerOp, "bytes/op")
				b.ReportMetric(bytesPerSec, "bytes/sec")
			})
		}
	}
}
