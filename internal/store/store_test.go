package store

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func setupTestStore(t *testing.T) (string, *store) {
	dir, err := os.MkdirTemp("", "store_test")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	filePath := filepath.Join(dir, "testfile.bin")
	store, err := NewStore(filePath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	return dir, store
}

func createObject(t *testing.T, store *store, data []byte) ObjectId {
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

	// Verify the initial offset is zero using ReadObj
	reader, err := store.LateReadObj(0)
	if err != nil {
		t.Fatalf("expected no error reading initial offset, got %v", err)
	}

	var initialOffset int64
	err = binary.Read(reader, binary.LittleEndian, &initialOffset)
	if err != nil {
		t.Fatalf("expected no error reading initial offset, got %v", err)
	}

	if initialOffset != 0 {
		t.Fatalf("expected initial offset to be 0, got %d", initialOffset)
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

	if objectId != 8 { // Initial offset is 8 bytes
		t.Fatalf("expected offset to be 8, got %d", objectId)
	}

	if len(store.objectMap.store) != 2 {
		t.Fatalf("expected objectMap length to be 2, got %d", len(store.objectMap.store))
	}

	obj, found := store.objectMap.Get(ObjectId(objectId))
	if !found {
		t.Fatalf("expected object to be found in objectMap")
	}

	if int64(obj.Offset) != int64(objectId) {
		t.Fatalf("expected object offset to be %d, got %d", objectId, obj.Offset)
	}

	if obj.Size != 10 {
		t.Fatalf("expected object size to be 10, got %d", obj.Size)
	}
}

func TestReadObj(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	data := []byte("testdata")
	offset, writer, err := store.LateWriteNewObj(len(data))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if _, err := writer.Write(data); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	reader, err := store.LateReadObj(ObjectId(offset))
	if err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
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
	objId, writer, err := store.LateWriteNewObj(len(data))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
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
	reader, err := store.LateReadObj(ObjectId(objId))
	if err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}

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
	objId1, writer1, err := store.LateWriteNewObj(len(data1))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if _, err := writer1.Write(data1); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	// Write the second object
	data2 := []byte("object2")
	objId2, writer2, err := store.LateWriteNewObj(len(data2))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
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
	reader1, err := store.LateReadObj(ObjectId(objId1))
	if err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}
	readData1 := make([]byte, len(data1))
	if _, err := io.ReadFull(reader1, readData1); err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}
	if !bytes.Equal(newData1, readData1[:len(newData1)]) {
		t.Fatalf("expected read data to be %v, got %v", newData1, readData1[:len(newData1)])
	}

	// Read back the second object and verify the data
	reader2, err := store.LateReadObj(ObjectId(objId2))
	if err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
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
	objId1, writer1, err := store.LateWriteNewObj(len(data1))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if _, err := writer1.Write(data1); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	// Attempt to modify the first object with data larger than the original
	newData1 := []byte("newobject1data") // Ensure new data is larger than the old data
	if len(newData1) <= len(data1) {
		t.Fatalf("new data is not larger than the old data")
	}
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
	reader1, err := store.LateReadObj(ObjectId(objId1))
	if err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}
	readData1 := make([]byte, len(data1))
	if _, err := io.ReadFull(reader1, readData1); err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}
	if !bytes.Equal(data1, readData1) {
		t.Fatalf("expected read data to be %v, got %v", data1, readData1)
	}
}

// FIXME Create a test that writes to some objects
// Then Reads ome of the middle objects
// then writes a new object
// Check that all objects exist with correct data
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
		cryptorand.Read(testObjects[i]) // Fill with random data
	}

	// Create a dict to track the test objects
	testObjectDict := make(map[int]*TstObject)

	// Create the test objects in the store and update the dict with the object ids
	for i, data := range testObjects {
		objId, writer, err := store.LateWriteNewObj(len(data))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
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

func mutateOneObject(testObj *TstObject, store *store) error {
	// We own the lock on this object for the duration of this function
	testObj.mutex.Lock()
	defer testObj.mutex.Unlock()

	// Write some arbitrary data to the object
	newData := make([]byte, rand.Intn(testObj.size)+1) // Ensure new data is not larger than the old data
	cryptorand.Read(newData)
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
	reader, err := store.LateReadObj(testObj.id)
	if err != nil {
		return fmt.Errorf("expected no error reading data, got %v", err)

	}
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
	objId, writer, err := store.LateWriteNewObj(len(data))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if _, err := writer.Write(data); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	store.Close()

	loadedStore, err := LoadStore(store.filePath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer loadedStore.Close()

	obj, found := loadedStore.objectMap.Get(ObjectId(objId))
	if !found {
		t.Fatalf("expected object to be found in objectMap")
	}

	if int64(obj.Offset) != int64(objId) {
		t.Fatalf("expected object offset to be %d, got %d", objId, obj.Offset)
	}

	if obj.Size != len(data) {
		t.Fatalf("expected object size to be %d, got %d", len(data), obj.Size)
	}
}
func TestDeleteObj(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Write a new object
	data := []byte("testdata")
	objId, writer, err := store.LateWriteNewObj(len(data))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if _, err := writer.Write(data); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	// Delete the object
	err = store.DeleteObj(ObjectId(objId))
	if err != nil {
		t.Fatalf("expected no error deleting object, got %v", err)
	}

	// Attempt to read the deleted object
	_, err = store.LateReadObj(ObjectId(objId))
	if err == nil {
		t.Fatalf("expected error reading deleted object, got nil")
	}

	// Verify the object is removed from the object map
	if _, found := store.objectMap.Get(ObjectId(objId)); found {
		t.Fatalf("expected object to be removed from objectMap")
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
