package store

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cbehopkins/bobbob"
)

func TestNewConcurrentStore(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_new.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("expected no error creating concurrent store, got %v", err)
	}
	defer cs.Close()

	if cs == nil {
		t.Fatal("expected NewConcurrentStore to return non-nil store")
	}
	if cs.innerStore == nil {
		t.Error("expected innerStore to be initialized")
	}
	if cs.lockMap == nil {
		t.Error("expected lockMap to be initialized")
	}
}

func TestNewConcurrentStoreWrapping(t *testing.T) {
	// Test wrapping a baseStore with concurrency
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_wrapping.db")
	defer os.Remove(tmpFile)

	// Create a basic store
	basicStore, err := NewBasicStore(tmpFile)
	if err != nil {
		t.Fatalf("failed to create basic store: %v", err)
	}
	defer basicStore.Close()

	// Wrap it with concurrent store
	concStore := NewConcurrentStoreWrapping(basicStore, 0)
	defer concStore.Close()

	if concStore.innerStore != basicStore {
		t.Error("expected innerStore to be the wrapped basicStore")
	}
	if concStore.lockMap == nil {
		t.Error("expected lockMap to be initialized")
	}

	// Test that operations work through the wrapped store
	objId, err := concStore.NewObj(100)
	if err != nil {
		t.Fatalf("failed to create object: %v", err)
	}
	if objId == bobbob.ObjNotAllocated {
		t.Error("expected valid object ID")
	}

	// Verify GetObjectInfo works through type assertion
	info, found := concStore.GetObjectInfo(objId)
	if !found {
		t.Error("expected to find object info")
	}
	// Allocator may round up to block size
	if info.Size < 100 {
		t.Errorf("expected size >= 100, got %d", info.Size)
	}
}

func TestConcurrentStoreNewObj(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_newobj.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	objId, err := cs.NewObj(100)
	if err != nil {
		t.Fatalf("expected no error creating object, got %v", err)
	}
	if objId == bobbob.ObjNotAllocated {
		t.Error("expected valid object ID")
	}
}

func TestConcurrentStorePrimeObject(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_prime.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	primeId, err := cs.PrimeObject(64)
	if err != nil {
		t.Fatalf("expected no error getting prime object, got %v", err)
	}
	expectedPrime := ObjectId(PrimeObjectStart())
	if primeId != expectedPrime {
		t.Errorf("expected prime object to be ObjectId(%d), got %d", expectedPrime, primeId)
	}

	// Second call should return same ID
	primeId2, err := cs.PrimeObject(64)
	if err != nil {
		t.Fatalf("expected no error on second prime object call, got %v", err)
	}
	if primeId2 != primeId {
		t.Errorf("expected same prime object ID, got %d and %d", primeId, primeId2)
	}
}

func TestConcurrentStoreLateWriteAndRead(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_writeread.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	// Write data
	objId, writer, finisher, err := cs.LateWriteNewObj(100)
	if err != nil {
		t.Fatalf("LateWriteNewObj failed: %v", err)
	}

	testData := []byte("hello concurrent world")
	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("expected to write %d bytes, wrote %d", len(testData), n)
	}

	if finisher != nil {
		err = finisher()
		if err != nil {
			t.Fatalf("finisher failed: %v", err)
		}
	}

	// Read data back
	reader, finisher, err := cs.LateReadObj(objId)
	if err != nil {
		t.Fatalf("LateReadObj failed: %v", err)
	}

	readData := make([]byte, len(testData))
	n, err = reader.Read(readData)
	if err != nil && err != io.EOF {
		t.Fatalf("read failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("expected to read %d bytes, read %d", len(testData), n)
	}

	if finisher != nil {
		err = finisher()
		if err != nil {
			t.Fatalf("read finisher failed: %v", err)
		}
	}

	if string(readData) != string(testData) {
		t.Errorf("expected data %q, got %q", testData, readData)
	}
}

func TestConcurrentStoreWriteToObj(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_writetoobj.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	// Create object
	objId, err := cs.NewObj(50)
	if err != nil {
		t.Fatalf("NewObj failed: %v", err)
	}

	// Write to existing object
	writer, finisher, err := cs.WriteToObj(objId)
	if err != nil {
		t.Fatalf("WriteToObj failed: %v", err)
	}

	testData := []byte("update test")
	_, err = writer.Write(testData)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if finisher != nil {
		err = finisher()
		if err != nil {
			t.Fatalf("finisher failed: %v", err)
		}
	}

	// Read back
	reader, finisher, err := cs.LateReadObj(objId)
	if err != nil {
		t.Fatalf("LateReadObj failed: %v", err)
	}

	readData := make([]byte, len(testData))
	_, err = reader.Read(readData)
	if err != nil && err != io.EOF {
		t.Fatalf("read failed: %v", err)
	}

	if finisher != nil {
		err = finisher()
		if err != nil {
			t.Fatalf("read finisher failed: %v", err)
		}
	}

	if string(readData) != string(testData) {
		t.Errorf("expected %q, got %q", testData, readData)
	}
}

func TestConcurrentStoreDeleteObj(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_delete.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	// Create object
	objId, err := cs.NewObj(50)
	if err != nil {
		t.Fatalf("NewObj failed: %v", err)
	}

	// Delete it
	err = cs.DeleteObj(objId)
	if err != nil {
		t.Fatalf("DeleteObj failed: %v", err)
	}

	// Try to read deleted object - should fail
	_, _, err = cs.LateReadObj(objId)
	if err == nil {
		t.Error("expected error reading deleted object, got nil")
	}
}

func TestConcurrentStoreGetObjectInfo(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_getinfo.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	objId, err := cs.NewObj(123)
	if err != nil {
		t.Fatalf("NewObj failed: %v", err)
	}

	info, found := cs.GetObjectInfo(objId)
	if !found {
		t.Error("expected to find object info")
	}
	// Allocator may round up to block size
	if info.Size < 123 {
		t.Errorf("expected size >= 123, got %d", info.Size)
	}
}

func TestConcurrentStoreWriteBatchedObjs(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_batched.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	// Create multiple objects with a size that matches allocator block stride to ensure contiguity
	sizes := []int{64, 64, 64}
	objIds, _, err := cs.AllocateRun(64, len(sizes))
	if err == ErrAllocateRunUnsupported {
		t.Skip("AllocateRun unsupported")
	}
	if err != nil {
		t.Fatalf("AllocateRun failed: %v", err)
	}

	// Write batched data
	data := []byte("AAA BBB CCC")
	err = cs.WriteBatchedObjs(objIds, data, sizes)
	if err != nil {
		t.Fatalf("WriteBatchedObjs failed: %v", err)
	}

	// Read back first object
	reader, finisher, err := cs.LateReadObj(objIds[0])
	if err != nil {
		t.Fatalf("LateReadObj failed: %v", err)
	}

	readData := make([]byte, 10)
	_, err = reader.Read(readData)
	if err != nil && err != io.EOF {
		t.Fatalf("read failed: %v", err)
	}
	if finisher != nil {
		finisher()
	}

	expected := "AAA BBB C"
	if string(readData[:9]) != expected {
		t.Errorf("expected %q, got %q", expected, string(readData[:9]))
	}
}

func TestConcurrentStoreConcurrentReadsDifferentObjects(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_reads.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	// Create multiple objects with different data
	numObjects := 10
	objIds := make([]ObjectId, numObjects)
	for i := 0; i < numObjects; i++ {
		objId, writer, finisher, err := cs.LateWriteNewObj(20)
		if err != nil {
			t.Fatalf("LateWriteNewObj failed: %v", err)
		}
		objIds[i] = objId

		data := []byte(fmt.Sprintf("object-%02d", i))
		writer.Write(data)
		if finisher != nil {
			finisher()
		}
	}

	// Concurrently read all objects
	var wg sync.WaitGroup
	errors := make(chan error, numObjects)

	for i := 0; i < numObjects; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			reader, finisher, err := cs.LateReadObj(objIds[idx])
			if err != nil {
				errors <- fmt.Errorf("read %d failed: %v", idx, err)
				return
			}
			defer finisher()

			data := make([]byte, 20)
			n, _ := reader.Read(data)

			expected := fmt.Sprintf("object-%02d", idx)
			if string(data[:len(expected)]) != expected {
				errors <- fmt.Errorf("object %d: expected %q, got %q", idx, expected, string(data[:n]))
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

func TestConcurrentStoreConcurrentWritesDifferentObjects(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_writes.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	// Concurrently create and write multiple objects
	numObjects := 20
	var wg sync.WaitGroup
	errors := make(chan error, numObjects)
	objIds := make([]ObjectId, numObjects)
	var objIdMutex sync.Mutex

	for i := 0; i < numObjects; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			objId, writer, finisher, err := cs.LateWriteNewObj(30)
			if err != nil {
				errors <- fmt.Errorf("write %d failed: %v", idx, err)
				return
			}

			objIdMutex.Lock()
			objIds[idx] = objId
			objIdMutex.Unlock()

			data := []byte(fmt.Sprintf("concurrent-write-%02d", idx))
			_, err = writer.Write(data)
			if err != nil {
				errors <- fmt.Errorf("write %d data failed: %v", idx, err)
			}

			err = finisher()
			if err != nil {
				errors <- fmt.Errorf("finisher %d failed: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	// Verify all objects were created
	for i, objId := range objIds {
		if objId == bobbob.ObjNotAllocated {
			t.Errorf("object %d was not allocated", i)
		}
	}
}

func TestConcurrentStoreConcurrentReadWriteSameObject(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_rw_same.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	// Create an object
	objId, writer, finisher, err := cs.LateWriteNewObj(100)
	if err != nil {
		t.Fatalf("failed to create object: %v", err)
	}
	writer.Write([]byte("initial data"))
	if finisher != nil {
		finisher()
	}

	// Concurrently read and write the same object
	var wg sync.WaitGroup
	numOps := 10

	// Writers
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			writer, finisher, err := cs.WriteToObj(objId)
			if err != nil {
				t.Errorf("WriteToObj failed: %v", err)
				return
			}
			defer finisher()

			data := []byte(fmt.Sprintf("write-%02d", idx))
			writer.Write(data)
		}(i)
	}

	// Readers
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			reader, finisher, err := cs.LateReadObj(objId)
			if err != nil {
				t.Errorf("LateReadObj failed: %v", err)
				return
			}
			defer finisher()

			data := make([]byte, 20)
			reader.Read(data)
		}(i)
	}

	wg.Wait()
}

func TestConcurrentStoreLookupObjectMutex(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_mutex.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	objId := ObjectId(42)

	// First acquire creates the lock
	lock1 := cs.acquireObjectLock(objId)
	if lock1 == nil {
		t.Fatal("expected non-nil lock")
	}
	if lock1.refCount.Load() != 1 {
		t.Errorf("expected refCount=1, got %d", lock1.refCount.Load())
	}

	// Second acquire returns the same lock (while ref count > 0)
	lock2 := cs.acquireObjectLock(objId)
	if lock2 != lock1 {
		t.Error("expected same lock instance while in use")
	}
	if lock2.refCount.Load() != 2 {
		t.Errorf("expected refCount=2, got %d", lock2.refCount.Load())
	}

	// Release both references
	cs.releaseObjectLock(objId, lock1)
	cs.releaseObjectLock(objId, lock2)

	// Lock should be removed from map after all references released
	cs.lock.RLock()
	_, stillInMap := cs.lockMap[objId]
	cs.lock.RUnlock()
	if stillInMap {
		t.Error("expected lock to be removed from map after all references released")
	}

	// After removal, next acquire gets a lock (may be pooled, that's fine)
	lock3 := cs.acquireObjectLock(objId)
	if lock3 == nil {
		t.Fatal("expected non-nil lock on re-acquire")
	}
	if lock3.refCount.Load() != 1 {
		t.Errorf("expected refCount=1 on new acquire, got %d", lock3.refCount.Load())
	}
	cs.releaseObjectLock(objId, lock3)

	// Different object gets different lock (while both are in the map)
	lock4 := cs.acquireObjectLock(ObjectId(99))
	lock5 := cs.acquireObjectLock(objId)
	if lock4 == lock5 {
		t.Error("expected different lock instances for different objects")
	}
	cs.releaseObjectLock(ObjectId(99), lock4)
	cs.releaseObjectLock(objId, lock5)
}

func TestConcurrentStoreFinisherUnlocksCorrectly(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_finisher.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	// Create object
	objId, writer, finisher, err := cs.LateWriteNewObj(50)
	if err != nil {
		t.Fatalf("LateWriteNewObj failed: %v", err)
	}

	writer.Write([]byte("test"))

	// Call finisher to unlock
	err = finisher()
	if err != nil {
		t.Fatalf("finisher failed: %v", err)
	}

	// Should be able to read immediately after write finishes
	reader, finisher2, err := cs.LateReadObj(objId)
	if err != nil {
		t.Fatalf("LateReadObj failed: %v", err)
	}

	data := make([]byte, 4)
	reader.Read(data)
	if finisher2 != nil {
		finisher2()
	}

	if string(data) != "test" {
		t.Errorf("expected %q, got %q", "test", data)
	}
}

func TestConcurrentStoreClose(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_close.db")
	defer os.Remove(tmpFile)

	cs, err := NewConcurrentStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Create some objects
	cs.NewObj(100)
	cs.NewObj(200)

	err = cs.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}
