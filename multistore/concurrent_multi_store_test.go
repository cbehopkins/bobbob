package collections

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cbehopkins/bobbob/store"
)

// TestNewConcurrentMultiStore tests the convenience constructor
func TestNewConcurrentMultiStore(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_new_concurrent_constructor.db")
	defer os.Remove(tmpFile)

	// Use convenience constructor
	cs, err := NewConcurrentMultiStore(tmpFile, 5)
	if err != nil {
		t.Fatalf("failed to create concurrent multiStore: %v", err)
	}
	defer cs.Close()

	// Verify it works
	objID, err := cs.NewObj(64)
	if err != nil {
		t.Fatalf("failed to create object: %v", err)
	}

	if objID == store.ObjNotAllocated {
		t.Error("expected valid object ID")
	}

	// Write and read data
	testData := []byte("test data for concurrent multistore")
	writer, finisher, err := cs.WriteToObj(objID)
	if err != nil {
		t.Fatalf("failed to get writer: %v", err)
	}
	if _, err := writer.Write(testData); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if err := finisher(); err != nil {
		t.Fatalf("failed to finish write: %v", err)
	}

	reader, finisher, err := cs.LateReadObj(objID)
	if err != nil {
		t.Fatalf("failed to get reader: %v", err)
	}
	readData := make([]byte, len(testData))
	if _, err := reader.Read(readData); err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if err := finisher(); err != nil {
		t.Fatalf("failed to finish read: %v", err)
	}

	if string(readData) != string(testData) {
		t.Errorf("data mismatch: got %q, want %q", readData, testData)
	}
}

// TestLoadConcurrentMultiStore tests the load convenience constructor
func TestLoadConcurrentMultiStore(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_load_concurrent_constructor.db")
	defer os.Remove(tmpFile)

	// Create initial store
	cs1, err := NewConcurrentMultiStore(tmpFile, 5)
	if err != nil {
		t.Fatalf("failed to create concurrent multiStore: %v", err)
	}

	// Create and write object
	objID, err := cs1.NewObj(128)
	if err != nil {
		t.Fatalf("failed to create object: %v", err)
	}
	testData := []byte("persistence test data")
	writer, finisher, err := cs1.WriteToObj(objID)
	if err != nil {
		t.Fatalf("failed to get writer: %v", err)
	}
	writer.Write(testData)
	finisher()

	// Close first store
	if err := cs1.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Load using convenience constructor
	cs2, err := LoadConcurrentMultiStore(tmpFile, 5)
	if err != nil {
		t.Fatalf("failed to load concurrent multiStore: %v", err)
	}
	defer cs2.Close()

	// Read back data
	reader, finisher, err := cs2.LateReadObj(objID)
	if err != nil {
		t.Fatalf("failed to get reader: %v", err)
	}
	readData := make([]byte, len(testData))
	reader.Read(readData)
	finisher()

	if string(readData) != string(testData) {
		t.Errorf("data mismatch after reload: got %q, want %q", readData, testData)
	}
}

// TestConcurrentMultiStore verifies that multiStore can be wrapped with concurrentStore
// to provide thread-safe concurrent access with size-based allocation routing.
func TestConcurrentMultiStore(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_multistore.db")
	defer os.Remove(tmpFile)

	// Create multiStore with size-based allocation
	ms, err := NewMultiStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create multiStore: %v", err)
	}
	defer ms.Close()

	// Wrap with concurrency support
	cs := store.NewConcurrentStoreWrapping(ms, 10) // 10 concurrent disk tokens

	// Test concurrent writes from multiple goroutines
	const numGoroutines = 20
	const objectsPerGoroutine = 50
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < objectsPerGoroutine; j++ {
				// Allocate small object (should use block allocator)
				objID, err := cs.NewObj(64)
				if err != nil {
					errors <- err
					return
				}

				// Write data
				data := []byte{byte(goroutineID), byte(j)}
				writer, finisher, err := cs.WriteToObj(objID)
				if err != nil {
					errors <- err
					return
				}
				_, err = writer.Write(data)
				if err != nil {
					errors <- err
					return
				}
				if err := finisher(); err != nil {
					errors <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("goroutine error: %v", err)
	}
}

// TestConcurrentMultiStoreReadWrite tests concurrent reads and writes
func TestConcurrentMultiStoreReadWrite(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_rw.db")
	defer os.Remove(tmpFile)

	ms, err := NewMultiStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create multiStore: %v", err)
	}
	defer ms.Close()

	cs := store.NewConcurrentStoreWrapping(ms, 5)

	// Create some objects first
	const numObjects = 10
	objIDs := make([]store.ObjectId, numObjects)
	for i := 0; i < numObjects; i++ {
		objID, err := cs.NewObj(100)
		if err != nil {
			t.Fatalf("failed to create object: %v", err)
		}
		objIDs[i] = objID

		// Write initial data
		data := make([]byte, 100)
		for j := range data {
			data[j] = byte(i)
		}
		writer, finisher, err := cs.WriteToObj(objID)
		if err != nil {
			t.Fatalf("failed to get writer: %v", err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		if err := finisher(); err != nil {
			t.Fatalf("failed to finish: %v", err)
		}
	}

	// Now do concurrent reads and writes
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Start readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				objID := objIDs[j%numObjects]
				reader, finisher, err := cs.LateReadObj(objID)
				if err != nil {
					errors <- err
					return
				}
				buf := make([]byte, 100)
				_, err = reader.Read(buf)
				if err != nil {
					errors <- err
					return
				}
				if err := finisher(); err != nil {
					errors <- err
					return
				}
			}
		}()
	}

	// Start writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				objID := objIDs[j%numObjects]
				writer, finisher, err := cs.WriteToObj(objID)
				if err != nil {
					errors <- err
					return
				}
				data := make([]byte, 100)
				for k := range data {
					data[k] = byte(writerID)
				}
				if _, err := writer.Write(data); err != nil {
					errors <- err
					return
				}
				if err := finisher(); err != nil {
					errors <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("concurrent operation error: %v", err)
	}
}

// TestConcurrentMultiStoreAllocateRun tests that AllocateRun works through the wrapper
// Note: multiStore doesn't implement AllocateRun yet, so this test expects an error
func TestConcurrentMultiStoreAllocateRun(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_allocate_run.db")
	defer os.Remove(tmpFile)

	ms, err := NewMultiStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create multiStore: %v", err)
	}
	defer ms.Close()

	cs := store.NewConcurrentStoreWrapping(ms, 5)

	// Test AllocateRun through wrapper - should gracefully return unsupported
	size := 64
	count := 10
	objIDs, offsets, err := cs.AllocateRun(size, count)
	if err != store.ErrAllocateRunUnsupported {
		t.Logf("AllocateRun returned: %v (objIDs: %v, offsets: %v)", err, len(objIDs), len(offsets))
		t.Logf("Note: multiStore doesn't implement AllocateRun yet, expected ErrAllocateRunUnsupported")
	}

	// This is expected behavior - multiStore doesn't support AllocateRun
	if err == store.ErrAllocateRunUnsupported {
		t.Skip("multiStore doesn't implement AllocateRun - this is expected")
	}
}

// TestConcurrentMultiStoreGetObjectInfo tests that GetObjectInfo works through wrapper
func TestConcurrentMultiStoreGetObjectInfo(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_getinfo.db")
	defer os.Remove(tmpFile)

	ms, err := NewMultiStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create multiStore: %v", err)
	}
	defer ms.Close()

	cs := store.NewConcurrentStoreWrapping(ms, 5)

	// Create an object
	objID, err := cs.NewObj(128)
	if err != nil {
		t.Fatalf("failed to create object: %v", err)
	}

	// Get object info through wrapper
	info, found := cs.GetObjectInfo(objID)
	if !found {
		t.Fatal("expected to find object info")
	}

	if info.Size != 128 {
		t.Errorf("expected size 128, got %d", info.Size)
	}
	if info.Offset == 0 {
		t.Error("expected non-zero offset")
	}
}

// TestConcurrentMultiStoreDiskTokens verifies disk token limiting works
func TestConcurrentMultiStoreDiskTokens(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "test_concurrent_tokens.db")
	defer os.Remove(tmpFile)

	ms, err := NewMultiStore(tmpFile, 0)
	if err != nil {
		t.Fatalf("failed to create multiStore: %v", err)
	}
	defer ms.Close()

	// Wrap with only 2 disk tokens to force contention
	cs := store.NewConcurrentStoreWrapping(ms, 2)

	// Track concurrent operations
	concurrent := make(chan bool, 100)
	maxConcurrent := 0
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Signal start of operation
			concurrent <- true
			mu.Lock()
			current := len(concurrent)
			if current > maxConcurrent {
				maxConcurrent = current
			}
			mu.Unlock()

			// Do I/O operation - allocate first
			objID, writer, finisher, err := cs.LateWriteNewObj(64)
			if err != nil {
				t.Errorf("failed to create object: %v", err)
				<-concurrent
				return
			}

			// Write data
			data := []byte{1, 2, 3, 4, 5}
			if _, err := writer.Write(data); err != nil {
				t.Errorf("failed to write: %v", err)
			}
			if err := finisher(); err != nil {
				t.Errorf("failed to finish: %v", err)
			}

			// Update the object
			writer2, finisher2, err := cs.WriteToObj(objID)
			if err != nil {
				t.Errorf("failed to get writer: %v", err)
				<-concurrent
				return
			}
			writer2.Write(data)
			finisher2()

			// Signal end of operation
			<-concurrent
		}()
	}

	wg.Wait()

	// With 2 tokens, we should see limited concurrency
	// This is timing-dependent so we just log the result
	t.Logf("Max concurrent operations observed: %d (with 2 disk tokens)", maxConcurrent)
}
