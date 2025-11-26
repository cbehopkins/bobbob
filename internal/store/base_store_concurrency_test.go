package store

import (
	"bytes"
	cryptorand "crypto/rand"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// TestBaseStoreConcurrentWriteRace demonstrates a race condition when multiple
// goroutines write to the baseStore simultaneously without synchronization
func TestBaseStoreConcurrentWriteRace(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition test in short mode")
	}

	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	const numGoroutines = 10
	const objectsPerGoroutine = 10

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines*objectsPerGoroutine)

	// Launch concurrent writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < objectsPerGoroutine; j++ {
				data := []byte(fmt.Sprintf("worker-%d-object-%d", workerID, j))

				// This can race with other goroutines accessing objectMap and allocator
				objId, writer, finisher, err := store.LateWriteNewObj(len(data))
				if err != nil {
					errChan <- fmt.Errorf("worker %d: LateWriteNewObj failed: %w", workerID, err)
					return
				}

				_, err = writer.Write(data)
				if err != nil {
					errChan <- fmt.Errorf("worker %d: Write failed: %w", workerID, err)
					if finisher != nil {
						finisher()
					}
					return
				}

				if finisher != nil {
					finisher()
				}

				// Verify the write
				reader, finisher2, err := store.LateReadObj(objId)
				if err != nil {
					errChan <- fmt.Errorf("worker %d: LateReadObj failed: %w", workerID, err)
					return
				}

				readData := make([]byte, len(data))
				_, err = reader.Read(readData)
				if err != nil {
					errChan <- fmt.Errorf("worker %d: Read failed: %w", workerID, err)
					if finisher2 != nil {
						finisher2()
					}
					return
				}

				if finisher2 != nil {
					finisher2()
				}

				if !bytes.Equal(data, readData) {
					errChan <- fmt.Errorf("worker %d: data mismatch for object %d", workerID, objId)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		t.Logf("Detected %d errors due to race conditions:", len(errors))
		for _, err := range errors {
			t.Logf("  - %v", err)
		}
		t.Fatal("Race conditions detected in concurrent writes")
	}
}

// TestBaseStoreConcurrentReadWriteRace demonstrates race between reads and writes
func TestBaseStoreConcurrentReadWriteRace(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition test in short mode")
	}

	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Create some initial objects
	const numObjects = 20
	objects := make([]ObjectId, numObjects)
	for i := 0; i < numObjects; i++ {
		data := make([]byte, 100)
		cryptorand.Read(data)
		objId, writer, finisher, err := store.LateWriteNewObj(len(data))
		if err != nil {
			t.Fatalf("Failed to create initial object %d: %v", i, err)
		}
		writer.Write(data)
		if finisher != nil {
			finisher()
		}
		objects[i] = objId
	}

	const numReaders = 5
	const numWriters = 5
	const duration = 2 * time.Second

	var wg sync.WaitGroup
	errChan := make(chan error, numReaders+numWriters)
	done := make(chan bool)

	// Start readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					// Read random existing objects
					objId := objects[readerID%len(objects)]
					reader, finisher, err := store.LateReadObj(objId)
					if err != nil {
						errChan <- fmt.Errorf("reader %d: read failed: %w", readerID, err)
						return
					}
					data := make([]byte, 100)
					reader.Read(data)
					if finisher != nil {
						finisher()
					}
				}
			}
		}(i)
	}

	// Start writers (creating new objects)
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					data := make([]byte, 100)
					cryptorand.Read(data)
					objId, writer, finisher, err := store.LateWriteNewObj(len(data))
					if err != nil {
						errChan <- fmt.Errorf("writer %d: write failed: %w", writerID, err)
						return
					}
					writer.Write(data)
					if finisher != nil {
						finisher()
					}
					// Also try to read it back
					reader, finisher2, err := store.LateReadObj(objId)
					if err != nil {
						errChan <- fmt.Errorf("writer %d: read-back failed: %w", writerID, err)
						return
					}
					readData := make([]byte, len(data))
					reader.Read(readData)
					if finisher2 != nil {
						finisher2()
					}
				}
			}
		}(i)
	}

	// Run for a duration
	time.Sleep(duration)
	close(done)
	wg.Wait()
	close(errChan)

	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		t.Logf("Detected %d errors due to race conditions:", len(errors))
		for _, err := range errors {
			t.Logf("  - %v", err)
		}
		t.Fatal("Race conditions detected in concurrent read/write")
	}
}

// TestBaseStoreDeleteWhileReading demonstrates race between delete and read
func TestBaseStoreDeleteWhileReading(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition test in short mode")
	}

	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Create an object
	data := make([]byte, 1000)
	cryptorand.Read(data)
	objId, writer, finisher, err := store.LateWriteNewObj(len(data))
	if err != nil {
		t.Fatalf("Failed to create object: %v", err)
	}
	writer.Write(data)
	if finisher != nil {
		finisher()
	}

	const numReaders = 10
	var wg sync.WaitGroup
	errChan := make(chan error, numReaders+1)

	// Start multiple readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			time.Sleep(time.Duration(readerID) * time.Millisecond)
			reader, finisher, err := store.LateReadObj(objId)
			if err != nil {
				// This might legitimately fail if object was deleted
				return
			}
			if finisher != nil {
				defer finisher()
			}
			readData := make([]byte, len(data))
			_, err = reader.Read(readData)
			if err != nil {
				errChan <- fmt.Errorf("reader %d: read failed: %w", readerID, err)
			}
		}(i)
	}

	// Delete the object while readers are trying to read it
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		err := store.DeleteObj(objId)
		if err != nil {
			errChan <- fmt.Errorf("delete failed: %w", err)
		}
	}()

	wg.Wait()
	close(errChan)

	// In a properly synchronized implementation, this test should not crash
	// and should handle the race gracefully
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	// Note: Some errors are expected here (reads failing after delete)
	// But we should not crash or corrupt data
	if len(errors) > 0 {
		t.Logf("Got %d errors (some expected due to delete):", len(errors))
		for _, err := range errors {
			t.Logf("  - %v", err)
		}
	}
}

// TestBaseStoreMapCorruption tests for objectMap corruption under concurrent access
func TestBaseStoreMapCorruption(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition test in short mode")
	}

	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	const numGoroutines = 20
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines*opsPerGoroutine)

	// Multiple goroutines performing mixed operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			createdObjects := make([]ObjectId, 0, opsPerGoroutine)

			for j := 0; j < opsPerGoroutine; j++ {
				// Create object
				data := []byte(fmt.Sprintf("worker-%d-op-%d", workerID, j))
				objId, writer, finisher, err := store.LateWriteNewObj(len(data))
				if err != nil {
					errChan <- fmt.Errorf("worker %d op %d: create failed: %w", workerID, j, err)
					continue
				}
				writer.Write(data)
				if finisher != nil {
					finisher()
				}
				createdObjects = append(createdObjects, objId)

				// Sometimes delete old objects
				if len(createdObjects) > 5 && j%3 == 0 {
					deleteIdx := j % len(createdObjects)
					err := store.DeleteObj(createdObjects[deleteIdx])
					if err != nil {
						errChan <- fmt.Errorf("worker %d op %d: delete failed: %w", workerID, j, err)
					} else {
						// Remove from list
						createdObjects = append(createdObjects[:deleteIdx], createdObjects[deleteIdx+1:]...)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		t.Logf("Detected %d errors due to race conditions:", len(errors))
		for i, err := range errors {
			if i < 10 { // Only show first 10 errors
				t.Logf("  - %v", err)
			}
		}
		if len(errors) > 10 {
			t.Logf("  ... and %d more errors", len(errors)-10)
		}
		t.Fatal("Race conditions detected in concurrent operations")
	}
}
