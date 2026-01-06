package vault

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestData is a simple test payload type
type TestData struct {
	ID    int    `json:"id"`
	Value string `json:"value"`
}

// TestVaultConcurrentReaders verifies that multiple goroutines can safely
// read from the vault concurrently without conflicts.
func TestVaultConcurrentReaders(t *testing.T) {
	// Setup: Create a vault with concurrent store
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "concurrent_readers.db")
	stre, err := store.NewConcurrentStore(storePath, 0)
	if err != nil {
		t.Fatalf("Failed to create concurrent store: %v", err)
	}

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}

	// Register types
	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[TestData]{})

	// Create a collection and populate it with test data
	coll, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[TestData]](
		v,
		"test_data",
		types.IntLess,
		(*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert 100 items
	itemCount := 100
	for i := range itemCount {
		key := types.IntKey(i)
		payload := types.JsonPayload[TestData]{
			Value: TestData{
				ID:    i,
				Value: fmt.Sprintf("value-%d", i),
			},
		}
		coll.Insert(&key, payload)
	}

	// Test: Launch multiple concurrent readers
	readerCount := 10
	readsPerReader := 100
	var wg sync.WaitGroup
	var errorCount atomic.Int32
	var successCount atomic.Int32

	wg.Add(readerCount)
	for i := range readerCount {
		go func(readerID int) {
			defer wg.Done()

			for j := range readsPerReader {
				// Read random items
				keyID := (readerID*readsPerReader + j) % itemCount
				key := types.IntKey(keyID)
				node := coll.Search(&key)

				if node.IsNil() {
					errorCount.Add(1)
					t.Errorf("Reader %d: Failed to find key %d", readerID, keyID)
					continue
				}

				payload := node.GetPayload()
				if payload.Value.ID != keyID {
					errorCount.Add(1)
					t.Errorf("Reader %d: Data mismatch for key %d: expected ID %d, got %d",
						readerID, keyID, keyID, payload.Value.ID)
					continue
				}

				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	// Verify all reads succeeded
	expectedSuccess := int32(readerCount * readsPerReader)
	if successCount.Load() != expectedSuccess {
		t.Errorf("Expected %d successful reads, got %d", expectedSuccess, successCount.Load())
	}
	if errorCount.Load() > 0 {
		t.Errorf("Encountered %d errors during concurrent reads", errorCount.Load())
	}

	// Cleanup
	if err := v.Close(); err != nil {
		t.Fatalf("Failed to close vault: %v", err)
	}
}

// TestVaultSingleWriterMultipleReaders verifies the typical access pattern:
// one writer making updates while multiple readers are reading concurrently.
func TestVaultSingleWriterMultipleReaders(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "single_writer_multi_readers.db")
	stre, err := store.NewConcurrentStore(storePath, 0)
	if err != nil {
		t.Fatalf("Failed to create concurrent store: %v", err)
	}

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[TestData]{})

	coll, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[TestData]](
		v,
		"test_data",
		types.IntLess,
		(*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Pre-populate with some data
	initialCount := 100
	for i := range initialCount {
		key := types.IntKey(i)
		payload := types.JsonPayload[TestData]{
			Value: TestData{
				ID:    i,
				Value: fmt.Sprintf("initial-%d", i),
			},
		}
		coll.Insert(&key, payload)
	}

	var wg sync.WaitGroup
	stopReaders := make(chan struct{})
	var readErrors atomic.Int32
	var writeErrors atomic.Int32
	var totalReads atomic.Int64
	var totalWrites atomic.Int64

	// Partition the keyspace: readers use keys 0-49, writer uses keys 50-99
	readerKeyRange := initialCount / 2

	// Start multiple concurrent readers
	readerCount := 5
	wg.Add(readerCount)
	for i := range readerCount {
		go func(readerID int) {
			defer wg.Done()

			for {
				select {
				case <-stopReaders:
					return
				default:
					// Read from reader keyspace only
					keyID := readerID % readerKeyRange
					key := types.IntKey(keyID)
					node := coll.Search(&key)

					if node.IsNil() {
						readErrors.Add(1)
						continue
					}

					payload := node.GetPayload()
					if payload.Value.ID != keyID {
						readErrors.Add(1)
						t.Errorf("Reader %d: Data corruption for key %d", readerID, keyID)
					}

					totalReads.Add(1)
					time.Sleep(1 * time.Millisecond) // Small delay to allow interleaving
				}
			}
		}(i)
	}

	// Single writer updating entries in the writer keyspace only
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := range 100 {
			// Use keys from the writer range (50-99)
			keyID := readerKeyRange + (i % readerKeyRange)
			key := types.IntKey(keyID)

			// Update the payload
			newPayload := types.JsonPayload[TestData]{
				Value: TestData{
					ID:    keyID,
					Value: fmt.Sprintf("updated-%d-%d", keyID, i),
				},
			}

			err := coll.UpdatePayload(&key, newPayload)
			if err != nil {
				writeErrors.Add(1)
				t.Errorf("Writer: Failed to update key %d: %v", keyID, err)
			} else {
				totalWrites.Add(1)
			}

			time.Sleep(2 * time.Millisecond) // Small delay
		}

		close(stopReaders)
	}()

	wg.Wait()

	// Verify operations completed
	t.Logf("Total reads: %d, Total writes: %d", totalReads.Load(), totalWrites.Load())
	t.Logf("Read errors: %d, Write errors: %d", readErrors.Load(), writeErrors.Load())

	if writeErrors.Load() > 0 {
		t.Errorf("Encountered %d write errors", writeErrors.Load())
	}

	if readErrors.Load() > 0 {
		t.Errorf("Read errors: %d out of %d reads", readErrors.Load(), totalReads.Load())
	}

	if err := v.Close(); err != nil {
		t.Fatalf("Failed to close vault: %v", err)
	}
}

// TestVaultConcurrentReadersMultipleCollections tests concurrent access
// to different collections within the same vault.
func TestVaultConcurrentReadersMultipleCollections(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "multi_collection_concurrent.db")
	stre, err := store.NewConcurrentStore(storePath, 0)
	if err != nil {
		t.Fatalf("Failed to create concurrent store: %v", err)
	}

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType((*types.StringKey)(new(string)))
	v.RegisterType(types.JsonPayload[TestData]{})

	// Create multiple collections
	coll1, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[TestData]](
		v, "collection1", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create collection1: %v", err)
	}

	coll2, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[TestData]](
		v, "collection2", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create collection2: %v", err)
	}

	// Populate both collections
	for i := range 50 {
		key := types.IntKey(i)
		payload1 := types.JsonPayload[TestData]{
			Value: TestData{ID: i, Value: fmt.Sprintf("coll1-%d", i)},
		}
		payload2 := types.JsonPayload[TestData]{
			Value: TestData{ID: i, Value: fmt.Sprintf("coll2-%d", i)},
		}
		coll1.Insert(&key, payload1)
		coll2.Insert(&key, payload2)
	}

	// Concurrent readers on different collections
	var wg sync.WaitGroup
	var errors atomic.Int32

	// Readers for collection1
	wg.Add(3)
	for i := range 3 {
		go func(id int) {
			defer wg.Done()
			for j := range 100 {
				keyID := (id*100 + j) % 50
				key := types.IntKey(keyID)
				node := coll1.Search(&key)
				if node.IsNil() {
					errors.Add(1)
					continue
				}
				payload := node.GetPayload()
				expectedValue := fmt.Sprintf("coll1-%d", keyID)
				if payload.Value.Value != expectedValue {
					errors.Add(1)
					t.Errorf("Collection1: Expected %s, got %s", expectedValue, payload.Value.Value)
				}
			}
		}(i)
	}

	// Readers for collection2
	wg.Add(3)
	for i := range 3 {
		go func(id int) {
			defer wg.Done()
			for j := range 100 {
				keyID := (id*100 + j) % 50
				key := types.IntKey(keyID)
				node := coll2.Search(&key)
				if node.IsNil() {
					errors.Add(1)
					continue
				}
				payload := node.GetPayload()
				expectedValue := fmt.Sprintf("coll2-%d", keyID)
				if payload.Value.Value != expectedValue {
					errors.Add(1)
					t.Errorf("Collection2: Expected %s, got %s", expectedValue, payload.Value.Value)
				}
			}
		}(i)
	}

	wg.Wait()

	if errors.Load() > 0 {
		t.Errorf("Encountered %d errors during concurrent multi-collection reads", errors.Load())
	}

	if err := v.Close(); err != nil {
		t.Fatalf("Failed to close vault: %v", err)
	}
}

// TestVaultSequentialWritersWithReaders tests a scenario where writers
// take turns (simulating a write lock pattern) while readers run continuously.
// This tests the desired pattern: only one writer at a time, but many readers.
func TestVaultSequentialWritersWithReaders(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "sequential_writers.db")
	stre, err := store.NewConcurrentStore(storePath, 0)
	if err != nil {
		t.Fatalf("Failed to create concurrent store: %v", err)
	}

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[TestData]{})

	coll, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[TestData]](
		v, "test_data", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Pre-populate
	for i := range 100 {
		key := types.IntKey(i)
		payload := types.JsonPayload[TestData]{
			Value: TestData{ID: i, Value: fmt.Sprintf("initial-%d", i)},
		}
		coll.Insert(&key, payload)
	}

	var wg sync.WaitGroup
	stopReaders := make(chan struct{})
	var readErrors atomic.Int32
	var writeErrors atomic.Int32

	// Simulate application-level write lock (only one writer at a time)
	var writeLock sync.Mutex

	// Start readers
	readerCount := 5
	wg.Add(readerCount)
	for i := range readerCount {
		go func(readerID int) {
			defer wg.Done()
			localReads := 0
			for {
				select {
				case <-stopReaders:
					t.Logf("Reader %d completed %d reads", readerID, localReads)
					return
				default:
					keyID := readerID % 100
					key := types.IntKey(keyID)
					node := coll.Search(&key)
					if node.IsNil() {
						readErrors.Add(1)
					}
					localReads++
					time.Sleep(500 * time.Microsecond)
				}
			}
		}(i)
	}

	// Sequential writers (enforced by mutex)
	writerCount := 3
	wg.Add(writerCount)
	for i := 0; i < writerCount; i++ {
		go func(writerID int) {
			defer wg.Done()

			for j := 0; j < 20; j++ {
				// Acquire application-level write lock
				writeLock.Lock()

				keyID := (writerID*20 + j) % 100
				key := types.IntKey(keyID)
				newPayload := types.JsonPayload[TestData]{
					Value: TestData{
						ID:    keyID,
						Value: fmt.Sprintf("writer%d-update%d", writerID, j),
					},
				}

				err := coll.UpdatePayload(&key, newPayload)
				if err != nil {
					writeErrors.Add(1)
					t.Errorf("Writer %d: Update failed for key %d: %v", writerID, keyID, err)
				}

				// Release application-level write lock
				writeLock.Unlock()
				time.Sleep(2 * time.Millisecond)
			}
		}(i)
	}

	// Wait for writers to complete
	time.Sleep(200 * time.Millisecond)
	close(stopReaders)
	wg.Wait()

	t.Logf("Read errors: %d, Write errors: %d", readErrors.Load(), writeErrors.Load())

	if writeErrors.Load() > 0 {
		t.Errorf("Encountered %d write errors", writeErrors.Load())
	}

	if err := v.Close(); err != nil {
		t.Fatalf("Failed to close vault: %v", err)
	}
}

// TestVaultConcurrentCollectionCreation tests whether creating collections
// concurrently is safe. NOTE: This tests a potential race condition in the
// vault's activeCollections map.
func TestVaultConcurrentCollectionCreation(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "concurrent_collection_creation.db")
	stre, err := store.NewConcurrentStore(storePath, 0)
	if err != nil {
		t.Fatalf("Failed to create concurrent store: %v", err)
	}

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[TestData]{})

	// WARNING: This test may expose a race condition if multiple goroutines
	// try to create the same collection simultaneously (accessing activeCollections map)
	var wg sync.WaitGroup
	collectionCount := 10
	var errors atomic.Int32

	wg.Add(collectionCount)
	for i := 0; i < collectionCount; i++ {
		go func(id int) {
			defer wg.Done()

			collName := fmt.Sprintf("collection-%d", id)
			_, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[TestData]](
				v, collName, types.IntLess, (*types.IntKey)(new(int32)),
			)
			if err != nil {
				errors.Add(1)
				t.Errorf("Failed to create collection %s: %v", collName, err)
			}
		}(i)
	}

	wg.Wait()

	if errors.Load() > 0 {
		t.Errorf("Encountered %d errors during concurrent collection creation", errors.Load())
	}

	// Verify all collections were created
	collections := v.ListCollections()
	if len(collections) != collectionCount {
		t.Errorf("Expected %d collections, got %d", collectionCount, len(collections))
	}

	if err := v.Close(); err != nil {
		t.Fatalf("Failed to close vault: %v", err)
	}
}

// TestVaultConcurrentSameCollectionCreation tests the specific case where
// multiple goroutines try to get/create the SAME collection simultaneously.
// This verifies that collection creation is thread-safe, and demonstrates
// that concurrent writes to the same collection require application-level synchronization.
func TestVaultConcurrentSameCollectionCreation(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "concurrent_same_collection.db")
	stre, err := store.NewConcurrentStore(storePath, 0)
	if err != nil {
		t.Fatalf("Failed to create concurrent store: %v", err)
	}

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[TestData]{})

	// CRITICAL TEST: Multiple goroutines trying to get/create the same collection
	// and write to it concurrently. Collection-level locking ensures thread-safety.
	var wg sync.WaitGroup
	goroutineCount := 10
	var errors atomic.Int32
	var nilCollections atomic.Int32

	wg.Add(goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		go func(id int) {
			defer wg.Done()

			// All goroutines try to get the same collection
			coll, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[TestData]](
				v, "shared_collection", types.IntLess, (*types.IntKey)(new(int32)),
			)
			if err != nil {
				errors.Add(1)
				t.Errorf("Goroutine %d: Failed to get collection: %v", id, err)
				return
			}
			if coll == nil {
				nilCollections.Add(1)
				t.Errorf("Goroutine %d: Got nil collection", id)
				return
			}

			// Collection has built-in thread-safety
			key := types.IntKey(id)
			payload := types.JsonPayload[TestData]{
				Value: TestData{ID: id, Value: fmt.Sprintf("value-%d", id)},
			}
			coll.Insert(&key, payload)
		}(i)
	}

	wg.Wait()

	if errors.Load() > 0 {
		t.Errorf("Encountered %d errors during concurrent same-collection creation", errors.Load())
	}
	if nilCollections.Load() > 0 {
		t.Errorf("Got %d nil collections", nilCollections.Load())
	}

	// Verify the collection exists and has data
	collections := v.ListCollections()
	if len(collections) != 1 {
		t.Errorf("Expected 1 collection, got %d", len(collections))
	}

	if err := v.Close(); err != nil {
		t.Fatalf("Failed to close vault: %v", err)
	}
}
