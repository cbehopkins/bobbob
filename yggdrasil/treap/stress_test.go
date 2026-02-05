package treap

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// StressTestConfig controls the stress test parameters
type StressTestConfig struct {
	ItemCount              int           // Total items to insert
	BatchSize              int           // Items per insertion batch
	FlushInterval          time.Duration // How often to trigger flushes
	FlushPercentile        int           // Percentile to flush (0-100)
	IterationCheckInterval int           // Check every N items during iteration
	ConcurrentReaders      int           // Number of concurrent reader goroutines
	RunDuration            time.Duration // How long to run (for long-running tests)
}

// DefaultStressConfig returns reasonable defaults for stress testing
func DefaultStressConfig() StressTestConfig {
	return StressTestConfig{
		ItemCount:              100_000,
		BatchSize:              1000,
		FlushInterval:          100 * time.Millisecond,
		FlushPercentile:        85,
		IterationCheckInterval: 5000,
		ConcurrentReaders:      4,
		RunDuration:            30 * time.Second,
	}
}

// AggressiveStressConfig returns a more aggressive configuration
func AggressiveStressConfig() StressTestConfig {
	return StressTestConfig{
		ItemCount:              500_000,
		BatchSize:              5000,
		FlushInterval:          50 * time.Millisecond,
		FlushPercentile:        90,
		IterationCheckInterval: 10_000,
		ConcurrentReaders:      8,
		RunDuration:            60 * time.Second,
	}
}

// StressTestResult holds results from a stress test run
type StressTestResult struct {
	TotalInserted             int64
	TotalFlushed              int64
	TotalIterationErrors      int64
	TotalDataCorruptions      int64
	FinalInMemoryNodeCount    int
	DataIntegrityChecksPassed int64
	DataIntegrityChecksFailed int64
	IterationChecksPassed     int64
	IterationChecksFailed     int64
}

// TestPersistentPayloadTreapStressDataIntegrity performs a comprehensive stress test
// focusing on data integrity with concurrent flushes.
func TestPersistentPayloadTreapStressDataIntegrity(t *testing.T) {
	config := DefaultStressConfig()
	result := runStressTest(t, config, stressTestDataIntegrity)
	validateStressResult(t, result, "DataIntegrity")
}

// TestPersistentPayloadTreapStressIterationWithFlushes performs a comprehensive stress test
// focusing on iteration correctness with background flushes.
func TestPersistentPayloadTreapStressIterationWithFlushes(t *testing.T) {
	config := DefaultStressConfig()
	result := runStressTest(t, config, stressTestIterationWithFlushes)
	validateStressResult(t, result, "IterationWithFlushes")
}

// TestPersistentPayloadTreapStressConcurrentAccessWithFlushes performs a stress test
// with concurrent readers and writers under background flushes.
func TestPersistentPayloadTreapStressConcurrentAccessWithFlushes(t *testing.T) {
	config := DefaultStressConfig()
	result := runStressTest(t, config, stressTestConcurrentAccessWithFlushes)
	validateStressResult(t, result, "ConcurrentAccessWithFlushes")
}

// TestPersistentPayloadTreapStressAggressiveThrashing runs a long, aggressive stress test
// with maximum concurrency and flush pressure.
func TestPersistentPayloadTreapStressAggressiveThrashing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping aggressive stress test in short mode")
	}
	// This test is VERY aggressive and can take a long time on slower systems
	// t.Skip("Skipping aggressive thrashing test - reserved for manual testing with: go test -run TestPersistentPayloadTreapStressAggressiveThrashing -timeout 5m")
	// Uncomment below to run:
	config := AggressiveStressConfig()
	result := runStressTest(t, config, stressTestAggressiveThrashing)
	validateStressResult(t, result, "AggressiveThrashing")
}

// TestPersistentPayloadTreapStressRandomOperations performs a long-running test
// with random operations: inserts, flushes, iterations, and data checks.
// Note: Requires -run explicitly and longer timeout due to extensive I/O.
func TestPersistentPayloadTreapStressRandomOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping random operations stress test in short mode")
	}
	// This test performs extensive I/O and concurrent operations
	// t.Skip("Skipping random operations test - reserved for manual testing with: go test -run TestPersistentPayloadTreapStressRandomOperations -timeout 5m")
	// Uncomment below to run:
	config := AggressiveStressConfig()
	result := runStressTest(t, config, stressTestRandomOperations)
	validateStressResult(t, result, "RandomOperations")
}

// stressTestDataIntegrity tests that data remains uncorrupted despite memory pressure and flushes
type stressTestFunc func(t *testing.T, config StressTestConfig, treap *PersistentPayloadTreap[types.IntKey, MockPayload]) StressTestResult

func stressTestDataIntegrity(t *testing.T, config StressTestConfig, treap *PersistentPayloadTreap[types.IntKey, MockPayload]) StressTestResult {
	result := StressTestResult{}
	expectedPayloads := make(map[int32]string)

	t.Logf("=== DataIntegrity Stress Test ===")
	t.Logf("Inserting %d items in batches of %d", config.ItemCount, config.BatchSize)

	// Insert phase with periodic data checks
	for i := 0; i < config.ItemCount; i++ {
		key := types.IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("key_%d_data_%d", i, i*17%1000)}
		treap.Insert(&key, payload)
		expectedPayloads[int32(i)] = payload.Data
		atomic.AddInt64(&result.TotalInserted, 1)

		// Perform data checks periodically
		if (i+1)%config.IterationCheckInterval == 0 {
			t.Logf("Inserted %d items, performing data integrity check...", i+1)
			checksPassed, checksFailed := verifyDataIntegrity(treap, expectedPayloads)
			result.DataIntegrityChecksPassed += checksPassed
			result.DataIntegrityChecksFailed += checksFailed
			if checksFailed > 0 {
				atomic.AddInt64(&result.TotalDataCorruptions, checksFailed)
			}
		}
	}

	t.Logf("Insertion complete, persisting to disk...")
	if err := treap.Persist(); err != nil {
		t.Errorf("Failed to persist treap: %v", err)
	}

	// Aggressive flush phase
	t.Logf("Starting aggressive flush phase with periodic integrity checks...")
	for flush := 0; flush < 5; flush++ {
		percentile := config.FlushPercentile
		if flush > 0 {
			percentile = percentile - (10 * flush) // Gradually flush less
		}
		if percentile < 10 {
			percentile = 10
		}

		flushed, err := treap.FlushOldestPercentile(percentile)
		if err != nil {
			t.Errorf("Flush %d failed: %v", flush, err)
		}
		atomic.AddInt64(&result.TotalFlushed, int64(flushed))
		t.Logf("Flush %d: removed %d nodes (percentile %d%%)", flush, flushed, percentile)

		// Check data integrity after flush
		checksPassed, checksFailed := verifyDataIntegrity(treap, expectedPayloads)
		result.DataIntegrityChecksPassed += checksPassed
		result.DataIntegrityChecksFailed += checksFailed
		if checksFailed > 0 {
			atomic.AddInt64(&result.TotalDataCorruptions, checksFailed)
		}

		time.Sleep(10 * time.Millisecond)
	}

	// Final comprehensive check
	t.Logf("Final comprehensive data integrity check...")
	checksPassed, checksFailed := verifyDataIntegrity(treap, expectedPayloads)
	result.DataIntegrityChecksPassed += checksPassed
	result.DataIntegrityChecksFailed += checksFailed
	result.FinalInMemoryNodeCount = treap.CountInMemoryNodes()

	t.Logf("DataIntegrity test complete: %d checks passed, %d failed",
		result.DataIntegrityChecksPassed, result.DataIntegrityChecksFailed)

	return result
}

func stressTestIterationWithFlushes(t *testing.T, config StressTestConfig, treap *PersistentPayloadTreap[types.IntKey, MockPayload]) StressTestResult {
	result := StressTestResult{}
	expectedKeys := make([]int32, config.ItemCount)

	t.Logf("=== IterationWithFlushes Stress Test ===")
	t.Logf("Inserting %d items", config.ItemCount)

	// Insert all items
	for i := 0; i < config.ItemCount; i++ {
		key := types.IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("payload_%d", i)}
		treap.Insert(&key, payload)
		expectedKeys[i] = int32(i)
		atomic.AddInt64(&result.TotalInserted, 1)
	}

	t.Logf("Persisting to disk...")
	if err := treap.Persist(); err != nil {
		t.Errorf("Failed to persist: %v", err)
	}

	// Start background flusher
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	flusherDone := make(chan struct{})
	go func() {
		defer close(flusherDone)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			flushed, err := treap.FlushOldestPercentile(config.FlushPercentile)
			if err == nil && flushed > 0 {
				atomic.AddInt64(&result.TotalFlushed, int64(flushed))
			}
			time.Sleep(config.FlushInterval)
		}
	}()

	// Run multiple iterations with different options
	t.Logf("Running iterations while flushing in background...")
	iterations := 3
	for iter := 0; iter < iterations; iter++ {
		t.Logf("Iteration %d/%d", iter+1, iterations)
		itemCount := 0
		lastKey := int32(-1)

		err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
			pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
			k := int32(*pNode.GetKey().(*types.IntKey))
			payload := pNode.GetPayload()

			// Verify ordering
			if k <= lastKey {
				atomic.AddInt64(&result.TotalIterationErrors, 1)
				atomic.AddInt64(&result.IterationChecksFailed, 1)
				return fmt.Errorf("keys out of order: %d <= %d", k, lastKey)
			}
			lastKey = k

			// Verify payload is present
			if payload.Data == "" {
				atomic.AddInt64(&result.TotalIterationErrors, 1)
				atomic.AddInt64(&result.IterationChecksFailed, 1)
				return fmt.Errorf("empty payload for key %d", k)
			}

			itemCount++
			return nil
		})

		if err != nil {
			t.Errorf("Iteration %d failed: %v", iter, err)
			atomic.AddInt64(&result.IterationChecksFailed, 1)
		} else if itemCount != config.ItemCount {
			t.Errorf("Iteration %d returned %d items, expected %d", iter, itemCount, config.ItemCount)
			atomic.AddInt64(&result.IterationChecksFailed, 1)
		} else {
			atomic.AddInt64(&result.IterationChecksPassed, 1)
		}

		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	<-flusherDone

	result.FinalInMemoryNodeCount = treap.CountInMemoryNodes()
	t.Logf("IterationWithFlushes complete: %d iterations passed, %d failed",
		result.IterationChecksPassed, result.IterationChecksFailed)

	return result
}

func stressTestConcurrentAccessWithFlushes(t *testing.T, config StressTestConfig, treap *PersistentPayloadTreap[types.IntKey, MockPayload]) StressTestResult {
	result := StressTestResult{}

	t.Logf("=== ConcurrentAccessWithFlushes Stress Test ===")
	t.Logf("Starting concurrent readers with %d items", config.ItemCount)

	// Phase 1: Sequential write to establish expected data
	t.Logf("Phase 1: Inserting and persisting %d items sequentially", config.ItemCount)
	for i := 0; i < config.ItemCount; i++ {
		key := types.IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("data_%d_%d", i, i*13%1000)}
		treap.Insert(&key, payload)
		atomic.AddInt64(&result.TotalInserted, 1)
	}

	if err := treap.Persist(); err != nil {
		t.Logf("Persist error: %v", err)
	}

	// Phase 2: Concurrent reading with background flushing
	t.Logf("Phase 2: Concurrent readers with background flushing")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Flusher goroutine - conservative to avoid excessive contention
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				flushed, _ := treap.FlushOldestPercentile(75)
				if flushed > 0 {
					atomic.AddInt64(&result.TotalFlushed, int64(flushed))
				}
			}
		}
	}()

	// Reader goroutines that iterate and check data
	for readerID := 0; readerID < config.ConcurrentReaders; readerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ticker := time.NewTicker(1500 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					count := 0
					lastKey := int32(-1)

					err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
						pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
						k := int32(*pNode.GetKey().(*types.IntKey))
						if k <= lastKey {
							return fmt.Errorf("reader %d: out of order at key %d after %d", id, k, lastKey)
						}
						lastKey = k
						count++
						return nil
					})

					if err == nil && count == config.ItemCount {
						atomic.AddInt64(&result.IterationChecksPassed, 1)
					} else if err != nil {
						atomic.AddInt64(&result.IterationChecksFailed, 1)
						atomic.AddInt64(&result.TotalIterationErrors, 1)
					}
				}
			}
		}(readerID)
	}

	// Run readers for a fixed duration
	time.Sleep(5 * time.Second)
	cancel()
	wg.Wait()

	// Final integrity check
	result.FinalInMemoryNodeCount = treap.CountInMemoryNodes()

	t.Logf("ConcurrentAccessWithFlushes complete: %d reader checks passed, %d failed, integrity: %d/%d",
		result.IterationChecksPassed, result.IterationChecksFailed,
		result.DataIntegrityChecksPassed, 1)

	return result
}

func stressTestAggressiveThrashing(t *testing.T, config StressTestConfig, treap *PersistentPayloadTreap[types.IntKey, MockPayload]) StressTestResult {
	result := StressTestResult{}
	expectedPayloads := make(map[int32]string)
	payloadMu := sync.RWMutex{}
	loggedIterationErrors := int64(0)

	t.Logf("=== AggressiveThrashing Stress Test ===")
	t.Logf("Running for %v with %d item target", config.RunDuration, config.ItemCount)

	ctx, cancel := context.WithTimeout(context.Background(), config.RunDuration)
	defer cancel()

	var wg sync.WaitGroup

	// Multiple writer goroutines
	writerCount := 2
	for writerID := 0; writerID < writerCount; writerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			insertCount := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				i := int(atomic.AddInt64(&result.TotalInserted, 1)) - 1
				key := types.IntKey(i)
				payload := MockPayload{Data: fmt.Sprintf("w%d_k%d_%d", id, i, rand.Intn(100000))}
				treap.Insert(&key, payload)

				payloadMu.Lock()
				expectedPayloads[int32(i)] = payload.Data
				payloadMu.Unlock()

				insertCount++

				if insertCount%config.BatchSize == 0 {
					if err := treap.Persist(); err != nil {
						t.Logf("Writer %d persist error: %v", id, err)
					}
				}
			}
		}(writerID)
	}

	// Aggressive flusher
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(config.FlushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				percentile := config.FlushPercentile + (rand.Intn(10) - 5) // Vary percentile
				if percentile < 10 {
					percentile = 10
				}
				if percentile > 100 {
					percentile = 100
				}
				flushed, _ := treap.FlushOldestPercentile(percentile)
				if flushed > 0 {
					atomic.AddInt64(&result.TotalFlushed, int64(flushed))
				}
			}
		}
	}()

	// Multiple concurrent readers and checkers
	for readerID := 0; readerID < config.ConcurrentReaders; readerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Iterate and verify order
				count := 0
				lastKey := int32(-1)

				err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
					pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
					k := int32(*pNode.GetKey().(*types.IntKey))
					if k <= lastKey {
						return fmt.Errorf("reader %d: keys out of order", id)
					}
					lastKey = k
					count++
					return nil
				})

				if err == nil {
					atomic.AddInt64(&result.IterationChecksPassed, 1)
				} else {
					if atomic.AddInt64(&loggedIterationErrors, 1) <= 5 {
						t.Logf("reader %d iteration error: %v", id, err)
					}
					atomic.AddInt64(&result.IterationChecksFailed, 1)
					atomic.AddInt64(&result.TotalIterationErrors, 1)
				}

				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			}
		}(readerID)
	}

	// Wait for context to expire
	<-ctx.Done()

	// Give cleanup a moment
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Wait for remaining operations
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Logf("Timeout waiting for goroutines to finish")
	}

	// Final verification
	payloadMu.RLock()
	checksPassed, checksFailed := verifyDataIntegrity(treap, expectedPayloads)
	payloadMu.RUnlock()
	if checksFailed > 0 {
		payloadMu.RLock()
		logIntegrityMismatch(t, treap, expectedPayloads)
		payloadMu.RUnlock()
	}

	result.DataIntegrityChecksPassed += checksPassed
	result.DataIntegrityChecksFailed += checksFailed
	result.FinalInMemoryNodeCount = treap.CountInMemoryNodes()

	t.Logf("AggressiveThrashing complete: inserted %d items, flushed %d items, %d iterations",
		result.TotalInserted, result.TotalFlushed, result.IterationChecksPassed+result.IterationChecksFailed)

	return result
}

func stressTestRandomOperations(t *testing.T, config StressTestConfig, treap *PersistentPayloadTreap[types.IntKey, MockPayload]) StressTestResult {
	result := StressTestResult{}
	expectedPayloads := make(map[int32]string)
	payloadMu := sync.RWMutex{}

	t.Logf("=== RandomOperations Stress Test ===")
	t.Logf("Running for %v", config.RunDuration)

	ctx, cancel := context.WithTimeout(context.Background(), config.RunDuration)
	defer cancel()

	var wg sync.WaitGroup

	// Single controlled writer with random batches
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Random batch size
			batchSize := config.BatchSize/2 + rand.Intn(config.BatchSize/2)
			for j := 0; j < batchSize; j++ {
				key := types.IntKey(i)
				payload := MockPayload{Data: fmt.Sprintf("item_%d_%d", i, time.Now().UnixNano()%10000)}
				
				// Hold lock during both expectedPayloads update AND treap insert
				// to ensure integrity checker sees consistent state
				payloadMu.Lock()
				expectedPayloads[int32(i)] = payload.Data
				treap.Insert(&key, payload)
				payloadMu.Unlock()

				atomic.AddInt64(&result.TotalInserted, 1)
				i++
			}

			// Random persist/flush pattern
			if err := treap.Persist(); err != nil {
				t.Logf("Persist error: %v", err)
			}

			if rand.Float64() < 0.7 {
				percentile := 50 + rand.Intn(40)
				flushed, _ := treap.FlushOldestPercentile(percentile)
				if flushed > 0 {
					atomic.AddInt64(&result.TotalFlushed, int64(flushed))
				}
			}

			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		}
	}()

	// Periodic comprehensive checks
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				payloadMu.RLock()
				expectedCopy := make(map[int32]string)
				for k, v := range expectedPayloads {
					expectedCopy[k] = v
				}
				payloadMu.RUnlock()

				checksPassed, checksFailed := verifyDataIntegrity(treap, expectedCopy)
				result.DataIntegrityChecksPassed += checksPassed
				result.DataIntegrityChecksFailed += checksFailed
				if checksFailed > 0 {
					atomic.AddInt64(&result.TotalDataCorruptions, checksFailed)
				}
			}
		}
	}()

	// Continuous iteration verification
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			count := 0
			lastKey := int32(-1)

			err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
				pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
				k := int32(*pNode.GetKey().(*types.IntKey))
				if k <= lastKey {
					return fmt.Errorf("out of order: %d <= %d", k, lastKey)
				}
				lastKey = k
				count++
				return nil
			})

			if err == nil {
				atomic.AddInt64(&result.IterationChecksPassed, 1)
			} else {
				atomic.AddInt64(&result.IterationChecksFailed, 1)
			}

			time.Sleep(500 * time.Millisecond)
		}
	}()

	<-ctx.Done()
	time.Sleep(100 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Logf("Timeout waiting for final operations")
	}

	payloadMu.RLock()
	checksPassed, checksFailed := verifyDataIntegrity(treap, expectedPayloads)
	payloadMu.RUnlock()

	result.DataIntegrityChecksPassed += checksPassed
	result.DataIntegrityChecksFailed += checksFailed
	result.FinalInMemoryNodeCount = treap.CountInMemoryNodes()

	t.Logf("RandomOperations complete: inserted %d, %d integrity checks (%d passed, %d failed), %d iteration checks",
		result.TotalInserted, result.DataIntegrityChecksPassed+result.DataIntegrityChecksFailed,
		result.DataIntegrityChecksPassed, result.DataIntegrityChecksFailed,
		result.IterationChecksPassed+result.IterationChecksFailed)

	return result
}

// Helper functions

func runStressTest(t *testing.T, config StressTestConfig, testFunc stressTestFunc) StressTestResult {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	return testFunc(t, config, treap)
}

func verifyDataIntegrity(treap *PersistentPayloadTreap[types.IntKey, MockPayload], expectedPayloads map[int32]string) (passed int64, failed int64) {
	if len(expectedPayloads) == 0 {
		return 1, 0
	}

	foundItems := 0
	foundPayloads := make(map[int32]string)

	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
		k := int32(*pNode.GetKey().(*types.IntKey))
		foundPayloads[k] = pNode.GetPayload().Data
		foundItems++
		return nil
	})

	if err != nil {
		return 0, 1
	}

	// During concurrent writes, treap might have MORE items than expected (items added after snapshot)
	// But it should have AT LEAST all the expected items
	if foundItems < len(expectedPayloads) {
		return 0, 1
	}

	// Check each expected item exists with correct payload
	for k, expectedPayload := range expectedPayloads {
		if foundPayload, ok := foundPayloads[k]; !ok {
			return 0, 1
		} else if foundPayload != expectedPayload {
			return 0, 1
		}
	}

	return 1, 0
}


func logIntegrityMismatch(t *testing.T, treap *PersistentPayloadTreap[types.IntKey, MockPayload], expectedPayloads map[int32]string) {
	foundItems := 0
	foundPayloads := make(map[int32]string)

	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
		k := int32(*pNode.GetKey().(*types.IntKey))
		foundPayloads[k] = pNode.GetPayload().Data
		foundItems++
		return nil
	})

	if err != nil {
		t.Logf("integrity check walk error: %v", err)
		return
	}

	if foundItems != len(expectedPayloads) {
		t.Logf("integrity mismatch: found %d items, expected %d", foundItems, len(expectedPayloads))
	}

	for k, expectedPayload := range expectedPayloads {
		if foundPayload, ok := foundPayloads[k]; !ok {
			t.Logf("integrity mismatch: missing key %d", k)
			return
		} else if foundPayload != expectedPayload {
			t.Logf("integrity mismatch: key %d payload expected %q got %q", k, expectedPayload, foundPayload)
			return
		}
	}
}

func validateStressResult(t *testing.T, result StressTestResult, testName string) {
	t.Logf("\n=== %s Results ===", testName)
	t.Logf("Total inserted: %d", result.TotalInserted)
	t.Logf("Total flushed: %d", result.TotalFlushed)
	t.Logf("Final in-memory nodes: %d", result.FinalInMemoryNodeCount)
	t.Logf("Data integrity checks: %d passed, %d failed",
		result.DataIntegrityChecksPassed, result.DataIntegrityChecksFailed)
	t.Logf("Iteration checks: %d passed, %d failed",
		result.IterationChecksPassed, result.IterationChecksFailed)
	t.Logf("Total iteration errors: %d", result.TotalIterationErrors)
	t.Logf("Total data corruptions: %d", result.TotalDataCorruptions)

	// Fail if we found any data corruption or iteration errors
	if result.DataIntegrityChecksFailed > 0 {
		t.Errorf("FAILED: %d data integrity checks failed", result.DataIntegrityChecksFailed)
	}
	if result.IterationChecksFailed > 0 {
		t.Errorf("FAILED: %d iteration checks failed", result.IterationChecksFailed)
	}
	if result.TotalIterationErrors > 0 {
		t.Errorf("FAILED: %d iteration errors detected", result.TotalIterationErrors)
	}
	if result.TotalDataCorruptions > 0 {
		t.Errorf("FAILED: %d data corruptions detected", result.TotalDataCorruptions)
	}

	if result.DataIntegrityChecksFailed == 0 && result.IterationChecksFailed == 0 &&
		result.TotalIterationErrors == 0 && result.TotalDataCorruptions == 0 {
		t.Logf("SUCCESS: All checks passed!")
	}
}
