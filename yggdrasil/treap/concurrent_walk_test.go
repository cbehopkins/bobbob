package treap

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestConcurrentWalkWithWrites tests iteration while tree is being modified
func TestConcurrentWalkWithWrites(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	// Insert initial 1000 items
	for i := range 1000 {
		key := types.IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("item_%d", i)}
		treap.Insert(&key, payload)
	}

	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist: %v", err)
	}

	var iterErrors int64
	var iterCount int64
	var writeCount int64

	done := make(chan struct{})
	var wg sync.WaitGroup

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1000; i < 2000; i++ {
			select {
			case <-done:
				return
			default:
			}
			key := types.IntKey(i)
			payload := MockPayload{Data: fmt.Sprintf("item_%d", i)}
			treap.Insert(&key, payload)
			atomic.AddInt64(&writeCount, 1)
		}
	}()

	// Flusher goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				treap.FlushOldestPercentile(75)
			}
		}
	}()

	// Reader goroutines
	for reader := range 8 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}

				count := 0
				lastKey := int32(-1)

				err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
					pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
					k := int32(*pNode.GetKey().(*types.IntKey))
					if k <= lastKey {
						t.Logf("Reader %d: out of order at key %d (after %d)", id, k, lastKey)
						atomic.AddInt64(&iterErrors, 1)
						return fmt.Errorf("out of order: %d <= %d", k, lastKey)
					}
					lastKey = k
					count++
					return nil
				})

				if err != nil {
					atomic.AddInt64(&iterErrors, 1)
				} else {
					atomic.AddInt64(&iterCount, 1)
				}

				time.Sleep(50 * time.Millisecond)
			}
		}(reader)
	}

	// Run for 5 seconds
	time.Sleep(5 * time.Second)
	close(done)
	wg.Wait()

	t.Logf("Results: %d iterations, %d errors, %d writes", iterCount, iterErrors, writeCount)
	if iterErrors > 0 {
		t.Errorf("FAILED: %d iteration errors detected", iterErrors)
	}
}

// TestConcurrentInsertIterateWithFlush runs a worker that alternates between
// inserting batches and iterating, while a separate goroutine periodically flushes.
// The iterator uses default options (MemoryOnly=false) to ensure full traversal.
func TestConcurrentInsertIterateWithFlush(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	const (
		cycles      = 200
		batchSize   = 10
		maxNodes    = 10000
		enableFlush = true
	)

	expected := make(map[int]MockPayload)
	keys := make([]int, 0, maxNodes)
	rng := rand.New(rand.NewSource(42))

	var flushCount int64
	var flushErrCount int64
	var lastFlushErr atomic.Value
	done := make(chan struct{})
	var wg sync.WaitGroup

	// Flusher goroutine (timer-driven)
	if enableFlush {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Perform an initial flush so we don't miss the first tick in short tests
			if _, err := treap.FlushOldestPercentile(50); err == nil {
				atomic.AddInt64(&flushCount, 1)
			} else {
				atomic.AddInt64(&flushErrCount, 1)
				lastFlushErr.Store(err)
			}
			ticker := time.NewTicker(25 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					if _, err := treap.FlushOldestPercentile(50); err == nil {
						atomic.AddInt64(&flushCount, 1)
					} else {
						atomic.AddInt64(&flushErrCount, 1)
						lastFlushErr.Store(err)
					}
				}
			}
		}()
	}

	var sawFlushBetweenCycles bool

	// Inserter/Checker worker
	for cycle := range cycles {
		startFlush := atomic.LoadInt64(&flushCount)

		deleteRandom := func() {
			if len(keys) == 0 {
				return
			}
			idx := rng.Intn(len(keys))
			keyVal := keys[idx]
			key := types.IntKey(keyVal)
			treap.Delete(&key)
			delete(expected, keyVal)
			last := len(keys) - 1
			keys[idx] = keys[last]
			keys = keys[:last]
		}

		// Insert a batch
		startKey := cycle * batchSize
		for i := range batchSize {
			keyVal := startKey + i
			key := types.IntKey(keyVal)
			payload := MockPayload{Data: fmt.Sprintf("item_%d", keyVal)}
			treap.Insert(&key, payload)
			expected[keyVal] = payload
			keys = append(keys, keyVal)
		}

		// Random deletions each cycle
		deleteCount := rng.Intn(batchSize / 2)
		for i := 0; i < deleteCount; i++ {
			deleteRandom()
		}

		// Cap total nodes
		for len(keys) > maxNodes {
			deleteRandom()
		}

		// Walk and verify all expected items are present
		seen := make(map[int]bool, len(expected))
		err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
			pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
			k := int(*pNode.GetKey().(*types.IntKey))
			exp, ok := expected[k]
			if !ok {
				return fmt.Errorf("unexpected key %d", k)
			}
			payload := pNode.GetPayload()
			if exp.Data != payload.Data {
				return fmt.Errorf("payload mismatch for key %d: %s != %s", k, payload.Data, exp.Data)
			}
			seen[k] = true
			return nil
		})
		if err != nil {
			close(done)
			wg.Wait()
			t.Fatalf("InOrderVisit failed in cycle %d: %v", cycle, err)
		}

		if len(seen) != len(expected) {
			close(done)
			wg.Wait()
			errorsFound := treap.ValidateAgainstDisk()
			for _, ef := range errorsFound {
				t.Logf("Validation error: %v", ef)
			}
			t.Fatalf("cycle %d: WalkInOrder yielded %d/%d items", cycle, len(seen), len(expected))
		}

		if enableFlush {
			endFlush := atomic.LoadInt64(&flushCount)
			if endFlush > startFlush {
				sawFlushBetweenCycles = true
				fCount := endFlush - startFlush
				t.Logf("cycle %d: observed %d flushes during insert/iterate", cycle, fCount)
			} else {
				t.Logf("cycle %d: no flush observed during insert/iterate", cycle)
			}
		}
	}

	close(done)
	wg.Wait()

	if enableFlush {
		// Give the flusher a brief window to run if the cycles finished quickly.
		if atomic.LoadInt64(&flushCount) == 0 {
			deadline := time.After(100 * time.Millisecond)
		WaitForFlush:
			for atomic.LoadInt64(&flushCount) == 0 {
				select {
				case <-deadline:
					break WaitForFlush
				default:
					// let the flusher run
					time.Sleep(1 * time.Millisecond)
				}
			}
		}
		if atomic.LoadInt64(&flushCount) == 0 {
			if err, ok := lastFlushErr.Load().(error); ok && err != nil {
				t.Logf("last flush error: %v", err)
			}
			if errCount := atomic.LoadInt64(&flushErrCount); errCount > 0 {
				t.Logf("flush error count: %d", errCount)
			}
			t.Fatalf("expected at least one flush to run")
		}
		if !sawFlushBetweenCycles {
			t.Logf("no cycles observed a flush between insert and iterate")
		}
	}
}
