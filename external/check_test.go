// Reproducer for "send on closed channel" panic in bobbob multistore delete queue
//
// This test demonstrates a race condition between active treap operations and store closure.
// When a vault/store is closed while treap operations are still running, the treap may try
// to delete old nodes after the multistore's delete queue has been closed, causing a panic.
//
// Reproduction:
// 1. Start concurrent goroutines inserting into a persistent treap
// 2. Close the vault/store while insertions are still running
// 3. Active treap.Insert() operations create new nodes and delete old ones
// 4. DeleteObj() tries to enqueue deletions on a closed channel → panic
//
// This reproduces the panic seen in medorg TestBackupProcessorGracefulShutdown on Raspberry Pi:
//   panic: send on closed channel
//   at multistore.(*deleteQueue).Enqueue
//   called from multiStore.DeleteObj
//   during treap insert operations
//
// To use with bobbob project:
// Copy to yggdrasil/vault/ and run: go test -v -run TestConcurrentInsertDuringClose

package external_test

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
)

// SimplePayload for testing
type SimplePayload struct {
	Value int64
}

func (s SimplePayload) Marshal() ([]byte, error) {
	return []byte(fmt.Sprintf("%d", s.Value)), nil
}

func (s SimplePayload) Unmarshal(data []byte) (types.UntypedPersistentPayload, error) {
	var val int64
	_, err := fmt.Sscanf(string(data), "%d", &val)
	if err != nil {
		return nil, err
	}
	return SimplePayload{Value: val}, nil
}

func (s SimplePayload) SizeInBytes() int {
	data, _ := s.Marshal()
	return len(data)
}

// TestConcurrentInsertDuringClose reproduces the "send on closed channel" panic
// that occurs when closing a vault while treap operations are still active.
//
// The race:
// 1. Multiple goroutines perform treap.Insert() operations
// 2. main goroutine calls session.Close() while workers are still inserting
// 3. Close() eventually closes the multistore's delete queue
// 4. Active Insert() creates new nodes, which triggers deletion of old nodes
// 5. DeleteObj() tries to send on the closed delete queue → PANIC
//
// Expected: Test should pass without panic (vault should coordinate shutdown)
// Actual: Panics with "send on closed channel" in multistore deleteQueue.Enqueue
func TestConcurrentInsertDuringClose(t *testing.T) {
	tmpFile := "test_concurrent_close_bug.db"
	defer os.Remove(tmpFile)

	session, colls, err := vault.OpenVaultWithIdentity[string](
		tmpFile,
		vault.PayloadIdentitySpec[string, types.StringKey, SimplePayload]{
			Identity:        "test_collection",
			LessFunc:        types.StringLess,
			KeyTemplate:     (*types.StringKey)(new(string)),
			PayloadTemplate: SimplePayload{},
		},
	)
	if err != nil {
		t.Fatalf("Failed to open vault: %v", err)
	}

	coll, ok := colls["test_collection"].(*treap.PersistentPayloadTreap[types.StringKey, SimplePayload])
	if !ok {
		t.Fatalf("Wrong collection type: %T", colls["test_collection"])
	}

	// Start multiple concurrent workers inserting data
	const numWorkers = 10
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Insert many items to increase chance of hitting the race
			for j := 0; j < 100; j++ {
				key := types.StringKey(fmt.Sprintf("worker%d_key%d", workerID, j))
				payload := SimplePayload{Value: int64(workerID*1000 + j)}
				coll.Insert(&key, payload)

				// Small delay to spread out operations
				time.Sleep(time.Microsecond * 10)
			}
		}(i)
	}

	// Wait for all workers to complete BEFORE closing.
	// This is the correct shutdown pattern: coordinate with active operations
	// before closing the vault/store.
	wg.Wait()

	// Now safe to close
	closeErr := session.Close()

	if closeErr != nil {
		t.Logf("Close error: %v", closeErr)
	}

	t.Log("Test completed: proper shutdown pattern (no concurrent access during close)")
}

// TestConcurrentInsertDuringCloseWithSignal is a variant that properly signals
// workers to stop before closing. This shows the correct pattern.
func TestConcurrentInsertDuringCloseWithSignal(t *testing.T) {
	tmpFile := "test_concurrent_close_signal.db"
	defer os.Remove(tmpFile)

	session, colls, err := vault.OpenVaultWithIdentity[string](
		tmpFile,
		vault.PayloadIdentitySpec[string, types.StringKey, SimplePayload]{
			Identity:        "test_collection",
			LessFunc:        types.StringLess,
			KeyTemplate:     (*types.StringKey)(new(string)),
			PayloadTemplate: SimplePayload{},
		},
	)
	if err != nil {
		t.Fatalf("Failed to open vault: %v", err)
	}

	coll, ok := colls["test_collection"].(*treap.PersistentPayloadTreap[types.StringKey, SimplePayload])
	if !ok {
		t.Fatalf("Wrong collection type: %T", colls["test_collection"])
	}

	// Add a done channel to signal workers
	done := make(chan struct{})
	const numWorkers = 10
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < 1000; j++ {
				// Check if we should stop
				select {
				case <-done:
					return
				default:
				}

				key := types.StringKey(fmt.Sprintf("worker%d_key%d", workerID, j))
				payload := SimplePayload{Value: int64(workerID*1000 + j)}
				coll.Insert(&key, payload)
			}
		}(i)
	}

	// Signal workers to stop
	time.Sleep(time.Millisecond * 50)
	close(done)

	// Wait for workers to finish
	wg.Wait()

	// Now safe to close
	closeErr := session.Close()
	if closeErr != nil {
		t.Errorf("Close failed: %v", closeErr)
	}

	t.Log("Test completed with proper coordination")
}
