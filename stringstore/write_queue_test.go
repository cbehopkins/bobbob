package stringstore

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestWriteQueue_ConcurrentEnqueue tests the write queue's ability to handle
// concurrent enqueue operations without deadlocking. This reproduces the
// conditions seen in vault persistence where multiple goroutines write strings.
func TestWriteQueue_ConcurrentEnqueue(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "concurrent_writes.bin")

	cfg := Config{
		FilePath:           storePath,
		MaxNumberOfStrings: 10000,
		StartingObjectId:   1000000000,
		ObjectIdInterval:   4,
		WriteFlushInterval: 100 * time.Millisecond,
		WriteMaxBatchBytes: 1024 * 1024,
	}

	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Close()

	// Test parameters similar to vault persist workload
	numGoroutines := 4
	writesPerGoroutine := 500

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	t.Logf("Starting %d goroutines, each writing %d strings...", numGoroutines, writesPerGoroutine)

	startTime := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < writesPerGoroutine; j++ {
				data := fmt.Sprintf("goroutine_%d_write_%d", goroutineID, j)

				// Allocate slot
				objId, err := shard.NewObj(len(data))
				if err != nil {
					errChan <- fmt.Errorf("goroutine %d: NewObj failed: %w", goroutineID, err)
					return
				}

				// Write data (this internally calls enqueueWrite)
				writer, finisher, err := shard.WriteToObj(objId)
				if err != nil {
					errChan <- fmt.Errorf("goroutine %d: WriteToObj failed: %w", goroutineID, err)
					return
				}

				_, err = writer.Write([]byte(data))
				if err != nil {
					errChan <- fmt.Errorf("goroutine %d: Write failed: %w", goroutineID, err)
					return
				}

				// This calls enqueueWrite and blocks on job.done
				if err := finisher(); err != nil {
					errChan <- fmt.Errorf("goroutine %d: finisher failed: %w", goroutineID, err)
					return
				}
			}
		}(i)
	}

	// Wait with timeout to catch deadlocks
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		duration := time.Since(startTime)
		t.Logf("SUCCESS: All writes completed in %v", duration)
	case err := <-errChan:
		t.Fatalf("Write error: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("DEADLOCK: Test timed out after 30 seconds")
	}

	// Verify no errors occurred
	close(errChan)
	for err := range errChan {
		t.Errorf("Goroutine error: %v", err)
	}
}

// TestWriteQueue_HighVolume tests a single goroutine writing many strings
// to isolate whether the issue is concurrency-related or volume-related.
func TestWriteQueue_HighVolume(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "high_volume.bin")

	cfg := Config{
		FilePath:           storePath,
		MaxNumberOfStrings: 10000,
		StartingObjectId:   1000000000,
		ObjectIdInterval:   4,
		WriteFlushInterval: 100 * time.Millisecond,
		WriteMaxBatchBytes: 1024 * 1024,
	}

	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Close()

	numWrites := 5000

	t.Logf("Writing %d strings sequentially...", numWrites)

	startTime := time.Now()

	done := make(chan error, 1)
	go func() {
		for i := 0; i < numWrites; i++ {
			data := fmt.Sprintf("write_%d", i)

			// Use LateWriteNewObj for new objects
			objId, writer, finisher, err := shard.LateWriteNewObj(len(data))
			if err != nil {
				done <- fmt.Errorf("LateWriteNewObj failed at %d: %w", i, err)
				return
			}
			_ = objId // Track for debugging

			_, err = writer.Write([]byte(data))
			if err != nil {
				done <- fmt.Errorf("Write failed at %d: %w", i, err)
				return
			}

			if err := finisher(); err != nil {
				done <- fmt.Errorf("finisher failed at %d: %w", i, err)
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write error: %v", err)
		}
		duration := time.Since(startTime)
		t.Logf("SUCCESS: All writes completed in %v", duration)
	case <-time.After(30 * time.Second):
		t.Fatal("DEADLOCK: Test timed out after 30 seconds")
	}
}

// TestWriteQueue_WorkerState monitors the worker goroutine state during writes.
func TestWriteQueue_WorkerState(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "worker_state.bin")

	cfg := Config{
		FilePath:           storePath,
		MaxNumberOfStrings: 1000,
		StartingObjectId:   1000000000,
		ObjectIdInterval:   4,
		WriteFlushInterval: 50 * time.Millisecond, // Faster flush for testing
		WriteMaxBatchBytes: 1024 * 1024,
	}

	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Close()

	// Write a few strings and monitor queue state
	for i := 0; i < 100; i++ {
		data := fmt.Sprintf("test_string_%d", i)

		objId, err := shard.NewObj(len(data))
		if err != nil {
			t.Fatalf("NewObj failed: %v", err)
		}

		writer, finisher, err := shard.WriteToObj(objId)
		if err != nil {
			t.Fatalf("WriteToObj failed: %v", err)
		}

		_, err = writer.Write([]byte(data))
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// Create a timeout for finisher to detect blocking
		finishDone := make(chan error, 1)
		go func() {
			finishDone <- finisher()
		}()

		select {
		case err := <-finishDone:
			if err != nil {
				t.Fatalf("finisher failed at write %d: %v", i, err)
			}
			// Success
		case <-time.After(5 * time.Second):
			t.Fatalf("BLOCKED: finisher hung at write %d. Queue len: %d", i, len(shard.writeQueue))
		}
	}

	t.Log("SUCCESS: All writes completed without blocking")
}

// TestWriteQueue_QueueFull tests behavior when writeQueue buffer is exhausted.
func TestWriteQueue_QueueFull(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "queue_full.bin")

	cfg := Config{
		FilePath:           storePath,
		MaxNumberOfStrings: 10000,
		StartingObjectId:   1000000000,
		ObjectIdInterval:   4,
		WriteFlushInterval: 10 * time.Second,  // Very slow flush to fill queue
		WriteMaxBatchBytes: 1024 * 1024 * 100, // Large batch to prevent early flush
	}

	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Close()

	// Try to overfill the queue (buffer is 4096)
	// Each enqueueWrite will block on job.done AFTER sending to queue,
	// so we need concurrent senders to actually fill the buffer
	numGoroutines := 10
	writesPerGoroutine := 500

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	t.Logf("Attempting to fill queue with %d concurrent writers...", numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < writesPerGoroutine; j++ {
				data := fmt.Sprintf("goroutine_%d_write_%d", id, j)

				objId, err := shard.NewObj(len(data))
				if err != nil {
					errChan <- err
					return
				}

				writer, finisher, err := shard.WriteToObj(objId)
				if err != nil {
					errChan <- err
					return
				}

				_, err = writer.Write([]byte(data))
				if err != nil {
					errChan <- err
					return
				}

				if err := finisher(); err != nil {
					errChan <- err
					return
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("SUCCESS: All writes completed despite queue pressure")
	case err := <-errChan:
		t.Fatalf("Write error: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatalf("DEADLOCK: Test timed out with queue len: %d", len(shard.writeQueue))
	}
}
