package stringstore

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestWriteQueue_SequentialWritesDeadlock shows that even a SINGLE write
// will deadlock if the ticker doesn't fire, because enqueueWrite blocks
// immediately after sending to the queue.
func TestWriteQueue_SequentialWritesDeadlock(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "sequential.bin")

	cfg := Config{
		FilePath:           storePath,
		MaxNumberOfStrings: 1000,
		StartingObjectId:   1000000000,
		ObjectIdInterval:   4,
		WriteFlushInterval: 30 * time.Second, // Very long - won't fire
		WriteMaxBatchBytes: 1024 * 1024,      // Large batch
	}

	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Close()

	t.Log("Attempting a SINGLE write...")
	t.Log("The write will:")
	t.Log("  1. Send job to writeQueue (non-blocking, buffered)")
	t.Log("  2. Block on job.done waiting for response")
	t.Log("  3. Worker receives job, adds to batch")
	t.Log("  4. Worker goes back to select (batch not full, ticker hasn't fired)")
	t.Log("  5. DEADLOCK: Sender blocked, worker waiting, ticker won't fire for 30s")

	done := make(chan error, 1)
	go func() {
		data := "test_string_12345"

		// Use LateWriteNewObj for new objects (allocates + provides writer)
		objId, writer, finisher, err := shard.LateWriteNewObj(len(data))
		if err != nil {
			done <- fmt.Errorf("LateWriteNewObj failed: %w", err)
			return
		}
		_ = objId // Track for debugging

		_, err = writer.Write([]byte(data))
		if err != nil {
			done <- fmt.Errorf("Write failed: %w", err)
			return
		}

		// This will block forever (until ticker fires at 30s)
		t.Log("Calling finisher (will block on enqueueWrite -> job.done)...")
		if err := finisher(); err != nil {
			done <- fmt.Errorf("finisher failed: %w", err)
			return
		}

		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write error: %v", err)
		}
		// NEW DESIGN: Write completes within 10ms (ticker period)
		t.Log("✓ Write completed successfully (no deadlock)")
		t.Log("  New design: Non-blocking enqueue, worker flushes every 10ms")
		t.Log("  Old design would have deadlocked here for 30+ seconds")
	case <-time.After(5 * time.Second):
		t.Fatal("Write did not complete within 5s - unexpected!")
	}
}

// TestWriteQueue_RootCause documents the architectural flaw.
func TestWriteQueue_RootCause(t *testing.T) {
	t.Log("ROOT CAUSE ANALYSIS:")
	t.Log("")
	t.Log("The StringStore write queue has a fundamental design flaw:")
	t.Log("")
	t.Log("1. enqueueWrite() sends job to buffered writeQueue")
	t.Log("2. enqueueWrite() IMMEDIATELY blocks on unbuffered job.done channel")
	t.Log("3. Worker receives job from writeQueue")
	t.Log("4. Worker adds job to batch[]")
	t.Log("5. Worker goes back to select (waiting for more jobs or ticker)")
	t.Log("6. Worker does NOT send response on job.done yet")
	t.Log("7. Response only sent during flush() which happens when:")
	t.Log("   - Batch fills (>= WriteMaxBatchBytes)")
	t.Log("   - Ticker fires (WriteFlushInterval)")
	t.Log("")
	t.Log("8. If batch doesn't fill AND ticker hasn't fired:")
	t.Log("   - Sender blocked on job.done")
	t.Log("   - Worker blocked in select waiting for more jobs")
	t.Log("   - No more jobs can arrive (sender blocked)")
	t.Log("   → DEADLOCK")
	t.Log("")
	t.Log("SOLUTIONS:")
	t.Log("  A. Make job.done buffered AND send response immediately in worker")
	t.Log("  B. Don't block in enqueueWrite - make writes asynchronous")
	t.Log("  C. Flush immediately after each job (defeats batching purpose)")
	t.Log("  D. Add timeout to enqueueWrite with retry on ticker")
	t.Log("  E. Redesign: separate 'enqueue' from 'wait for completion'")
}
