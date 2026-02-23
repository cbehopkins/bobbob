package stringstore

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestWriteQueue_BatchingDeadlock demonstrates the core issue:
// The worker batches jobs and only sends responses during flush,
// but if all senders block waiting for responses before the batch
// fills or ticker fires, we get a deadlock.
func TestWriteQueue_BatchingDeadlock(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "batching_deadlock.bin")

	cfg := Config{
		FilePath:           storePath,
		MaxNumberOfStrings: 1000,
		StartingObjectId:   1000000000,
		ObjectIdInterval:   4,
		WriteFlushInterval: 10 * time.Second, // Very long - won't fire during test
		WriteMaxBatchBytes: 1024 * 1024,      // 1MB - won't fill with small writes
	}

	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Close()

	// Write small strings that won't fill the 1MB batch
	// Each write will block on job.done waiting for flush
	numWrites := 10
	writeSize := 15 // Small enough that 10 writes << 1MB

	t.Logf("Writing %d small strings (total %d bytes, batch limit %d bytes)...",
		numWrites, numWrites*writeSize, cfg.WriteMaxBatchBytes)
	t.Logf("Flush interval: %v (should not fire during test)", cfg.WriteFlushInterval)

	done := make(chan error, 1)
	go func() {
		for i := 0; i < numWrites; i++ {
			data := fmt.Sprintf("test_%d", i)

			objId, err := shard.NewObj(len(data))
			if err != nil {
				done <- err
				return
			}

			writer, finisher, err := shard.WriteToObj(objId)
			if err != nil {
				done <- err
				return
			}

			_, err = writer.Write([]byte(data))
			if err != nil {
				done <- err
				return
			}

			// This will block waiting for job.done
			t.Logf("  Write %d: calling finisher (will block on job.done)...", i)
			if err := finisher(); err != nil {
				done <- err
				return
			}
			t.Logf("  Write %d: finisher completed", i)
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write error: %v", err)
		}
		t.Log("✓ All writes completed (unexpected! Should have deadlocked)")
	case <-time.After(5 * time.Second):
		t.Logf("✓ CONFIRMED DEADLOCK: Worker has batched jobs but hasn't flushed.")
		t.Logf("   Queue length: %d", len(shard.writeQueue))
		t.Logf("   Theory: All %d senders are blocked on job.done, waiting for flush", numWrites)
		t.Logf("   Worker is in [select], waiting for more jobs or ticker")
		t.Logf("   But no more jobs can arrive because senders are blocked!")
		t.Fatal("DEADLOCK CONFIRMED (this is expected for this test)")
	}
}

// TestWriteQueue_TickerSavesUs confirms that the ticker CAN save us from deadlock
// if we wait long enough.
func TestWriteQueue_TickerSavesUs(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "ticker_saves.bin")

	cfg := Config{
		FilePath:           storePath,
		MaxNumberOfStrings: 1000,
		StartingObjectId:   1000000000,
		ObjectIdInterval:   4,
		WriteFlushInterval: 200 * time.Millisecond, // Short interval
		WriteMaxBatchBytes: 1024 * 1024,            // Large batch
	}

	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Close()

	numWrites := 10

	t.Logf("Writing %d small strings with flush interval %v...", numWrites, cfg.WriteFlushInterval)

	done := make(chan error, 1)
	startTime := time.Now()

	go func() {
		for i := 0; i < numWrites; i++ {
			data := fmt.Sprintf("test_%d", i)

			objId, _ := shard.NewObj(len(data))
			writer, finisher, _ := shard.WriteToObj(objId)
			writer.Write([]byte(data))

			if err := finisher(); err != nil {
				done <- err
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
		t.Logf("✓ All writes completed in %v", duration)
		t.Logf("   Ticker fired and flushed batched jobs, preventing deadlock")
		if duration < cfg.WriteFlushInterval {
			t.Logf("   (Completed faster than ticker interval - batch may have filled)")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("DEADLOCK: Ticker should have saved us but didn't")
	}
}

// TestWriteQueue_LargeBatchSavesUs confirms that filling the batch CAN save us.
func TestWriteQueue_LargeBatchSavesUs(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "batch_saves.bin")

	cfg := Config{
		FilePath:           storePath,
		MaxNumberOfStrings: 10000,
		StartingObjectId:   1000000000,
		ObjectIdInterval:   4,
		WriteFlushInterval: 10 * time.Second, // Very long
		WriteMaxBatchBytes: 1024,             // Small batch (1KB)
	}

	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Close()

	// Write enough data to fill the batch
	numWrites := 100
	writeSize := 20 // 100 * 20 = 2000 bytes > 1024 bytes

	t.Logf("Writing %d strings (%d bytes each) to fill %d byte batch...",
		numWrites, writeSize, cfg.WriteMaxBatchBytes)

	done := make(chan error, 1)
	startTime := time.Now()

	go func() {
		for i := 0; i < numWrites; i++ {
			data := fmt.Sprintf("test_string_%05d", i) // 20 bytes

			objId, _ := shard.NewObj(len(data))
			writer, finisher, _ := shard.WriteToObj(objId)
			writer.Write([]byte(data))

			if err := finisher(); err != nil {
				done <- err
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
		t.Logf("✓ All writes completed in %v", duration)
		t.Logf("   Batch filled and flushed multiple times, preventing deadlock")
	case <-time.After(10 * time.Second):
		t.Fatal("DEADLOCK: Batch should have filled but didn't")
	}
}
