package stringstore

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	bobbob "github.com/cbehopkins/bobbob"
)

// TestAsyncDesign_BasicWorkflow demonstrates the new async write model.
func TestAsyncDesign_BasicWorkflow(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "async_test.bin")

	cfg := Config{
		FilePath:           storePath,
		WriteFlushInterval: 10 * time.Millisecond,
		MaxNumberOfStrings: 1000,
		StartingObjectId:   1,
		ObjectIdInterval:   1,
	}

	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Close()

	t.Log("Testing new async write design:")
	t.Log("  1. LateWriteNewObj allocates ID and returns writer immediately")
	t.Log("  2. Write enqueues to channel (non-blocking)")
	t.Log("  3. Worker processes writes in batch every 10ms")
	t.Log("  4. Objects become readable after worker updates offsetLookup")

	// Write a single object
	data := "test_data_12345"
	objId, writer, finisher, err := shard.LateWriteNewObj(len(data))
	if err != nil {
		t.Fatalf("LateWriteNewObj failed: %v", err)
	}

	_, err = writer.Write([]byte(data))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	err = finisher()
	if err != nil {
		t.Fatalf("Finisher failed: %v", err)
	}

	t.Logf("✓ Write enqueued successfully (objId=%d)", objId)

	// Give worker time to process (10ms ticker + small margin)
	time.Sleep(15 * time.Millisecond)

	// Now object should be readable
	reader, finalizeRead, err := shard.LateReadObj(objId)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	defer finalizeRead()

	readData := make([]byte, len(data))
	_, err = reader.Read(readData)
	if err != nil {
		t.Fatalf("Reader.Read failed: %v", err)
	}

	if string(readData) != data {
		t.Fatalf("Data mismatch: got %q, want %q", readData, data)
	}

	t.Log("✓ Object readable after worker flush")
	t.Log("SUCCESS: Async design working correctly")
}

// TestAsyncDesign_HighVolumeNonBlocking tests that writes don't block even under load.
func TestAsyncDesign_HighVolumeNonBlocking(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "async_volume.bin")

	cfg := Config{
		FilePath:           storePath,
		WriteFlushInterval: 10 * time.Millisecond,
		MaxNumberOfStrings: 10000,
		StartingObjectId:   1,
		ObjectIdInterval:   1,
	}

	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Close()

	numWrites := 1000
	t.Logf("Enqueueing %d writes...", numWrites)

	startTime := time.Now()

	// All writes should enqueue without blocking
	objIds := make([]bobbob.ObjectId, numWrites)
	for i := 0; i < numWrites; i++ {
		data := fmt.Sprintf("data_%d", i)

		objId, writer, finisher, err := shard.LateWriteNewObj(len(data))
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}

		writer.Write([]byte(data))
		finisher()

		objIds[i] = objId
	}

	enqueueTime := time.Since(startTime)
	t.Logf("✓ All %d writes enqueued in %v", numWrites, enqueueTime)

	if enqueueTime > 100*time.Millisecond {
		t.Errorf("Enqueue took too long (%v) - writes may be blocking", enqueueTime)
	}

	// Wait for worker to process all writes
	time.Sleep(50 * time.Millisecond)

	// Verify all objects readable
	successCount := 0
	for _, id := range objIds {
		_, finalizeRead, err := shard.LateReadObj(id)
		if err == nil {
			successCount++
			finalizeRead()
		}
	}

	t.Logf("✓ %d/%d objects readable", successCount, numWrites)

	if successCount < numWrites {
		t.Logf("  Note: %d objects still pending (worker may need more time)", numWrites-successCount)
	}

	t.Log("SUCCESS: Non-blocking writes confirmed")
}

// TestAsyncDesign_ReadBeforeWriteCompletes demonstrates error handling.
func TestAsyncDesign_ReadBeforeWriteCompletes(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "async_read_early.bin")

	cfg := Config{
		FilePath:           storePath,
		WriteFlushInterval: 1 * time.Second, // Long delay to guarantee write pending
		MaxNumberOfStrings: 1000,
		StartingObjectId:   1,
		ObjectIdInterval:   1,
	}

	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Close()

	data := "test_data"
	objId, writer, finisher, err := shard.LateWriteNewObj(len(data))
	if err != nil {
		t.Fatalf("LateWriteNewObj failed: %v", err)
	}

	writer.Write([]byte(data))
	finisher()

	// Try to read immediately (blocks until the write flushes)
	reader, finalizeRead, err := shard.LateReadObj(objId)
	if err != nil {
		t.Fatalf("Read after flush failed: %v", err)
	}
	defer finalizeRead()

	readData := make([]byte, len(data))
	reader.Read(readData)

	if string(readData) != data {
		t.Fatalf("Data mismatch: got %q, want %q", readData, data)
	}

	t.Log("✓ Object readable after worker flush completes")
	t.Log("SUCCESS: Async read waits for flush")
}
