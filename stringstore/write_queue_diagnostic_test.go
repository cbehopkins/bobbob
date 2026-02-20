package stringstore

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// TestWriteQueue_Diagnostic uses a smaller volume with detailed logging
// to understand exactly when and why the deadlock occurs.
func TestWriteQueue_Diagnostic(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "diagnostic.bin")

	cfg := Config{
		FilePath:           storePath,
		MaxNumberOfStrings: 1000,
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

	// Try different volumes to find the threshold
	testVolumes := []int{50, 100, 200, 500, 1000}

	for _, volume := range testVolumes {
		t.Run(fmt.Sprintf("volume_%d", volume), func(t *testing.T) {
			// Fresh shard for each test
			tempDir := t.TempDir()
			storePath := filepath.Join(tempDir, fmt.Sprintf("test_%d.bin", volume))

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

			done := make(chan error, 1)
			go func() {
				for i := 0; i < volume; i++ {
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
				t.Logf("✓ Volume %d completed successfully", volume)
			case <-time.After(10 * time.Second):
				// Print goroutine states
				buf := make([]byte, 1<<20)
				stackLen := runtime.Stack(buf, true)
				t.Logf("DEADLOCK at volume %d\nGoroutine dump:\n%s", volume, buf[:stackLen])
				t.Fatalf("Volume %d: DEADLOCK after 10 seconds. Queue len: %d", volume, len(shard.writeQueue))
			}
		})
	}
}

// TestWriteQueue_InspectWorker checks if the worker goroutine is actually running.
func TestWriteQueue_InspectWorker(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "inspect.bin")

	cfg := Config{
		FilePath:           storePath,
		MaxNumberOfStrings: 1000,
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

	// Count goroutines before and after
	initialGoroutines := runtime.NumGoroutine()
	t.Logf("Initial goroutines: %d", initialGoroutines)

	// Write a single item
	data := "test_string"
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

	// Monitor finisher with timeout
	finishDone := make(chan error, 1)
	go func() {
		finishDone <- finisher()
	}()

	select {
	case err := <-finishDone:
		if err != nil {
			t.Fatalf("finisher failed: %v", err)
		}
		t.Log("✓ First write completed")
	case <-time.After(5 * time.Second):
		currentGoroutines := runtime.NumGoroutine()
		t.Fatalf("Worker appears stuck. Goroutines: initial=%d, current=%d, queue_len=%d",
			initialGoroutines, currentGoroutines, len(shard.writeQueue))
	}

	// Try more writes
	for i := 0; i < 10; i++ {
		data := fmt.Sprintf("test_%d", i)
		objId, _ := shard.NewObj(len(data))
		writer, finisher, _ := shard.WriteToObj(objId)
		writer.Write([]byte(data))

		finishDone := make(chan error, 1)
		go func() {
			finishDone <- finisher()
		}()

		select {
		case err := <-finishDone:
			if err != nil {
				t.Fatalf("Write %d failed: %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Write %d: Worker stuck. Queue len: %d", i, len(shard.writeQueue))
		}
	}

	t.Log("✓ All writes completed. Worker is functioning.")
}
