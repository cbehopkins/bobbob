package stringstore

import (
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/internal/testutil"
)

func TestUnloadOnFullAndLazyLoad(t *testing.T) {
	_, parent, cleanup := testutil.SetupTestStore(t)
	defer cleanup()

	cfg := Config{
		FilePath:           filepath.Join(t.TempDir(), "strings.blob"),
		MaxNumberOfStrings: 2,
		StartingObjectId:   100,
		ObjectIdInterval:   1,
		WriteFlushInterval: 5 * time.Millisecond,
		WriteMaxBatchBytes: 1024,
		UnloadOnFull:       true,
		LazyLoadShards:     true,
	}

	store, err := NewStringStore(cfg)
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()
	store.AttachParentStore(parent)

	payloads := []string{"one", "two", "three"}
	objIds := make([]bobbob.ObjectId, 0, len(payloads))
	for i, payload := range payloads {
		objId, writer, finisher, err := store.LateWriteNewObj(len(payload))
		if err != nil {
			t.Fatalf("LateWriteNewObj %d failed: %v", i, err)
		}
		if _, err := writer.Write([]byte(payload)); err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
		if finisher != nil {
			if err := finisher(); err != nil {
				t.Fatalf("finish %d failed: %v", i, err)
			}
		}
		objIds = append(objIds, objId)
	}

	waitForWrites()

	if len(store.shards) < 2 {
		t.Fatalf("expected 2 shards, got %d", len(store.shards))
	}
	if !store.shards[0].unloaded {
		t.Fatalf("expected shard 0 to be unloaded after reaching full capacity")
	}

	reader, finisher, err := store.LateReadObj(objIds[0])
	if err != nil {
		t.Fatalf("LateReadObj failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	if finisher != nil {
		if finishErr := finisher(); finishErr != nil {
			t.Fatalf("read finisher failed: %v", finishErr)
		}
	}
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(data) != payloads[0] {
		t.Fatalf("payload mismatch: got %q want %q", string(data), payloads[0])
	}
	if store.shards[0].unloaded {
		t.Fatalf("expected shard 0 to be loaded after access")
	}
}

func TestUnloadAfterIdle(t *testing.T) {
	_, parent, cleanup := testutil.SetupTestStore(t)
	defer cleanup()

	cfg := Config{
		FilePath:           filepath.Join(t.TempDir(), "strings.blob"),
		MaxNumberOfStrings: 8,
		StartingObjectId:   200,
		ObjectIdInterval:   1,
		WriteFlushInterval: 5 * time.Millisecond,
		WriteMaxBatchBytes: 1024,
		UnloadAfterIdle:    1 * time.Millisecond,
		UnloadScanInterval: 1 * time.Millisecond,
	}

	store, err := NewStringStore(cfg)
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()
	store.AttachParentStore(parent)

	objId, writer, finisher, err := store.LateWriteNewObj(3)
	if err != nil {
		t.Fatalf("LateWriteNewObj failed: %v", err)
	}
	if _, err := writer.Write([]byte("abc")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if finisher != nil {
		if err := finisher(); err != nil {
			t.Fatalf("finisher failed: %v", err)
		}
	}

	waitForWrites()
	time.Sleep(2 * time.Millisecond)
	store.unloadIdleShards()

	if len(store.shards) != 1 {
		t.Fatalf("expected 1 shard, got %d", len(store.shards))
	}
	if !store.shards[0].unloaded {
		t.Fatalf("expected shard to be unloaded after idle")
	}

	reader, finisher, err := store.LateReadObj(objId)
	if err != nil {
		t.Fatalf("LateReadObj failed: %v", err)
	}
	_, _ = io.ReadAll(reader)
	if finisher != nil {
		if err := finisher(); err != nil {
			t.Fatalf("finisher failed: %v", err)
		}
	}
	if store.shards[0].unloaded {
		t.Fatalf("expected shard to be loaded after access")
	}
}
