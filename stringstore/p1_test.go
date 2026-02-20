package stringstore

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob"
)

func newTestConfig(t *testing.T) Config {
	return Config{
		FilePath:           filepath.Join(t.TempDir(), "stringstore-test.blob"),
		MaxNumberOfStrings: 100,
		StartingObjectId:   1000,
		ObjectIdInterval:   4,
	}
}

func flushWriteQueue(t *testing.T, shard *stringStoreShard) {
	t.Helper()
	flushCh := make(chan struct{})
	job := &writeJob{
		objectId: 0,
		data:     nil,
		doneChan: flushCh,
	}
	select {
	case shard.writeQueue <- job:
		select {
		case <-flushCh:
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("flush timed out")
		}
	case <-shard.closeCh:
		t.Fatalf("shard closed before flush")
	}
}

func writeStringToShard(t *testing.T, shard *stringStoreShard, data string) bobbob.ObjectId {
	t.Helper()
	objId, err := shard.NewObj(len(data))
	if err != nil {
		t.Fatalf("NewObj: %v", err)
	}
	writer, finisher, err := shard.WriteToObj(objId)
	if err != nil {
		t.Fatalf("WriteToObj: %v", err)
	}
	if _, err := writer.Write([]byte(data)); err != nil {
		t.Fatalf("writer.Write: %v", err)
	}
	if err := finisher(); err != nil {
		t.Fatalf("finisher: %v", err)
	}
	flushWriteQueue(t, shard)
	return objId
}

// TestStringStoreShard_MarshalState_Basic tests basic marshalling of empty shard state.
func TestStringStoreShard_MarshalState_Basic(t *testing.T) {
	cfg := newTestConfig(t)
	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("newStringStoreShard: %v", err)
	}
	defer shard.Close()

	// Marshal initial empty state
	data, err := shard.MarshalState()
	if err != nil {
		t.Fatalf("MarshalState: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("marshalled state should not be empty")
	}

	// Verify version byte is 2
	if data[0] != 2 {
		t.Fatalf("expected version 2, got %d", data[0])
	}
}

// TestStringStoreShard_MarshalState_RoundTrip tests marshalling and unmarshalling.
func TestStringStoreShard_MarshalState_RoundTrip(t *testing.T) {
	cfg := newTestConfig(t)
	shard1, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("newStringStoreShard: %v", err)
	}
	defer shard1.Close()

	// Add some strings to create state
	ids := make([]bobbob.ObjectId, 5)
	for i := 0; i < 5; i++ {
		ids[i] = writeStringToShard(t, shard1, "data")
	}

	// Delete one to create freelist
	if err := shard1.DeleteObj(ids[2]); err != nil {
		t.Fatalf("DeleteObj: %v", err)
	}
	flushWriteQueue(t, shard1)

	shard1.strdataObjId = 999

	// Marshal state
	data, err := shard1.MarshalState()
	if err != nil {
		t.Fatalf("MarshalState: %v", err)
	}

	// Create new shard and unmarshal
	shard2, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("newStringStoreShard 2: %v", err)
	}
	defer shard2.Close()

	if err := shard2.UnmarshalState(data); err != nil {
		t.Fatalf("UnmarshalState: %v", err)
	}

	// Verify state matches
	if shard2.nextId != shard1.nextId {
		t.Fatalf("nextId mismatch: %d != %d", shard2.nextId, shard1.nextId)
	}
	if shard2.maxAllocated != shard1.maxAllocated {
		t.Fatalf("maxAllocated mismatch: %d != %d", shard2.maxAllocated, shard1.maxAllocated)
	}
	if shard2.nextOffset != shard1.nextOffset {
		t.Fatalf("nextOffset mismatch: %d != %d", shard2.nextOffset, shard1.nextOffset)
	}
	if shard2.strdataObjId != shard1.strdataObjId {
		t.Fatalf("strdataObjId mismatch: %d != %d", shard2.strdataObjId, shard1.strdataObjId)
	}

	// Verify offset lookup matches
	if len(shard2.offsetLookup) != len(shard1.offsetLookup) {
		t.Fatalf("offsetLookup length mismatch: %d != %d", len(shard2.offsetLookup), len(shard1.offsetLookup))
	}
	for objId, loc1 := range shard1.offsetLookup {
		loc2, ok := shard2.offsetLookup[objId]
		if !ok {
			t.Fatalf("offsetLookup missing objId %d", objId)
		}
		if loc1.fileOffset != loc2.fileOffset || loc1.length != loc2.length {
			t.Fatalf("offsetLookup[%d] mismatch: (%d,%d) != (%d,%d)", objId, loc1.fileOffset, loc1.length, loc2.fileOffset, loc2.length)
		}
	}

	// Verify freelist matches
	if len(shard2.freeList) != len(shard1.freeList) {
		t.Fatalf("freeList length mismatch: %d != %d", len(shard2.freeList), len(shard1.freeList))
	}
	for objId := range shard1.freeList {
		if _, ok := shard2.freeList[objId]; !ok {
			t.Fatalf("freeList missing objId %d", objId)
		}
	}

	// Verify modified flag is false after unmarshal
	if shard2.modified.Load() {
		t.Fatal("modified should be false after unmarshal")
	}
}

// TestStringStoreShard_UnmarshalState_BadVersion tests version validation.
func TestStringStoreShard_UnmarshalState_BadVersion(t *testing.T) {
	cfg := newTestConfig(t)
	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("newStringStoreShard: %v", err)
	}
	defer shard.Close()

	// Create data with wrong version
	data := []byte{99}

	if err := shard.UnmarshalState(data); err == nil {
		t.Fatal("expected error for bad version")
	}
}

// TestStringStoreShard_UnmarshalState_ConfigMismatch tests config validation.
func TestStringStoreShard_UnmarshalState_ConfigMismatch(t *testing.T) {
	cfg1 := newTestConfig(t)
	shard1, err := newStringStoreShard(cfg1)
	if err != nil {
		t.Fatalf("newStringStoreShard: %v", err)
	}
	defer shard1.Close()

	// Marshal with config 1
	data, err := shard1.MarshalState()
	if err != nil {
		t.Fatalf("MarshalState: %v", err)
	}

	// Try to unmarshal with different config
	cfg2 := newTestConfig(t)
	cfg2.MaxNumberOfStrings = 200
	shard2, err := newStringStoreShard(cfg2)
	if err != nil {
		t.Fatalf("newStringStoreShard 2: %v", err)
	}
	defer shard2.Close()

	if err := shard2.UnmarshalState(data); err == nil {
		t.Fatal("expected error for config mismatch")
	}
}

// TestStringStoreShard_MarshalData returns file contents.
func TestStringStoreShard_MarshalData(t *testing.T) {
	cfg := newTestConfig(t)
	shard, err := newStringStoreShard(cfg)
	if err != nil {
		t.Fatalf("newStringStoreShard: %v", err)
	}
	defer shard.Close()

	// Write some data
	_ = writeStringToShard(t, shard, "hello")

	// Marshal data
	data, err := shard.MarshalData()
	if err != nil {
		t.Fatalf("MarshalData: %v", err)
	}

	// Should return some bytes (file content)
	if len(data) == 0 {
		t.Fatal("marshalled data should not be empty for written shard")
	}
}

// TestMockStringStore_Basic tests the mock implementation.
func TestMockStringStore_Basic(t *testing.T) {
	store := NewMockStringStore()

	// Test NewStringObj
	objId, err := store.NewStringObj("hello")
	if err != nil {
		t.Fatalf("NewStringObj: %v", err)
	}
	if objId <= 0 {
		t.Fatalf("expected positive ObjectId, got %d", objId)
	}

	// Test HasStringObj
	if !store.HasStringObj(objId) {
		t.Fatal("HasStringObj should return true for allocated ObjectId")
	}

	// Test StringFromObjId
	value, err := store.StringFromObjId(objId)
	if err != nil {
		t.Fatalf("StringFromObjId: %v", err)
	}
	if value != "hello" {
		t.Fatalf("expected 'hello', got '%s'", value)
	}

	// Test non-existent ObjectId
	if store.HasStringObj(99999) {
		t.Fatal("HasStringObj should return false for non-existent ObjectId")
	}
	if _, err := store.StringFromObjId(99999); err == nil {
		t.Fatal("StringFromObjId should return error for non-existent ObjectId")
	}
}

// TestMockStringStore_Concurrent tests concurrent access.
func TestMockStringStore_Concurrent(t *testing.T) {
	store := NewMockStringStore()
	done := make(chan bool, 10)

	// Multiple goroutines writing
	for i := 0; i < 10; i++ {
		go func(idx int) {
			for j := 0; j < 100; j++ {
				_, _ = store.NewStringObj("test")
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify no collision issues (count should be > 1000)
	// (The mock uses map, so no real collision issue, but we verify it works)
	objId, err := store.NewStringObj("final")
	if err != nil {
		t.Fatalf("NewStringObj: %v", err)
	}
	if objId <= 1000 {
		t.Fatalf("expected ObjectId > 1000 after concurrent writes, got %d", objId)
	}
}

// TestMockBaseStore_Basic tests the mock BaseStore.
func TestMockBaseStore_Basic(t *testing.T) {
	store := NewMockBaseStore()

	// Test NewObj
	objId, err := store.NewObj(100)
	if err != nil {
		t.Fatalf("NewObj: %v", err)
	}
	if objId <= 0 {
		t.Fatalf("expected positive ObjectId, got %d", objId)
	}

	// Verify allocation was tracked
	if len(store.AllocCalls) != 1 {
		t.Fatalf("expected 1 alloc call, got %d", len(store.AllocCalls))
	}
	if store.AllocCalls[0] != 100 {
		t.Fatalf("expected alloc size 100, got %d", store.AllocCalls[0])
	}

	// Test WriteToObj
	data := []byte("test data")
	writer, finisher, err := store.WriteToObj(objId)
	if err != nil {
		t.Fatalf("WriteToObj: %v", err)
	}
	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("writer.Write: %v", err)
	}
	if n != len(data) {
		t.Fatalf("expected write %d bytes, got %d", len(data), n)
	}
	err = finisher()
	if err != nil {
		t.Fatalf("finisher: %v", err)
	}

	// Verify write was tracked
	if len(store.WriteHist) != 1 {
		t.Fatalf("expected 1 write call, got %d", len(store.WriteHist))
	}

	// Test GetObjectData
	readData, err := store.GetObjectData(objId)
	if err != nil {
		t.Fatalf("GetObjectData: %v", err)
	}
	if string(readData) != string(data) {
		t.Fatalf("expected '%s', got '%s'", string(data), string(readData))
	}

	// Test DeleteObj
	err = store.DeleteObj(objId)
	if err != nil {
		t.Fatalf("DeleteObj: %v", err)
	}

	// Verify delete was tracked
	if len(store.DeleteCalls) != 1 {
		t.Fatalf("expected 1 delete call, got %d", len(store.DeleteCalls))
	}
	if store.DeleteCalls[0] != objId {
		t.Fatalf("expected delete objId %d, got %d", objId, store.DeleteCalls[0])
	}
}
