package testutil

import (
	"bytes"
	"io"
	"testing"

	"github.com/cbehopkins/bobbob/store"
)

func TestMockStore_NewObj(t *testing.T) {
	ms := NewMockStore()

	objId, err := ms.NewObj(100)
	if err != nil {
		t.Fatalf("NewObj failed: %v", err)
	}

	if objId == store.ObjNotAllocated {
		t.Error("Expected valid object ID")
	}

	// Should be able to read from newly allocated object
	reader, finisher, err := ms.LateReadObj(objId)
	if err != nil {
		t.Fatalf("LateReadObj failed: %v", err)
	}
	defer finisher()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	if len(data) != 100 {
		t.Errorf("Expected 100 bytes, got %d", len(data))
	}
}

func TestMockStore_WriteAndRead(t *testing.T) {
	ms := NewMockStore()

	// Write data
	testData := []byte("hello world")
	objId, writer, finisher, err := ms.LateWriteNewObj(len(testData))
	if err != nil {
		t.Fatalf("LateWriteNewObj failed: %v", err)
	}

	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(testData), n)
	}
	finisher()

	// Read data back
	reader, finisher2, err := ms.LateReadObj(objId)
	if err != nil {
		t.Fatalf("LateReadObj failed: %v", err)
	}
	defer finisher2()

	readData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	if !bytes.Equal(testData, readData) {
		t.Errorf("Data mismatch: wrote %q, read %q", testData, readData)
	}
}

func TestMockStore_WriteToObj(t *testing.T) {
	ms := NewMockStore()

	// Create object
	objId, err := ms.NewObj(20)
	if err != nil {
		t.Fatalf("NewObj failed: %v", err)
	}

	// Write to it
	testData := []byte("updated data")
	writer, finisher, err := ms.WriteToObj(objId)
	if err != nil {
		t.Fatalf("WriteToObj failed: %v", err)
	}

	_, err = writer.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	finisher()

	// Read back
	reader, finisher2, err := ms.LateReadObj(objId)
	if err != nil {
		t.Fatalf("LateReadObj failed: %v", err)
	}
	defer finisher2()

	readData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	if !bytes.Equal(testData, readData) {
		t.Errorf("Data mismatch: wrote %q, read %q", testData, readData)
	}
}

func TestMockStore_DeleteObj(t *testing.T) {
	ms := NewMockStore()

	objId, err := ms.NewObj(100)
	if err != nil {
		t.Fatalf("NewObj failed: %v", err)
	}

	// Delete it
	err = ms.DeleteObj(objId)
	if err != nil {
		t.Fatalf("DeleteObj failed: %v", err)
	}

	// Should not be readable after deletion
	_, _, err = ms.LateReadObj(objId)
	if err == nil {
		t.Error("Expected error reading deleted object")
	}
}

func TestMockStore_PrimeObject(t *testing.T) {
	ms := NewMockStore()

	// Get prime object
	primeId, err := ms.PrimeObject(256)
	if err != nil {
		t.Fatalf("PrimeObject failed: %v", err)
	}

	if primeId != store.ObjectId(8) {
		t.Errorf("Expected prime object ID to be 8, got %d", primeId)
	}

	// Should be idempotent
	primeId2, err := ms.PrimeObject(256)
	if err != nil {
		t.Fatalf("Second PrimeObject failed: %v", err)
	}

	if primeId != primeId2 {
		t.Errorf("Prime object ID changed: %d -> %d", primeId, primeId2)
	}

	// Should be readable
	reader, finisher, err := ms.LateReadObj(primeId)
	if err != nil {
		t.Fatalf("LateReadObj on prime object failed: %v", err)
	}
	defer finisher()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read prime object: %v", err)
	}

	if len(data) != 256 {
		t.Errorf("Expected prime object size 256, got %d", len(data))
	}
}

func TestMockStore_Close(t *testing.T) {
	ms := NewMockStore()

	err := ms.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Operations after close should fail
	_, err = ms.NewObj(100)
	if err == nil {
		t.Error("Expected error after Close")
	}
}

func TestMockStore_GetObjectInfo(t *testing.T) {
	ms := NewMockStore()

	objId, writer, finisher, err := ms.LateWriteNewObj(50)
	if err != nil {
		t.Fatalf("LateWriteNewObj failed: %v", err)
	}
	writer.Write(make([]byte, 50))
	finisher()

	info, exists := ms.GetObjectInfo(objId)
	if !exists {
		t.Error("Expected object info to exist")
	}

	if info.Size != 50 {
		t.Errorf("Expected size 50, got %d", info.Size)
	}

	// Non-existent object
	_, exists = ms.GetObjectInfo(store.ObjectId(9999))
	if exists {
		t.Error("Expected non-existent object info to not exist")
	}
}

func TestMockStore_ConcurrentAccess(t *testing.T) {
	ms := NewMockStore()

	// Create multiple objects concurrently
	const numGoroutines = 10
	const numOps = 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < numOps; j++ {
				objId, err := ms.NewObj(10)
				if err != nil {
					t.Errorf("NewObj failed: %v", err)
					done <- false
					return
				}

				// Write
				writer, finisher, err := ms.WriteToObj(objId)
				if err != nil {
					t.Errorf("WriteToObj failed: %v", err)
					done <- false
					return
				}
				writer.Write([]byte("test"))
				finisher()

				// Read
				reader, finisher2, err := ms.LateReadObj(objId)
				if err != nil {
					t.Errorf("LateReadObj failed: %v", err)
					done <- false
					return
				}
				io.ReadAll(reader)
				finisher2()
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		if !<-done {
			t.Fatal("Goroutine failed")
		}
	}
}
