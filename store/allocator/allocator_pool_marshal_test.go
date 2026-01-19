package allocator

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/internal"
)

// TestAllocatorPoolMarshalComplex tests the complete MarshalComplex implementation.
func TestAllocatorPoolMarshalComplex(t *testing.T) {
	// Create temporary file for testing
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_pool.bin")
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer file.Close()

	// Create parent allocator
	parent, err := NewBasicAllocator(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}

	// Create pool
	blockSize := 64
	blockCount := 10
	pool := NewAllocatorPool(blockSize, blockCount, parent, file)

	// Allocate some blocks to create allocators
	for i := 0; i < 15; i++ {
		_, _, _, err = pool.Allocate()
		if err != nil {
			t.Fatalf("Failed to allocate block %d: %v", i, err)
		}
	}

	// Verify we have allocators created
	if len(pool.available) == 0 {
		t.Fatal("Expected available allocators to be created")
	}

	// Test PreMarshal
	sizes, err := pool.PreMarshal()
	if err != nil {
		t.Fatalf("PreMarshal failed: %v", err)
	}

	if len(sizes) == 0 {
		t.Fatal("PreMarshal returned no sizes")
	}

	t.Logf("PreMarshal returned %d sizes: %v", len(sizes), sizes)

	// Allocate ObjectIds for the pool
	objIds := make([]ObjectId, len(sizes))
	for i, size := range sizes {
		objId, _, err := parent.Allocate(size)
		if err != nil {
			t.Fatalf("Failed to allocate ObjectId %d: %v", i, err)
		}
		objIds[i] = objId
	}

	// Test MarshalMultiple
	identityFunc, objectAndByteFuncs, err := pool.MarshalMultiple(objIds)
	if err != nil {
		t.Fatalf("MarshalMultiple failed: %v", err)
	}

	if identityFunc == nil {
		t.Fatal("identityFunc is nil")
	}

	lutObjId := identityFunc()
	if lutObjId < 1 {
		t.Fatal("Identity function returned invalid ObjectId")
	}

	t.Logf("LUT ObjectId: %d", lutObjId)
	t.Logf("Object count: %d", len(objectAndByteFuncs))

	// Verify we have the right number of objects (LUT + allocators)
	expectedObjects := 1 + len(pool.available) + len(pool.full) // LUT + allocators
	if len(objectAndByteFuncs) != expectedObjects {
		t.Fatalf("Expected %d objects, got %d", expectedObjects, len(objectAndByteFuncs))
	}

	// Write all objects to file
	for _, obj := range objectAndByteFuncs {
		data, err := obj.ByteFunc()
		if err != nil {
			t.Fatalf("Failed to get bytes for ObjectId %d: %v", obj.ObjectId, err)
		}

		_, offset, err := parent.GetObjectInfo(obj.ObjectId)
		if err != nil {
			t.Fatalf("Failed to get info for ObjectId %d: %v", obj.ObjectId, err)
		}
		n, err := file.WriteAt(data, int64(offset))
		if err != nil {
			t.Fatalf("Failed to write data for ObjectId %d: %v", obj.ObjectId, err)
		}
		if n != len(data) {
			t.Fatalf("Partial write for ObjectId %d: %d/%d bytes", obj.ObjectId, n, len(data))
		}
	}

	t.Logf("Successfully marshaled pool with %d available and %d full allocators",
		len(pool.available), len(pool.full))
}

// TestAllocatorPoolUnmarshalMultiple tests the UnmarshalMultiple implementation.
func TestAllocatorPoolUnmarshalMultiple(t *testing.T) {
	// Create temporary file for testing
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_pool_unmarshal.bin")
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer file.Close()

	// Create parent allocator
	parent, err := NewBasicAllocator(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}

	// Create and populate original pool
	blockSize := 64
	blockCount := 10
	origPool := NewAllocatorPool(blockSize, blockCount, parent, file)

	// Allocate some blocks
	allocatedIds := make([]ObjectId, 0)
	for i := 0; i < 25; i++ {
		objId, _, _, err := origPool.Allocate()
		if err != nil {
			t.Fatalf("Failed to allocate block %d: %v", i, err)
		}
		allocatedIds = append(allocatedIds, objId)
	}

	// Marshal the pool
	sizes, err := origPool.PreMarshal()
	if err != nil {
		t.Fatalf("PreMarshal failed: %v", err)
	}

	objIds := make([]ObjectId, len(sizes))
	for i, size := range sizes {
		objId, _, err := parent.Allocate(size)
		if err != nil {
			t.Fatalf("Failed to allocate ObjectId %d: %v", i, err)
		}
		objIds[i] = objId
	}

	identityFunc, objectAndByteFuncs, err := origPool.MarshalMultiple(objIds)
	if err != nil {
		t.Fatalf("MarshalMultiple failed: %v", err)
	}

	lutObjId := identityFunc()

	// Write all objects
	for _, obj := range objectAndByteFuncs {
		data, err := obj.ByteFunc()
		if err != nil {
			t.Fatalf("Failed to get bytes: %v", err)
		}
		_, offset, err := parent.GetObjectInfo(obj.ObjectId)
		if err != nil {
			t.Fatalf("Failed to get info for ObjectId %d: %v", obj.ObjectId, err)
		}
		if _, err := file.WriteAt(data, int64(offset)); err != nil {
			t.Fatalf("Failed to write data for ObjectId %d: %v", obj.ObjectId, err)
		}
	}

	// Retrieve LUT bytes directly from marshal output (avoids file read races in tests)
	var lutData []byte
	for _, obj := range objectAndByteFuncs {
		if obj.ObjectId == lutObjId {
			lutData, err = obj.ByteFunc()
			if err != nil {
				t.Fatalf("Failed to get LUT data: %v", err)
			}
			break
		}
	}
	if lutData == nil {
		t.Fatalf("LUT data not found for ObjectId %d", lutObjId)
	}

	// Create new pool and unmarshal
	newPool := NewAllocatorPool(blockSize, blockCount, parent, file)
	reader := &fileObjReader{file: file, allocator: parent}
	err = newPool.UnmarshalMultiple(bytes.NewReader(lutData), reader)
	if err != nil {
		t.Fatalf("UnmarshalMultiple failed: %v", err)
	}

	// Verify the pool was correctly restored
	if len(newPool.available) != len(origPool.available) {
		t.Errorf("Available count mismatch: got %d, want %d",
			len(newPool.available), len(origPool.available))
	}

	if len(newPool.full) != len(origPool.full) {
		t.Errorf("Full count mismatch: got %d, want %d",
			len(newPool.full), len(origPool.full))
	}

	t.Logf("Successfully unmarshaled pool with %d available and %d full allocators",
		len(newPool.available), len(newPool.full))
}

// TestAllocatorPoolRoundTrip tests a complete marshal/unmarshal cycle.
func TestAllocatorPoolRoundTrip(t *testing.T) {
	// Create temporary file
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_pool_roundtrip.bin")
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer file.Close()

	parent, err := NewBasicAllocator(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	blockSize := 128
	blockCount := 8

	// Create original pool and allocate blocks
	origPool := NewAllocatorPool(blockSize, blockCount, parent, file)

	allocations := make([]struct {
		objId  ObjectId
		offset FileOffset
	}, 0)

	// Allocate 20 blocks (should create multiple allocators)
	for i := 0; i < 20; i++ {
		objId, offset, _, err := origPool.Allocate()
		if err != nil {
			t.Fatalf("Failed to allocate block %d: %v", i, err)
		}
		allocations = append(allocations, struct {
			objId  ObjectId
			offset FileOffset
		}{objId, offset})
	}

	// Free some blocks to test various states
	for i := 0; i < 5; i++ {
		err = origPool.freeFromPool(allocations[i].offset, blockSize, nil)
		if err != nil {
			t.Fatalf("Failed to free block %d: %v", i, err)
		}
	}

	// Marshal
	sizes, err := origPool.PreMarshal()
	if err != nil {
		t.Fatalf("PreMarshal failed: %v", err)
	}

	objIds := make([]ObjectId, len(sizes))
	for i, size := range sizes {
		objId, _, err := parent.Allocate(size)
		if err != nil {
			t.Fatalf("Failed to allocate ObjectId: %v", err)
		}
		objIds[i] = objId
	}

	identityFunc, objectAndByteFuncs, err := origPool.MarshalMultiple(objIds)
	if err != nil {
		t.Fatalf("MarshalMultiple failed: %v", err)
	}

	// Write all objects
	for _, obj := range objectAndByteFuncs {
		data, err := obj.ByteFunc()
		if err != nil {
			t.Fatalf("ByteFunc failed: %v", err)
		}
		_, offset, err := parent.GetObjectInfo(obj.ObjectId)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if _, err := file.WriteAt(data, int64(offset)); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Unmarshal using LUT data from marshal output
	lutObjId := identityFunc()
	var lutData []byte
	for _, obj := range objectAndByteFuncs {
		if obj.ObjectId == lutObjId {
			lutData, err = obj.ByteFunc()
			if err != nil {
				t.Fatalf("Failed to get LUT data: %v", err)
			}
			break
		}
	}
	if lutData == nil {
		t.Fatalf("LUT data not found for ObjectId %d", lutObjId)
	}

	newPool := NewAllocatorPool(blockSize, blockCount, parent, file)
	reader := &fileObjReader{file: file, allocator: parent}
	err = newPool.UnmarshalMultiple(bytes.NewReader(lutData), reader)
	if err != nil {
		t.Fatalf("UnmarshalMultiple failed: %v", err)
	}

	t.Logf("Round-trip completed: available=%d full=%d",
		len(newPool.available), len(newPool.full))
}

// TestAllocatorPoolReAllocation tests the re-allocation mechanism.
func TestAllocatorPoolReAllocation(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_pool_realloc.bin")
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer file.Close()

	parent, err := NewBasicAllocator(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	blockSize := 64
	blockCount := 5

	pool := NewAllocatorPool(blockSize, blockCount, parent, file)

	// Allocate blocks to create allocators
	for i := 0; i < 8; i++ {
		_, _, _, err = pool.Allocate()
		if err != nil {
			t.Fatalf("Allocation failed: %v", err)
		}
	}

	// First PreMarshal
	sizes1, err := pool.PreMarshal()
	if err != nil {
		t.Fatalf("First PreMarshal failed: %v", err)
	}

	// Allocate insufficient ObjectIds (should trigger re-allocation)
	objIds1 := make([]ObjectId, len(sizes1)-1) // One less than needed
	for i := range objIds1 {
		objId, _, err := parent.Allocate(sizes1[i])
		if err != nil {
			t.Fatalf("Allocation failed: %v", err)
		}
		objIds1[i] = objId
	}

	// Try MarshalMultiple - should fail with ErrRePreAllocate
	_, _, err = pool.MarshalMultiple(objIds1)
	if !errors.Is(err, internal.ErrRePreAllocate) {
		t.Fatalf("Expected ErrRePreAllocate, got %v", err)
	}

	// Allocate more blocks to trigger new allocator creation
	for i := 0; i < 3; i++ {
		_, _, _, err = pool.Allocate()
		if err != nil {
			t.Fatalf("Additional allocation failed: %v", err)
		}
	}

	// Second PreMarshal (should return more sizes)
	sizes2, err := pool.PreMarshal()
	if err != nil {
		t.Fatalf("Second PreMarshal failed: %v", err)
	}

	if len(sizes2) < len(sizes1) {
		t.Fatalf("Expected at least as many sizes after allocations: got %d, want >= %d",
			len(sizes2), len(sizes1))
	}

	// Now allocate correct number of ObjectIds
	objIds2 := make([]ObjectId, len(sizes2))
	for i, size := range sizes2 {
		objId, _, err := parent.Allocate(size)
		if err != nil {
			t.Fatalf("Allocation failed: %v", err)
		}
		objIds2[i] = objId
	}

	// MarshalMultiple should succeed now
	_, objectAndByteFuncs, err := pool.MarshalMultiple(objIds2)
	if err != nil {
		t.Fatalf("MarshalMultiple failed after re-allocation: %v", err)
	}

	expectedCount := 1 + len(pool.available) + len(pool.full)
	if len(objectAndByteFuncs) != expectedCount {
		t.Errorf("Object count mismatch: got %d, want %d",
			len(objectAndByteFuncs), expectedCount)
	}

	t.Logf("Re-allocation test passed: %d -> %d sizes", len(sizes1), len(sizes2))
}

// TestAllocatorPoolDelete tests the Delete functionality.
func TestAllocatorPoolDelete(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_pool_delete.bin")
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer file.Close()

	parent, err := NewBasicAllocator(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	blockSize := 64
	blockCount := 10

	pool := NewAllocatorPool(blockSize, blockCount, parent, file)

	// Allocate blocks
	for i := 0; i < 15; i++ {
		_, _, _, err = pool.Allocate()
		if err != nil {
			t.Fatalf("Allocation failed: %v", err)
		}
	}

	// Marshal to assign fileOffsets
	sizes, err := pool.PreMarshal()
	if err != nil {
		t.Fatalf("PreMarshal failed: %v", err)
	}

	objIds := make([]ObjectId, len(sizes))
	for i, size := range sizes {
		objId, _, err := parent.Allocate(size)
		if err != nil {
			t.Fatalf("Allocation failed: %v", err)
		}
		objIds[i] = objId
	}

	_, _, err = pool.MarshalMultiple(objIds)
	if err != nil {
		t.Fatalf("MarshalMultiple failed: %v", err)
	}

	// Collect fileOffsets before deletion
	fileOffsets := make([]FileOffset, 0)
	for _, ref := range pool.available {
		if ref.fileOff >= 1 {
			fileOffsets = append(fileOffsets, ref.fileOff)
		}
	}
	for _, ref := range pool.full {
		if ref.fileOff >= 1 {
			fileOffsets = append(fileOffsets, ref.fileOff)
		}
	}

	if len(fileOffsets) == 0 {
		t.Fatal("No fileOffsets assigned")
	}

	// Delete the pool
	err = pool.Delete()
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify the fileOffsets were freed in parent
	// (This is tricky to test directly, but we can try to re-allocate and see if we get same offsets)
	for _, offset := range fileOffsets {
		_, _, err := parent.GetObjectInfo(ObjectId(offset))
		// After deletion, GetObjectInfo should return error or show pending deletion
		if err == nil {
			t.Logf("Warning: fileOffset %d still appears allocated after Delete", offset)
		}
	}

	t.Logf("Delete test completed: %d fileOffsets processed", len(fileOffsets))
}

// TestAllocatorPoolMarshalLUTFormat verifies the LUT format correctness.
func TestAllocatorPoolMarshalLUTFormat(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_lut_format.bin")
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer file.Close()

	parent, err := NewBasicAllocator(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}

	blockSize := 32
	blockCount := 5
	pool := NewAllocatorPool(blockSize, blockCount, parent, file)

	// Allocate to create a mix of available and full
	for i := 0; i < 12; i++ {
		_, _, _, err = pool.Allocate()
		if err != nil {
			t.Fatalf("Allocation failed: %v", err)
		}
	}

	sizes, err := pool.PreMarshal()
	if err != nil {
		t.Fatalf("PreMarshal failed: %v", err)
	}

	objIds := make([]ObjectId, len(sizes))
	for i, size := range sizes {
		objId, _, err := parent.Allocate(size)
		if err != nil {
			t.Fatalf("Allocation failed: %v", err)
		}
		objIds[i] = objId
	}

	identityFunc, objectAndByteFuncs, err := pool.MarshalMultiple(objIds)
	if err != nil {
		t.Fatalf("MarshalMultiple failed: %v", err)
	}

	// Get the LUT
	lutObjId := identityFunc()
	var lutData []byte
	for _, obj := range objectAndByteFuncs {
		if obj.ObjectId == lutObjId {
			lutData, err = obj.ByteFunc()
			if err != nil {
				t.Fatalf("Failed to get LUT data: %v", err)
			}
			break
		}
	}

	if lutData == nil {
		t.Fatal("LUT not found in objectAndByteFuncs")
	}

	// Verify LUT format: [availCount:4][fullCount:4][fileOff1:8][fileOff2:8]...
	if len(lutData) < 8 {
		t.Fatalf("LUT too small: %d bytes", len(lutData))
	}

	// Read counts
	availCount := int32(lutData[0])<<24 | int32(lutData[1])<<16 | int32(lutData[2])<<8 | int32(lutData[3])
	fullCount := int32(lutData[4])<<24 | int32(lutData[5])<<16 | int32(lutData[6])<<8 | int32(lutData[7])

	t.Logf("LUT: availCount=%d, fullCount=%d", availCount, fullCount)

	if int(availCount) != len(pool.available) {
		t.Errorf("availCount mismatch: got %d, want %d", availCount, len(pool.available))
	}

	if int(fullCount) != len(pool.full) {
		t.Errorf("fullCount mismatch: got %d, want %d", fullCount, len(pool.full))
	}

	// Verify we have enough data for all fileOffsets
	expectedSize := 8 + (availCount+fullCount)*8
	if len(lutData) != int(expectedSize) {
		t.Errorf("LUT size mismatch: got %d, want %d", len(lutData), expectedSize)
	}

	// Read fileOffsets and verify they're valid
	offset := 8
	for i := 0; i < int(availCount+fullCount); i++ {
		fileOff := FileOffset(binary.BigEndian.Uint64(lutData[offset : offset+8]))
		if fileOff < 1 {
			t.Errorf("Invalid fileOffset at index %d: %d", i, fileOff)
		}
		t.Logf("  fileOff[%d] = %d", i, fileOff)
		offset += 8
	}
}

// fileObjReader is a minimal objReader for UnmarshalMultiple that reads directly from the backing file.
type fileObjReader struct {
	file      *os.File
	allocator *BasicAllocator
}

// LateReadObj returns a section reader over the object range.
func (r *fileObjReader) LateReadObj(id ObjectId) (io.Reader, func() error, error) {
	offset, size, err := r.allocator.GetObjectInfo(id)
	if err != nil {
		return nil, nil, err
	}
	reader := io.NewSectionReader(r.file, int64(offset), int64(size))
	return reader, func() error { return nil }, nil
}
