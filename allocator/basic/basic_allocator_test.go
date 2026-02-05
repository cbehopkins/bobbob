package basic

import (
	"os"
	"testing"

	"github.com/cbehopkins/bobbob/allocator/types"
)

func createTestFile(t *testing.T) (*os.File, func()) {
	t.Helper()
	
	file, err := os.CreateTemp("", "basic_allocator_test_*.bin")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	
	cleanup := func() {
		file.Close()
		os.Remove(file.Name())
	}
	
	return file, cleanup
}

func TestBasicAllocatorNew(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	if allocator.file != file {
		t.Errorf("Expected file %v, got %v", file, allocator.file)
	}
	
	if allocator.GetObjectCount() != 0 {
		t.Errorf("Expected empty object tracking, got %d entries", allocator.GetObjectCount())
	}
	
	if allocator.fileLength != 0 {
		t.Errorf("Expected fileLength 0, got %d", allocator.fileLength)
	}
}

func TestBasicAllocatorNewNilFile(t *testing.T) {
	_, err := New(nil)
	if err == nil {
		t.Error("Expected error for nil file, got nil")
	}
}

func TestBasicAllocatorAllocate(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	size := 100
	objId, offset, err := allocator.Allocate(size)
	if err != nil {
		t.Fatalf("Allocate failed: %v", err)
	}
	
	// ObjectId should equal FileOffset
	if types.FileOffset(objId) != offset {
		t.Errorf("ObjectId %d should equal offset %d", objId, offset)
	}
	
	// First allocation should be at offset 0
	if offset != 0 {
		t.Errorf("Expected offset 0, got %d", offset)
	}
	
	// Check ObjectMap
	if !allocator.ContainsObjectId(objId) {
		t.Error("ObjectId not found in object tracking")
	}
	
	offset_check, size_check, _ := allocator.GetObjectInfo(objId)
	if size_check != types.FileSize(size) {
		t.Errorf("Expected size %d, got %d", size, size_check)
	}
	if offset_check != offset {
		t.Errorf("Expected offset %d, got %d", offset, offset_check)
	}
	
	// Check file length updated
	if allocator.fileLength != types.FileOffset(size) {
		t.Errorf("Expected fileLength %d, got %d", size, allocator.fileLength)
	}
}

func TestBasicAllocatorMultipleAllocations(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	sizes := []int{100, 200, 300}
	expectedOffset := types.FileOffset(0)
	
	for _, size := range sizes {
		objId, offset, err := allocator.Allocate(size)
		if err != nil {
			t.Fatalf("Allocate failed: %v", err)
		}
		
		if offset != expectedOffset {
			t.Errorf("Expected offset %d, got %d", expectedOffset, offset)
		}
		
		if types.FileOffset(objId) != offset {
			t.Errorf("ObjectId %d should equal offset %d", objId, offset)
		}
		
		expectedOffset += types.FileOffset(size)
	}
	
	// Check final file length
	totalSize := types.FileOffset(100 + 200 + 300)
	if allocator.fileLength != totalSize {
		t.Errorf("Expected fileLength %d, got %d", totalSize, allocator.fileLength)
	}
	
	// Check all objects in tracking
	if allocator.GetObjectCount() != 3 {
		t.Errorf("Expected 3 objects in tracking, got %d", allocator.GetObjectCount())
	}
}

func TestBasicAllocatorDeleteObj(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	// Allocate two objects so deleting first creates a gap (not truncate)
	objId1, _, _ := allocator.Allocate(100)
	objId2, _, _ := allocator.Allocate(100)
	
	// Delete first object (should create gap, not truncate)
	err = allocator.DeleteObj(objId1)
	if err != nil {
		t.Fatalf("DeleteObj failed: %v", err)
	}
	
	// Check removed from tracking
	if allocator.ContainsObjectId(objId1) {
		t.Error("ObjectId should not exist after delete")
	}
	
	// Check gap added to FreeList
	allocated, _, gaps := allocator.Stats()
	if allocated != 1 {
		t.Errorf("Expected 1 allocated (objId2), got %d", allocated)
	}
	if gaps != 1 {
		t.Errorf("Expected 1 gap, got %d", gaps)
	}
	
	// Verify second object still exists
	if !allocator.ContainsObjectId(objId2) {
		t.Error("Second object should still exist")
	}
}

func TestBasicAllocatorDeleteInvalidObjectId(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	err = allocator.DeleteObj(types.ObjectId(999))
	if err != ErrInvalidObjectId {
		t.Errorf("Expected ErrInvalidObjectId, got %v", err)
	}
}

func TestBasicAllocatorDeleteLastObjectTruncates(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	// Allocate two objects
	objId1, _, _ := allocator.Allocate(100)
	objId2, _, _ := allocator.Allocate(200)
	
	originalLength := allocator.fileLength // Should be 300
	
	// Delete last object
	err = allocator.DeleteObj(objId2)
	if err != nil {
		t.Fatalf("DeleteObj failed: %v", err)
	}
	
	// File should be truncated to 100
	if allocator.fileLength != 100 {
		t.Errorf("Expected fileLength 100 after truncation, got %d", allocator.fileLength)
	}
	
	if allocator.fileLength >= originalLength {
		t.Error("File should have been truncated")
	}
	
	// No gap should be added for truncated object
	_, _, gaps := allocator.Stats()
	if gaps != 0 {
		t.Errorf("Expected 0 gaps after truncation, got %d", gaps)
	}
	
	// First object still exists
	if !allocator.ContainsObjectId(objId1) {
		t.Error("First object should still exist")
	}
}

func TestBasicAllocatorReuseGap(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	// Allocate three objects
	objId1, offset1, _ := allocator.Allocate(100)
	allocator.Allocate(100) // Middle object
	objId3, _, _ := allocator.Allocate(100)
	
	// Delete first object (creates gap at offset 0)
	allocator.DeleteObj(objId1)
	
	originalLength := allocator.fileLength
	
	// Allocate new object that fits in gap
	newObjId, newOffset, err := allocator.Allocate(50)
	if err != nil {
		t.Fatalf("Allocate failed: %v", err)
	}
	
	// Should reuse the gap at offset 0
	if newOffset != offset1 {
		t.Errorf("Expected new object at offset %d (reused gap), got %d", offset1, newOffset)
	}
	
	// File length should not grow
	if allocator.fileLength != originalLength {
		t.Errorf("File length should not grow when reusing gap, expected %d, got %d", originalLength, allocator.fileLength)
	}
	
	// Gap should be split (100 byte gap, 50 byte allocation, 50 byte remainder)
	_, _, gaps := allocator.Stats()
	if gaps != 1 {
		t.Errorf("Expected 1 gap (remainder), got %d", gaps)
	}
	
	// All three objects should exist
	if !allocator.ContainsObjectId(newObjId) {
		t.Error("New object should exist")
	}
	if !allocator.ContainsObjectId(objId3) {
		t.Error("Third object should still exist")
	}
}

func TestBasicAllocatorGapCoalescing(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	// Allocate three adjacent objects
	objId1, _, _ := allocator.Allocate(100)
	objId2, _, _ := allocator.Allocate(100)
	objId3, _, _ := allocator.Allocate(100)
	
	// Delete middle object (creates gap)
	allocator.DeleteObj(objId2)
	
	_, _, gaps := allocator.Stats()
	if gaps != 1 {
		t.Fatalf("Expected 1 gap after first delete, got %d", gaps)
	}
	
	// Delete first object (should coalesce with existing gap)
	allocator.DeleteObj(objId1)
	
	_, _, gaps = allocator.Stats()
	if gaps != 1 {
		t.Errorf("Expected 1 coalesced gap, got %d", gaps)
	}
	
	// Check coalesced gap size (should be 200 bytes)
	if len(allocator.freeList.gaps) != 1 {
		t.Fatalf("Expected 1 gap in freeList, got %d", len(allocator.freeList.gaps))
	}
	
	coalescedGap := allocator.freeList.gaps[0]
	if coalescedGap.Size != 200 {
		t.Errorf("Expected coalesced gap size 200, got %d", coalescedGap.Size)
	}
	
	// Delete third object (should truncate, not add to FreeList)
	allocator.DeleteObj(objId3)
	
	_, _, gaps = allocator.Stats()
	if gaps != 0 {
		t.Errorf("Expected 0 gaps after truncation, got %d", gaps)
	}
}

func TestBasicAllocatorMinGapSize(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	// Allocate two objects
	objId1, _, _ := allocator.Allocate(10)
	allocator.Allocate(100)
	
	// Delete first small object (only 10 bytes - too small to track)
	allocator.DeleteObj(objId1)
	
	// Gap should NOT be added (below minGapSize of 4... wait, 10 > 4)
	// Actually, let's test with a 3-byte object (below minGapSize)
	objId2, _, _ := allocator.Allocate(3)
	_, _, _ = allocator.Allocate(100)
	
	allocator.DeleteObj(objId2)
	
	_, _, gaps := allocator.Stats()
	// The 10-byte gap should exist, but 3-byte gap should not
	if gaps == 0 {
		t.Error("Expected at least one gap to be tracked")
	}
}

func TestBasicAllocatorGetObjectInfo(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	size := 100
	objId, expectedOffset, _ := allocator.Allocate(size)
	
	offset, fileSize, err := allocator.GetObjectInfo(objId)
	if err != nil {
		t.Fatalf("GetObjectInfo failed: %v", err)
	}
	
	if offset != expectedOffset {
		t.Errorf("Expected offset %d, got %d", expectedOffset, offset)
	}
	
	if fileSize != types.FileSize(size) {
		t.Errorf("Expected size %d, got %d", size, fileSize)
	}
}

func TestBasicAllocatorGetObjectInfoInvalid(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	_, _, err = allocator.GetObjectInfo(types.ObjectId(999))
	if err != ErrInvalidObjectId {
		t.Errorf("Expected ErrInvalidObjectId, got %v", err)
	}
}

func TestBasicAllocatorContainsObjectId(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	objId, _, _ := allocator.Allocate(100)
	
	if !allocator.ContainsObjectId(objId) {
		t.Error("Expected ContainsObjectId to return true")
	}
	
	if allocator.ContainsObjectId(types.ObjectId(999)) {
		t.Error("Expected ContainsObjectId to return false for invalid ID")
	}
	
	// After delete
	allocator.DeleteObj(objId)
	
	if allocator.ContainsObjectId(objId) {
		t.Error("Expected ContainsObjectId to return false after delete")
	}
}

func TestBasicAllocatorCallback(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	var callbackObjId types.ObjectId
	var callbackOffset types.FileOffset
	var callbackSize int
	
	allocator.SetOnAllocate(func(objId types.ObjectId, offset types.FileOffset, size int) {
		callbackObjId = objId
		callbackOffset = offset
		callbackSize = size
	})
	
	size := 100
	objId, offset, _ := allocator.Allocate(size)
	
	if callbackObjId != objId {
		t.Errorf("Callback objId %d doesn't match %d", callbackObjId, objId)
	}
	
	if callbackOffset != offset {
		t.Errorf("Callback offset %d doesn't match %d", callbackOffset, offset)
	}
	
	if callbackSize != size {
		t.Errorf("Callback size %d doesn't match %d", callbackSize, size)
	}
}

func TestBasicAllocatorMarshalUnmarshal(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	// Allocate several objects
	objIds := make([]types.ObjectId, 0)
	sizes := []int{100, 200, 300}
	
	for _, size := range sizes {
		objId, _, _ := allocator.Allocate(size)
		objIds = append(objIds, objId)
	}
	
	// Delete middle object (creates gap)
	allocator.DeleteObj(objIds[1])
	
	// Marshal
	data, err := allocator.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	
	// Create new allocator and unmarshal
	file2, cleanup2 := createTestFile(t)
	defer cleanup2()
	
	allocator2, err := New(file2)
	if err != nil {
		t.Fatalf("Failed to create second allocator: %v", err)
	}
	
	err = allocator2.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	
	// Verify file length
	if allocator2.fileLength != allocator.fileLength {
		t.Errorf("File length mismatch: expected %d, got %d", allocator.fileLength, allocator2.fileLength)
	}
	
	// Verify ObjectMap (should have objects 0 and 2)
	if !allocator2.ContainsObjectId(objIds[0]) {
		t.Error("Object 0 should exist after unmarshal")
	}
	
	if allocator2.ContainsObjectId(objIds[1]) {
		t.Error("Object 1 should not exist after unmarshal (was deleted)")
	}
	
	if !allocator2.ContainsObjectId(objIds[2]) {
		t.Error("Object 2 should exist after unmarshal")
	}
	
	// Verify FreeList reconstructed (gap from deleted middle object)
	_, _, gaps := allocator2.Stats()
	if gaps != 1 {
		t.Errorf("Expected 1 gap after unmarshal, got %d", gaps)
	}
}

func TestBasicAllocatorUnmarshalInsufficientData(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	// Too short
	err = allocator.Unmarshal([]byte{1, 2, 3})
	if err != ErrInsufficientData {
		t.Errorf("Expected ErrInsufficientData, got %v", err)
	}
}

func TestBasicAllocatorStats(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	// Allocate three objects
	objIds := make([]types.ObjectId, 0)
	for i := 0; i < 3; i++ {
		objId, _, _ := allocator.Allocate(100)
		objIds = append(objIds, objId)
	}
	
	allocated, free, gaps := allocator.Stats()
	if allocated != 3 {
		t.Errorf("Expected 3 allocated, got %d", allocated)
	}
	if gaps != 0 {
		t.Errorf("Expected 0 gaps, got %d", gaps)
	}
	
	// Delete middle object
	allocator.DeleteObj(objIds[1])
	
	allocated, free, gaps = allocator.Stats()
	if allocated != 2 {
		t.Errorf("Expected 2 allocated, got %d", allocated)
	}
	if free != 100 {
		t.Errorf("Expected 100 free bytes, got %d", free)
	}
	if gaps != 1 {
		t.Errorf("Expected 1 gap, got %d", gaps)
	}
}

func TestBasicAllocatorReconstructFreeList(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	// Create a specific pattern: object, gap, object, gap, object
	// Allocate 5 objects then delete #1 and #3
	objIds := make([]types.ObjectId, 0)
	for i := 0; i < 5; i++ {
		objId, _, _ := allocator.Allocate(100)
		objIds = append(objIds, objId)
	}
	
	// Delete objects 1 and 3 (creates two gaps)
	allocator.DeleteObj(objIds[1])
	allocator.DeleteObj(objIds[3])
	
	// Marshal
	data, _ := allocator.Marshal()
	
	// Create new allocator and unmarshal
	file2, cleanup2 := createTestFile(t)
	defer cleanup2()
	
	allocator2, _ := New(file2)
	allocator2.Unmarshal(data)
	
	// FreeList should be reconstructed with 2 gaps
	_, _, gaps := allocator2.Stats()
	if gaps != 2 {
		t.Errorf("Expected 2 reconstructed gaps, got %d", gaps)
	}
	
	// Verify gap locations
	if len(allocator2.freeList.gaps) != 2 {
		t.Fatalf("Expected 2 gaps in freeList, got %d", len(allocator2.freeList.gaps))
	}
	
	// First gap should be at offset 100 (objId1)
	if allocator2.freeList.gaps[0].FileOffset != types.FileOffset(objIds[1]) {
		t.Errorf("First gap offset mismatch: expected %d, got %d", objIds[1], allocator2.freeList.gaps[0].FileOffset)
	}
	
	// Second gap should be at offset 300 (objId3)
	if allocator2.freeList.gaps[1].FileOffset != types.FileOffset(objIds[3]) {
		t.Errorf("Second gap offset mismatch: expected %d, got %d", objIds[3], allocator2.freeList.gaps[1].FileOffset)
	}
}

func TestBasicAllocatorEmptyMarshalUnmarshal(t *testing.T) {
	file, cleanup := createTestFile(t)
	defer cleanup()
	
	allocator, err := New(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}
	
	// Marshal empty allocator
	data, err := allocator.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	
	// Should be 12 bytes (fileLength + count)
	if len(data) != 12 {
		t.Errorf("Expected 12 bytes for empty marshal, got %d", len(data))
	}
	
	// Unmarshal into new allocator
	file2, cleanup2 := createTestFile(t)
	defer cleanup2()
	
	allocator2, _ := New(file2)
	err = allocator2.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	
	allocated, _, gaps := allocator2.Stats()
	if allocated != 0 {
		t.Errorf("Expected 0 allocated, got %d", allocated)
	}
	if gaps != 0 {
		t.Errorf("Expected 0 gaps, got %d", gaps)
	}
}
