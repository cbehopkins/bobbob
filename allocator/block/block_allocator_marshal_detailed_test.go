package block

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cbehopkins/bobbob/allocator/types"
)

// TestBlockAllocatorMarshalUnmarshalDetailed verifies complete Marshal/Unmarshal cycle
// with allocated blocks and bitmap verification
func TestBlockAllocatorMarshalUnmarshalDetailed(t *testing.T) {
	const blockSize = 64
	const blockCount = 1024
	const startingFileOffset = types.FileOffset(0)
	const startingObjId = types.ObjectId(0)

	// Create first allocator and allocate some blocks
	alloc1 := New(blockSize, blockCount, startingFileOffset, startingObjId, nil)

	// Allocate 10 blocks (slots 0-9)
	allocatedIds := make([]types.ObjectId, 10)
	for i := 0; i < 10; i++ {
		objId, offset, err := alloc1.Allocate(blockSize)
		if err != nil {
			t.Fatalf("Allocation %d failed: %v", i, err)
		}
		allocatedIds[i] = objId
		expectedObjId := startingObjId + types.ObjectId(i)
		expectedOffset := startingFileOffset + types.FileOffset(i*blockSize)

		if objId != expectedObjId {
			t.Errorf("Allocation %d: expected ObjectId %d, got %d", i, expectedObjId, objId)
		}
		if offset != expectedOffset {
			t.Errorf("Allocation %d: expected offset %d, got %d", i, expectedOffset, offset)
		}

		t.Logf("Allocated: ObjectId=%d, Offset=%d, Slot=%d", objId, offset, i)
	}

	// Verify all 10 are accessible
	for i := 0; i < 10; i++ {
		offset, size, err := alloc1.GetObjectInfo(allocatedIds[i])
		if err != nil {
			t.Errorf("GetObjectInfo(%d) failed: %v", allocatedIds[i], err)
		}
		if size != types.FileSize(blockSize) {
			t.Errorf("GetObjectInfo(%d): expected size %d, got %d", allocatedIds[i], blockSize, size)
		}
		t.Logf("Verified: ObjectId=%d exists at offset=%d", allocatedIds[i], offset)
	}

	// Marshal the allocator
	marshaledData, err := alloc1.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	t.Logf("Marshaled to %d bytes", len(marshaledData))

	// Inspect the marshaled data
	if len(marshaledData) < 8 {
		t.Fatalf("Marshaled data too short: %d bytes", len(marshaledData))
	}

	marshaledOffset := types.FileOffset(binary.LittleEndian.Uint64(marshaledData[0:8]))
	t.Logf("Marshaled data starts with FileOffset: %d", marshaledOffset)

	// Inspect bitmap bytes
	bitmapBytes := (blockCount + 7) / 8
	if len(marshaledData) < 8+bitmapBytes {
		t.Fatalf("Marshaled data doesn't contain full bitmap: got %d bytes, need %d", len(marshaledData), 8+bitmapBytes)
	}

	bitmap := marshaledData[8 : 8+bitmapBytes]
	t.Logf("First bitmap byte: 0x%02X (should be 0xFF for bits 0-7)", bitmap[0])
	t.Logf("Second bitmap byte: 0x%02X (should be 0x03 for bits 8-9)", bitmap[1])

	if bitmap[0] != 0xFF {
		t.Errorf("Bitmap byte 0: expected 0xFF (bits 0-7), got 0x%02X", bitmap[0])
	}
	if bitmap[1] != 0x03 {
		t.Errorf("Bitmap byte 1: expected 0x03 (bits 8-9), got 0x%02X", bitmap[1])
	}

	// Create second allocator and unmarshal
	alloc2 := New(blockSize, blockCount, 0, 0, nil)

	// Before unmarshal, verify it's empty
	_, _, err = alloc2.GetObjectInfo(0)
	if err != ErrSlotNotFound {
		t.Errorf("Before unmarshal, allocator should be empty but GetObjectInfo(0) returned: %v", err)
	}

	// Unmarshal
	err = alloc2.Unmarshal(marshaledData)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify offset was restored
	if alloc2.startingFileOffset != startingFileOffset {
		t.Errorf("After unmarshal: expected startingFileOffset %d, got %d", startingFileOffset, alloc2.startingFileOffset)
	}

	// Verify bitmap was restored
	for i := 0; i < 10; i++ {
		if !alloc2.allocatedBitmap[i] {
			t.Errorf("Bitmap slot %d should be allocated but is free", i)
		}
	}

	// Verify slots 10+ are free
	for i := 10; i < 20; i++ {
		if alloc2.allocatedBitmap[i] {
			t.Errorf("Bitmap slot %d should be free but is allocated", i)
		}
	}

	t.Logf("Bitmap restored correctly: slots 0-9 allocated, slots 10+ free")

	// Verify all 10 are still accessible after unmarshal
	for i := 0; i < 10; i++ {
		offset, size, err := alloc2.GetObjectInfo(allocatedIds[i])
		if err != nil {
			t.Fatalf("After unmarshal, GetObjectInfo(%d) failed: %v", allocatedIds[i], err)
		}
		if size != types.FileSize(blockSize) {
			t.Errorf("GetObjectInfo(%d): expected size %d, got %d", allocatedIds[i], blockSize, size)
		}
		expectedOffset := startingFileOffset + types.FileOffset(i*blockSize)
		if offset != expectedOffset {
			t.Errorf("GetObjectInfo(%d): expected offset %d, got %d", allocatedIds[i], expectedOffset, offset)
		}

		t.Logf("After unmarshal: ObjectId=%d accessible at offset=%d", allocatedIds[i], offset)
	}

	// Verify free slots are still free
	for i := 10; i < 20; i++ {
		objId := startingObjId + types.ObjectId(i)
		_, _, err := alloc2.GetObjectInfo(objId)
		if err != ErrSlotNotFound {
			t.Errorf("GetObjectInfo(%d) should fail for free slot but got: %v", objId, err)
		}
	}

	t.Log("✓ BlockAllocator Marshal/Unmarshal cycle works correctly with bitmap preservation")
}

// TestBlockAllocatorMarshalUnmarshalRoundTrip tests multiple round trips
func TestBlockAllocatorMarshalUnmarshalRoundTrip(t *testing.T) {
	const blockSize = 128
	const blockCount = 512
	const startingFileOffset = types.FileOffset(4096)
	const startingObjId = types.ObjectId(100)

	// Create allocator with specific starting values
	alloc := New(blockSize, blockCount, startingFileOffset, startingObjId, nil)

	// Allocate in pattern: 0, 2, 4, 6, 8 (leave odd slots free)
	for i := 0; i < 5; i++ {
		_, _, err := alloc.Allocate(blockSize)
		if err != nil {
			t.Fatalf("Allocation %d failed: %v", i*2, err)
		}
		// Allocate and then skip (by allocating again, we move the cursor)
		if i < 4 {
			_, _, _ = alloc.Allocate(blockSize) // This will skip to next even slot
		}
	}

	t.Logf("After allocations: freeCount=%d, allAllocated=%v", alloc.freeCount, alloc.allAllocated)

	// Marshal
	data1, err := alloc.Marshal()
	if err != nil {
		t.Fatalf("First marshal failed: %v", err)
	}

	// Unmarshal into new allocator
	alloc2 := New(blockSize, blockCount, 0, 0, nil)
	err = alloc2.Unmarshal(data1)
	if err != nil {
		t.Fatalf("First unmarshal failed: %v", err)
	}

	// Marshal again
	data2, err := alloc2.Marshal()
	if err != nil {
		t.Fatalf("Second marshal failed: %v", err)
	}

	// Verify marshaled data is identical
	if !bytes.Equal(data1, data2) {
		t.Errorf("Marshaled data differs after round trip")
		t.Logf("First marshal: %d bytes", len(data1))
		t.Logf("Second marshal: %d bytes", len(data2))
		// Show first difference
		for i := 0; i < len(data1) && i < len(data2); i++ {
			if data1[i] != data2[i] {
				t.Logf("First difference at byte %d: 0x%02X vs 0x%02X", i, data1[i], data2[i])
				break
			}
		}
	}

	// Unmarshal into third allocator
	alloc3 := New(blockSize, blockCount, 0, 0, nil)
	err = alloc3.Unmarshal(data2)
	if err != nil {
		t.Fatalf("Second unmarshal failed: %v", err)
	}

	// Verify all three allocators have same state
	if alloc2.freeCount != alloc3.freeCount {
		t.Errorf("freeCount differs: %d vs %d", alloc2.freeCount, alloc3.freeCount)
	}
	if alloc2.allAllocated != alloc3.allAllocated {
		t.Errorf("allAllocated differs: %v vs %v", alloc2.allAllocated, alloc3.allAllocated)
	}
	// Compare bitmaps element by element
	for i := range alloc2.allocatedBitmap {
		if alloc2.allocatedBitmap[i] != alloc3.allocatedBitmap[i] {
			t.Errorf("Bitmap slot %d differs: %v vs %v", i, alloc2.allocatedBitmap[i], alloc3.allocatedBitmap[i])
		}
	}

	t.Log("✓ Multiple round-trip marshaling works correctly")
}
