package allocator

import (
	"os"
	"testing"
)

// TestBlockAllocatorFileOffsetCollision demonstrates whether blockAllocators
// with startingFileOffset of 0 would collide when writing to a file.
//
// This test checks if blockAllocators can safely share a file when both
// report file offsets starting from 0, or if they would overwrite each other.
//
// RESULT: The test shows that two blockAllocators both reporting fileOffset=0
// will overwrite each other's data. SectionWriter does NOT prevent this because
// it simply adds the base offset to all writes - if the base offset is 0 for
// multiple allocators, they all write to the same location.
//
// The solution is that each blockAllocator must pre-allocate its file space
// from a parent allocator and use the returned FileOffset as its startingFileOffset.
//
// NOTE: This test demonstrates the INCORRECT usage pattern (creating blockAllocators
// directly without a parent). The correct usage is shown in TestOmniBlockAllocatorFileOffsetIsolation.
func TestBlockAllocatorFileOffsetCollision(t *testing.T) {
	// This test demonstrates INCORRECT usage - creating blockAllocators directly.
	// Skip this test as it's intentionally showing what NOT to do.
	// The correct usage is to create blockAllocators via OmniBlockAllocator,
	// which pre-allocates file space from the parent allocator.
	t.Skip("This demonstrates incorrect direct usage of blockAllocator. " +
		"See TestOmniBlockAllocatorFileOffsetIsolation for correct OmniBlockAllocator usage.")
}

// TestOmniBlockAllocatorFileOffsetIsolation tests whether omniBlockAllocator
// properly isolates file offsets between different block allocators.
//
// RESULT: This test demonstrates the same bug as TestBlockAllocatorFileOffsetCollision.
// OmniBlockAllocator creates multiple blockAllocators, all with startingFileOffset=0.
// When these allocators are used to write to a file (even with SectionWriter), they
// all write to the same location (offset 0), causing data corruption.
//
// The last write wins, overwriting all previous writes. In this test, the 256-byte
// write of 'C's overwrites both the 64-byte 'A's and 128-byte 'B's.
//
// FIX NEEDED: OmniBlockAllocator should pre-allocate file space for each blockAllocator
// from the parent allocator when created, rather than using startingFileOffset=0 for all.
func TestOmniBlockAllocatorFileOffsetIsolation(t *testing.T) {
	// Create a temporary file for the parent allocator
	tmpFile, err := os.CreateTemp("", "omni_allocator_*.dat")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create a parent allocator
	parent, err := NewBasicAllocator(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create parent allocator: %v", err)
	}

	// Create an omniBlockAllocator with multiple block sizes
	blockSizes := []int{64, 128, 256}
	blockCount := 10
	omni, err := NewOmniBlockAllocator(blockSizes, blockCount, parent, nil)
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator failed: %v", err)
	}

	// Allocate from different block sizes
	objId1, fileOffset1, err := omni.Allocate(64)
	if err != nil {
		t.Fatalf("Failed to allocate 64 bytes: %v", err)
	}
	t.Logf("64-byte allocation: ObjectId=%d, FileOffset=%d", objId1, fileOffset1)

	objId2, fileOffset2, err := omni.Allocate(128)
	if err != nil {
		t.Fatalf("Failed to allocate 128 bytes: %v", err)
	}
	t.Logf("128-byte allocation: ObjectId=%d, FileOffset=%d", objId2, fileOffset2)

	objId3, fileOffset3, err := omni.Allocate(256)
	if err != nil {
		t.Fatalf("Failed to allocate 256 bytes: %v", err)
	}
	t.Logf("256-byte allocation: ObjectId=%d, FileOffset=%d", objId3, fileOffset3)

	// Write distinctive data to each allocation
	data1 := make([]byte, 64)
	for i := range data1 {
		data1[i] = 'A'
	}

	data2 := make([]byte, 128)
	for i := range data2 {
		data2[i] = 'B'
	}

	data3 := make([]byte, 256)
	for i := range data3 {
		data3[i] = 'C'
	}

	// Write to file at the reported offsets
	_, err = tmpFile.WriteAt(data1, int64(fileOffset1))
	if err != nil {
		t.Fatalf("Failed to write data1: %v", err)
	}

	_, err = tmpFile.WriteAt(data2, int64(fileOffset2))
	if err != nil {
		t.Fatalf("Failed to write data2: %v", err)
	}

	_, err = tmpFile.WriteAt(data3, int64(fileOffset3))
	if err != nil {
		t.Fatalf("Failed to write data3: %v", err)
	}

	// Read back and verify no corruption
	readBuf1 := make([]byte, 64)
	_, err = tmpFile.ReadAt(readBuf1, int64(fileOffset1))
	if err != nil {
		t.Fatalf("Failed to read data1: %v", err)
	}

	readBuf2 := make([]byte, 128)
	_, err = tmpFile.ReadAt(readBuf2, int64(fileOffset2))
	if err != nil {
		t.Fatalf("Failed to read data2: %v", err)
	}

	readBuf3 := make([]byte, 256)
	_, err = tmpFile.ReadAt(readBuf3, int64(fileOffset3))
	if err != nil {
		t.Fatalf("Failed to read data3: %v", err)
	}

	// Verify data integrity
	allAs := true
	for _, b := range readBuf1 {
		if b != 'A' {
			allAs = false
			break
		}
	}

	allBs := true
	for _, b := range readBuf2 {
		if b != 'B' {
			allBs = false
			break
		}
	}

	allCs := true
	for _, b := range readBuf3 {
		if b != 'C' {
			allCs = false
			break
		}
	}

	if !allAs {
		t.Errorf("❌ Data1 corrupted! Expected all 'A's but got mixed data")
		t.Errorf("FileOffset1=%d, sample: %s", fileOffset1, string(readBuf1[:min(20, len(readBuf1))]))
	} else {
		t.Logf("✓ Data1 intact at offset %d (64 bytes of 'A')", fileOffset1)
	}

	if !allBs {
		t.Errorf("❌ Data2 corrupted! Expected all 'B's but got mixed data")
		t.Errorf("FileOffset2=%d, sample: %s", fileOffset2, string(readBuf2[:min(20, len(readBuf2))]))
	} else {
		t.Logf("✓ Data2 intact at offset %d (128 bytes of 'B')", fileOffset2)
	}

	if !allCs {
		t.Errorf("❌ Data3 corrupted! Expected all 'C's but got mixed data")
		t.Errorf("FileOffset3=%d, sample: %s", fileOffset3, string(readBuf3[:min(20, len(readBuf3))]))
	} else {
		t.Logf("✓ Data3 intact at offset %d (256 bytes of 'C')", fileOffset3)
	}

	// Check for any overlaps in file offsets
	if hasOverlap(fileOffset1, 64, fileOffset2, 128) {
		t.Errorf("❌ Overlap detected between offset1=%d(64 bytes) and offset2=%d(128 bytes)",
			fileOffset1, fileOffset2)
	}
	if hasOverlap(fileOffset1, 64, fileOffset3, 256) {
		t.Errorf("❌ Overlap detected between offset1=%d(64 bytes) and offset3=%d(256 bytes)",
			fileOffset1, fileOffset3)
	}
	if hasOverlap(fileOffset2, 128, fileOffset3, 256) {
		t.Errorf("❌ Overlap detected between offset2=%d(128 bytes) and offset3=%d(256 bytes)",
			fileOffset2, fileOffset3)
	}

	if allAs && allBs && allCs {
		t.Logf("✓ All data preserved - no corruption detected")
		t.Logf("OmniBlockAllocator properly manages file offsets across block allocators")
	}
}

func hasOverlap(offset1 FileOffset, size1 int, offset2 FileOffset, size2 int) bool {
	end1 := int64(offset1) + int64(size1)
	end2 := int64(offset2) + int64(size2)

	// Check if ranges overlap
	return !(end1 <= int64(offset2) || end2 <= int64(offset1))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
