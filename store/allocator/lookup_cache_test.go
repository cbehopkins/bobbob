package allocator

import (
	"testing"
)

func TestNewObjectIdLookupCache(t *testing.T) {
	cache := NewObjectIdLookupCache()
	
	if cache == nil {
		t.Fatal("NewObjectIdLookupCache returned nil")
	}
	
	if cache.Len() != 0 {
		t.Errorf("Expected empty cache, got %d ranges", cache.Len())
	}
}

func TestAddRange(t *testing.T) {
	cache := NewObjectIdLookupCache()
	
	// Add a range
	err := cache.AddRange(1, 256, FileOffset(0))
	if err != nil {
		t.Fatalf("AddRange failed: %v", err)
	}
	
	if cache.Len() != 1 {
		t.Errorf("Expected 1 range, got %d", cache.Len())
	}
	
	// Add another range
	err = cache.AddRange(1001, 256, FileOffset(256000))
	if err != nil {
		t.Fatalf("AddRange failed: %v", err)
	}
	
	if cache.Len() != 2 {
		t.Errorf("Expected 2 ranges, got %d", cache.Len())
	}
}

func TestAddRangeOverlap(t *testing.T) {
	cache := NewObjectIdLookupCache()
	
	// Add a range
	err := cache.AddRange(1, 256, FileOffset(0))
	if err != nil {
		t.Fatalf("AddRange failed: %v", err)
	}
	
	// Update it to have a proper end
	err = cache.UpdateRangeEnd(1, 1000)
	if err != nil {
		t.Fatalf("UpdateRangeEnd failed: %v", err)
	}
	
	// Try to add an overlapping range
	err = cache.AddRange(500, 256, FileOffset(128000))
	if err == nil {
		t.Error("Expected error for overlapping ranges, got nil")
	}
}

func TestLookup(t *testing.T) {
	cache := NewObjectIdLookupCache()
	
	// Add ranges
	err := cache.AddRange(1, 256, FileOffset(0))
	if err != nil {
		t.Fatalf("AddRange failed: %v", err)
	}
	err = cache.UpdateRangeEnd(1, 1000)
	if err != nil {
		t.Fatalf("UpdateRangeEnd failed: %v", err)
	}
	
	err = cache.AddRange(1000, 256, FileOffset(256000))
	if err != nil {
		t.Fatalf("AddRange failed: %v", err)
	}
	err = cache.UpdateRangeEnd(1000, 2000)
	if err != nil {
		t.Fatalf("UpdateRangeEnd failed: %v", err)
	}
	
	tests := []struct {
		objectId         int64
		shouldFind       bool
		expectedOffset   FileOffset
		expectedBlockSize int
	}{
		{1, true, FileOffset(0), 256},
		{500, true, FileOffset(499 * 256), 256},
		{999, true, FileOffset(998 * 256), 256},
		{1000, true, FileOffset(256000), 256},
		{1500, true, FileOffset(256000 + 500*256), 256},
		{1999, true, FileOffset(256000 + 999*256), 256},
		{0, false, 0, 0},      // Before first range
		{2000, false, 0, 0},   // After last range
		{10000, false, 0, 0},  // Far after last range
	}
	
	for _, tc := range tests {
		rangeInfo, err := cache.Lookup(tc.objectId)
		
		if tc.shouldFind {
			if err != nil {
				t.Errorf("Lookup(%d) failed: %v", tc.objectId, err)
				continue
			}
			if rangeInfo.BlockSize != tc.expectedBlockSize {
				t.Errorf("Lookup(%d): expected blockSize %d, got %d", tc.objectId, tc.expectedBlockSize, rangeInfo.BlockSize)
			}
			// Calculate expected offset
			slotIndex := tc.objectId - rangeInfo.StartObjectId
			expectedOffset := rangeInfo.StartingFileOffset + FileOffset(slotIndex*int64(rangeInfo.BlockSize))
			if expectedOffset != tc.expectedOffset {
				t.Errorf("Lookup(%d): expected offset %d, got %d", tc.objectId, tc.expectedOffset, expectedOffset)
			}
		} else {
			if err == nil {
				t.Errorf("Lookup(%d): expected error, got range %v", tc.objectId, rangeInfo)
			}
		}
	}
}

func TestLookupUnsorted(t *testing.T) {
	cache := NewObjectIdLookupCache()
	
	// Add ranges in non-sorted order
	err := cache.AddRange(3000, 256, FileOffset(768000))
	if err != nil {
		t.Fatalf("AddRange failed: %v", err)
	}
	err = cache.UpdateRangeEnd(3000, 4000)
	if err != nil {
		t.Fatalf("UpdateRangeEnd failed: %v", err)
	}
	
	err = cache.AddRange(1, 256, FileOffset(0))
	if err != nil {
		t.Fatalf("AddRange failed: %v", err)
	}
	err = cache.UpdateRangeEnd(1, 2000)
	if err != nil {
		t.Fatalf("UpdateRangeEnd failed: %v", err)
	}
	
	err = cache.AddRange(2000, 256, FileOffset(512000))
	if err != nil {
		t.Fatalf("AddRange failed: %v", err)
	}
	err = cache.UpdateRangeEnd(2000, 3000)
	if err != nil {
		t.Fatalf("UpdateRangeEnd failed: %v", err)
	}
	
	// Verify all lookups work correctly despite unsorted insertion
	rangeInfo, err := cache.Lookup(500)
	if err != nil || rangeInfo.StartObjectId != 1 {
		t.Errorf("Lookup(500) failed: startObjectId=%v, err=%v", rangeInfo.StartObjectId, err)
	}
	
	rangeInfo, err = cache.Lookup(2500)
	if err != nil || rangeInfo.StartObjectId != 2000 {
		t.Errorf("Lookup(2500) failed: startObjectId=%v, err=%v", rangeInfo.StartObjectId, err)
	}
	
	rangeInfo, err = cache.Lookup(3500)
	if err != nil || rangeInfo.StartObjectId != 3000 {
		t.Errorf("Lookup(3500) failed: startObjectId=%v, err=%v", rangeInfo.StartObjectId, err)
	}
}

func TestClear(t *testing.T) {
	cache := NewObjectIdLookupCache()
	
	// Add multiple ranges
	for i := 0; i < 5; i++ {
		err := cache.AddRange(int64(i*1000), 256, FileOffset(i*256000))
		if err != nil {
			t.Fatalf("AddRange failed: %v", err)
		}
	}
	
	if cache.Len() != 5 {
		t.Errorf("Expected 5 ranges before clear, got %d", cache.Len())
	}
	
	cache.Clear()
	
	if cache.Len() != 0 {
		t.Errorf("Expected 0 ranges after clear, got %d", cache.Len())
	}
	
	// Verify lookup fails after clear
	_, err := cache.Lookup(0)
	if err == nil {
		t.Error("Expected lookup error after clear, got nil")
	}
}

func TestUpdateRangeEnd(t *testing.T) {
	cache := NewObjectIdLookupCache()
	
	err := cache.AddRange(100, 256, FileOffset(25600))
	if err != nil {
		t.Fatalf("AddRange failed: %v", err)
	}
	
	// Initially, the range has zero size
	_, err = cache.Lookup(100)
	if err == nil {
		t.Error("Expected lookup error before UpdateRangeEnd")
	}
	
	// Update the range end
	err = cache.UpdateRangeEnd(100, 500)
	if err != nil {
		t.Fatalf("UpdateRangeEnd failed: %v", err)
	}
	
	// Now lookup should work
	rangeInfo, err := cache.Lookup(250)
	if err != nil {
		t.Fatalf("Lookup(250) failed: %v", err)
	}
	if rangeInfo.StartObjectId != 100 {
		t.Errorf("Expected startObjectId 100, got %d", rangeInfo.StartObjectId)
	}
	if rangeInfo.BlockSize != 256 {
		t.Errorf("Expected blockSize 256, got %d", rangeInfo.BlockSize)
	}
}

func TestUpdateRangeEndNotFound(t *testing.T) {
	cache := NewObjectIdLookupCache()
	
	err := cache.UpdateRangeEnd(999, 5000)
	if err == nil {
		t.Error("Expected error for non-existent range, got nil")
	}
}

func TestBoundaryConditions(t *testing.T) {
	cache := NewObjectIdLookupCache()
	
	// Add a range [1000, 2000)
	err := cache.AddRange(1000, 256, FileOffset(256000))
	if err != nil {
		t.Fatalf("AddRange failed: %v", err)
	}
	err = cache.UpdateRangeEnd(1000, 2000)
	if err != nil {
		t.Fatalf("UpdateRangeEnd failed: %v", err)
	}
	
	// Test boundaries
	_, err = cache.Lookup(999)
	if err == nil {
		t.Error("Lookup(999) should fail - outside range")
	}
	
	_, err = cache.Lookup(1000)
	if err != nil {
		t.Error("Lookup(1000) should succeed - at start of range")
	}
	
	_, err = cache.Lookup(1999)
	if err != nil {
		t.Error("Lookup(1999) should succeed - at end-1 of range")
	}
	
	_, err = cache.Lookup(2000)
	if err == nil {
		t.Error("Lookup(2000) should fail - at exclusive end of range")
	}
}
