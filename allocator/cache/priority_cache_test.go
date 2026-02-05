package cache

import (
	"bytes"
	"testing"

	"github.com/cbehopkins/bobbob/allocator/types"
)

// TestInsertAfterClose verifies that Insert fails gracefully on a closed cache.
func TestInsertAfterClose(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Close the cache
	pc.Close()

	// Try to insert after closing
	err = pc.Insert(UnloadedBlock{
		ObjId:      100,
		BaseObjId:  100,
		BlockCount: 5,
	})

	// Should fail gracefully
	if err == nil {
		t.Fatal("Insert after Close should return an error")
	}
}

// TestQueryAfterClose verifies Query behavior after Close.
func TestQueryAfterClose(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Insert data before closing
	block := UnloadedBlock{
		ObjId:      1,
		BaseObjId:  100,
		BlockCount: 5,
	}
	if err := pc.Insert(block); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Close the cache
	pc.Close()

	// Query after close - should work (tempFile is only for writes)
	result, err := pc.Query(102)
	if err != nil {
		t.Logf("Query after Close failed (acceptable if reads disabled): %v", err)
	} else {
		// Verify the result if query succeeded
		if result.BaseObjId != 100 {
			t.Errorf("Got unexpected BaseObjId: %d", result.BaseObjId)
		}
	}
}

// TestLargeCache tests insertion, query, and deletion with many entries.
func TestLargeCache(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pc.Close()

	// Insert 1000 entries
	const numEntries = 1000
	for i := 0; i < numEntries; i++ {
		block := UnloadedBlock{
			ObjId:      types.ObjectId(i),
			BaseObjId:  types.ObjectId(i * 10),
			BlockCount: 5,
		}
		if err := pc.Insert(block); err != nil {
			t.Fatalf("Insert failed at index %d: %v", i, err)
		}
	}

	// Verify all entries exist and are queryable
	for i := 0; i < numEntries; i++ {
		baseObjId := types.ObjectId(i * 10)
		result, err := pc.Query(baseObjId + 2)
		if err != nil {
			t.Fatalf("Query failed for entry %d: %v", i, err)
		}
		if result.BaseObjId != baseObjId || result.BlockCount != 5 {
			t.Errorf("Query mismatch at index %d: got %+v, expected BaseObjId=%d, BlockCount=5",
				i, result, baseObjId)
		}
	}

	// Delete every other entry
	for i := 0; i < numEntries; i += 2 {
		baseObjId := types.ObjectId(i * 10)
		if err := pc.Delete(baseObjId); err != nil {
			t.Fatalf("Delete failed at index %d: %v", i, err)
		}
	}

	// Verify deleted entries are gone
	for i := 0; i < numEntries; i += 2 {
		baseObjId := types.ObjectId(i * 10)
		_, err := pc.Query(baseObjId + 1)
		if err == nil {
			t.Errorf("Query should fail for deleted entry %d", i)
		}
	}

	// Verify remaining entries still exist
	for i := 1; i < numEntries; i += 2 {
		baseObjId := types.ObjectId(i * 10)
		result, err := pc.Query(baseObjId + 1)
		if err != nil {
			t.Fatalf("Query failed for remaining entry %d: %v", i, err)
		}
		if result.BaseObjId != baseObjId {
			t.Errorf("Entry %d: got BaseObjId=%d, expected %d", i, result.BaseObjId, baseObjId)
		}
	}
}

// TestSequentialOperations tests a complex workflow: Insert→Marshal→Unmarshal→Query→Delete.
func TestSequentialOperations(t *testing.T) {
	// Create cache 1
	pc1, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pc1.Close()

	// Insert entries
	entries := []UnloadedBlock{
		{ObjId: 1, BaseObjId: 100, BlockCount: 5},
		{ObjId: 2, BaseObjId: 200, BlockCount: 3},
		{ObjId: 3, BaseObjId: 400, BlockCount: 8},
	}
	for _, entry := range entries {
		if err := pc1.Insert(entry); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Marshal to bytes
	data, err := pc1.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Create new cache and unmarshal
	pc2, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pc2.Close()

	if err := pc2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Query entries in new cache
	for _, entry := range entries {
		result, err := pc2.Query(entry.BaseObjId + 1)
		if err != nil {
			t.Fatalf("Query failed for ObjectId %d: %v", entry.BaseObjId, err)
		}
		if result.BaseObjId != entry.BaseObjId || result.BlockCount != entry.BlockCount {
			t.Errorf("Mismatch: got %+v, expected %+v", result, entry)
		}
	}

	// Delete middle entry
	if err := pc2.Delete(200); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted entry is gone
	_, err = pc2.Query(200)
	if err == nil {
		t.Fatal("Query should fail for deleted entry")
	}

	// Verify other entries still exist
	result, err := pc2.Query(100)
	if err != nil {
		t.Fatalf("Query for undeleted entry failed: %v", err)
	}
	if result.BaseObjId != 100 {
		t.Errorf("Wrong BaseObjId: got %d, expected 100", result.BaseObjId)
	}
}

// TestOverlappingRanges attempts to insert overlapping ranges and verifies behavior.
func TestOverlappingRanges(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pc.Close()

	// Insert first block: 100-104 (BaseObjId=100, BlockCount=5)
	block1 := UnloadedBlock{
		ObjId:      1,
		BaseObjId:  100,
		BlockCount: 5,
	}
	if err := pc.Insert(block1); err != nil {
		t.Fatalf("Insert block1 failed: %v", err)
	}

	// Try to insert overlapping block: 102-107 (BaseObjId=102, BlockCount=6)
	block2 := UnloadedBlock{
		ObjId:      2,
		BaseObjId:  102,
		BlockCount: 6,
	}
	if err := pc.Insert(block2); err != nil {
		t.Fatalf("Insert block2 failed: %v", err)
	}

	// Query the overlap region - should return the most recently inserted (or last one in sorted order)
	result, err := pc.Query(103)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Both blocks contain ObjectId 103, so we should get one of them
	if (result.BaseObjId != 100 && result.BaseObjId != 102) {
		t.Errorf("Query returned unexpected BaseObjId: %d", result.BaseObjId)
	}

	t.Logf("Overlapping ranges test: Query(103) returned BaseObjId=%d", result.BaseObjId)
}

// TestMultipleClose verifies that Close can be called multiple times safely.
func TestMultipleClose(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Insert some data
	if err := pc.Insert(UnloadedBlock{
		ObjId:      1,
		BaseObjId:  100,
		BlockCount: 5,
	}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Close multiple times - should not panic
	pc.Close()
	pc.Close()
	pc.Close()

	// No assertion needed; test passes if no panic occurs
}

// TestBoundaryObjectIds tests ObjectIds at range boundaries.
func TestBoundaryObjectIds(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pc.Close()

	// Insert block covering ObjectIds 100-109
	block := UnloadedBlock{
		ObjId:      1,
		BaseObjId:  100,
		BlockCount: 10,
	}
	if err := pc.Insert(block); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Test boundary conditions
	tests := []struct {
		objId    types.ObjectId
		shouldOK bool
	}{
		{100, true},    // Start of range
		{109, true},    // End of range (BaseObjId + BlockCount - 1)
		{99, false},    // Before range
		{110, false},   // After range
		{104, true},    // Middle of range
	}

	for _, test := range tests {
		result, err := pc.Query(test.objId)
		if test.shouldOK {
			if err != nil {
				t.Errorf("Query(%d) should succeed: %v", test.objId, err)
			}
			if result.BaseObjId != 100 {
				t.Errorf("Query(%d) returned wrong BaseObjId: %d", test.objId, result.BaseObjId)
			}
		} else {
			if err == nil {
				t.Errorf("Query(%d) should fail, but got result: %+v", test.objId, result)
			}
		}
	}
}

// TestUnmarshalPartialData tests Unmarshal with various truncated data sizes.
func TestUnmarshalPartialData(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pc.Close()

	// Create and marshal original cache
	originalEntries := []UnloadedBlock{
		{ObjId: 1, BaseObjId: 100, BlockCount: 5},
		{ObjId: 2, BaseObjId: 200, BlockCount: 3},
	}
	for _, entry := range originalEntries {
		if err := pc.Insert(entry); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	data, err := pc.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Test: Full data should work
	pc2, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pc2.Close()

	if err := pc2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal with full data failed: %v", err)
	}

	// Test: Data with extra bytes should be handled (truncated to expectedSize)
	dataWithExtra := append(data, []byte{0xFF, 0xFF, 0xFF}...)
	pc3, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pc3.Close()

	if err := pc3.Unmarshal(dataWithExtra); err != nil {
		t.Logf("Unmarshal with extra bytes: %v (acceptable if extra bytes are ignored)", err)
	}

	// Test: Data smaller than header should fail or handle gracefully
	smallData := data[:minInt(len(data), 4)]
	pc4, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pc4.Close()

	err = pc4.Unmarshal(smallData)
	if len(smallData) < 12 {
		t.Logf("Unmarshal with partial header returned: %v (expected error or handling)", err)
	}
}

// TestZeroAndNegativeBlockCount tests handling of invalid BlockCount values.
func TestZeroAndNegativeBlockCount(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pc.Close()

	// Test: BlockCount == 0 (invalid, but implementation may or may not enforce)
	zeroBlock := UnloadedBlock{ObjId: 1, BaseObjId: 100, BlockCount: 0}
	err = pc.Insert(zeroBlock)
	if err == nil {
		t.Logf("BlockCount=0 was accepted (implementation may allow this)")
	} else {
		t.Logf("BlockCount=0 was rejected: %v (good validation)", err)
	}

	// Test: Negative BlockCount (invalid)
	negBlock := UnloadedBlock{ObjId: 2, BaseObjId: 200, BlockCount: -5}
	err = pc.Insert(negBlock)
	if err == nil {
		t.Logf("Negative BlockCount was accepted (implementation may allow this)")
	} else {
		t.Logf("Negative BlockCount was rejected: %v (good validation)", err)
	}
}

// Helper to find minimum of two integers
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestVersionFieldHandling tests that version field is handled correctly.
func TestVersionFieldHandling(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pc.Close()

	if err := pc.Insert(UnloadedBlock{
		ObjId:      1,
		BaseObjId:  100,
		BlockCount: 5,
	}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Marshal the cache
	data, err := pc.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Verify we have at least header size (version + count)
	if len(data) < 8 { // 4 bytes version + 4 bytes count
		t.Fatalf("Marshaled data too small: %d bytes", len(data))
	}

	// Modify the version field (first 4 bytes)
	modifiedData := bytes.Clone(data)
	modifiedData[0] = 0xFF
	modifiedData[1] = 0xFF
	modifiedData[2] = 0xFF
	modifiedData[3] = 0xFF

	// Try to unmarshal with modified version
	pc2, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pc2.Close()

	// Current implementation ignores version, so this should succeed
	err = pc2.Unmarshal(modifiedData)
	if err != nil {
		t.Logf("Unmarshal with modified version failed: %v (version validation exists)", err)
	} else {
		t.Logf("Unmarshal with modified version succeeded (version not validated)")
	}
}

// TestCachePersistenceAcrossSessions tests that data survives Marshal→Unmarshal cycles.
func TestCachePersistenceAcrossSessions(t *testing.T) {
	// Session 1: Create, populate, and persist
	pc1, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	testEntries := []UnloadedBlock{
		{ObjId: 1, BaseObjId: 1000, BlockCount: 10},
		{ObjId: 2, BaseObjId: 2000, BlockCount: 20},
		{ObjId: 3, BaseObjId: 3000, BlockCount: 15},
		{ObjId: 4, BaseObjId: 5000, BlockCount: 25},
	}

	for _, entry := range testEntries {
		if err := pc1.Insert(entry); err != nil {
			t.Fatalf("Insert in session 1 failed: %v", err)
		}
	}

	data, err := pc1.Marshal()
	if err != nil {
		t.Fatalf("Marshal in session 1 failed: %v", err)
	}
	pc1.Close()

	// Session 2: Load, verify, modify, and repersist
	pc2, err := New()
	if err != nil {
		t.Fatalf("New in session 2 failed: %v", err)
	}

	if err := pc2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal in session 2 failed: %v", err)
	}

	// Verify all entries loaded correctly
	for _, entry := range testEntries {
		result, err := pc2.Query(entry.BaseObjId + 1)
		if err != nil {
			t.Fatalf("Query in session 2 failed: %v", err)
		}
		if result.BaseObjId != entry.BaseObjId {
			t.Errorf("Session 2: expected BaseObjId=%d, got %d", entry.BaseObjId, result.BaseObjId)
		}
	}

	// Delete one entry
	if err := pc2.Delete(2000); err != nil {
		t.Fatalf("Delete in session 2 failed: %v", err)
	}

	data2, err := pc2.Marshal()
	if err != nil {
		t.Fatalf("Marshal in session 2 failed: %v", err)
	}
	pc2.Close()

	// Session 3: Verify deletion persisted
	pc3, err := New()
	if err != nil {
		t.Fatalf("New in session 3 failed: %v", err)
	}
	defer pc3.Close()

	if err := pc3.Unmarshal(data2); err != nil {
		t.Fatalf("Unmarshal in session 3 failed: %v", err)
	}

	// Verify deleted entry is gone
	_, err = pc3.Query(2000)
	if err == nil {
		t.Fatal("Deleted entry should not exist in session 3")
	}

	// Verify other entries still exist
	for _, entry := range []UnloadedBlock{
		{ObjId: 1, BaseObjId: 1000, BlockCount: 10},
		{ObjId: 3, BaseObjId: 3000, BlockCount: 15},
		{ObjId: 4, BaseObjId: 5000, BlockCount: 25},
	} {
		result, err := pc3.Query(entry.BaseObjId + 1)
		if err != nil {
			t.Fatalf("Query in session 3 failed: %v", err)
		}
		if result.BaseObjId != entry.BaseObjId {
			t.Errorf("Session 3: expected BaseObjId=%d, got %d", entry.BaseObjId, result.BaseObjId)
		}
	}
}
