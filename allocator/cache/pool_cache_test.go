package cache

import (
	"os"
	"testing"

	"github.com/cbehopkins/bobbob/allocator/types"
)

func TestPoolCacheNew(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	if pc.tempFile == nil {
		t.Error("Expected temp file to be created")
	}

	if len(pc.entries) != 0 {
		t.Errorf("Expected empty entries, got %d", len(pc.entries))
	}
}

func TestPoolCacheInsert(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	entry := UnloadedBlock{
		ObjId:          types.ObjectId(100),
		BaseObjId:      types.ObjectId(1000),
		BaseFileOffset: types.FileOffset(5000),
		BlockSize:      types.FileSize(1024),
		BlockCount:     1024,
		Available:      true,
	}

	err = pc.Insert(entry)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if len(pc.entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(pc.entries))
	}

	if pc.entries[0].ObjId != entry.ObjId {
		t.Errorf("Expected ObjId %d, got %d", entry.ObjId, pc.entries[0].ObjId)
	}
}

func TestPoolCacheInsertMaintainsSorted(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	// Insert in non-sorted order
	entries := []UnloadedBlock{
		{ObjId: 100, BaseObjId: 3000, BlockCount: 1024},
		{ObjId: 101, BaseObjId: 1000, BlockCount: 1024},
		{ObjId: 102, BaseObjId: 2000, BlockCount: 1024},
	}

	for _, e := range entries {
		pc.Insert(e)
	}

	// Check sorted by BaseObjId
	if pc.entries[0].BaseObjId != 1000 {
		t.Errorf("Expected first BaseObjId 1000, got %d", pc.entries[0].BaseObjId)
	}
	if pc.entries[1].BaseObjId != 2000 {
		t.Errorf("Expected second BaseObjId 2000, got %d", pc.entries[1].BaseObjId)
	}
	if pc.entries[2].BaseObjId != 3000 {
		t.Errorf("Expected third BaseObjId 3000, got %d", pc.entries[2].BaseObjId)
	}
}

func TestPoolCacheInsertDuplicate(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	entry := UnloadedBlock{
		ObjId:     types.ObjectId(100),
		BaseObjId: types.ObjectId(1000),
		BlockCount: 1024,
	}

	pc.Insert(entry)

	// Try to insert duplicate
	err = pc.Insert(entry)
	if err != ErrInvalidEntry {
		t.Errorf("Expected ErrInvalidEntry, got %v", err)
	}
}

func TestPoolCacheQuery(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	entry := UnloadedBlock{
		ObjId:     types.ObjectId(100),
		BaseObjId: types.ObjectId(1000),
		BlockSize: types.FileSize(1024),
		BlockCount: 1024,
		Available: true,
	}

	pc.Insert(entry)

	// Query for ObjectId within range
	result, err := pc.Query(types.ObjectId(1000))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.ObjId != entry.ObjId {
		t.Errorf("Expected ObjId %d, got %d", entry.ObjId, result.ObjId)
	}
}

func TestPoolCacheQueryNotFound(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	entry := UnloadedBlock{
		ObjId:     types.ObjectId(100),
		BaseObjId: types.ObjectId(1000),
		BlockCount: 1024,
	}

	pc.Insert(entry)

	// Query for ObjectId not in cache
	_, err = pc.Query(types.ObjectId(500))
	if err != ErrObjectIdNotFound {
		t.Errorf("Expected ErrObjectIdNotFound, got %v", err)
	}
}

func TestPoolCacheQueryMultipleEntries(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	// Insert multiple entries
	entries := []UnloadedBlock{
		{ObjId: 100, BaseObjId: 1000, BlockCount: 1024},
		{ObjId: 101, BaseObjId: 2000, BlockCount: 1024},
		{ObjId: 102, BaseObjId: 3000, BlockCount: 1024},
	}

	for _, e := range entries {
		pc.Insert(e)
	}

	// Query for each range
	tests := []struct {
		queryId  types.ObjectId
		expected types.ObjectId
	}{
		{1000, 100},
		{2000, 101},
		{3000, 102},
	}

	for _, test := range tests {
		result, err := pc.Query(test.queryId)
		if err != nil {
			t.Fatalf("Query for %d failed: %v", test.queryId, err)
		}
		if result.ObjId != test.expected {
			t.Errorf("Query for %d: expected ObjId %d, got %d", test.queryId, test.expected, result.ObjId)
		}
	}
}

func TestPoolCacheDelete(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	entry := UnloadedBlock{
		ObjId:     types.ObjectId(100),
		BaseObjId: types.ObjectId(1000),
		BlockCount: 1024,
	}

	pc.Insert(entry)

	if len(pc.entries) != 1 {
		t.Fatalf("Expected 1 entry before delete, got %d", len(pc.entries))
	}

	err = pc.Delete(types.ObjectId(1000))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if len(pc.entries) != 0 {
		t.Errorf("Expected 0 entries after delete, got %d", len(pc.entries))
	}

	// Query should now fail
	_, err = pc.Query(types.ObjectId(1000))
	if err != ErrObjectIdNotFound {
		t.Errorf("Expected ErrObjectIdNotFound after delete, got %v", err)
	}
}

func TestPoolCacheDeleteNotFound(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	err = pc.Delete(types.ObjectId(9999))
	if err != ErrObjectIdNotFound {
		t.Errorf("Expected ErrObjectIdNotFound, got %v", err)
	}
}

func TestPoolCacheSizeInBytes(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	// Empty cache
	if pc.SizeInBytes() != headerSize {
		t.Errorf("Expected empty cache size %d, got %d", headerSize, pc.SizeInBytes())
	}

	// Add entries
	for i := 0; i < 3; i++ {
		entry := UnloadedBlock{
			ObjId:     types.ObjectId(100 + i),
			BaseObjId: types.ObjectId(1000 + i*1000),
			BlockCount: 1024,
		}
		pc.Insert(entry)
	}

	expectedSize := headerSize + (3 * entrySize)
	if pc.SizeInBytes() != expectedSize {
		t.Errorf("Expected cache size %d, got %d", expectedSize, pc.SizeInBytes())
	}
}

func TestPoolCacheMarshalUnmarshal(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	// Add entries
	entries := []UnloadedBlock{
		{ObjId: 100, BaseObjId: 1000, BaseFileOffset: 5000, BlockSize: 1024, BlockCount: 1024, Available: true},
		{ObjId: 101, BaseObjId: 2000, BaseFileOffset: 6000, BlockSize: 2048, BlockCount: 2048, Available: false},
		{ObjId: 102, BaseObjId: 3000, BaseFileOffset: 7000, BlockSize: 4096, BlockCount: 4096, Available: true},
	}

	for _, e := range entries {
		pc.Insert(e)
	}

	// Marshal
	data, err := pc.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	if len(data) != pc.SizeInBytes() {
		t.Errorf("Expected marshaled size %d, got %d", pc.SizeInBytes(), len(data))
	}

	// Create new cache and unmarshal
	pc2, err := New()
	if err != nil {
		t.Fatalf("Failed to create second PoolCache: %v", err)
	}
	defer pc2.Close()

	err = pc2.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify entries restored
	if len(pc2.entries) != 3 {
		t.Errorf("Expected 3 restored entries, got %d", len(pc2.entries))
	}

	for i, expected := range entries {
		actual := pc2.entries[i]
		if actual.ObjId != expected.ObjId {
			t.Errorf("Entry %d: Expected ObjId %d, got %d", i, expected.ObjId, actual.ObjId)
		}
		if actual.BaseObjId != expected.BaseObjId {
			t.Errorf("Entry %d: Expected BaseObjId %d, got %d", i, expected.BaseObjId, actual.BaseObjId)
		}
		if actual.BaseFileOffset != expected.BaseFileOffset {
			t.Errorf("Entry %d: Expected BaseFileOffset %d, got %d", i, expected.BaseFileOffset, actual.BaseFileOffset)
		}
		if actual.BlockSize != expected.BlockSize {
			t.Errorf("Entry %d: Expected BlockSize %d, got %d", i, expected.BlockSize, actual.BlockSize)
		}
		if actual.Available != expected.Available {
			t.Errorf("Entry %d: Expected Available %v, got %v", i, expected.Available, actual.Available)
		}
	}
}

func TestPoolCacheMarshalEmpty(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	// Marshal empty cache
	data, err := pc.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	if len(data) != headerSize {
		t.Errorf("Expected empty marshal size %d, got %d", headerSize, len(data))
	}

	// Unmarshal empty
	pc2, err := New()
	if err != nil {
		t.Fatalf("Failed to create second PoolCache: %v", err)
	}
	defer pc2.Close()

	err = pc2.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if len(pc2.entries) != 0 {
		t.Errorf("Expected 0 entries, got %d", len(pc2.entries))
	}
}

func TestPoolCacheUnmarshalInvalidSize(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	// Too short
	err = pc.Unmarshal([]byte{1, 2, 3})
	if err != ErrInvalidEntry {
		t.Errorf("Expected ErrInvalidEntry for short data, got %v", err)
	}
}

func TestPoolCacheGetAll(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	// Add entries
	entries := []UnloadedBlock{
		{ObjId: 100, BaseObjId: 1000, BlockCount: 1024},
		{ObjId: 101, BaseObjId: 2000, BlockCount: 1024},
		{ObjId: 102, BaseObjId: 3000, BlockCount: 1024},
	}

	for _, e := range entries {
		pc.Insert(e)
	}

	all := pc.GetAll()
	if len(all) != 3 {
		t.Errorf("Expected 3 entries from GetAll, got %d", len(all))
	}

	// Verify they're in sorted order
	if all[0].BaseObjId != 1000 || all[1].BaseObjId != 2000 || all[2].BaseObjId != 3000 {
		t.Error("Entries not in sorted order")
	}

	// Verify it's a copy (modifying shouldn't affect cache)
	all[0].ObjId = types.ObjectId(999)
	if pc.entries[0].ObjId == types.ObjectId(999) {
		t.Error("GetAll should return a copy, not a reference")
	}
}

func TestPoolCacheClose(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}

	tempName := pc.tempFile.Name()

	// Verify file exists
	if _, err := os.Stat(tempName); err != nil {
		t.Fatalf("Temp file should exist: %v", err)
	}

	// Close cache
	err = pc.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify file is deleted
	if _, err := os.Stat(tempName); !os.IsNotExist(err) {
		t.Error("Temp file should be deleted after Close")
	}
}

func TestPoolCacheComplexScenario(t *testing.T) {
	pc, err := New()
	if err != nil {
		t.Fatalf("Failed to create PoolCache: %v", err)
	}
	defer pc.Close()

	// Simulate caching 5 BlockAllocators from different pools
	for i := 0; i < 5; i++ {
		entry := UnloadedBlock{
			ObjId:           types.ObjectId(1000 + i),
			BaseObjId:       types.ObjectId(10000 + i*5000),
			BaseFileOffset:  types.FileOffset(50000 + i*50000),
			BlockSize:       types.FileSize(1024 * (i + 1)),
			BlockCount:      1024,
			Available:       i%2 == 0,
		}
		pc.Insert(entry)
	}

	// Query several ObjectIds
	result, err := pc.Query(types.ObjectId(10000))
	if err != nil || result.ObjId != 1000 {
		t.Error("Query for first entry failed")
	}

	result, err = pc.Query(types.ObjectId(15000))
	if err != nil || result.ObjId != 1001 {
		t.Error("Query for second entry failed")
	}

	// Delete middle entry (by BaseObjId, which is 20000 for i=2)
	pc.Delete(types.ObjectId(20000))
	if len(pc.entries) != 4 {
		t.Errorf("Expected 4 entries after delete, got %d", len(pc.entries))
	}

	// Query should not find deleted entry (PoolCache validates range)
	_, err = pc.Query(types.ObjectId(20000))
	if err != ErrObjectIdNotFound {
		t.Error("Query for deleted entry should fail - PoolCache validates ObjectId range")
	}

	// Marshal and unmarshal
	data, _ := pc.Marshal()
	pc2, _ := New()
	defer pc2.Close()
	
	pc2.Unmarshal(data)
	if len(pc2.entries) != 4 {
		t.Errorf("Expected 4 restored entries, got %d", len(pc2.entries))
	}
}
