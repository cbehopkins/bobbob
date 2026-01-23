package allocator

import (
	"testing"

	"github.com/cbehopkins/bobbob/allocator/types"
)

// TestPrimeTableBasicExample demonstrates the complete read/write flow.
func TestPrimeTableBasicExample(t *testing.T) {
	// ==================== CREATE PHASE ====================
	// Setup: Creating a new store with BasicAllocator and OmniAllocator
	
	table := NewPrimeTable()
	
	// Register allocators (order matters - matches load order)
	table.Add()   // Index 0: BasicAllocator
	table.Add()   // Index 1: OmniAllocator
	
	// Calculate space needed for table
	tableSize := table.SizeInBytes()
	if tableSize != 52 { // 4 + 2*24 = 52 bytes
		t.Errorf("SizeInBytes() = %d, want 52", tableSize)
	}
	
	// ==================== SAVE PHASE ====================
	// Simulate saving allocators (reverse order: Omni first, Basic last)
	
	// OmniAllocator persists first
	omniInfo := FileInfo{
		ObjId: types.ObjectId(2048),
		Fo:    types.FileOffset(2048),
		Sz:    types.FileSize(512),
	}
	if err := table.Store(omniInfo); err != nil {
		t.Fatalf("Store(omni) failed: %v", err)
	}
	
	// BasicAllocator persists last
	basicInfo := FileInfo{
		ObjId: types.ObjectId(4096),
		Fo:    types.FileOffset(4096),
		Sz:    types.FileSize(1024),
	}
	if err := table.Store(basicInfo); err != nil {
		t.Fatalf("Store(basic) failed: %v", err)
	}
	
	// Marshal table for persistence
	data, err := table.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	
	// Verify size matches
	if len(data) != int(tableSize) {
		t.Errorf("Marshaled size = %d, want %d", len(data), tableSize)
	}
	
	// At this point, data would be written to file at offset 0
	
	// ==================== LOAD PHASE ====================
	// Simulate loading from file
	
	loadedTable := NewPrimeTable()
	if err := loadedTable.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	
	// Load BasicAllocator info (first Load call)
	loadedBasic, err := loadedTable.Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}
	
	if loadedBasic.ObjId != basicInfo.ObjId {
		t.Errorf("BasicAllocator ObjId = %d, want %d", loadedBasic.ObjId, basicInfo.ObjId)
	}
	if loadedBasic.Fo != basicInfo.Fo {
		t.Errorf("BasicAllocator Fo = %d, want %d", loadedBasic.Fo, basicInfo.Fo)
	}
	if loadedBasic.Sz != basicInfo.Sz {
		t.Errorf("BasicAllocator Sz = %d, want %d", loadedBasic.Sz, basicInfo.Sz)
	}
	
	// Load OmniAllocator info (second Load call)
	loadedOmni, err := loadedTable.Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}
	
	if loadedOmni.ObjId != omniInfo.ObjId {
		t.Errorf("OmniAllocator ObjId = %d, want %d", loadedOmni.ObjId, omniInfo.ObjId)
	}
	if loadedOmni.Fo != omniInfo.Fo {
		t.Errorf("OmniAllocator Fo = %d, want %d", loadedOmni.Fo, omniInfo.Fo)
	}
	if loadedOmni.Sz != omniInfo.Sz {
		t.Errorf("OmniAllocator Sz = %d, want %d", loadedOmni.Sz, omniInfo.Sz)
	}
}

// TestPrimeTableAdd verifies registration behavior.
func TestPrimeTableAdd(t *testing.T) {
	table := NewPrimeTable()
	
	// Add multiple entries
	idx0 := table.AddForTest()
	idx1 := table.AddForTest()
	idx2 := table.AddForTest()
	
	if idx0 != 0 || idx1 != 1 || idx2 != 2 {
		t.Errorf("Add returned unexpected indices: %d, %d, %d", idx0, idx1, idx2)
	}
	
	if table.NumEntries() != 3 {
		t.Errorf("NumEntries() = %d, want 3", table.NumEntries())
	}
}

// TestPrimeTableStoreSealing verifies that Add is forbidden after Store.
func TestPrimeTableStoreSealing(t *testing.T) {
	table := NewPrimeTable()
	table.Add()
	table.Add()
	
	// First Store should seal the table
	err := table.Store(FileInfo{ObjId: 100, Fo: 100, Sz: 50})
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	
	// Attempt to Add should panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("Add after Store should panic")
		}
	}()
	table.Add()
}

// TestPrimeTableStoreOrder verifies reverse-order Store behavior.
func TestPrimeTableStoreOrder(t *testing.T) {
	table := NewPrimeTable()
	table.Add() // Index 0
	table.Add() // Index 1
	table.Add() // Index 2
	
	// Store in reverse order (2, 1, 0)
	info2 := FileInfo{ObjId: 2000, Fo: 2000, Sz: 200}
	info1 := FileInfo{ObjId: 1000, Fo: 1000, Sz: 100}
	info0 := FileInfo{ObjId: 500, Fo: 500, Sz: 50}
	
	if err := table.Store(info2); err != nil {
		t.Fatalf("Store(2) failed: %v", err)
	}
	if err := table.Store(info1); err != nil {
		t.Fatalf("Store(1) failed: %v", err)
	}
	if err := table.Store(info0); err != nil {
		t.Fatalf("Store(0) failed: %v", err)
	}
	
	// Verify entries are stored correctly by loading in sequence
	loaded0, _ := table.Load()
	loaded1, _ := table.Load()
	loaded2, _ := table.Load()
	
	if loaded0.ObjId != info0.ObjId {
		t.Errorf("Entry 0 ObjId = %d, want %d", loaded0.ObjId, info0.ObjId)
	}
	if loaded1.ObjId != info1.ObjId {
		t.Errorf("Entry 1 ObjId = %d, want %d", loaded1.ObjId, info1.ObjId)
	}
	if loaded2.ObjId != info2.ObjId {
		t.Errorf("Entry 2 ObjId = %d, want %d", loaded2.ObjId, info2.ObjId)
	}
}

// TestPrimeTableStoreTooMany verifies error on excess Store calls.
func TestPrimeTableStoreTooMany(t *testing.T) {
	table := NewPrimeTable()
	table.Add()
	table.Add()
	
	// Store twice (should succeed)
	table.Store(FileInfo{ObjId: 100, Fo: 100, Sz: 50})
	table.Store(FileInfo{ObjId: 200, Fo: 200, Sz: 60})
	
	// Third Store should fail
	err := table.Store(FileInfo{ObjId: 300, Fo: 300, Sz: 70})
	if err != ErrInvalidEntry {
		t.Errorf("Store overflow returned %v, want %v", err, ErrInvalidEntry)
	}
}

// TestPrimeTableLoadBounds verifies error handling for Load().
func TestPrimeTableLoadBounds(t *testing.T) {
	table := NewPrimeTable()
	table.Add()
	table.Add()
	
	// Load first two entries (should succeed)
	_, err := table.Load()
	if err != nil {
		t.Errorf("Load() #1 returned error: %v", err)
	}
	_, err = table.Load()
	if err != nil {
		t.Errorf("Load() #2 returned error: %v", err)
	}
	
	// Third Load should fail (out of bounds)
	_, err = table.Load()
	if err != ErrInvalidEntry {
		t.Errorf("Load() #3 returned %v, want %v", err, ErrInvalidEntry)
	}
}

// TestPrimeTableMarshalUnmarshal verifies serialization round-trip.
func TestPrimeTableMarshalUnmarshal(t *testing.T) {
	// Create and populate table
	table := NewPrimeTable()
	table.Add()
	table.Add()
	table.Add()
	
	info0 := FileInfo{ObjId: 1024, Fo: 1024, Sz: 256}
	info1 := FileInfo{ObjId: 2048, Fo: 2048, Sz: 512}
	info2 := FileInfo{ObjId: 4096, Fo: 4096, Sz: 1024}
	
	table.Store(info2)
	table.Store(info1)
	table.Store(info0)
	
	// Marshal
	data, err := table.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	
	// Unmarshal into new table
	newTable := NewPrimeTable()
	if err := newTable.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	
	// Verify all entries match
	for i := 0; i < 3; i++ {
		original, _ := table.Load()
		restored, _ := newTable.Load()
		
		if original.ObjId != restored.ObjId {
			t.Errorf("Entry %d ObjId mismatch: %d != %d", i, original.ObjId, restored.ObjId)
		}
		if original.Fo != restored.Fo {
			t.Errorf("Entry %d Fo mismatch: %d != %d", i, original.Fo, restored.Fo)
		}
		if original.Sz != restored.Sz {
			t.Errorf("Entry %d Sz mismatch: %d != %d", i, original.Sz, restored.Sz)
		}
	}
}

// TestPrimeTableSizeInBytes verifies size calculation.
func TestPrimeTableSizeInBytes(t *testing.T) {
	tests := []struct {
		name      string
		numAdds   int
		wantBytes types.FileSize
	}{
		{"empty", 0, 4},                     // Just count field
		{"one entry", 1, 4 + 24},            // Count + 1 entry
		{"two entries", 2, 4 + 2*24},        // Count + 2 entries
		{"five entries", 5, 4 + 5*24},       // Count + 5 entries
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := NewPrimeTable()
			for i := 0; i < tt.numAdds; i++ {
				table.Add()
			}
			
			got := table.SizeInBytes()
			if got != tt.wantBytes {
				t.Errorf("SizeInBytes() = %d, want %d", got, tt.wantBytes)
			}
			
			// Verify Marshal produces that size
			data, _ := table.Marshal()
			if types.FileSize(len(data)) != tt.wantBytes {
				t.Errorf("Marshal produced %d bytes, want %d", len(data), tt.wantBytes)
			}
		})
	}
}

// TestPrimeTableUnmarshalInsufficientData verifies error handling.
func TestPrimeTableUnmarshalInsufficientData(t *testing.T) {
	table := NewPrimeTable()
	
	// Empty data
	err := table.Unmarshal([]byte{})
	if err != ErrInsufficientData {
		t.Errorf("Unmarshal(empty) returned %v, want %v", err, ErrInsufficientData)
	}
	
	// Truncated data (only count field)
	data := make([]byte, 4)
	data[0] = 3 // Claims 3 entries
	
	err = table.Unmarshal(data)
	if err != ErrInsufficientData {
		t.Errorf("Unmarshal(truncated) returned %v, want %v", err, ErrInsufficientData)
	}
}

// TestPrimeTableReset verifies Reset functionality.
func TestPrimeTableReset(t *testing.T) {
	table := NewPrimeTable()
	table.Add()
	table.Add()
	table.Store(FileInfo{ObjId: 100, Fo: 100, Sz: 50})
	
	// Reset should clear everything
	table.Reset()
	
	if table.NumEntries() != 0 {
		t.Errorf("After Reset, NumEntries() = %d, want 0", table.NumEntries())
	}
	
	if table.sealed {
		t.Error("After Reset, table should not be sealed")
	}
	
	// Should be able to Add again
	idx := table.AddForTest()
	if idx != 0 {
		t.Errorf("After Reset, AddForTest() = %d, want 0", idx)
	}
}

// TestPrimeTableEmptyTableMarshal verifies empty table behavior.
func TestPrimeTableEmptyTableMarshal(t *testing.T) {
	table := NewPrimeTable()
	
	data, err := table.Marshal()
	if err != nil {
		t.Fatalf("Marshal empty table failed: %v", err)
	}
	
	// Should just be 4-byte count of 0
	if len(data) != 4 {
		t.Errorf("Empty table marshal size = %d, want 4", len(data))
	}
	
	// Unmarshal and verify
	newTable := NewPrimeTable()
	if err := newTable.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal empty table failed: %v", err)
	}
	
	if newTable.NumEntries() != 0 {
		t.Errorf("Unmarshaled empty table has %d entries, want 0", newTable.NumEntries())
	}
}

// TestPrimeTableRealWorldScenario simulates actual allocator persistence flow.
func TestPrimeTableRealWorldScenario(t *testing.T) {
	// ========== STORE CREATION ==========
	table := NewPrimeTable()
	
	// Register allocators during initialization
	table.Add()  // BasicAllocator
	table.Add()  // OmniAllocator
	
	// Calculate how much space table needs
	tableSize := table.SizeInBytes()
	t.Logf("PrimeTable requires %d bytes at offset 0", tableSize)
	
	// BasicAllocator knows to start allocating at offset tableSize
	nextAvailableOffset := tableSize
	t.Logf("BasicAllocator begins allocations at offset %d", nextAvailableOffset)
	
	// ========== ALLOCATOR SAVES ==========
	// Simulate OmniAllocator persisting (calls BasicAllocator.Allocate)
	omniOffset := types.FileOffset(nextAvailableOffset)
	omniSize := types.FileSize(1024)
	omniObjId := types.ObjectId(omniOffset) // BasicAllocator: ObjectId == FileOffset
	nextAvailableOffset += types.FileSize(1024)
	
	if err := table.Store(FileInfo{
		ObjId: omniObjId,
		Fo:    omniOffset,
		Sz:    omniSize,
	}); err != nil {
		t.Fatalf("Store(Omni) failed: %v", err)
	}
	
	// Simulate BasicAllocator persisting (allocates at end of file)
	basicOffset := types.FileOffset(nextAvailableOffset)
	basicSize := types.FileSize(2048)
	basicObjId := types.ObjectId(basicOffset)
	
	if err := table.Store(FileInfo{
		ObjId: basicObjId,
		Fo:    basicOffset,
		Sz:    basicSize,
	}); err != nil {
		t.Fatalf("Store(Basic) failed: %v", err)
	}
	
	// Marshal and "write" to file
	tableData, _ := table.Marshal()
	t.Logf("PrimeTable marshaled to %d bytes", len(tableData))
	
	// ========== STORE LOAD ==========
	// Simulate loading from file
	loadedTable := NewPrimeTable()
	if err := loadedTable.Unmarshal(tableData); err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	
	// Load BasicAllocator first
	basicInfo, err := loadedTable.Load()
	if err != nil {
		t.Fatalf("Load BasicAllocator failed: %v", err)
	}
	t.Logf("BasicAllocator at offset %d, size %d, objId %d", basicInfo.Fo, basicInfo.Sz, basicInfo.ObjId)
	
	// Load OmniAllocator second
	omniInfo, err := loadedTable.Load()
	if err != nil {
		t.Fatalf("Load OmniAllocator failed: %v", err)
	}
	t.Logf("OmniAllocator at offset %d, size %d, objId %d", omniInfo.Fo, omniInfo.Sz, omniInfo.ObjId)
	
	// Verify offsets and sizes match
	if basicInfo.Fo != basicOffset || basicInfo.Sz != basicSize {
		t.Error("BasicAllocator info mismatch after round-trip")
	}
	if omniInfo.Fo != omniOffset || omniInfo.Sz != omniSize {
		t.Error("OmniAllocator info mismatch after round-trip")
	}
}
