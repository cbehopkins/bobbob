package basic

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/allocator/types"
)

func createTempTrackerFile(t *testing.T) (*os.File, func()) {
	t.Helper()

	tmpDir := t.TempDir()
	trackerPath := filepath.Join(tmpDir, "tracker.bin")

	file, err := os.Create(trackerPath)
	if err != nil {
		t.Fatalf("Failed to create temp tracker file: %v", err)
	}

	cleanup := func() {
		file.Close()
		os.Remove(trackerPath)
	}

	return file, cleanup
}

func TestFileBasedTrackerNew(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	if tracker.header.Magic != magicBytes {
		t.Errorf("Expected magic %x, got %x", magicBytes, tracker.header.Magic)
	}

	if tracker.header.NumBuckets != defaultNumBuckets {
		t.Errorf("Expected %d buckets, got %d", defaultNumBuckets, tracker.header.NumBuckets)
	}

	if tracker.Len() != 0 {
		t.Errorf("Expected empty tracker, got %d entries", tracker.Len())
	}
}

func TestFileBasedTrackerSetGet(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	objId := types.ObjectId(100)
	size := types.FileSize(256)

	// Set entry
	tracker.Set(objId, size)

	// Get entry
	gotSize, found := tracker.Get(objId)
	if !found {
		t.Fatal("Entry not found after Set")
	}

	if gotSize != size {
		t.Errorf("Expected size %d, got %d", size, gotSize)
	}

	if tracker.Len() != 1 {
		t.Errorf("Expected count 1, got %d", tracker.Len())
	}
}

func TestFileBasedTrackerUpdate(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	objId := types.ObjectId(100)
	size1 := types.FileSize(256)
	size2 := types.FileSize(512)

	// Set entry
	tracker.Set(objId, size1)

	// Update entry
	tracker.Set(objId, size2)

	// Get entry
	gotSize, found := tracker.Get(objId)
	if !found {
		t.Fatal("Entry not found after update")
	}

	if gotSize != size2 {
		t.Errorf("Expected size %d, got %d", size2, gotSize)
	}

	// Count should still be 1 (update, not insert)
	if tracker.Len() != 1 {
		t.Errorf("Expected count 1 after update, got %d", tracker.Len())
	}
}

func TestFileBasedTrackerDelete(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	objId := types.ObjectId(100)
	size := types.FileSize(256)

	// Set entry
	tracker.Set(objId, size)

	// Delete entry
	tracker.Delete(objId)

	// Check not found
	_, found := tracker.Get(objId)
	if found {
		t.Error("Entry should not be found after delete")
	}

	if tracker.Len() != 0 {
		t.Errorf("Expected count 0 after delete, got %d", tracker.Len())
	}
}

func TestFileBasedTrackerMultipleEntries(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Add multiple entries
	entries := []struct {
		id   types.ObjectId
		size types.FileSize
	}{
		{100, 256},
		{200, 512},
		{300, 1024},
	}

	for _, entry := range entries {
		tracker.Set(entry.id, entry.size)
	}

	// Verify all entries
	for _, entry := range entries {
		gotSize, found := tracker.Get(entry.id)
		if !found {
			t.Errorf("Entry %d not found", entry.id)
		}
		if gotSize != entry.size {
			t.Errorf("Entry %d: expected size %d, got %d", entry.id, entry.size, gotSize)
		}
	}

	if tracker.Len() != len(entries) {
		t.Errorf("Expected count %d, got %d", len(entries), tracker.Len())
	}
}

func TestFileBasedTrackerContains(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	objId := types.ObjectId(100)
	size := types.FileSize(256)

	// Check not contained
	if tracker.Contains(objId) {
		t.Error("Entry should not be contained before Set")
	}

	// Add entry
	tracker.Set(objId, size)

	// Check contained
	if !tracker.Contains(objId) {
		t.Error("Entry should be contained after Set")
	}
}

func TestFileBasedTrackerForEach(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Add entries
	entries := map[types.ObjectId]types.FileSize{
		100: 256,
		200: 512,
		300: 1024,
	}

	for id, size := range entries {
		tracker.Set(id, size)
	}

	// ForEach
	seen := make(map[types.ObjectId]types.FileSize)
	tracker.ForEach(func(id types.ObjectId, size types.FileSize) {
		seen[id] = size
	})

	if len(seen) != len(entries) {
		t.Errorf("Expected %d entries in ForEach, got %d", len(entries), len(seen))
	}

	for id, size := range entries {
		if seenSize, ok := seen[id]; !ok {
			t.Errorf("Entry %d not seen in ForEach", id)
		} else if seenSize != size {
			t.Errorf("Entry %d: expected size %d, got %d", id, size, seenSize)
		}
	}
}

func TestFileBasedTrackerGetAllObjectIds(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Add entries
	expected := []types.ObjectId{100, 200, 300}
	for _, id := range expected {
		tracker.Set(id, 256)
	}

	// Get all IDs
	ids := tracker.GetAllObjectIds()

	if len(ids) != len(expected) {
		t.Errorf("Expected %d IDs, got %d", len(expected), len(ids))
	}

	// Convert to map for easier checking
	idMap := make(map[types.ObjectId]bool)
	for _, id := range ids {
		idMap[id] = true
	}

	for _, id := range expected {
		if !idMap[id] {
			t.Errorf("Expected ID %d not found", id)
		}
	}
}

func TestFileBasedTrackerClear(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Add entries
	tracker.Set(100, 256)
	tracker.Set(200, 512)

	if tracker.Len() != 2 {
		t.Errorf("Expected 2 entries before clear, got %d", tracker.Len())
	}

	// Clear
	tracker.Clear()

	if tracker.Len() != 0 {
		t.Errorf("Expected 0 entries after clear, got %d", tracker.Len())
	}

	// Check entries are gone
	if _, found := tracker.Get(100); found {
		t.Error("Entry 100 should not exist after clear")
	}
	if _, found := tracker.Get(200); found {
		t.Error("Entry 200 should not exist after clear")
	}
}

func TestFileBasedTrackerMarshalUnmarshal(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Add entries
	entries := map[types.ObjectId]types.FileSize{
		100: 256,
		200: 512,
		300: 1024,
	}

	for id, size := range entries {
		tracker.Set(id, size)
	}

	// Marshal
	data, err := tracker.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Create new tracker for unmarshal
	file2, cleanup2 := createTempTrackerFile(t)
	defer cleanup2()

	tracker2, err := newFileBasedObjectTracker(file2)
	if err != nil {
		t.Fatalf("Failed to create second tracker: %v", err)
	}

	// Unmarshal
	if err := tracker2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify entries
	if tracker2.Len() != len(entries) {
		t.Errorf("Expected %d entries after unmarshal, got %d", len(entries), tracker2.Len())
	}

	for id, size := range entries {
		gotSize, found := tracker2.Get(id)
		if !found {
			t.Errorf("Entry %d not found after unmarshal", id)
		}
		if gotSize != size {
			t.Errorf("Entry %d: expected size %d, got %d", id, size, gotSize)
		}
	}
}

func TestFileBasedTrackerPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	trackerPath := filepath.Join(tmpDir, "tracker.bin")

	// Create tracker and add entries
	file, err := os.Create(trackerPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	entries := map[types.ObjectId]types.FileSize{
		100: 256,
		200: 512,
		300: 1024,
	}

	for id, size := range entries {
		tracker.Set(id, size)
	}

	tracker.Close()

	// Reopen and verify
	file2, err := os.OpenFile(trackerPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to reopen file: %v", err)
	}
	defer file2.Close()

	tracker2, err := newFileBasedObjectTracker(file2)
	if err != nil {
		t.Fatalf("Failed to load tracker: %v", err)
	}

	if tracker2.Len() != len(entries) {
		t.Errorf("Expected %d entries after reload, got %d", len(entries), tracker2.Len())
	}

	for id, size := range entries {
		gotSize, found := tracker2.Get(id)
		if !found {
			t.Errorf("Entry %d not found after reload", id)
		}
		if gotSize != size {
			t.Errorf("Entry %d: expected size %d, got %d after reload", id, size, gotSize)
		}
	}
}

func TestFileBasedTrackerHashCollisions(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Add many entries to trigger collisions
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		objId := types.ObjectId(i * 100)
		size := types.FileSize(i * 10)
		tracker.Set(objId, size)
	}

	// Verify all entries
	for i := 0; i < numEntries; i++ {
		objId := types.ObjectId(i * 100)
		expectedSize := types.FileSize(i * 10)

		gotSize, found := tracker.Get(objId)
		if !found {
			t.Errorf("Entry %d not found", objId)
		}
		if gotSize != expectedSize {
			t.Errorf("Entry %d: expected size %d, got %d", objId, expectedSize, gotSize)
		}
	}

	if tracker.Len() != numEntries {
		t.Errorf("Expected count %d, got %d", numEntries, tracker.Len())
	}
}

func TestFileBasedTrackerFreeListReuse(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Add entries
	tracker.Set(100, 256)
	tracker.Set(200, 512)
	tracker.Set(300, 1024)

	// Delete one
	tracker.Delete(200)

	// Add new entry (should reuse deleted slot)
	tracker.Set(400, 128)

	// Verify
	if tracker.Len() != 3 {
		t.Errorf("Expected count 3, got %d", tracker.Len())
	}

	// Check that deleted entry is gone
	if _, found := tracker.Get(200); found {
		t.Error("Deleted entry should not be found")
	}

	// Check that new entry exists
	gotSize, found := tracker.Get(400)
	if !found {
		t.Error("New entry should be found")
	}
	if gotSize != 128 {
		t.Errorf("New entry: expected size 128, got %d", gotSize)
	}
}

// Phase 2 Tests - Statistics

func TestFileBasedTrackerStats(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Initial stats
	stats := tracker.Stats()
	if stats.LiveEntries != 0 {
		t.Errorf("Expected 0 live entries, got %d", stats.LiveEntries)
	}
	if stats.EmptyBuckets != int(defaultNumBuckets) {
		t.Errorf("Expected %d empty buckets, got %d", defaultNumBuckets, stats.EmptyBuckets)
	}

	// Add entries
	for i := 0; i < 100; i++ {
		tracker.Set(types.ObjectId(i*100), types.FileSize(i*10))
	}

	stats = tracker.Stats()
	if stats.LiveEntries != 100 {
		t.Errorf("Expected 100 live entries, got %d", stats.LiveEntries)
	}
	if stats.LoadFactorPercent <= 0 {
		t.Errorf("Expected positive load factor, got %f", stats.LoadFactorPercent)
	}

	// Delete some entries
	for i := 0; i < 50; i += 2 {
		tracker.Delete(types.ObjectId(i * 100))
	}

	stats = tracker.Stats()
	if stats.LiveEntries != 75 {
		t.Errorf("Expected 75 live entries, got %d", stats.LiveEntries)
	}
	if stats.DeletedEntries != 25 {
		t.Errorf("Expected 25 deleted entries, got %d", stats.DeletedEntries)
	}
	if stats.FreeListLength != 25 {
		t.Errorf("Expected 25 entries in free list, got %d", stats.FreeListLength)
	}
}

func TestFileBasedTrackerStatsChainLengths(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Add many entries to create collision chains
	numEntries := 5000
	for i := 0; i < numEntries; i++ {
		tracker.Set(types.ObjectId(i), types.FileSize(i*10))
	}

	stats := tracker.Stats()

	if stats.MaxChainLength <= 0 {
		t.Errorf("Expected positive max chain length, got %d", stats.MaxChainLength)
	}
	if stats.AvgChainLength <= 0 {
		t.Errorf("Expected positive avg chain length, got %f", stats.AvgChainLength)
	}

	t.Logf("With %d entries: max chain=%d, avg chain=%.2f, empty buckets=%d",
		numEntries, stats.MaxChainLength, stats.AvgChainLength, stats.EmptyBuckets)
}

// Phase 2 Tests - Rehashing

func TestFileBasedTrackerNeedsRehash(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Initially should not need rehash
	if tracker.NeedsRehash() {
		t.Error("Empty tracker should not need rehash")
	}

	// Fill to just under threshold (75% of 4096 = 3072)
	threshold := uint32(float64(defaultNumBuckets) * float64(loadFactorPercent) / 100.0)
	for i := uint32(0); i < threshold-10; i++ {
		tracker.Set(types.ObjectId(i*100), types.FileSize(100))
	}

	if tracker.NeedsRehash() {
		t.Error("Should not need rehash below threshold")
	}

	// Add more to exceed threshold
	for i := threshold - 10; i < threshold+10; i++ {
		tracker.Set(types.ObjectId(i*100), types.FileSize(100))
	}

	if !tracker.NeedsRehash() {
		t.Error("Should need rehash above threshold")
	}
}

func TestFileBasedTrackerRehash(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Add entries
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		tracker.Set(types.ObjectId(i*100), types.FileSize(i*10))
	}

	// Get stats before rehash
	statsBefore := tracker.Stats()
	t.Logf("Before rehash: buckets=%d, entries=%d, max chain=%d, avg chain=%.2f",
		statsBefore.NumBuckets, statsBefore.LiveEntries,
		statsBefore.MaxChainLength, statsBefore.AvgChainLength)

	// Rehash
	if err := tracker.Rehash(); err != nil {
		t.Fatalf("Rehash failed: %v", err)
	}

	// Get stats after rehash
	statsAfter := tracker.Stats()
	t.Logf("After rehash: buckets=%d, entries=%d, max chain=%d, avg chain=%.2f",
		statsAfter.NumBuckets, statsAfter.LiveEntries,
		statsAfter.MaxChainLength, statsAfter.AvgChainLength)

	// Verify bucket count doubled
	if statsAfter.NumBuckets != statsBefore.NumBuckets*2 {
		t.Errorf("Expected %d buckets after rehash, got %d",
			statsBefore.NumBuckets*2, statsAfter.NumBuckets)
	}

	// Verify same number of entries
	if statsAfter.LiveEntries != statsBefore.LiveEntries {
		t.Errorf("Expected %d entries after rehash, got %d",
			statsBefore.LiveEntries, statsAfter.LiveEntries)
	}

	// Verify all entries still accessible
	for i := 0; i < numEntries; i++ {
		objId := types.ObjectId(i * 100)
		expectedSize := types.FileSize(i * 10)

		gotSize, found := tracker.Get(objId)
		if !found {
			t.Errorf("Entry %d not found after rehash", objId)
		}
		if gotSize != expectedSize {
			t.Errorf("Entry %d: expected size %d, got %d after rehash",
				objId, expectedSize, gotSize)
		}
	}

	// Verify no deleted entries after rehash
	if statsAfter.DeletedEntries != 0 {
		t.Errorf("Expected 0 deleted entries after rehash, got %d", statsAfter.DeletedEntries)
	}
}

// Phase 2 Tests - Compaction

func TestFileBasedTrackerNeedsCompaction(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Add entries
	for i := 0; i < 100; i++ {
		tracker.Set(types.ObjectId(i*100), types.FileSize(100))
	}

	// Initially should not need compaction
	if tracker.NeedsCompaction() {
		t.Error("Should not need compaction with no deleted entries")
	}

	// Delete 10% of entries (below 25% threshold)
	for i := 0; i < 10; i++ {
		tracker.Delete(types.ObjectId(i * 100))
	}

	if tracker.NeedsCompaction() {
		t.Error("Should not need compaction with only 10% deleted")
	}

	// Delete more (total 30%, above 25% threshold)
	for i := 10; i < 30; i++ {
		tracker.Delete(types.ObjectId(i * 100))
	}

	if !tracker.NeedsCompaction() {
		t.Error("Should need compaction with 30% deleted")
	}
}

func TestFileBasedTrackerCompact(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Add entries
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		tracker.Set(types.ObjectId(i*100), types.FileSize(i*10))
	}

	// Delete half of them
	deletedIds := make(map[types.ObjectId]bool)
	for i := 0; i < numEntries/2; i++ {
		objId := types.ObjectId(i * 100)
		tracker.Delete(objId)
		deletedIds[objId] = true
	}

	// Get stats before compaction
	statsBefore := tracker.Stats()
	t.Logf("Before compact: live=%d, deleted=%d, total=%d, file size=%d bytes",
		statsBefore.LiveEntries, statsBefore.DeletedEntries,
		statsBefore.TotalEntries, statsBefore.FileSize)

	// Compact
	if err := tracker.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Get stats after compaction
	statsAfter := tracker.Stats()
	t.Logf("After compact: live=%d, deleted=%d, total=%d, file size=%d bytes",
		statsAfter.LiveEntries, statsAfter.DeletedEntries,
		statsAfter.TotalEntries, statsAfter.FileSize)

	// Verify same number of live entries
	if statsAfter.LiveEntries != statsBefore.LiveEntries {
		t.Errorf("Expected %d live entries after compact, got %d",
			statsBefore.LiveEntries, statsAfter.LiveEntries)
	}

	// Verify no deleted entries
	if statsAfter.DeletedEntries != 0 {
		t.Errorf("Expected 0 deleted entries after compact, got %d", statsAfter.DeletedEntries)
	}

	// Verify total entries equals live entries
	if statsAfter.TotalEntries != statsAfter.LiveEntries {
		t.Errorf("Expected total=%d to equal live=%d after compact",
			statsAfter.TotalEntries, statsAfter.LiveEntries)
	}

	// Verify file size decreased
	if statsAfter.FileSize >= statsBefore.FileSize {
		t.Errorf("File size should decrease: before=%d, after=%d",
			statsBefore.FileSize, statsAfter.FileSize)
	}

	// Verify deleted entries are still not accessible
	for objId := range deletedIds {
		if _, found := tracker.Get(objId); found {
			t.Errorf("Deleted entry %d should not be found after compact", objId)
		}
	}

	// Verify remaining entries are accessible
	for i := numEntries / 2; i < numEntries; i++ {
		objId := types.ObjectId(i * 100)
		expectedSize := types.FileSize(i * 10)

		gotSize, found := tracker.Get(objId)
		if !found {
			t.Errorf("Entry %d not found after compact", objId)
		}
		if gotSize != expectedSize {
			t.Errorf("Entry %d: expected size %d, got %d after compact",
				objId, expectedSize, gotSize)
		}
	}

	// Verify no free list after compaction
	if statsAfter.FreeListLength != 0 {
		t.Errorf("Expected empty free list after compact, got %d", statsAfter.FreeListLength)
	}
}

func TestFileBasedTrackerCompactAndRehash(t *testing.T) {
	file, cleanup := createTempTrackerFile(t)
	defer cleanup()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Add many entries
	numEntries := 2000
	for i := 0; i < numEntries; i++ {
		tracker.Set(types.ObjectId(i*100), types.FileSize(i*10))
	}

	// Delete half
	for i := 0; i < numEntries/2; i++ {
		tracker.Delete(types.ObjectId(i * 100))
	}

	statsBefore := tracker.Stats()
	t.Logf("Before: buckets=%d, live=%d, deleted=%d, max chain=%d",
		statsBefore.NumBuckets, statsBefore.LiveEntries,
		statsBefore.DeletedEntries, statsBefore.MaxChainLength)

	// Compact first
	if err := tracker.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	statsAfterCompact := tracker.Stats()
	t.Logf("After compact: buckets=%d, live=%d, deleted=%d, max chain=%d",
		statsAfterCompact.NumBuckets, statsAfterCompact.LiveEntries,
		statsAfterCompact.DeletedEntries, statsAfterCompact.MaxChainLength)

	// Then rehash
	if err := tracker.Rehash(); err != nil {
		t.Fatalf("Rehash failed: %v", err)
	}

	statsAfterRehash := tracker.Stats()
	t.Logf("After rehash: buckets=%d, live=%d, deleted=%d, max chain=%d",
		statsAfterRehash.NumBuckets, statsAfterRehash.LiveEntries,
		statsAfterRehash.DeletedEntries, statsAfterRehash.MaxChainLength)

	// Verify entries preserved
	if statsAfterRehash.LiveEntries != statsAfterCompact.LiveEntries {
		t.Errorf("Expected %d live entries, got %d",
			statsAfterCompact.LiveEntries, statsAfterRehash.LiveEntries)
	}

	// Verify all remaining entries accessible
	for i := numEntries / 2; i < numEntries; i++ {
		objId := types.ObjectId(i * 100)
		if _, found := tracker.Get(objId); !found {
			t.Errorf("Entry %d not found after compact+rehash", objId)
		}
	}
}
