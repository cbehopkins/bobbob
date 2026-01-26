package basic

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/allocator/types"
)

func TestBasicAllocatorWithFileBasedTracking(t *testing.T) {
	tmpDir := t.TempDir()
	dataPath := filepath.Join(tmpDir, "data.bin")
	trackerPath := filepath.Join(tmpDir, "tracker.bin")

	// Create files
	dataFile, err := os.Create(dataPath)
	if err != nil {
		t.Fatalf("Failed to create data file: %v", err)
	}

	trackerFile, err := os.Create(trackerPath)
	if err != nil {
		t.Fatalf("Failed to create tracker file: %v", err)
	}

	// Create allocator with file-based tracking
	allocator, err := NewWithFileBasedTracking(dataFile, trackerFile)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}

	// Allocate some objects
	obj1, _, err := allocator.Allocate(100)
	if err != nil {
		t.Fatalf("Allocate failed: %v", err)
	}

	obj2, _, err := allocator.Allocate(200)
	if err != nil {
		t.Fatalf("Allocate failed: %v", err)
	}

	obj3, _, err := allocator.Allocate(300)
	if err != nil {
		t.Fatalf("Allocate failed: %v", err)
	}

	// Verify object tracking
	if !allocator.ContainsObjectId(obj1) {
		t.Error("Object 1 should be tracked")
	}
	if !allocator.ContainsObjectId(obj2) {
		t.Error("Object 2 should be tracked")
	}
	if !allocator.ContainsObjectId(obj3) {
		t.Error("Object 3 should be tracked")
	}

	if allocator.GetObjectCount() != 3 {
		t.Errorf("Expected 3 objects, got %d", allocator.GetObjectCount())
	}

	// Delete one object
	if err := allocator.DeleteObj(obj2); err != nil {
		t.Fatalf("DeleteObj failed: %v", err)
	}

	if allocator.GetObjectCount() != 2 {
		t.Errorf("Expected 2 objects after delete, got %d", allocator.GetObjectCount())
	}

	// Close files
	dataFile.Close()
	if ft, ok := allocator.objectTracking.(*fileBasedObjectTracker); ok {
		ft.Close()
	}

	// Reopen and verify persistence
	dataFile2, err := os.OpenFile(dataPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to reopen data file: %v", err)
	}
	defer dataFile2.Close()

	trackerFile2, err := os.OpenFile(trackerPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to reopen tracker file: %v", err)
	}
	defer trackerFile2.Close()

	allocator2, err := NewWithFileBasedTracking(dataFile2, trackerFile2)
	if err != nil {
		t.Fatalf("Failed to reload allocator: %v", err)
	}

	// Verify persistence
	if allocator2.GetObjectCount() != 2 {
		t.Errorf("Expected 2 objects after reload, got %d", allocator2.GetObjectCount())
	}

	if !allocator2.ContainsObjectId(obj1) {
		t.Error("Object 1 should be tracked after reload")
	}
	if allocator2.ContainsObjectId(obj2) {
		t.Error("Object 2 should not be tracked after reload (was deleted)")
	}
	if !allocator2.ContainsObjectId(obj3) {
		t.Error("Object 3 should be tracked after reload")
	}
}

func TestBasicAllocatorFileBasedVsMemory(t *testing.T) {
	tmpDir := t.TempDir()

	// Create memory-based allocator
	dataPath1 := filepath.Join(tmpDir, "data1.bin")
	dataFile1, err := os.Create(dataPath1)
	if err != nil {
		t.Fatalf("Failed to create data file 1: %v", err)
	}
	defer dataFile1.Close()

	memAllocator, err := New(dataFile1)
	if err != nil {
		t.Fatalf("Failed to create memory allocator: %v", err)
	}

	// Create file-based allocator
	dataPath2 := filepath.Join(tmpDir, "data2.bin")
	trackerPath := filepath.Join(tmpDir, "tracker.bin")

	dataFile2, err := os.Create(dataPath2)
	if err != nil {
		t.Fatalf("Failed to create data file 2: %v", err)
	}
	defer dataFile2.Close()

	trackerFile, err := os.Create(trackerPath)
	if err != nil {
		t.Fatalf("Failed to create tracker file: %v", err)
	}
	defer trackerFile.Close()

	fileAllocator, err := NewWithFileBasedTracking(dataFile2, trackerFile)
	if err != nil {
		t.Fatalf("Failed to create file allocator: %v", err)
	}

	// Perform same operations on both
	numObjects := 100
	memObjects := make([]types.ObjectId, 0, numObjects)
	fileObjects := make([]types.ObjectId, 0, numObjects)

	for i := 0; i < numObjects; i++ {
		size := i*10 + 100

		memObj, _, err := memAllocator.Allocate(size)
		if err != nil {
			t.Fatalf("Memory allocator failed: %v", err)
		}
		memObjects = append(memObjects, memObj)

		fileObj, _, err := fileAllocator.Allocate(size)
		if err != nil {
			t.Fatalf("File allocator failed: %v", err)
		}
		fileObjects = append(fileObjects, fileObj)
	}

	// Verify both have same count
	if memAllocator.GetObjectCount() != fileAllocator.GetObjectCount() {
		t.Errorf("Object counts differ: memory=%d, file=%d",
			memAllocator.GetObjectCount(), fileAllocator.GetObjectCount())
	}

	// Verify both contain all objects
	for i := 0; i < numObjects; i++ {
		if !memAllocator.ContainsObjectId(memObjects[i]) {
			t.Errorf("Memory allocator missing object %d", i)
		}
		if !fileAllocator.ContainsObjectId(fileObjects[i]) {
			t.Errorf("File allocator missing object %d", i)
		}
	}

	// Delete every other object
	for i := 0; i < numObjects; i += 2 {
		if err := memAllocator.DeleteObj(memObjects[i]); err != nil {
			t.Fatalf("Memory delete failed: %v", err)
		}
		if err := fileAllocator.DeleteObj(fileObjects[i]); err != nil {
			t.Fatalf("File delete failed: %v", err)
		}
	}

	// Verify both have same count after deletes
	if memAllocator.GetObjectCount() != fileAllocator.GetObjectCount() {
		t.Errorf("Object counts differ after deletes: memory=%d, file=%d",
			memAllocator.GetObjectCount(), fileAllocator.GetObjectCount())
	}

	expectedCount := numObjects / 2
	if memAllocator.GetObjectCount() != expectedCount {
		t.Errorf("Expected %d objects after deletes, got %d",
			expectedCount, memAllocator.GetObjectCount())
	}
}

func BenchmarkFileBasedTrackerSet(b *testing.B) {
	tmpDir := b.TempDir()
	trackerPath := filepath.Join(tmpDir, "tracker.bin")

	file, err := os.Create(trackerPath)
	if err != nil {
		b.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		b.Fatalf("Failed to create tracker: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		objId := types.ObjectId(i)
		size := types.FileSize(i * 10)
		tracker.Set(objId, size)
	}
}

func BenchmarkFileBasedTrackerGet(b *testing.B) {
	tmpDir := b.TempDir()
	trackerPath := filepath.Join(tmpDir, "tracker.bin")

	file, err := os.Create(trackerPath)
	if err != nil {
		b.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	tracker, err := newFileBasedObjectTracker(file)
	if err != nil {
		b.Fatalf("Failed to create tracker: %v", err)
	}

	// Pre-populate with entries
	numEntries := 10000
	for i := 0; i < numEntries; i++ {
		tracker.Set(types.ObjectId(i), types.FileSize(i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		objId := types.ObjectId(i % numEntries)
		tracker.Get(objId)
	}
}

func BenchmarkMemoryTrackerSet(b *testing.B) {
	tracker := newMemoryObjectTracker()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		objId := types.ObjectId(i)
		size := types.FileSize(i * 10)
		tracker.Set(objId, size)
	}
}

func BenchmarkMemoryTrackerGet(b *testing.B) {
	tracker := newMemoryObjectTracker()

	// Pre-populate with entries
	numEntries := 10000
	for i := 0; i < numEntries; i++ {
		tracker.Set(types.ObjectId(i), types.FileSize(i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		objId := types.ObjectId(i % numEntries)
		tracker.Get(objId)
	}
}
