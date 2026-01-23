package allocator

import (
	"os"
	"testing"

	"github.com/cbehopkins/bobbob/allocator/types"
)

func TestTopNew(t *testing.T) {
	tmpFile, err := os.CreateTemp(os.TempDir(), "top_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	blockSizes := []int{64, 256, 1024}
	top, err := NewTop(tmpFile, blockSizes, 1024)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if top == nil {
		t.Fatal("expected non-nil Top allocator")
	}

	if top.basicAllocator == nil {
		t.Fatal("expected BasicAllocator to be initialized")
	}

	if top.omniAllocator == nil {
		t.Fatal("expected OmniAllocator to be initialized")
	}

	if top.primeTable == nil {
		t.Fatal("expected PrimeTable to be initialized")
	}

	if top.primeTable.NumEntries() != 2 {
		t.Errorf("expected PrimeTable to have 2 entries, got %d", top.primeTable.NumEntries())
	}
}

func TestTopNewNilFile(t *testing.T) {
	blockSizes := []int{64, 256}
	_, err := NewTop(nil, blockSizes, 1024)
	if err == nil {
		t.Fatal("expected error for nil file")
	}
}

func TestTopAllocate(t *testing.T) {
	tmpFile, err := os.CreateTemp(os.TempDir(), "top_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	blockSizes := []int{64, 256, 1024}
	top, err := NewTop(tmpFile, blockSizes, 1024)
	if err != nil {
		t.Fatalf("failed to create Top: %v", err)
	}

	// Allocate a fixed-size object (64 fits in 64 pool)
	_, _, err = top.Allocate(64)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify we can allocate multiple times
	_, _, err = top.Allocate(256)
	if err != nil {
		t.Fatalf("expected no error on second allocation, got %v", err)
	}
}

func TestTopDeleteObj(t *testing.T) {
	tmpFile, err := os.CreateTemp(os.TempDir(), "top_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	blockSizes := []int{64, 256, 1024}
	top, err := NewTop(tmpFile, blockSizes, 1024)
	if err != nil {
		t.Fatalf("failed to create Top: %v", err)
	}

	// Allocate an object that doesn't fit any pool (forces BasicAllocator)
	objId, _, err := top.Allocate(5000)
	if err != nil {
		t.Fatalf("failed to allocate: %v", err)
	}

	// Delete it
	err = top.DeleteObj(objId)
	if err != nil {
		t.Fatalf("expected no error deleting object, got %v", err)
	}

	// Verify it's gone
	if top.ContainsObjectId(objId) {
		t.Fatal("expected ObjectId to be deleted")
	}
}

func TestTopGetObjectInfo(t *testing.T) {
	tmpFile, err := os.CreateTemp(os.TempDir(), "top_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	blockSizes := []int{64, 256, 1024}
	top, err := NewTop(tmpFile, blockSizes, 1024)
	if err != nil {
		t.Fatalf("failed to create Top: %v", err)
	}

	// Allocate an object that doesn't fit any pool (forces BasicAllocator)
	requestSize := 5000
	objId, expectedOffset, err := top.Allocate(requestSize)
	if err != nil {
		t.Fatalf("failed to allocate: %v", err)
	}

	// Retrieve its info
	offset, size, err := top.GetObjectInfo(objId)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if offset != expectedOffset {
		t.Errorf("expected offset %d, got %d", expectedOffset, offset)
	}

	if int(size) != requestSize {
		t.Errorf("expected size %d, got %d", requestSize, size)
	}
}

func TestTopSaveLoad(t *testing.T) {
	tmpFile, err := os.CreateTemp(os.TempDir(), "top_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	blockSizes := []int{64, 256, 1024}

	// Create and allocate objects (use BasicAllocator path)
	top, err := NewTop(tmpFile, blockSizes, 1024)
	if err != nil {
		t.Fatalf("failed to create Top: %v", err)
	}

	objIds := make([]types.ObjectId, 0)
	for i := 0; i < 5; i++ {
		objId, _, err := top.Allocate(5000)
		if err != nil {
			t.Fatalf("failed to allocate: %v", err)
		}
		objIds = append(objIds, objId)
	}

	// Save to disk
	err = top.Save()
	if err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	// Load from disk
	top2, err := NewTopFromFile(tmpFile, blockSizes, 1024)
	if err != nil {
		t.Fatalf("failed to load: %v", err)
	}

	// Verify objects still exist
	for _, objId := range objIds {
		if !top2.ContainsObjectId(objId) {
			t.Errorf("expected ObjectId %d to exist after load", objId)
		}
	}

	// Verify object info is correct
	for _, objId := range objIds {
		_, size, err := top2.GetObjectInfo(objId)
		if err != nil {
			t.Errorf("expected no error getting object info, got %v", err)
		}

		if int(size) != 5000 {
			t.Errorf("expected size 5000, got %d", size)
		}
	}
}

func TestTopMultipleAllocations(t *testing.T) {
	tmpFile, err := os.CreateTemp(os.TempDir(), "top_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	blockSizes := []int{64, 256, 1024}
	top, err := NewTop(tmpFile, blockSizes, 1024)
	if err != nil {
		t.Fatalf("failed to create Top: %v", err)
	}

	// Allocate objects that don't fit any pool (forces BasicAllocator)
	objIds := make([]types.ObjectId, 0)
	sizes := []int{5000, 6000, 7000, 8000}

	for _, size := range sizes {
		objId, _, err := top.Allocate(size)
		if err != nil {
			t.Fatalf("failed to allocate %d bytes: %v", size, err)
		}
		objIds = append(objIds, objId)
	}

	// Verify all objects exist and have correct sizes
	for i, objId := range objIds {
		_, size, err := top.GetObjectInfo(objId)
		if err != nil {
			t.Fatalf("failed to get info for object %d: %v", objId, err)
		}

		expectedSize := sizes[i]
		if int(size) != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, size)
		}
	}
}

func TestTopContainsObjectId(t *testing.T) {
	tmpFile, err := os.CreateTemp(os.TempDir(), "top_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	blockSizes := []int{64, 256, 1024}
	top, err := NewTop(tmpFile, blockSizes, 1024)
	if err != nil {
		t.Fatalf("failed to create Top: %v", err)
	}

	objId, _, err := top.Allocate(5000)
	if err != nil {
		t.Fatalf("failed to allocate: %v", err)
	}

	if !top.ContainsObjectId(objId) {
		t.Fatal("expected ObjectId to be contained")
	}

	if top.ContainsObjectId(999999) {
		t.Fatal("expected non-existent ObjectId to not be contained")
	}
}

func TestTopGetFile(t *testing.T) {
	tmpFile, err := os.CreateTemp(os.TempDir(), "top_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	blockSizes := []int{64, 256, 1024}
	top, err := NewTop(tmpFile, blockSizes, 1024)
	if err != nil {
		t.Fatalf("failed to create Top: %v", err)
	}

	file := top.GetFile()
	if file != tmpFile {
		t.Fatal("expected GetFile to return the file handle")
	}
}

func TestTopImplementsTopAllocator(t *testing.T) {
	tmpFile, err := os.CreateTemp(os.TempDir(), "top_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	blockSizes := []int{64, 256, 1024}
	top, err := NewTop(tmpFile, blockSizes, 1024)
	if err != nil {
		t.Fatalf("failed to create Top: %v", err)
	}

	// Verify Top implements types.TopAllocator
	var _ types.TopAllocator = top
}
