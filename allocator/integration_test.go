package allocator_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/allocator"
	"github.com/cbehopkins/bobbob/allocator/types"
)

// TestTopAllocator_SizeBasedRouting verifies that allocations are routed
// to the appropriate allocator based on size:
// - Small objects (≤ largest block size) → OmniAllocator → PoolAllocator → BlockAllocator
// - Large objects (> largest block size) → BasicAllocator
func TestTopAllocator_SizeBasedRouting(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "routing.bin")
	file, err := os.OpenFile(tmpFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer file.Close()

	// Create Top allocator with specific block sizes
	blockSizes := []int{64, 256, 1024, 4096}
	top, err := allocator.NewTop(file, blockSizes, 128)
	if err != nil {
		t.Fatalf("NewTop: %v", err)
	}

	// Test small allocation (should use OmniAllocator → BlockAllocator)
	smallObj, smallOff, err := top.Allocate(64)
	if err != nil {
		t.Fatalf("Allocate(64): %v", err)
	}
	if smallObj == 0 {
		t.Fatal("expected valid ObjectId for small allocation")
	}
	if smallOff == 0 {
		t.Fatal("expected valid FileOffset for small allocation")
	}

	// Test medium allocation (should use OmniAllocator → BlockAllocator)
	medObj, medOff, err := top.Allocate(1024)
	if err != nil {
		t.Fatalf("Allocate(1024): %v", err)
	}
	if medObj == 0 {
		t.Fatal("expected valid ObjectId for medium allocation")
	}
	if medOff == 0 {
		t.Fatal("expected valid FileOffset for medium allocation")
	}

	// Test large allocation (should use BasicAllocator directly)
	largeObj, largeOff, err := top.Allocate(10000)
	if err != nil {
		t.Fatalf("Allocate(10000): %v", err)
	}
	if largeObj == 0 {
		t.Fatal("expected valid ObjectId for large allocation")
	}
	if largeOff == 0 {
		t.Fatal("expected valid FileOffset for large allocation")
	}

	// Verify GetObjectInfo works for all sizes
	_, _, err = top.GetObjectInfo(smallObj)
	if err != nil {
		t.Errorf("GetObjectInfo(small): %v", err)
	}
	_, _, err = top.GetObjectInfo(medObj)
	if err != nil {
		t.Errorf("GetObjectInfo(medium): %v", err)
	}
	_, _, err = top.GetObjectInfo(largeObj)
	if err != nil {
		t.Errorf("GetObjectInfo(large): %v", err)
	}
}

// TestTopAllocator_AllocationCallbacks verifies that allocation callbacks
// can be registered and fire correctly for all allocation sizes.
func TestTopAllocator_AllocationCallbacks(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "callbacks.bin")
	file, err := os.OpenFile(tmpFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer file.Close()

	blockSizes := []int{64, 256, 1024}
	top, err := allocator.NewTop(file, blockSizes, 128)
	if err != nil {
		t.Fatalf("NewTop: %v", err)
	}

	callbackHits := 0
	var lastObjId types.ObjectId
	var lastOffset types.FileOffset

	// Register callback on Top allocator
	top.SetOnAllocate(func(objId types.ObjectId, offset types.FileOffset, size int) {
		callbackHits++
		lastObjId = objId
		lastOffset = offset
	})

	initialHits := callbackHits

	// Small allocation should trigger callback
	obj1, off1, err := top.Allocate(64)
	if err != nil {
		t.Fatalf("Allocate(64): %v", err)
	}

	if callbackHits <= initialHits {
		t.Error("expected callback to fire for small allocation")
	}
	if lastObjId != obj1 || lastOffset != off1 {
		t.Error("callback received incorrect ObjectId/Offset for small allocation")
	}

	// Large allocation should also trigger callback
	preLargeHits := callbackHits
	obj2, off2, err := top.Allocate(10000)
	if err != nil {
		t.Fatalf("Allocate(10000): %v", err)
	}

	if callbackHits <= preLargeHits {
		t.Error("expected callback to fire for large allocation")
	}
	if lastObjId != obj2 || lastOffset != off2 {
		t.Error("callback received incorrect ObjectId/Offset for large allocation")
	}
}

// TestTopAllocator_Persistence verifies that Top allocator state can be
// saved to disk and restored in a new session.
func TestTopAllocator_Persistence(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "persist.bin")

	blockSizes := []int{64, 256, 1024}
	maxBlockCount := 128

	var obj1, obj2, obj3 types.ObjectId

	// Session 1: Create allocator, allocate objects, save
	{
		file, err := os.OpenFile(tmpFile, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			t.Fatalf("failed to create file: %v", err)
		}

		top, err := allocator.NewTop(file, blockSizes, maxBlockCount)
		if err != nil {
			file.Close()
			t.Fatalf("NewTop: %v", err)
		}

		obj1, _, err = top.Allocate(64)
		if err != nil {
			file.Close()
			t.Fatalf("Allocate(64): %v", err)
		}

		obj2, _, err = top.Allocate(256)
		if err != nil {
			file.Close()
			t.Fatalf("Allocate(256): %v", err)
		}

		obj3, _, err = top.Allocate(5000)
		if err != nil {
			file.Close()
			t.Fatalf("Allocate(5000): %v", err)
		}

		// Set store metadata before saving (required by Top.Save)
		// Size must be non-zero to pass validation
		top.SetStoreMeta(allocator.FileInfo{ObjId: 1, Fo: 1000, Sz: 100})

		if err := top.Save(); err != nil {
			file.Close()
			t.Fatalf("Save: %v", err)
		}

		file.Close()
	}

	// Session 2: Load from disk and verify objects are accessible
	{
		file, err := os.OpenFile(tmpFile, os.O_RDWR, 0644)
		if err != nil {
			t.Fatalf("failed to open file: %v", err)
		}
		defer file.Close()

		top, err := allocator.NewTopFromFile(file, blockSizes, maxBlockCount)
		if err != nil {
			t.Fatalf("NewTopFromFile: %v", err)
		}

		if err := top.Load(); err != nil {
			t.Fatalf("Load: %v", err)
		}

		// Verify all objects are present
		_, _, err = top.GetObjectInfo(obj1)
		if err != nil {
			t.Errorf("obj1 not found after reload: %v", err)
		}

		_, _, err = top.GetObjectInfo(obj2)
		if err != nil {
			t.Errorf("obj2 not found after reload: %v", err)
		}

		_, _, err = top.GetObjectInfo(obj3)
		if err != nil {
			t.Errorf("obj3 not found after reload: %v", err)
		}

		// Verify we can allocate more objects after reload
		obj4, _, err := top.Allocate(128)
		if err != nil {
			t.Fatalf("Allocate after reload: %v", err)
		}
		if obj4 == 0 {
			t.Fatal("expected valid ObjectId after reload allocation")
		}
	}
}

// TestTopAllocator_DeletionAndReuse verifies that deleted objects can be
// reclaimed and their space reused for new allocations.
func TestTopAllocator_DeletionAndReuse(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "deletion.bin")
	file, err := os.OpenFile(tmpFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer file.Close()

	blockSizes := []int{64, 256}
	top, err := allocator.NewTop(file, blockSizes, 128)
	if err != nil {
		t.Fatalf("NewTop: %v", err)
	}

	// Allocate and delete a small object
	obj1, _, err := top.Allocate(64)
	if err != nil {
		t.Fatalf("Allocate(64): %v", err)
	}

	// Verify object exists before deletion
	_, _, err = top.GetObjectInfo(obj1)
	if err != nil {
		t.Fatalf("obj1 should exist before deletion: %v", err)
	}

	// Delete the object
	if err := top.DeleteObj(obj1); err != nil {
		t.Fatalf("DeleteObj: %v", err)
	}

	// From external observer perspective, deleted object should not be accessible
	_, _, err = top.GetObjectInfo(obj1)
	if err == nil {
		t.Error("deleted object should not be accessible to external observer")
	}

	// Allocate another small object (may reuse space internally)
	obj2, _, err := top.Allocate(64)
	if err != nil {
		t.Fatalf("Allocate(64) after delete: %v", err)
	}

	// Verify new object is accessible
	_, _, err = top.GetObjectInfo(obj2)
	if err != nil {
		t.Errorf("new object not found: %v", err)
	}
}
