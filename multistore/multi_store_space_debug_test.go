package multistore

import (
	"os"
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/store"
)

// TestMultiStoreSpaceReuseDebug is a diagnostic version of TestMultiStoreSpaceReuse
// with detailed logging to understand space allocation behavior
func TestMultiStoreSpaceReuseDebug(t *testing.T) {
	filePath := "test_space_reuse_debug.dat"
	defer testutil.CleanupTempFile(t, filePath)

	ms, err := NewMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer ms.Close()

	size := 50
	numCycles := 3
	objectsPerCycle := 5

	// Get allocator stats before starting
	allocator := ms.Allocator()
	if allocator == nil {
		t.Fatal("Allocator is nil")
	}

	// Track file size growth
	initialSize := int64(0)
	allObjIds := make(map[int][]store.ObjectId) // Track which objects were allocated in each cycle

	for cycle := 0; cycle < numCycles; cycle++ {
		t.Logf("\n=== CYCLE %d START ===", cycle)

		// Allocate objects
		objIds := make([]store.ObjectId, objectsPerCycle)
		for i := range objIds {
			objId, err := ms.NewObj(size)
			if err != nil {
				t.Fatalf("Cycle %d: Failed to create object %d: %v", cycle, i, err)
			}
			objIds[i] = objId

			// Check what size was actually allocated
			info, err := ms.getObjectInfo(objId)
			if err != nil {
				t.Fatalf("Cycle %d: Failed to get object info for %d: %v", cycle, objId, err)
			}
			t.Logf("  Object %d: requested=%d, allocated=%d (block size), offset=%d",
				objId, size, info.Size, info.Offset)

			data := make([]byte, size)
			for j := range data {
				data[j] = byte(cycle*10 + i)
			}

			writer, finisher, err := ms.WriteToObj(objId)
			if err != nil {
				t.Fatalf("Cycle %d: Failed to get writer: %v", cycle, err)
			}
			if _, err := writer.Write(data); err != nil {
				t.Fatalf("Cycle %d: Failed to write: %v", cycle, err)
			}
			if err := finisher(); err != nil {
				t.Fatalf("Cycle %d: Failed to finish: %v", cycle, err)
			}
		}

		allObjIds[cycle] = objIds

		// Check file size after first cycle
		if cycle == 0 {
			fileInfo, err := os.Stat(filePath)
			if err != nil {
				t.Fatalf("Failed to stat file: %v", err)
			}
			initialSize = fileInfo.Size()
			t.Logf("Initial file size after first cycle: %d bytes", initialSize)
		}

		// Delete all objects from this cycle
		t.Logf("Deleting %d objects from cycle %d", len(objIds), cycle)
		for i, objId := range objIds {
			if err := ms.DeleteObj(objId); err != nil {
				t.Fatalf("Cycle %d: Failed to delete object %d: %v", cycle, i, err)
			}
		}

		// Wait for async deletions to complete before next cycle
		ms.flushDeletes()

		fileInfo, err := os.Stat(filePath)
		if err != nil {
			t.Fatalf("Failed to stat file: %v", err)
		}
		t.Logf("File size at end of cycle %d: %d bytes", cycle, fileInfo.Size())

		// Check for object ID reuse
		if cycle > 0 {
			reused := 0
			for _, newId := range allObjIds[cycle] {
				for prevCycle := 0; prevCycle < cycle; prevCycle++ {
					for _, oldId := range allObjIds[prevCycle] {
						if newId == oldId {
							reused++
							t.Logf("  ObjectId %d was REUSED from cycle %d!", newId, prevCycle)
						}
					}
				}
			}
			if reused == 0 {
				t.Logf("  WARNING: No ObjectIds were reused from previous cycles")
			} else {
				t.Logf("  Good: %d ObjectIds were reused", reused)
			}
		}
	}

	// Check final file size - it shouldn't have grown much if reuse is working
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}
	finalSize := fileInfo.Size()

	t.Logf("\n=== FINAL RESULTS ===")
	t.Logf("Initial file size (after cycle 0): %d bytes", initialSize)
	t.Logf("Final file size (after %d cycles): %d bytes", numCycles, finalSize)
	t.Logf("File grew by: %d bytes over %d cycles", finalSize-initialSize, numCycles-1)
	t.Logf("Growth factor: %.2fx", float64(finalSize)/float64(initialSize))

	// The file should not have grown proportionally to the number of cycles
	// Some growth is expected due to metadata, but not numCycles times the initial size
	maxExpectedSize := initialSize * int64(numCycles) / 2
	t.Logf("Max expected size (1.5x initial): %d bytes", maxExpectedSize)

	if finalSize > maxExpectedSize {
		t.Errorf("File grew too much (%d bytes), expected less than %d. Space reuse may not be working efficiently.",
			finalSize, maxExpectedSize)
		t.Logf("Expected: freed blocks from earlier cycles should be reused")
		t.Logf("Actual: file grew by %.0f%% instead of staying approximately constant",
			(float64(finalSize)/float64(initialSize)-1)*100)
	} else {
		t.Log("SUCCESS: Space reuse test passed")
	}
}
