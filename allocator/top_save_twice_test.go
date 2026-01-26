package allocator

import (
	"os"
	"testing"
)

// TestTopSaveLoadTwice tests saving, loading, modifying, and saving again.
// This mimics the vault persistence test pattern.
func TestTopSaveLoadTwice(t *testing.T) {
	tmpDir := t.TempDir()
	tmpPath := tmpDir + "/test_twice.db"

	blockSizes := []int{64, 256, 1024}

	// Session 1: Create and save
	{
		file, err := os.Create(tmpPath)
		if err != nil {
			t.Fatalf("failed to create file: %v", err)
		}

		top, err := NewTop(file, blockSizes, 1024)
		if err != nil {
			t.Fatalf("failed to create Top: %v", err)
		}

		// Allocate some objects
		for i := 0; i < 5; i++ {
			_, _, err := top.Allocate(5000)
			if err != nil {
				t.Fatalf("failed to allocate: %v", err)
			}
		}

		// Set store metadata and save
		top.SetStoreMeta(FileInfo{ObjId: 0, Fo: 0, Sz: 1})
		err = top.Save()
		if err != nil {
			t.Fatalf("failed to save in session 1: %v", err)
		}

		err = file.Close()
		if err != nil {
			t.Fatalf("failed to close file in session 1: %v", err)
		}
	}

	// Session 2: Load, modify, and save
	{
		file, err := os.OpenFile(tmpPath, os.O_RDWR, 0o666)
		if err != nil {
			t.Fatalf("failed to open file in session 2: %v", err)
		}

		top, err := NewTopFromFile(file, blockSizes, 1024)
		if err != nil {
			t.Fatalf("failed to load Top in session 2: %v", err)
		}

		// Allocate more objects
		for i := 0; i < 3; i++ {
			_, _, err := top.Allocate(3000)
			if err != nil {
				t.Fatalf("failed to allocate in session 2: %v", err)
			}
		}

		// Set store metadata and save again
		top.SetStoreMeta(FileInfo{ObjId: 0, Fo: 0, Sz: 1})
		err = top.Save()
		if err != nil {
			t.Fatalf("failed to save in session 2: %v", err)
		}

		err = file.Close()
		if err != nil {
			t.Fatalf("failed to close file in session 2: %v", err)
		}
	}

	// Session 3: Load again to verify
	{
		file, err := os.OpenFile(tmpPath, os.O_RDWR, 0o666)
		if err != nil {
			t.Fatalf("failed to open file in session 3: %v", err)
		}
		defer file.Close()

		top, err := NewTopFromFile(file, blockSizes, 1024)
		if err != nil {
			t.Fatalf("failed to load Top in session 3: %v", err)
		}

		// Should be able to allocate more
		_, _, err = top.Allocate(1000)
		if err != nil {
			t.Fatalf("failed to allocate in session 3: %v", err)
		}
	}
}
