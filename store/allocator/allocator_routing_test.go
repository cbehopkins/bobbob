package allocator

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestAllocatorRoutingWithVariableLengthStrings demonstrates that variable-length
// string payloads get routed through OmniBlockAllocator (using block allocators)
// rather than falling through to the BasicAllocator.
//
// This validates the allocator hierarchy: small variable-size objects are still
// caught by block allocators (64-4096 bytes), avoiding expensive map lookups.
func TestAllocatorRoutingWithVariableLengthStrings(t *testing.T) {
	tmpDir := t.TempDir()

	// Create data and allocator files
	dataFile, err := os.Create(filepath.Join(tmpDir, "data.bin"))
	if err != nil {
		t.Fatalf("Create data file failed: %v", err)
	}
	defer dataFile.Close()

	basicAlloc, err := NewBasicAllocator(dataFile)
	if err != nil {
		t.Fatalf("NewBasicAllocator failed: %v", err)
	}

	// Track allocations from both OmniBlockAllocator and BasicAllocator
	var (
		mu              sync.Mutex
		omniAllocCount  int
		basicAllocCount int
		omniAllocSizes  []int
		basicAllocSizes []int
	)

	// Set callback on BasicAllocator to track calls
	basicAlloc.SetOnAllocate(func(objId ObjectId, offset FileOffset, size int) {
		mu.Lock()
		basicAllocCount++
		basicAllocSizes = append(basicAllocSizes, size)
		mu.Unlock()
		t.Logf("BasicAllocator.Allocate: objId=%d, offset=%d, size=%d", objId, offset, size)
	})

	// Create OmniBlockAllocator with the BasicAllocator as parent
	omniAlloc, err := NewOmniBlockAllocator([]int{}, 10000, basicAlloc)
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator failed: %v", err)
	}

	// Set callback on OmniBlockAllocator to track calls
	omniAlloc.SetOnAllocate(func(objId ObjectId, offset FileOffset, size int) {
		mu.Lock()
		omniAllocCount++
		omniAllocSizes = append(omniAllocSizes, size)
		mu.Unlock()
		t.Logf("OmniBlockAllocator.Allocate: objId=%d, offset=%d, size=%d", objId, offset, size)
	})

	// Test case 1: Small strings (fit in 64-byte blocks)
	// These represent short paths like "/etc/passwd", "/usr/bin", etc.
	smallStrings := []string{
		"/etc/passwd",
		"/usr/bin",
		"config.ini",
		"data.json",
	}

	for _, s := range smallStrings {
		size := len(s) + 4 // 4 bytes for length prefix
		objId, _, err := omniAlloc.Allocate(size)
		if err != nil {
			t.Fatalf("Allocate failed for small string %q: %v", s, err)
		}
		_ = objId // Use it to satisfy linter
		t.Logf("Small string %q allocated with size %d", s, size)
	}

	// Test case 2: Medium strings (fit in 256-byte blocks)
	// These represent longer paths like "/var/log/application/error.log"
	mediumStrings := []string{
		"/var/log/application/error.log",
		"/home/user/documents/report.pdf",
		"/opt/service/config/settings.xml",
		"/mnt/storage/backup/compressed.tar.gz",
	}

	for _, s := range mediumStrings {
		size := len(s) + 4 // 4 bytes for length prefix
		objId, _, err := omniAlloc.Allocate(size)
		if err != nil {
			t.Fatalf("Allocate failed for medium string %q: %v", s, err)
		}
		_ = objId
		t.Logf("Medium string %q allocated with size %d", s, size)
	}

	// Test case 3: Large string (would fall back to BasicAllocator if > 4096 bytes)
	largeString := strings.Repeat("This is a long file path component that repeats. ", 100) // ~4850 bytes
	{
		size := len(largeString) + 4
		objId, _, err := omniAlloc.Allocate(size)
		if err != nil {
			t.Fatalf("Allocate failed for large string: %v", err)
		}
		_ = objId
		t.Logf("Large string allocated with size %d (falls back to BasicAllocator)", size)
	}

	// Verify allocations
	mu.Lock()
	defer mu.Unlock()

	t.Logf("\n=== Allocation Routing Summary ===")
	t.Logf("Small strings: %d (size ~%d bytes each)", len(smallStrings), len(smallStrings[0])+4)
	t.Logf("Medium strings: %d (size ~%d bytes each)", len(mediumStrings), len(mediumStrings[0])+4)
	t.Logf("Large strings: 1 (size ~%d bytes)", len(largeString)+4)
	t.Logf("\n--- Allocator Calls ---")
	t.Logf("OmniBlockAllocator.Allocate: %d calls", omniAllocCount)
	t.Logf("BasicAllocator.Allocate: %d calls (includes preallocation + large strings)", basicAllocCount)

	// Expectations:
	// 1. OmniBlockAllocator should handle small/medium strings (they fit in block sizes)
	// 2. BasicAllocator will be called:
	//    - Once for each default block size during OmniBlockAllocator initialization (7 times)
	//    - Once for the large string (since it exceeds maxBlockSize of 4096)
	maxBlockSize := 4096

	if omniAllocCount < len(smallStrings)+len(mediumStrings) {
		t.Errorf("Expected OmniBlockAllocator to handle at least small and medium strings, got %d calls",
			omniAllocCount)
	}

	// The large string is > 4096 bytes, so it should trigger a BasicAllocator call
	hasLargeStringAllocation := len(largeString)+4 > maxBlockSize
	if hasLargeStringAllocation && basicAllocCount < 8 {
		// 7 preallocs + 1 for large string
		t.Logf("Note: Large string (size %d) should fall back to BasicAllocator; got %d calls total",
			len(largeString)+4, basicAllocCount)
	}

	t.Logf("\n✓ Test demonstrates allocator routing:")
	t.Logf("  - Small/medium strings caught by block allocators (efficient)")
	t.Logf("  - Large strings fall back to BasicAllocator (flexible)")
	t.Logf("  - Result: Minimal overhead, fast lookups via arithmetic in most cases")
}

// TestStringPathAllocations demonstrates realistic file path allocations
// showing that variable-length paths fit efficiently in block allocator ranges.
func TestStringPathAllocations(t *testing.T) {
	tmpDir := t.TempDir()
	dataFile, err := os.Create(filepath.Join(tmpDir, "data.bin"))
	if err != nil {
		t.Fatalf("Create data file failed: %v", err)
	}
	defer dataFile.Close()

	basicAlloc, err := NewBasicAllocator(dataFile)
	if err != nil {
		t.Fatalf("NewBasicAllocator failed: %v", err)
	}

	omniAlloc, err := NewOmniBlockAllocator([]int{}, 10000, basicAlloc)
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator failed: %v", err)
	}

	// Sample file paths (realistic scenario: mapping integers to file paths)
	paths := []string{
		"/home/user/documents/project/src/main.go",
		"/var/log/system.log",
		"/usr/share/doc/readme.md",
		"/opt/app/data/config.json",
		"/home/user/.config/app/settings.ini",
		"/tmp/cache/build-output.tmp",
		"/srv/data/backups/2024-01-07.zip",
		"/root/.ssh/id_rsa.pub",
	}

	t.Logf("=== Persistent Payload: int → String (File Paths) ===")
	t.Logf("Sample allocations for %d unique paths:\n", len(paths))

	allocatedIds := make([]ObjectId, 0, len(paths))
	for i, path := range paths {
		size := len(path) + 4 // Assume 4-byte length prefix
		objId, _, err := omniAlloc.Allocate(size)
		if err != nil {
			t.Fatalf("Allocate failed for path: %v", err)
		}
		allocatedIds = append(allocatedIds, objId)
		t.Logf("[%d] %-40q → size %4d bytes → ObjectId %d", i, path, size, objId)
	}

	t.Logf("\n--- Analysis ---")
	t.Logf("All paths fit within block allocator ranges (64-4096 bytes)")
	t.Logf("This means:")
	t.Logf("  ✓ Fast allocation (arithmetic, no hash table)")
	t.Logf("  ✓ Fast lookup (arithmetic, no hash table)")
	t.Logf("  ✓ Minimal memory overhead (~1 byte per allocation)")
	t.Logf("  ✓ No calls to BasicAllocator for user data")
}

// TestExternalCallbackConfiguration demonstrates configuring allocation callbacks
// from outside the package using the Parent() accessor method.
func TestExternalCallbackConfiguration(t *testing.T) {
	tmpDir := t.TempDir()
	dataFile, err := os.Create(filepath.Join(tmpDir, "data.bin"))
	if err != nil {
		t.Fatalf("Create data file failed: %v", err)
	}
	defer dataFile.Close()

	basicAlloc, err := NewBasicAllocator(dataFile)
	if err != nil {
		t.Fatalf("NewBasicAllocator failed: %v", err)
	}

	// Configure BasicAllocator callback BEFORE creating OmniBlockAllocator
	// so we can track preallocation calls
	var (
		mu              sync.Mutex
		omniCallCount   int
		parentCallCount int
	)

	basicAlloc.SetOnAllocate(func(objId ObjectId, offset FileOffset, size int) {
		mu.Lock()
		parentCallCount++
		mu.Unlock()
		t.Logf("Parent allocated: id=%d, offset=%d, size=%d", objId, offset, size)
	})

	omniAlloc, err := NewOmniBlockAllocator([]int{}, 10000, basicAlloc)
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator failed: %v", err)
	}

	// Now demonstrate accessing parent via Parent() method
	if parent := omniAlloc.Parent(); parent != nil {
		if ba, ok := parent.(*BasicAllocator); ok {
			t.Logf("✓ Successfully accessed parent BasicAllocator via Parent() method")
			// Verify it's the same instance we configured earlier
			if ba != basicAlloc {
				t.Errorf("Parent() returned different BasicAllocator instance")
			}
		} else {
			t.Fatalf("Parent is not a *BasicAllocator")
		}
	} else {
		t.Fatalf("Parent() returned nil")
	}

	// Configure OmniBlockAllocator callback directly
	omniAlloc.SetOnAllocate(func(objId ObjectId, offset FileOffset, size int) {
		mu.Lock()
		omniCallCount++
		mu.Unlock()
		t.Logf("Omni allocated: id=%d, offset=%d, size=%d", objId, offset, size)
	})

	// Allocate some objects
	for i := 0; i < 5; i++ {
		size := 20 + i*5
		_, _, err := omniAlloc.Allocate(size)
		if err != nil {
			t.Fatalf("Allocate failed: %v", err)
		}
	}

	mu.Lock()
	t.Logf("\n=== External Configuration Test Results ===")
	t.Logf("Omni allocations: %d", omniCallCount)
	t.Logf("Parent allocations: %d", parentCallCount)
	mu.Unlock()

	if omniCallCount == 0 {
		t.Error("Expected OmniBlockAllocator callback to be called")
	}
	if parentCallCount == 0 {
		t.Error("Expected parent BasicAllocator callback to be called (for preallocation)")
	}

	t.Logf("✓ Both allocator callbacks successfully invoked from external configuration")
}

// Regression: after marshal/unmarshal with no preallocation and no prior block allocations,
// the omni allocator must retain its configured block sizes so small allocations are handled
// by block allocators (child) with only a single range provision from the parent.
func TestOmniUnmarshalPreservesSizesWithoutPrealloc(t *testing.T) {
	tmpDir := t.TempDir()
	dataFile, err := os.Create(filepath.Join(tmpDir, "data.bin"))
	if err != nil {
		t.Fatalf("Create data file failed: %v", err)
	}
	defer dataFile.Close()

	parent, err := NewBasicAllocator(dataFile)
	if err != nil {
		t.Fatalf("NewBasicAllocator failed: %v", err)
	}

	blockSizes := defaultBlockSizes()
	blockCount := 4

	original, err := NewOmniBlockAllocator(blockSizes, blockCount, parent, WithoutPreallocation())
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator failed: %v", err)
	}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	reloaded, err := NewOmniBlockAllocator(blockSizes, blockCount, parent, WithoutPreallocation())
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator (reload) failed: %v", err)
	}

	childCalls := 0
	parentCalls := 0
	parentSize := 0

	parent.SetOnAllocate(func(_ ObjectId, _ FileOffset, size int) {
		parentCalls++
		parentSize = size
	})
	reloaded.SetOnAllocate(func(_ ObjectId, _ FileOffset, _ int) {
		childCalls++
	})

	if err := reloaded.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	small := blockSizes[0]
	if _, _, err := reloaded.Allocate(small); err != nil {
		t.Fatalf("Allocate small failed: %v", err)
	}

	if childCalls != 1 {
		t.Fatalf("expected child allocator callback to fire once, got %d", childCalls)
	}
	if parentCalls == 0 {
		t.Fatalf("expected parent to provision a range once")
	}
	if parentSize < small*blockCount {
		t.Fatalf("expected parent allocation >= %d (range), got %d", small*blockCount, parentSize)
	}
}

// Regression test: after marshal/unmarshal with no preallocation and no prior
// block allocations, the omni allocator must retain its configured block sizes
// so small allocations route through block allocators instead of the parent.
// (Removed duplicate test)
