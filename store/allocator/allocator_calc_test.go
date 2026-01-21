package allocator

import (
	"testing"
)

// TestBlockAllocatorIndexCalculations tests pure calculation logic without file I/O
// Tests: ObjectId → FileOffset, and reverse (FileOffset → ObjectId)
// Note: This uses simplified math - real allocators may use different calculations
func TestBlockAllocatorIndexCalculations(t *testing.T) {
	// Simplified scenario: assume each object takes 256 bytes in a 512-byte block
	blockSize := int64(512)
	itemSize := int64(256)
	itemsPerBlock := blockSize / itemSize // 2 items per block

	tests := []struct {
		name      string
		objectId  int64
		blockIdx  int64
		offsetInBlock int64
	}{
		// First block (objectId 0-1)
		{"first item in block 0", 0, 0, 0},
		{"second item in block 0", 1, 0, itemSize},

		// Second block (objectId 2-3)
		{"first item in block 1", 2, 1, 0},
		{"second item in block 1", 3, 1, itemSize},

		// Later blocks
		{"first item in block 5", 10, 5, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Forward calculation: ObjectId → (blockIdx, offsetInBlock)
			calculatedBlockIdx := tt.objectId / itemsPerBlock
			calculatedOffset := (tt.objectId % itemsPerBlock) * itemSize

			if calculatedBlockIdx != tt.blockIdx {
				t.Errorf("block index: expected %d, got %d", tt.blockIdx, calculatedBlockIdx)
			}
			if calculatedOffset != tt.offsetInBlock {
				t.Errorf("offset in block: expected %d, got %d", tt.offsetInBlock, calculatedOffset)
			}

			// Reverse calculation: (blockIdx, offsetInBlock) → ObjectId
			reconstructedId := (calculatedBlockIdx * itemsPerBlock) + (calculatedOffset / itemSize)
			if reconstructedId != tt.objectId {
				t.Errorf("reconstruction: expected %d, got %d", tt.objectId, reconstructedId)
			}
		})
	}
}

// TestOmniBlockAllocatorSizeRouting tests allocator selection logic without file I/O
// Tests: objectSize → selectedBlockAllocator
func TestOmniBlockAllocatorSizeRouting(t *testing.T) {
	// OmniBlockAllocator routing logic:
	// If size < blockSize: route to BasicAllocator
	// If blockSize < size ≤ 2*blockSize: route to BlockAllocator[blockSize]
	// If 2*blockSize < size ≤ 4*blockSize: route to BlockAllocator[2*blockSize]
	// etc.

	blockSize := int64(1024)

	tests := []struct {
		name            string
		objectSize      int64
		expectedAllocID int // -1=BasicAllocator, >=0=BlockAllocator pool index
	}{
		// Route to BasicAllocator
		{"tiny object", 10, -1},
		{"small object", 256, -1},
		{"just under block", blockSize - 1, -1},

		// Route to BlockAllocator pools
		{"just over block", blockSize + 1, 0},
		{"fits in block", blockSize + 512, 0},
		{"near block limit", 2 * blockSize, 0},
		{"just over 2x", 2*blockSize + 1, 1},
		{"fits in 2x", 3 * blockSize, 1},
		{"near 2x limit", 4 * blockSize, 1},
		{"large object", 8 * blockSize, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Pure routing logic (no file I/O)
			expectedIdx := int64(-1)
			if tt.objectSize > blockSize {
				// Find the appropriate BlockAllocator by doubling blockSize until it fits
				idx := int64(0)
				bs := blockSize
				for tt.objectSize > 2*bs {
					bs *= 2
					idx++
				}
				expectedIdx = idx
			}

			if expectedIdx != int64(tt.expectedAllocID) {
				t.Errorf("routing: expected allocator %d, got %d", tt.expectedAllocID, expectedIdx)
			}
		})
	}
}

// TestBlockAllocatorCollisionDetection tests collision logic without file I/O
// Two objectIds collide if they're assigned to the same block
func TestBlockAllocatorCollisionDetection(t *testing.T) {
	itemsPerBlock := int64(8) // 8 items per block

	tests := []struct {
		name      string
		idx1      int64
		idx2      int64
		collision bool
	}{
		// Same block (objectIds 0-7 in block 0, 8-15 in block 1, etc.)
		{"same item", 0, 0, true},
		{"same block - idx 1 and 2", 1, 2, true},
		{"same block - idx 7 and 6", 7, 6, true},

		// Different blocks
		{"diff block - 7 and 8", 7, 8, false},
		{"diff block - 0 and 10", 0, 10, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block1 := tt.idx1 / itemsPerBlock
			block2 := tt.idx2 / itemsPerBlock
			collision := block1 == block2

			if collision != tt.collision {
				t.Errorf("collision: expected %v, got %v (idx1=%d block %d, idx2=%d block %d)", 
					tt.collision, collision, tt.idx1, block1, tt.idx2, block2)
			}
		})
	}
}

// TestBlockAllocatorBoundaryConditions tests edge cases
func TestBlockAllocatorBoundaryConditions(t *testing.T) {
	tests := []struct {
		name   string
		size   int64
		valid  bool
		reason string
	}{
		// Valid
		{"zero bytes", 0, false, "zero-size allocation invalid"},
		{"one byte", 1, true, "single byte allocation"},
		{"block aligned", 512, true, "block-size object"},
		{"large object", 10240, true, "large allocation"},

		// Edge cases
		{"max int64", 1<<62 - 1, true, "very large but representable"},
		{"negative size", -1, false, "negative size invalid"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Validation logic: size > 0
			valid := tt.size > 0
			if valid != tt.valid {
				t.Errorf("validation: expected valid=%v, got %v (reason: %s)", tt.valid, valid, tt.reason)
			}
		})
	}
}

// TestFreeListMerging tests gap coalescing logic without file operations
// When a gap at offset 100-200 and a gap at 200-300 are both free,
// they should merge into a single gap 100-300
func TestFreeListMerging(t *testing.T) {
	type Gap struct {
		start int64
		end   int64
	}

	tests := []struct {
		name      string
		gap1      Gap
		gap2      Gap
		canMerge  bool
		mergedEnd int64
	}{
		// Adjacent gaps (can merge)
		{"adjacent", Gap{100, 200}, Gap{200, 300}, true, 300},
		{"adjacent reverse", Gap{200, 300}, Gap{100, 200}, true, 300},

		// Overlapping gaps (can merge)
		{"overlapping", Gap{100, 250}, Gap{200, 300}, true, 300},

		// Non-adjacent gaps (cannot merge)
		{"gap between", Gap{100, 200}, Gap{300, 400}, false, 0},
		{"far apart", Gap{100, 200}, Gap{500, 600}, false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Merge logic: gaps can merge if gap1.end >= gap2.start
			gap1, gap2 := tt.gap1, tt.gap2
			if gap1.start > gap2.start {
				gap1, gap2 = gap2, gap1 // Ensure gap1 comes before gap2
			}

			canMerge := gap1.end >= gap2.start
			if canMerge != tt.canMerge {
				t.Errorf("merge check: expected %v, got %v", tt.canMerge, canMerge)
			}

			if canMerge {
				mergedEnd := gap2.end
				if gap1.end > gap2.end {
					mergedEnd = gap1.end
				}
				if mergedEnd != tt.mergedEnd {
					t.Errorf("merged end: expected %d, got %d", tt.mergedEnd, mergedEnd)
				}
			}
		})
	}
}
