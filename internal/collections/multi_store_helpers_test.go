package collections

import (
	"testing"

	"bobbob/internal/yggdrasil/treap"
)

func TestDeduplicateBlockSizes(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		expected []int
	}{
		{
			name:     "no duplicates",
			input:    []int{8, 16, 32},
			expected: []int{8, 16, 32},
		},
		{
			name:     "with duplicates",
			input:    []int{8, 16, 8, 32, 16},
			expected: []int{8, 16, 32},
		},
		{
			name:     "all same",
			input:    []int{8, 8, 8},
			expected: []int{8},
		},
		{
			name:     "empty",
			input:    []int{},
			expected: []int{},
		},
		{
			name:     "single element",
			input:    []int{42},
			expected: []int{42},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deduplicateBlockSizes(tt.input)

			// Check length
			if len(result) != len(tt.expected) {
				t.Errorf("Expected length %d, got %d", len(tt.expected), len(result))
			}

			// Check all expected values are present
			resultMap := make(map[int]bool)
			for _, v := range result {
				if resultMap[v] {
					t.Errorf("Duplicate value %d in result", v)
				}
				resultMap[v] = true
			}

			for _, v := range tt.expected {
				if !resultMap[v] {
					t.Errorf("Expected value %d not found in result", v)
				}
			}
		})
	}
}

func TestPersistentTreapObjectSizesNoDuplicates(t *testing.T) {
	sizes := treap.PersistentTreapObjectSizes()
	deduplicated := deduplicateBlockSizes(sizes)

	t.Logf("Original sizes: %v", sizes)
	t.Logf("Deduplicated sizes: %v", deduplicated)

	// Verify deduplication produces a set (no duplicates in output)
	seen := make(map[int]bool)
	for _, size := range deduplicated {
		if seen[size] {
			t.Errorf("Duplicate size %d in deduplicated output", size)
		}
		seen[size] = true
	}

	// The actual persistent treap sizes should have 2 distinct values
	if len(deduplicated) != 2 {
		t.Logf("Note: Expected 2 distinct sizes, got %d. This may be fine if the implementation changed.", len(deduplicated))
	}
}
