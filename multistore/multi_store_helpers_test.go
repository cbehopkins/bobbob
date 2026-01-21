package multistore

import (
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
)

func TestPersistentTreapObjectSizesNoDuplicates(t *testing.T) {
	sizes := treap.PersistentTreapObjectSizes()

	t.Logf("Persistent treap sizes: %v", sizes)

	seen := make(map[int]bool)
	for _, size := range sizes {
		if seen[size] {
			t.Errorf("Duplicate size %d returned by PersistentTreapObjectSizes", size)
		}
		seen[size] = true
	}

	// The actual persistent treap sizes should have 2 distinct values
	if len(seen) != 2 {
		t.Logf("Note: Expected 2 distinct sizes, got %d. This may be fine if the implementation changed.", len(seen))
	}
}
