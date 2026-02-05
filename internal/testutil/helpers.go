package testutil

import (
	"os"
	"testing"
)

// CleanupTempFile removes a temporary file and its associated allocator metadata files.
// This ensures all test artifacts are cleaned up, including:
// - The data file itself (.dat)
// - The allocation store (.allocs.json)
// - The allocator index (.allocs.idx)
func CleanupTempFile(tb testing.TB, filePath string) {
	tb.Helper()

	files := []string{
		filePath,
		filePath + ".allocs.json",
		filePath + ".allocs.idx",
	}

	for _, f := range files {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			tb.Logf("warning: failed to remove %s: %v", f, err)
		}
	}
}
