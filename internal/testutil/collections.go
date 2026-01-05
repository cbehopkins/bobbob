package testutil

import (
	"path/filepath"
	"testing"
)

// SetupMultiStore creates a temporary directory and a new multiStore for testing.
// Returns the directory path, the store, and a cleanup function.
// Note: This uses a type from internal/collections, so it must be in a separate file
// to avoid import cycles.
type MultiStoreCreator func(filePath string) (interface{ Close() error }, error)

// SetupMultiStoreWith creates a multi-store using the provided constructor function.
func SetupMultiStoreWith(tb testing.TB, creator MultiStoreCreator) (dir string, store interface{ Close() error }, cleanup func()) {
	tb.Helper()

	dir = tb.TempDir()
	filePath := filepath.Join(dir, "test_multi_store.bin")

	s, err := creator(filePath)
	if err != nil {
		tb.Fatalf("failed to create multi store: %v", err)
	}

	cleanup = func() {
		if s != nil {
			_ = s.Close()
		}
	}

	return dir, s, cleanup
}
