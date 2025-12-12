package testutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/store"
)

// SetupTestStore creates a temporary directory and a new basic store for testing.
// Returns the directory path, the store, and a cleanup function.
// The cleanup function should be called with defer to ensure proper cleanup.
func SetupTestStore(tb testing.TB) (dir string, s store.Storer, cleanup func()) {
	tb.Helper()

	dir, err := os.MkdirTemp("", "bobbob_test_*")
	if err != nil {
		tb.Fatalf("failed to create temp dir: %v", err)
	}

	filePath := filepath.Join(dir, "testfile.bin")
	s, err = store.NewBasicStore(filePath)
	if err != nil {
		os.RemoveAll(dir)
		tb.Fatalf("failed to create store: %v", err)
	}

	cleanup = func() {
		if s != nil {
			s.Close()
		}
		os.RemoveAll(dir)
	}

	return dir, s, cleanup
}

// SetupConcurrentStore creates a temporary directory and a new concurrent store for testing.
// Returns the directory path, the store, and a cleanup function.
func SetupConcurrentStore(tb testing.TB) (dir string, s store.Storer, cleanup func()) {
	tb.Helper()

	dir, err := os.MkdirTemp("", "bobbob_test_*")
	if err != nil {
		tb.Fatalf("failed to create temp dir: %v", err)
	}

	filePath := filepath.Join(dir, "testfile.bin")
	s, err = store.NewConcurrentStore(filePath)
	if err != nil {
		os.RemoveAll(dir)
		tb.Fatalf("failed to create concurrent store: %v", err)
	}

	cleanup = func() {
		if s != nil {
			s.Close()
		}
		os.RemoveAll(dir)
	}

	return dir, s, cleanup
}

// CreateTempFile creates a temporary file for testing.
// Returns the file path and a cleanup function.
func CreateTempFile(tb testing.TB, pattern string) (filePath string, cleanup func()) {
	tb.Helper()

	dir, err := os.MkdirTemp("", "bobbob_test_*")
	if err != nil {
		tb.Fatalf("failed to create temp dir: %v", err)
	}

	filePath = filepath.Join(dir, pattern)

	cleanup = func() {
		os.RemoveAll(dir)
	}

	return filePath, cleanup
}
