package testutil

import (
	cryptorand "crypto/rand"
	"testing"
)

// RandomBytes generates a slice of random bytes of the specified length.
func RandomBytes(tb testing.TB, length int) []byte {
	tb.Helper()

	data := make([]byte, length)
	_, err := cryptorand.Read(data)
	if err != nil {
		tb.Fatalf("failed to generate random bytes: %v", err)
	}
	return data
}

// RandomBytesSeeded generates deterministic random bytes for reproducible tests.
// Uses a simple seed-based generation for test reproducibility.
func RandomBytesSeeded(length int, seed byte) []byte {
	data := make([]byte, length)
	for i := range data {
		data[i] = byte((int(seed) + i) % 256)
	}
	return data
}
