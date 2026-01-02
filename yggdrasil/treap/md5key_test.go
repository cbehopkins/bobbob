package treap

import (
	"encoding/hex"
	"strings"
	"testing"
)

// Test successful parsing of a valid 32-character hex string.
func TestMD5KeyFromStringSuccess(t *testing.T) {
	hexStr := "0123456789abcdeffedcba9876543210"

	key, err := MD5KeyFromString(hexStr)
	if err != nil {
		t.Fatalf("MD5KeyFromString returned error: %v", err)
	}

	expectedBytes, err := hex.DecodeString(hexStr)
	if err != nil {
		t.Fatalf("failed to decode hex: %v", err)
	}

	var expected MD5Key
	copy(expected[:], expectedBytes)

	if key != expected {
		t.Fatalf("parsed key mismatch: got %x, want %x", key, expected)
	}
}

// Test error on too-short or too-long hex strings.
func TestMD5KeyFromStringInvalidLength(t *testing.T) {
	_, err := MD5KeyFromString("abcd")
	if err == nil {
		t.Fatalf("expected length error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid md5 hex string length") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Test error when the hex string contains invalid characters.
func TestMD5KeyFromStringInvalidHex(t *testing.T) {
	_, err := MD5KeyFromString("0123456789abcdeffedcba98765432zz")
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to parse hex") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Test successful and failing unmarshalling paths.
func TestMD5KeyUnmarshal(t *testing.T) {
	goodData := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	var key MD5Key

	if err := key.Unmarshal(goodData); err != nil {
		t.Fatalf("unexpected error unmarshalling good data: %v", err)
	}
	expected := MD5Key{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	if key != expected {
		t.Fatalf("unmarshal mismatch: got %v, want %v", key, expected)
	}

	badData := []byte{0, 1, 2}
	if err := key.Unmarshal(badData); err == nil {
		t.Fatalf("expected error unmarshalling short data, got nil")
	} else if err.Error() != "MD5Key must be exactly 16 bytes" {
		t.Fatalf("unexpected error: %v", err)
	}
}
