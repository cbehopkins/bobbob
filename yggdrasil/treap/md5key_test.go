package treap

import (
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// Test successful parsing of a valid 32-character hex string.
func TestMD5KeyFromStringSuccess(t *testing.T) {
	hexStr := "0123456789abcdeffedcba9876543210"

	key, err := types.MD5KeyFromString(hexStr)
	if err != nil {
		t.Fatalf("types.MD5KeyFromString returned error: %v", err)
	}

	expectedBytes, err := hex.DecodeString(hexStr)
	if err != nil {
		t.Fatalf("failed to decode hex: %v", err)
	}

	var expected types.MD5Key
	copy(expected[:], expectedBytes)

	if key != expected {
		t.Fatalf("parsed key mismatch: got %x, want %x", key, expected)
	}
}

// Test error on too-short or too-long hex strings.
func TestMD5KeyFromStringInvalidLength(t *testing.T) {
	_, err := types.MD5KeyFromString("abcd")
	if err == nil {
		t.Fatalf("expected length error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid md5 hex string length") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Test error when the hex string contains invalid characters.
func TestMD5KeyFromStringInvalidHex(t *testing.T) {
	_, err := types.MD5KeyFromString("0123456789abcdeffedcba98765432zz")
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
	var key types.MD5Key

	if err := key.Unmarshal(goodData); err != nil {
		t.Fatalf("unexpected error unmarshalling good data: %v", err)
	}
	expected := types.MD5Key{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
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

func TestMd5KeyFromBase64StringSuccess(t *testing.T) {
	hexStr := "0123456789abcdeffedcba9876543210"

	decodedHex, err := hex.DecodeString(hexStr)
	if err != nil {
		t.Fatalf("failed to decode hex: %v", err)
	}

	b64 := base64.RawStdEncoding.EncodeToString(decodedHex)

	key, err := types.Md5KeyFromBase64String(b64)
	if err != nil {
		t.Fatalf("types.Md5KeyFromBase64String returned error: %v", err)
	}

	var expected types.MD5Key
	copy(expected[:], decodedHex)

	if key != expected {
		t.Fatalf("parsed key mismatch: got %x, want %x", key, expected)
	}
}

func TestMd5KeyFromBase64StringInvalidLength(t *testing.T) {
	// base64 for 3 bytes, not 16
	_, err := types.Md5KeyFromBase64String("QUJD")
	if err == nil {
		t.Fatalf("expected error for wrong length, got nil")
	}
	if !strings.Contains(err.Error(), "expected 16 bytes") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMd5KeyFromBase64StringInvalidData(t *testing.T) {
	_, err := types.Md5KeyFromBase64String("***not-base64***")
	if err == nil {
		t.Fatalf("expected error for invalid base64, got nil")
	}
	if !strings.Contains(err.Error(), "invalid base64") {
		t.Fatalf("unexpected error: %v", err)
	}
}
