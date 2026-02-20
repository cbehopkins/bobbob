package stringstore

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/store"
)

func BenchmarkWriteSingleShard(b *testing.B) {
	cfg := Config{
		FilePath:           filepath.Join(b.TempDir(), "bench.blob"),
		MaxNumberOfStrings: 1_000_000,
		StartingObjectId:   100,
		ObjectIdInterval:   4,
	}
	storeInstance, err := NewStringStore(cfg)
	if err != nil {
		b.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = storeInstance.Close() }()

	payload := bytes.Repeat([]byte("x"), 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.WriteNewObjFromBytes(storeInstance, payload); err != nil {
			b.Fatalf("write failed: %v", err)
		}
	}
}

func BenchmarkWriteRollover(b *testing.B) {
	cfg := Config{
		FilePath:           filepath.Join(b.TempDir(), "bench.blob"),
		MaxNumberOfStrings: 128,
		StartingObjectId:   100,
		ObjectIdInterval:   4,
	}
	storeInstance, err := NewStringStore(cfg)
	if err != nil {
		b.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = storeInstance.Close() }()

	payload := bytes.Repeat([]byte("x"), 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.WriteNewObjFromBytes(storeInstance, payload); err != nil {
			b.Fatalf("write failed: %v", err)
		}
	}
}

func BenchmarkCompact(b *testing.B) {
	cfg := Config{
		FilePath:           filepath.Join(b.TempDir(), "bench.blob"),
		MaxNumberOfStrings: 4096,
		StartingObjectId:   100,
		ObjectIdInterval:   4,
	}
	storeInstance, err := NewStringStore(cfg)
	if err != nil {
		b.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = storeInstance.Close() }()

	payload := bytes.Repeat([]byte("x"), 128)
	for i := 0; i < 2048; i++ {
		id, err := store.WriteNewObjFromBytes(storeInstance, payload)
		if err != nil {
			b.Fatalf("write failed: %v", err)
		}
		if i%2 == 0 {
			_ = storeInstance.DeleteObj(id)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := storeInstance.Compact(); err != nil {
			b.Fatalf("compact failed: %v", err)
		}
	}
}
