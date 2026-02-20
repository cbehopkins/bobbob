package multistore

import (
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

type BenchmarkStruct struct {
	ID    int
	Name  string
	Email string
	Count int
}

// BenchmarkJsonPayload_StringStore benchmarks JsonPayload storage using StringStore.
// This measures the performance gain from using buffered writes instead of direct I/O.
func BenchmarkJsonPayload_StringStore(b *testing.B) {
	tmpFile := b.TempDir() + "/bench_jsonpayload.db"

	ms, err := NewMultiStore(tmpFile, 0)
	if err != nil {
		b.Fatalf("Failed to create multistore: %v", err)
	}
	defer ms.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		payload := types.JsonPayload[BenchmarkStruct]{
			Value: BenchmarkStruct{
				ID:    i,
				Name:  "User Name",
				Email: "user@example.com",
				Count: i * 10,
			},
		}

		objId, _, finisher := payload.LateMarshal(ms)
		if finisher != nil {
			if err := finisher(); err != nil {
				b.Fatalf("LateMarshal finisher failed: %v", err)
			}
		}

		var restored types.JsonPayload[BenchmarkStruct]
		finisher = restored.LateUnmarshal(objId, 0, ms)
		if finisher != nil {
			if err := finisher(); err != nil {
				b.Fatalf("LateUnmarshal finisher failed: %v", err)
			}
		}
	}
}

// BenchmarkStringPayload_StringStore benchmarks StringPayload storage (simple baseline).
func BenchmarkStringPayload_StringStore(b *testing.B) {
	tmpFile := b.TempDir() + "/bench_stringpayload.db"

	ms, err := NewMultiStore(tmpFile, 0)
	if err != nil {
		b.Fatalf("Failed to create multistore: %v", err)
	}
	defer ms.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		payload := types.StringPayload("This is a test string for benchmark")

		objId, _, finisher := payload.LateMarshal(ms)
		if finisher != nil {
			if err := finisher(); err != nil {
				b.Fatalf("LateMarshal finisher failed: %v", err)
			}
		}

		var restored types.StringPayload
		finisher = restored.LateUnmarshal(objId, 0, ms)
		if finisher != nil {
			if err := finisher(); err != nil {
				b.Fatalf("LateUnmarshal finisher failed: %v", err)
			}
		}
	}
}
