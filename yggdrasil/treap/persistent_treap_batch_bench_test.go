package treap_test

import (
	"fmt"
	"path/filepath"
	"testing"

	collections "github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// BenchmarkPersistentTreapPersist benchmarks the regular Persist method.
func BenchmarkPersistentTreapPersist(b *testing.B) {
	for _, count := range []int{10, 50, 100, 500} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			ms, err := collections.NewMultiStore(filepath.Join(b.TempDir(), "persist_bench.bin"), 0)
			if err != nil {
				b.Fatalf("failed to create multistore: %v", err)
			}
			defer ms.Close()

			var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				pt := treap.NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms)
				for j := 0; j < count; j++ {
					key := (*types.IntKey)(new(int32))
					*key = types.IntKey(j)
					pt.Insert(key)
				}
				b.StartTimer()

				if err := pt.Persist(); err != nil {
					b.Fatalf("Persist failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkPersistentTreapBatchPersist benchmarks the BatchPersist method.
func BenchmarkPersistentTreapBatchPersist(b *testing.B) {
	for _, count := range []int{10, 50, 100, 500} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			ms, err := collections.NewMultiStore(filepath.Join(b.TempDir(), "batch_bench.bin"), 0)
			if err != nil {
				b.Fatalf("failed to create multistore: %v", err)
			}
			defer ms.Close()

			var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				pt := treap.NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms)
				for j := 0; j < count; j++ {
					key := (*types.IntKey)(new(int32))
					*key = types.IntKey(j)
					pt.Insert(key)
				}
				b.StartTimer()

				if err := pt.BatchPersist(); err != nil {
					b.Fatalf("BatchPersist failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkPersistentPayloadTreapPersist benchmarks regular Persist for payload treaps.
func BenchmarkPersistentPayloadTreapPersist(b *testing.B) {
	for _, count := range []int{10, 50, 100} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			ms, err := collections.NewMultiStore(filepath.Join(b.TempDir(), "payload_persist_bench.bin"), 0)
			if err != nil {
				b.Fatalf("failed to create multistore: %v", err)
			}
			defer ms.Close()

			var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				pt := treap.NewPersistentPayloadTreap[types.IntKey, fixedPayload](types.IntLess, keyTemplate, ms)
				for j := 0; j < count; j++ {
					key := (*types.IntKey)(new(int32))
					*key = types.IntKey(j)
					pt.Insert(key, fixedPayload{A: uint64(j), B: uint64(j * 10)})
				}
				b.StartTimer()

				if err := pt.Persist(); err != nil {
					b.Fatalf("Persist failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkPersistentPayloadTreapBatchPersist benchmarks BatchPersist for payload treaps.
func BenchmarkPersistentPayloadTreapBatchPersist(b *testing.B) {
	for _, count := range []int{10, 50, 100} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			ms, err := collections.NewMultiStore(filepath.Join(b.TempDir(), "payload_batch_bench.bin"), 0)
			if err != nil {
				b.Fatalf("failed to create multistore: %v", err)
			}
			defer ms.Close()

			var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				pt := treap.NewPersistentPayloadTreap[types.IntKey, fixedPayload](types.IntLess, keyTemplate, ms)
				for j := 0; j < count; j++ {
					key := (*types.IntKey)(new(int32))
					*key = types.IntKey(j)
					pt.Insert(key, fixedPayload{A: uint64(j), B: uint64(j * 10)})
				}
				b.StartTimer()

				if err := pt.BatchPersist(); err != nil {
					b.Fatalf("BatchPersist failed: %v", err)
				}
			}
		})
	}
}
