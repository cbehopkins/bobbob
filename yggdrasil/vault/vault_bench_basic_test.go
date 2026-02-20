package vault

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// BenchmarkPersist_NoStringStore_ConcurrentStore benchmarks Persist() using
// ConcurrentMultiStore WITHOUT using StringStore (simulates old code).
func BenchmarkPersist_NoStringStore_ConcurrentStore(b *testing.B) {
	sizes := []int{50_000} // Smaller to complete faster
	batchSize := 5_000

	for _, size := range sizes {
		b.Run("nodes="+strconv.Itoa(size), func(b *testing.B) {
			tempDir := b.TempDir()
			storePath := filepath.Join(tempDir, "bench_persist_nostring_concurrent.db")
			stre, err := multistore.NewConcurrentMultiStore(storePath, 0)
			if err != nil {
				b.Fatalf("Failed to create concurrent multistore: %v", err)
			}
			defer stre.Close()

			v, err := LoadVault(stre)
			if err != nil {
				b.Fatalf("Failed to load vault: %v", err)
			}
			defer v.Close()

			v.RegisterType((*types.IntKey)(new(int32)))
			v.RegisterType(types.JsonPayload[string]{})

			coll, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[string]](
				v, "bench_nostring", types.IntLess, (*types.IntKey)(new(int32)),
			)
			if err != nil {
				b.Fatalf("Failed to create collection: %v", err)
			}

			// Initial load
			for i := range size {
				key := types.IntKey(i)
				payload := types.JsonPayload[string]{Value: "v_" + strconv.Itoa(i)}
				coll.Insert(&key, payload)
			}

			if err := coll.Persist(); err != nil {
				b.Fatalf("Initial Persist failed: %v", err)
			}

			nextKey := size

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for range batchSize {
					key := types.IntKey(nextKey)
					payload := types.JsonPayload[string]{Value: "v_" + strconv.Itoa(nextKey)}
					coll.Insert(&key, payload)
					nextKey++
				}

				if err := coll.Persist(); err != nil {
					b.Fatalf("Persist failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkPersist_BasicStore benchmark Control: using BasicStore (not ConcurrentStore)
func BenchmarkPersist_BasicStore(b *testing.B) {
	sizes := []int{50_000}
	batchSize := 5_000

	for _, size := range sizes {
		b.Run("nodes="+strconv.Itoa(size), func(b *testing.B) {
			tempDir := b.TempDir()
			storePath := filepath.Join(tempDir, "bench_persist_basic.db")
			stre, err := store.NewBasicStore(storePath)
			if err != nil {
				b.Fatalf("Failed to create basic store: %v", err)
			}
			defer stre.Close()

			v, err := LoadVault(stre)
			if err != nil {
				b.Fatalf("Failed to load vault: %v", err)
			}
			defer v.Close()

			v.RegisterType((*types.IntKey)(new(int32)))
			v.RegisterType(types.JsonPayload[string]{})

			coll, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[string]](
				v, "bench_basic", types.IntLess, (*types.IntKey)(new(int32)),
			)
			if err != nil {
				b.Fatalf("Failed to create collection: %v", err)
			}

			// Initial load
			for i := range size {
				key := types.IntKey(i)
				payload := types.JsonPayload[string]{Value: "v_" + strconv.Itoa(i)}
				coll.Insert(&key, payload)
			}

			if err := coll.Persist(); err != nil {
				b.Fatalf("Initial Persist failed: %v", err)
			}

			nextKey := size

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for range batchSize {
					key := types.IntKey(nextKey)
					payload := types.JsonPayload[string]{Value: "v_" + strconv.Itoa(nextKey)}
					coll.Insert(&key, payload)
					nextKey++
				}

				if err := coll.Persist(); err != nil {
					b.Fatalf("Persist failed: %v", err)
				}
			}
		})
	}
}
