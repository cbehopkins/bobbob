package vault

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// BenchmarkPersistWithBackgroundFlush_LargeDataset benchmarks Persist() performance
// with background memory monitoring enabled and a large dataset (>=200k nodes).
// It simulates typical production traffic: continuous inserts while the background
// monitor flushes under memory pressure.
func BenchmarkPersistWithBackgroundFlush_LargeDataset(b *testing.B) {
	sizes := []int{200_000}
	batchSize := 5_000

	for _, size := range sizes {
		b.Run("nodes="+strconv.Itoa(size), func(b *testing.B) {
			tempDir := b.TempDir()
			storePath := filepath.Join(tempDir, "bench_persist_bg.db")
			stre, err := store.NewBasicStore(storePath)
			if err != nil {
				b.Fatalf("Failed to create store: %v", err)
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
				v, "bench_bg", types.IntLess, (*types.IntKey)(new(int32)),
			)
			if err != nil {
				b.Fatalf("Failed to create collection: %v", err)
			}

			// Enable background monitoring with aggressive flushing.
			v.SetMemoryBudgetWithPercentile(10_000, 25)
			v.SetCheckInterval(1000)
			v.StartBackgroundMonitoring()

			// Initial load to reach large dataset size.
			for i := 0; i < size; i++ {
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
				// Insert a batch while background monitoring may flush.
				for j := 0; j < batchSize; j++ {
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

// BenchmarkPersistWithBackgroundFlush_LargeDataset_ConcurrentStore benchmarks Persist()
// using ConcurrentMultiStore, matching Vault's default store in production.
func BenchmarkPersistWithBackgroundFlush_LargeDataset_ConcurrentStore(b *testing.B) {
	sizes := []int{200_000}
	batchSize := 5_000

	for _, size := range sizes {
		b.Run("nodes="+strconv.Itoa(size), func(b *testing.B) {
			tempDir := b.TempDir()
			storePath := filepath.Join(tempDir, "bench_persist_bg_concurrent.db")
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
				v, "bench_bg", types.IntLess, (*types.IntKey)(new(int32)),
			)
			if err != nil {
				b.Fatalf("Failed to create collection: %v", err)
			}

			// Enable background monitoring with aggressive flushing.
			v.SetMemoryBudgetWithPercentile(10_000, 25)
			v.SetCheckInterval(1000)
			v.StartBackgroundMonitoring()

			// Initial load to reach large dataset size.
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
				// Insert a batch while background monitoring may flush.
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
