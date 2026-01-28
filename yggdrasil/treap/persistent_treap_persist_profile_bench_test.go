package treap_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	collections "github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// fixedPayload is a simple constant-size payload for persistence benchmarks.
type fixedPayload struct {
	A uint64
	B uint64
}

func (p fixedPayload) Marshal() ([]byte, error) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], p.A)
	binary.LittleEndian.PutUint64(buf[8:16], p.B)
	return buf, nil
}

func (p fixedPayload) Unmarshal(data []byte) (types.UntypedPersistentPayload, error) {
	if len(data) < 16 {
		return nil, errors.New("insufficient data for payload")
	}
	return fixedPayload{
		A: binary.LittleEndian.Uint64(data[0:8]),
		B: binary.LittleEndian.Uint64(data[8:16]),
	}, nil
}

func (p fixedPayload) SizeInBytes() int {
	return 16
}

// BenchmarkPersistLargeTrees benchmarks the Persist operation on large trees
// to identify bottlenecks, particularly in AllocatorIndex.Get() operations.
// This benchmark creates trees of various sizes (1K to 100K nodes) and measures
// the time taken to persist them to disk.
//
// To run with CPU profiling:
//
//	go test -bench=BenchmarkPersistLargeTrees -benchtime=10x -cpuprofile=cpu.prof ./yggdrasil/treap
//	go tool pprof cpu.prof
//
// To run with memory profiling:
//
//	go test -bench=BenchmarkPersistLargeTrees -benchtime=10x -memprofile=mem.prof ./yggdrasil/treap
//	go tool pprof mem.prof
//
// To run with specific size:
//
//	go test -bench=BenchmarkPersistLargeTrees/nodes=10000 -benchtime=5x ./yggdrasil/treap
func BenchmarkPersistLargeTrees(b *testing.B) {
	// Test with progressively larger trees to expose scalability issues
	nodeCounts := []int{
		1000,    // 1K nodes
		5000,    // 5K nodes
		10000,   // 10K nodes
		50000,   // 50K nodes
		100000,  // 100K nodes
		500000,  // 500K nodes - testing for searchDisk bottleneck
		1000000, // 1M nodes - large scale test
	}

	for _, nodeCount := range nodeCounts {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			// Create a fresh MultiStore for each sub-benchmark
			// Using blockSize=0 means no memory budget/auto-flush - persist only happens when explicitly called
			ms, err := collections.NewMultiStore(filepath.Join(b.TempDir(), "persist_profile.bin"), 0)
			if err != nil {
				b.Fatalf("failed to create multistore: %v", err)
			}
			defer ms.Close()

			var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

			// Report memory usage before benchmark
			var memStatsBefore runtime.MemStats
			runtime.ReadMemStats(&memStatsBefore)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				// Create treap and populate it with nodes
				pt := treap.NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms)
				for j := 0; j < nodeCount; j++ {
					key := (*types.IntKey)(new(int32))
					*key = types.IntKey(j)
					pt.Insert(key)
				}

				// Force GC before timing to get consistent measurements
				runtime.GC()

				b.StartTimer()
				// This is what we're benchmarking - the Persist call
				// Expected bottleneck: AllocatorIndex.Get() -> searchDisk() calls
				if err := pt.Persist(); err != nil {
					b.Fatalf("Persist failed: %v", err)
				}
				b.StopTimer()
			}

			// Report memory usage after benchmark
			var memStatsAfter runtime.MemStats
			runtime.ReadMemStats(&memStatsAfter)
			b.ReportMetric(float64(memStatsAfter.Alloc-memStatsBefore.Alloc)/float64(b.N), "B/persist")
		})
	}
}

// BenchmarkPersistLargeTreesWithPayload benchmarks Persist on payload treaps to confirm
// if the bottleneck is consistent across different node types.
func BenchmarkPersistLargeTreesWithPayload(b *testing.B) {
	nodeCounts := []int{
		1000,
		10000,
		50000,
		500000, // Large scale test
	}

	for _, nodeCount := range nodeCounts {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			ms, err := collections.NewMultiStore(filepath.Join(b.TempDir(), "persist_payload_profile.bin"), 0)
			if err != nil {
				b.Fatalf("failed to create multistore: %v", err)
			}
			defer ms.Close()

			var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				pt := treap.NewPersistentPayloadTreap[types.IntKey, fixedPayload](types.IntLess, keyTemplate, ms)
				for j := 0; j < nodeCount; j++ {
					key := (*types.IntKey)(new(int32))
					*key = types.IntKey(j)
					pt.Insert(key, fixedPayload{A: uint64(j), B: uint64(j * 10)})
				}

				runtime.GC()
				b.StartTimer()

				if err := pt.Persist(); err != nil {
					b.Fatalf("Persist failed: %v", err)
				}

				b.StopTimer()
			}
		})
	}
}

// BenchmarkPersistSequentialVsRandom compares persist performance with sequential vs random keys
// to see if key ordering affects the bottleneck.
func BenchmarkPersistSequentialVsRandom(b *testing.B) {
	nodeCount := 10000

	b.Run("Sequential_Keys", func(b *testing.B) {
		ms, err := collections.NewMultiStore(filepath.Join(b.TempDir(), "sequential.bin"), 0)
		if err != nil {
			b.Fatalf("failed to create multistore: %v", err)
		}
		defer ms.Close()

		var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			pt := treap.NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms)
			for j := 0; j < nodeCount; j++ {
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

	b.Run("Random_Keys", func(b *testing.B) {
		ms, err := collections.NewMultiStore(filepath.Join(b.TempDir(), "random.bin"), 0)
		if err != nil {
			b.Fatalf("failed to create multistore: %v", err)
		}
		defer ms.Close()

		var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			pt := treap.NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms)
			// Insert in pseudo-random order using simple permutation
			for j := 0; j < nodeCount; j++ {
				key := (*types.IntKey)(new(int32))
				// Simple permutation: (j * 7919) % nodeCount gives pseudo-random distribution
				*key = types.IntKey((j * 7919) % nodeCount)
				pt.Insert(key)
			}
			b.StartTimer()

			if err := pt.Persist(); err != nil {
				b.Fatalf("Persist failed: %v", err)
			}
		}
	})
}
