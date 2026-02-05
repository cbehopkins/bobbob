package treap

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// BenchmarkFlushOldestPercentile measures the cost of flushing oldest nodes.
// Tests various tree sizes and flush percentages to identify performance issues.
func BenchmarkFlushOldestPercentile(b *testing.B) {
	sizes := []int{100, 500, 1000, 5000}
	percentages := []int{10, 25, 50, 75}

	for _, size := range sizes {
		for _, pct := range percentages {
			b.Run(fmt.Sprintf("Size%d_Flush%dpct", size, pct), func(b *testing.B) {
				benchFlushOldestPercentile(b, size, pct)
			})
		}
	}
}

// BenchmarkFlushOldestPercentile_LargeTree measures flush performance on large trees
// to identify scaling issues with the sorting algorithm.
func BenchmarkFlushOldestPercentile_LargeTree(b *testing.B) {
	sizes := []int{10000, 50000}
	percentages := []int{10, 50}

	for _, size := range sizes {
		for _, pct := range percentages {
			b.Run(fmt.Sprintf("Size%d_Flush%dpct", size, pct), func(b *testing.B) {
				benchFlushOldestPercentile(b, size, pct)
			})
		}
	}
}

// BenchmarkFlushOldestPercentile_SmallFlush tests efficiency when flushing small percentages
func BenchmarkFlushOldestPercentile_SmallFlush(b *testing.B) {
	size := 5000
	percentages := []int{1, 5, 10}

	for _, pct := range percentages {
		b.Run(fmt.Sprintf("Size%d_Flush%dpct", size, pct), func(b *testing.B) {
			benchFlushOldestPercentile(b, size, pct)
		})
	}
}

// BenchmarkFlushOldestPercentile_Persist measures the persist overhead in flush
func BenchmarkFlushOldestPercentile_Persist(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-flush")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	filePath := filepath.Join(dir, "test.bin")
	storeObj, err := store.NewBasicStore(filePath)
	if err != nil {
		b.Fatal(err)
	}
	defer storeObj.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		storeObj,
	)

	// Build tree
	size := 1000
	for i := 0; i < size; i++ {
		key := types.IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("payload_%d", i)}
		treap.Insert(&key, payload)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Reset tree for next iteration
		treap = NewPersistentPayloadTreap[types.IntKey, MockPayload](
			types.IntLess,
			(*types.IntKey)(new(int32)),
			storeObj,
		)
		for j := 0; j < size; j++ {
			key := types.IntKey(j)
			payload := MockPayload{Data: fmt.Sprintf("payload_%d", j)}
			treap.Insert(&key, payload)
		}
		b.StartTimer()

		_, _ = treap.FlushOldestPercentile(50)
	}
}

// BenchmarkFlushOldestPercentile_SortOnly isolates the sorting cost
func BenchmarkFlushOldestPercentile_SortOnly(b *testing.B) {
	sizes := []int{1000, 5000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			// Pre-allocate and populate nodes with timestamps
			nodes := make([]PayloadNodeInfo[types.IntKey, MockPayload], size)
			for i := 0; i < size; i++ {
				nodes[i].LastAccessTime = int64(i) // Sequential timestamps
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Shuffle the timestamps to simulate randomness
				testNodes := make([]PayloadNodeInfo[types.IntKey, MockPayload], size)
				copy(testNodes, nodes)
				// Simple shuffle
				for j := 0; j < size; j++ {
					newTime := int64((j*17 + i*13) % size)
					testNodes[j].LastAccessTime = newTime
				}
				b.StartTimer()

				// Use sort.Slice (introsort) like the actual implementation
				sort.Slice(testNodes, func(a, c int) bool {
					return testNodes[a].LastAccessTime < testNodes[c].LastAccessTime
				})
			}
		})
	}
}

// Helper to run a flush benchmark
func benchFlushOldestPercentile(b *testing.B, size, percentage int) {
	dir, err := os.MkdirTemp("", "bench-flush")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	filePath := filepath.Join(dir, "test.bin")
	storeObj, err := store.NewBasicStore(filePath)
	if err != nil {
		b.Fatal(err)
	}
	defer storeObj.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		storeObj,
	)

	// Build tree
	for i := 0; i < size; i++ {
		key := types.IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("payload_%d", i)}
		treap.Insert(&key, payload)
	}

	// Persist to ensure all nodes are in stable state
	_ = treap.Persist()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Reset tree for next iteration
		treap = NewPersistentPayloadTreap[types.IntKey, MockPayload](
			types.IntLess,
			(*types.IntKey)(new(int32)),
			storeObj,
		)
		for j := 0; j < size; j++ {
			key := types.IntKey(j)
			payload := MockPayload{Data: fmt.Sprintf("payload_%d", j)}
			treap.Insert(&key, payload)
		}
		_ = treap.Persist()
		b.StartTimer()

		_, _ = treap.FlushOldestPercentile(percentage)
	}
}
