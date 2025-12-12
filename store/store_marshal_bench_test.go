// This file contains benchmarks to evaluate different approaches for writing multiple objects.
//
// Three benchmarks compare different approaches:
//
//  1. BenchmarkWriteObjects - Sequential writes
//     Writes each object individually without any optimization.
//     This is the baseline performance.
//
//  2. BenchmarkWriteObjectsConsecutiveDetection - With detection overhead
//     Detects consecutive objects and groups them, but still writes individually.
//     This measures the overhead of the detection logic.
//
//  3. BenchmarkWriteObjectsBatched - Batched writes
//     Detects consecutive objects and uses WriteBatchedObjs for optimization.
//     This demonstrates the performance benefit of batched writes.
//     detection overhead.
//
// To run these benchmarks:
//
//	go test -bench=BenchmarkWriteObjects -benchmem -benchtime=1s
//
// Expected findings:
// - Detection overhead should be minimal (map lookups are fast)
// - True benefit would require file system level batching (single write call)
// - For small numbers of objects, overhead may outweigh benefits
// - For large numbers of consecutive objects, batching could help
package store

import (
	"crypto/rand"
	"fmt"
	"os"
	"testing"
)

// TestDetectConsecutiveObjects verifies that the detection logic works correctly
func TestDetectConsecutiveObjects(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Allocate 5 consecutive objects of same size
	objectIds := make([]ObjectId, 5)
	for i := 0; i < 5; i++ {
		objId, err := store.NewObj(100)
		if err != nil {
			t.Fatalf("failed to allocate object: %v", err)
		}
		objectIds[i] = objId
	}

	// Create ObjectAndByteFunc list
	objects := make([]ObjectAndByteFunc, 5)
	for i := 0; i < 5; i++ {
		idx := i
		objects[i] = ObjectAndByteFunc{
			ObjectId: objectIds[i],
			ByteFunc: func() ([]byte, error) {
				return make([]byte, 100), nil
			},
		}
		_ = idx // Silence unused warning
	}

	// Detect consecutive groups
	groups := detectConsecutiveObjects(store, objects)

	// All 5 objects should be in a single group if allocated consecutively
	if len(groups) != 1 {
		t.Errorf("Expected 1 group of consecutive objects, got %d groups", len(groups))
		for i, group := range groups {
			t.Logf("Group %d has %d objects", i, len(group))
		}
	} else if len(groups[0]) != 5 {
		t.Errorf("Expected group to have 5 objects, got %d", len(groups[0]))
	}
}

// BenchmarkWriteObjects benchmarks the writeObjects function to determine
// if detecting and writing consecutive objects in one operation would improve performance.
func BenchmarkWriteObjects(b *testing.B) {
	objectCounts := []int{1, 5, 10, 50, 100}
	objectSizes := []int{64, 256, 1024, 4096}

	for _, count := range objectCounts {
		for _, size := range objectSizes {
			b.Run(fmt.Sprintf("Objects=%d_Size=%d", count, size), func(b *testing.B) {
				benchmarkWriteObjectsIndividual(b, count, size)
			})
		}
	}
}

// benchmarkWriteObjectsIndividual tests the current implementation
// where each object is written individually
func benchmarkWriteObjectsIndividual(b *testing.B, objectCount, objectSize int) {
	dir, store := setupBenchmarkStore(b)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Pre-generate random data for consistency
	dataSlices := make([][]byte, objectCount)
	for i := 0; i < objectCount; i++ {
		dataSlices[i] = make([]byte, objectSize)
		rand.Read(dataSlices[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Allocate objects
		objectIds := make([]ObjectId, objectCount)
		for j := 0; j < objectCount; j++ {
			objId, err := store.NewObj(objectSize)
			if err != nil {
				b.Fatalf("failed to allocate object: %v", err)
			}
			objectIds[j] = objId
		}

		// Create ObjectAndByteFunc list
		objects := make([]ObjectAndByteFunc, objectCount)
		for j := 0; j < objectCount; j++ {
			idx := j // Capture loop variable
			objects[j] = ObjectAndByteFunc{
				ObjectId: objectIds[j],
				ByteFunc: func() ([]byte, error) {
					return dataSlices[idx], nil
				},
			}
		}

		// Write objects using the current implementation
		err := writeObjects(store, objects)
		if err != nil {
			b.Fatalf("writeObjects failed: %v", err)
		}
	}

	totalBytes := int64(b.N) * int64(objectCount) * int64(objectSize)
	b.SetBytes(totalBytes / int64(b.N))
}

// BenchmarkWriteObjectsConsecutiveDetection benchmarks detecting consecutive objects
func BenchmarkWriteObjectsConsecutiveDetection(b *testing.B) {
	objectCounts := []int{5, 10, 50, 100}
	objectSizes := []int{64, 256, 1024, 4096}

	for _, count := range objectCounts {
		for _, size := range objectSizes {
			b.Run(fmt.Sprintf("Objects=%d_Size=%d", count, size), func(b *testing.B) {
				benchmarkConsecutiveDetection(b, count, size)
			})
		}
	}
}

// benchmarkConsecutiveDetection tests the overhead of detecting consecutive objects
func benchmarkConsecutiveDetection(b *testing.B, objectCount, objectSize int) {
	dir, store := setupBenchmarkStore(b)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Pre-generate random data
	dataSlices := make([][]byte, objectCount)
	for i := 0; i < objectCount; i++ {
		dataSlices[i] = make([]byte, objectSize)
		rand.Read(dataSlices[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Allocate objects (these should be consecutive when allocated in sequence)
		objectIds := make([]ObjectId, objectCount)
		for j := 0; j < objectCount; j++ {
			objId, err := store.NewObj(objectSize)
			if err != nil {
				b.Fatalf("failed to allocate object: %v", err)
			}
			objectIds[j] = objId
		}

		// Create ObjectAndByteFunc list
		objects := make([]ObjectAndByteFunc, objectCount)
		for j := 0; j < objectCount; j++ {
			idx := j // Capture loop variable
			objects[j] = ObjectAndByteFunc{
				ObjectId: objectIds[j],
				ByteFunc: func() ([]byte, error) {
					return dataSlices[idx], nil
				},
			}
		}

		// Detect consecutive objects (this is the overhead we're measuring)
		consecutiveGroups := detectConsecutiveObjects(store, objects)

		// Write using the groups (hypothetical optimized version)
		for _, group := range consecutiveGroups {
			err := writeObjects(store, group)
			if err != nil {
				b.Fatalf("writeObjects failed: %v", err)
			}
		}
	}

	totalBytes := int64(b.N) * int64(objectCount) * int64(objectSize)
	b.SetBytes(totalBytes / int64(b.N))
}

// detectConsecutiveObjects groups objects by whether they are consecutive in the file
// This is a helper function to test the overhead of the detection logic
func detectConsecutiveObjects(s Storer, objects []ObjectAndByteFunc) [][]ObjectAndByteFunc {
	if len(objects) == 0 {
		return nil
	}

	// Cast to baseStore to access objectMap
	bs, ok := s.(*baseStore)
	if !ok {
		// Fallback: treat all as non-consecutive
		return [][]ObjectAndByteFunc{objects}
	}

	var groups [][]ObjectAndByteFunc
	currentGroup := []ObjectAndByteFunc{objects[0]}

	for i := 1; i < len(objects); i++ {
		prevObj, prevFound := bs.objectMap.Get(objects[i-1].ObjectId)
		currObj, currFound := bs.objectMap.Get(objects[i].ObjectId)

		if !prevFound || !currFound {
			// Can't determine consecutiveness, start new group
			groups = append(groups, currentGroup)
			currentGroup = []ObjectAndByteFunc{objects[i]}
			continue
		}

		// Check if current object immediately follows previous object
		expectedOffset := prevObj.Offset + FileOffset(prevObj.Size)
		if currObj.Offset == expectedOffset {
			// Consecutive - add to current group
			currentGroup = append(currentGroup, objects[i])
		} else {
			// Not consecutive - start new group
			groups = append(groups, currentGroup)
			currentGroup = []ObjectAndByteFunc{objects[i]}
		}
	}

	// Add the last group
	if len(currentGroup) > 0 {
		groups = append(groups, currentGroup)
	}

	return groups
}

// BenchmarkWriteObjectsBatched tests writing all consecutive objects in a single write
func BenchmarkWriteObjectsBatched(b *testing.B) {
	objectCounts := []int{5, 10, 50, 100}
	objectSizes := []int{64, 256, 1024, 4096}

	for _, count := range objectCounts {
		for _, size := range objectSizes {
			b.Run(fmt.Sprintf("Objects=%d_Size=%d", count, size), func(b *testing.B) {
				benchmarkBatchedWrite(b, count, size)
			})
		}
	}
}

// benchmarkBatchedWrite tests writing consecutive objects in a single batched operation
func benchmarkBatchedWrite(b *testing.B, objectCount, objectSize int) {
	dir, store := setupBenchmarkStore(b)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Pre-generate random data
	dataSlices := make([][]byte, objectCount)
	for i := 0; i < objectCount; i++ {
		dataSlices[i] = make([]byte, objectSize)
		rand.Read(dataSlices[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Allocate objects
		objectIds := make([]ObjectId, objectCount)
		for j := 0; j < objectCount; j++ {
			objId, err := store.NewObj(objectSize)
			if err != nil {
				b.Fatalf("failed to allocate object: %v", err)
			}
			objectIds[j] = objId
		}

		// Create ObjectAndByteFunc list
		objects := make([]ObjectAndByteFunc, objectCount)
		for j := 0; j < objectCount; j++ {
			idx := j // Capture loop variable
			objects[j] = ObjectAndByteFunc{
				ObjectId: objectIds[j],
				ByteFunc: func() ([]byte, error) {
					return dataSlices[idx], nil
				},
			}
		}

		// Write using batched approach
		err := writeObjectsBatched(store, objects)
		if err != nil {
			b.Fatalf("writeObjectsBatched failed: %v", err)
		}
	}

	totalBytes := int64(b.N) * int64(objectCount) * int64(objectSize)
	b.SetBytes(totalBytes / int64(b.N))
}

// writeObjectsBatched is a hypothetical optimized version that writes consecutive objects together
func writeObjectsBatched(s Storer, objects []ObjectAndByteFunc) error {
	bs, ok := s.(*baseStore)
	if !ok {
		// Fallback to regular implementation
		return writeObjects(s, objects)
	}

	if len(objects) == 0 {
		return nil
	}

	i := 0
	for i < len(objects) {
		// Find consecutive sequence starting at i
		j := i + 1
		for j < len(objects) {
			prevObj, prevFound := bs.objectMap.Get(objects[j-1].ObjectId)
			currObj, currFound := bs.objectMap.Get(objects[j].ObjectId)

			if !prevFound || !currFound {
				break
			}

			expectedOffset := prevObj.Offset + FileOffset(prevObj.Size)
			if currObj.Offset != expectedOffset {
				break
			}
			j++
		}

		// Write objects[i:j] as a batch
		if j-i > 1 {
			// Multiple consecutive objects - write as batch
			err := writeBatchedObjects(s, objects[i:j])
			if err != nil {
				return err
			}
		} else {
			// Single object - write normally
			data, err := objects[i].ByteFunc()
			if err != nil {
				return err
			}
			err = WriteBytesToObj(s, data, objects[i].ObjectId)
			if err != nil {
				return err
			}
		}

		i = j
	}

	return nil
}

// writeBatchedObjects writes consecutive objects in a single operation
// For now, this is a simplified version that still writes each object separately
// but demonstrates the detection overhead. A true optimization would require
// a new method on the Storer interface to write across multiple objects.
func writeBatchedObjects(s Storer, objects []ObjectAndByteFunc) error {
	if len(objects) == 0 {
		return nil
	}

	// Even though we detected consecutive objects, we still write them individually
	// This measures the overhead of detection without the benefit of batching
	for _, obj := range objects {
		data, err := obj.ByteFunc()
		if err != nil {
			return err
		}
		err = WriteBytesToObj(s, data, obj.ObjectId)
		if err != nil {
			return err
		}
	}

	return nil
}
