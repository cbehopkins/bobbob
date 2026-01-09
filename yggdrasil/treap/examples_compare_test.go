package treap

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// ExampleTreap_Compare demonstrates how to compare two treaps and find keys that are:
// - Only in the first treap
// - Present in both treaps
// - Only in the second treap
func ExampleTreap_Compare() {
	// Create two treaps
	treapA := NewTreap(types.IntLess)
	treapB := NewTreap(types.IntLess)

	// Populate treapA with some keys
	for _, v := range []int{1, 2, 3, 4, 5} {
		treapA.Insert(types.IntKey(v))
	}

	// Populate treapB with overlapping keys
	for _, v := range []int{3, 4, 5, 6, 7} {
		treapB.Insert(types.IntKey(v))
	}

	// Compare the treaps
	err := treapA.Compare(treapB,
		func(node TreapNodeInterface[types.IntKey]) error {
			fmt.Printf("Only in A: %d\n", node.GetKey().Value())
			return nil
		},
		func(nodeA, nodeB TreapNodeInterface[types.IntKey]) error {
			fmt.Printf("In both: %d\n", nodeA.GetKey().Value())
			return nil
		},
		func(node TreapNodeInterface[types.IntKey]) error {
			fmt.Printf("Only in B: %d\n", node.GetKey().Value())
			return nil
		},
	)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	// Output:
	// Only in A: 1
	// Only in A: 2
	// In both: 3
	// In both: 4
	// In both: 5
	// Only in B: 6
	// Only in B: 7
}

// ExampleTreap_Compare_selective demonstrates using nil callbacks to only handle specific cases
func ExampleTreap_Compare_selective() {
	treapA := NewTreap(types.IntLess)
	treapB := NewTreap(types.IntLess)

	for _, v := range []int{1, 2, 3, 4, 5} {
		treapA.Insert(types.IntKey(v))
	}

	for _, v := range []int{4, 5, 6, 7, 8} {
		treapB.Insert(types.IntKey(v))
	}

	// Only interested in keys that exist in both treaps
	err := treapA.Compare(treapB,
		nil, // Don't care about keys only in A
		func(nodeA, nodeB TreapNodeInterface[types.IntKey]) error {
			fmt.Printf("Common key: %d\n", nodeA.GetKey().Value())
			return nil
		},
		nil, // Don't care about keys only in B
	)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	// Output:
	// Common key: 4
	// Common key: 5
}

// FileMetadata represents metadata about a file
type FileMetadata struct {
	Name string
	Size int64
}

// Marshal implements types.PersistentPayload interface
func (f *FileMetadata) Marshal() ([]byte, error) {
	data := fmt.Sprintf("%s:%d", f.Name, f.Size)
	return []byte(data), nil
}

// Unmarshal implements types.PersistentPayload interface
func (f *FileMetadata) Unmarshal(data []byte) (types.UntypedPersistentPayload, error) {
	var name string
	var size int64
	_, err := fmt.Sscanf(string(data), "%s:%d", &name, &size)
	if err != nil {
		return nil, err
	}
	f.Name = name
	f.Size = size
	return f, nil
}

// SizeInBytes implements types.PersistentPayload interface
func (f *FileMetadata) SizeInBytes() int {
	data, err := f.Marshal()
	if err != nil {
		return 0
	}
	return len(data)
}

// FileCollection wraps a PersistentPayloadTreap for file metadata
type FileCollection struct {
	treap *PersistentPayloadTreap[types.MD5Key, *FileMetadata]
}

// NewFileCollection creates a new file collection with the given store
func NewFileCollection(stre store.Storer) *FileCollection {
	return &FileCollection{
		treap: NewPersistentPayloadTreap[types.MD5Key, *FileMetadata](
			types.MD5Less,
			new(types.MD5Key),
			stre,
		),
	}
}

// AddFile adds a file to the collection using its content to generate the MD5 key
func (fc *FileCollection) AddFile(content string, metadata *FileMetadata) {
	hash := types.MD5Key(md5.Sum([]byte(content)))
	fc.treap.Insert(&hash, metadata)
}

// extractPayload is a helper to extract FileMetadata from a node
func extractPayload(node TreapNodeInterface[types.MD5Key]) *FileMetadata {
	if payloadNode, ok := node.(PersistentPayloadNodeInterface[types.MD5Key, *FileMetadata]); ok {
		return payloadNode.GetPayload()
	}
	return nil
}

// formatHash returns the first 8 characters of the hex-encoded MD5 hash
func formatHash(key types.MD5Key) string {
	return hex.EncodeToString(key[:])[:8]
}

// CompareResult holds the results of a file collection comparison
type CompareResult struct {
	OnlyInA   []string
	Identical []string
	Modified  []string
	OnlyInB   []string
}

// Compare compares this collection with another and returns categorized results
func (fc *FileCollection) Compare(other *FileCollection, logger func(string)) (*CompareResult, error) {
	result := &CompareResult{}

	err := fc.treap.Compare(other.treap,
		// Files only in this collection
		func(node TreapNodeInterface[types.MD5Key]) error {
			payload := extractPayload(node)
			if payload == nil {
				return nil
			}
			hash := formatHash(node.GetKey().Value())
			msg := fmt.Sprintf("Only in A: %s (size: %d bytes, hash: %s)",
				payload.Name, payload.Size, hash)
			if logger != nil {
				logger(msg)
			}
			result.OnlyInA = append(result.OnlyInA, payload.Name)
			return nil
		},
		// Files in both collections
		func(nodeA, nodeB TreapNodeInterface[types.MD5Key]) error {
			metaA := extractPayload(nodeA)
			metaB := extractPayload(nodeB)
			if metaA == nil || metaB == nil {
				return nil
			}

			hash := formatHash(nodeA.GetKey().Value())
			if metaA.Size == metaB.Size {
				msg := fmt.Sprintf("Identical: %s = %s (size: %d bytes, hash: %s)",
					metaA.Name, metaB.Name, metaA.Size, hash)
				if logger != nil {
					logger(msg)
				}
				result.Identical = append(result.Identical, metaA.Name)
			} else {
				msg := fmt.Sprintf("Modified: %s (A: %d bytes) vs %s (B: %d bytes, hash: %s)",
					metaA.Name, metaA.Size, metaB.Name, metaB.Size, hash)
				if logger != nil {
					logger(msg)
				}
				result.Modified = append(result.Modified, metaA.Name)
			}
			return nil
		},
		// Files only in other collection
		func(node TreapNodeInterface[types.MD5Key]) error {
			payload := extractPayload(node)
			if payload == nil {
				return nil
			}
			hash := formatHash(node.GetKey().Value())
			msg := fmt.Sprintf("Only in B: %s (size: %d bytes, hash: %s)",
				payload.Name, payload.Size, hash)
			if logger != nil {
				logger(msg)
			}
			result.OnlyInB = append(result.OnlyInB, payload.Name)
			return nil
		},
	)

	return result, err
}

// ExamplePersistentPayloadTreap_Compare demonstrates comparing two file collections
// by MD5 hash to find duplicates, unique files, and files with different sizes
func ExamplePersistentPayloadTreap_Compare() {
	// Create temporary stores for the example
	path := filepath.Join(os.TempDir(), "exampleA.db")
	store, _ := store.NewBasicStore(path)
	defer func() {
		store.Close()
		_ = os.Remove(path)
	}()

	// Create two file collections
	collectionA := NewFileCollection(store)
	collectionB := NewFileCollection(store)

	// Populate collection A with some files (using content as hash source)
	collectionA.AddFile("document1.txt", &FileMetadata{Name: "document1.txt", Size: 1024})
	collectionA.AddFile("image.png", &FileMetadata{Name: "image.png", Size: 2048})
	collectionA.AddFile("video.mp4", &FileMetadata{Name: "video.mp4", Size: 1048576})
	collectionA.AddFile("readme.md", &FileMetadata{Name: "readme.md", Size: 512})

	// Populate collection B with overlapping and different files
	collectionB.AddFile("document1.txt", &FileMetadata{Name: "copy_of_document1.txt", Size: 1024})
	collectionB.AddFile("image.png", &FileMetadata{Name: "image.png", Size: 3072})
	collectionB.AddFile("video.mp4", &FileMetadata{Name: "video.mp4", Size: 1048576})
	collectionB.AddFile("newfile.txt", &FileMetadata{Name: "newfile.txt", Size: 256})
	collectionB.AddFile("archive.zip", &FileMetadata{Name: "archive.zip", Size: 10240})

	fmt.Println("=== File Collection Comparison ===")

	// Compare and print results
	_, _ = collectionA.Compare(collectionB, func(msg string) {
		fmt.Println(msg)
	})

	// Output:
	// === File Collection Comparison ===
	// Only in A: readme.md (size: 512 bytes, hash: 0730bb7c)
	// Only in B: newfile.txt (size: 256 bytes, hash: 5b636c7e)
	// Identical: document1.txt = copy_of_document1.txt (size: 1024 bytes, hash: 82dd8794)
	// Only in B: archive.zip (size: 10240 bytes, hash: 8acbaea4)
	// Identical: video.mp4 = video.mp4 (size: 1048576 bytes, hash: 9d66725b)
	// Modified: image.png (A: 2048 bytes) vs image.png (B: 3072 bytes, hash: d2b5ca33)
}

// TestFileCollectionCompare demonstrates comparing two file collections by MD5 hash
// This is a full working test showing the real functionality
func TestFileCollectionCompare(t *testing.T) {
	// Create test stores
	tempDir := t.TempDir()
	store, err := store.NewBasicStore(tempDir + "/storeA.db")
	if err != nil {
		t.Fatalf("Failed to create store A: %v", err)
	}
	defer store.Close()

	// Create two file collections
	collectionA := NewFileCollection(store)
	collectionB := NewFileCollection(store)

	// Populate collection A with some files
	collectionA.AddFile("document1.txt", &FileMetadata{Name: "document1.txt", Size: 1024})
	collectionA.AddFile("image.png", &FileMetadata{Name: "image.png", Size: 2048})
	collectionA.AddFile("video.mp4", &FileMetadata{Name: "video.mp4", Size: 1048576})
	collectionA.AddFile("readme.md", &FileMetadata{Name: "readme.md", Size: 512})

	// Populate collection B with overlapping and different files
	collectionB.AddFile("document1.txt", &FileMetadata{Name: "copy_of_document1.txt", Size: 1024}) // Same content, different name
	collectionB.AddFile("image.png", &FileMetadata{Name: "image.png", Size: 3072})                 // Same content, different size (modified)
	collectionB.AddFile("video.mp4", &FileMetadata{Name: "video.mp4", Size: 1048576})              // Identical
	collectionB.AddFile("newfile.txt", &FileMetadata{Name: "newfile.txt", Size: 256})              // Only in B
	collectionB.AddFile("archive.zip", &FileMetadata{Name: "archive.zip", Size: 10240})            // Only in B

	t.Log("=== File Collection Comparison ===")

	// Compare the collections
	result, err := collectionA.Compare(collectionB, func(msg string) {
		t.Log(msg)
	})
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	// Verify we got the expected categorization
	if len(result.OnlyInA) != 1 || result.OnlyInA[0] != "readme.md" {
		t.Errorf("Expected 1 file only in A (readme.md), got %v", result.OnlyInA)
	}
	if len(result.Identical) != 2 {
		t.Errorf("Expected 2 identical files, got %d: %v", len(result.Identical), result.Identical)
	}
	if len(result.Modified) != 1 || result.Modified[0] != "image.png" {
		t.Errorf("Expected 1 modified file (image.png), got %v", result.Modified)
	}
	if len(result.OnlyInB) != 2 {
		t.Errorf("Expected 2 files only in B, got %d: %v", len(result.OnlyInB), result.OnlyInB)
	}
}

// TestMemoryManagementWithFileCollection demonstrates memory management in two ways:
// 1. Manual flushing using FlushOldestPercentile() at the treap level
// 2. Automatic memory budgeting at a higher level (would be at Vault level for real usage)
func TestMemoryManagementWithFileCollection(t *testing.T) {
	// === MANUAL MEMORY MANAGEMENT ===
	fmt.Println("=== Manual Memory Management ===")
	tempDir := t.TempDir()
	memPath := filepath.Join(tempDir, "exampleMemory.db")
	store, _ := store.NewBasicStore(memPath)
	defer store.Close()

	collection := NewFileCollection(store)

	// Add several files
	fmt.Println("Adding 5 files to collection...")
	for i := 1; i <= 5; i++ {
		filename := fmt.Sprintf("file%d.txt", i)
		collection.AddFile(filename, &FileMetadata{
			Name: filename,
			Size: int64(1024 * i),
		})
	}

	// Check in-memory nodes before flushing
	nodesBeforeFlush := collection.treap.CountInMemoryNodes()
	fmt.Printf("In-memory nodes before flush: %d\n", nodesBeforeFlush)

	// Manually flush oldest nodes to disk when memory becomes a concern
	fmt.Println("Flushing oldest 50% of nodes to disk...")
	collection.treap.FlushOldestPercentile(50)

	// Check nodes after manual flush
	nodesAfterFlush := collection.treap.CountInMemoryNodes()
	fmt.Printf("In-memory nodes after manual flush: %d\n", nodesAfterFlush)

	// Data is still accessible - nodes reload automatically from disk
	fmt.Println("Accessing collection (nodes reload from disk as needed)...")
	collection.Compare(collection, func(msg string) {})

	finalNodeCount := collection.treap.CountInMemoryNodes()
	fmt.Printf("Final in-memory nodes: %d\n", finalNodeCount)
}
