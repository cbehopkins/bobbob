package multistore

import (
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
)

// Prime object must be the very first allocation in the file. This test makes
// that explicit and guards against regressions where other allocators might
// consume space before priming.
func TestPrimeObjectIsFirstAllocation(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "prime_first.bin")

	ms, err := NewMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("failed to create multistore: %v", err)
	}
	defer ms.Close()

	primeSize := treap.PersistentTreapObjectSizes()[0]
	primeObjId, err := ms.PrimeObject(primeSize)
	if err != nil {
		t.Fatalf("PrimeObject failed: %v", err)
	}

	// Prime must be the first allocation right after the PrimeTable prefix.
	expectedPrime := store.ObjectId(store.PrimeObjectStart())
	if primeObjId != expectedPrime {
		t.Fatalf("expected prime ObjectId %d, got %d", expectedPrime, primeObjId)
	}

	primeInfo, found := ms.GetObjectInfo(primeObjId)
	if !found {
		t.Fatalf("prime object not found in allocator")
	}
	if primeInfo.Size != primeSize {
		t.Fatalf("expected prime size %d, got %d", primeSize, primeInfo.Size)
	}

	// Next allocations should come after the prime object.
	objId, err := ms.NewObj(primeSize)
	if err != nil {
		t.Fatalf("NewObj failed: %v", err)
	}
	if objId <= primeObjId {
		t.Fatalf("expected subsequent ObjectId > prime (got %d <= %d)", objId, primeObjId)
	}

	// Persist and reload to ensure ordering survives round-trip.
	if err := ms.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	ms2, err := LoadMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	defer ms2.Close()

	primeInfo2, found := ms2.GetObjectInfo(primeObjId)
	if !found {
		t.Fatalf("prime object missing after reload")
	}
	if primeInfo2.Offset != primeInfo.Offset || primeInfo2.Size != primeInfo.Size {
		t.Fatalf("prime info changed after reload: before %+v after %+v", primeInfo, primeInfo2)
	}
}
