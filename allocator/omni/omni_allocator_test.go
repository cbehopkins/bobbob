package omni_test

import (
	"testing"

	"github.com/cbehopkins/bobbob/allocator/cache"
	"github.com/cbehopkins/bobbob/allocator/omni"
	"github.com/cbehopkins/bobbob/allocator/testutil"
	"github.com/cbehopkins/bobbob/allocator/types"
)

func TestOmniAllocatorRouting(t *testing.T) {
    parent := testutil.NewMockAllocator()
    pc, err := cache.New()
    if err != nil {
        t.Fatalf("cache.New failed: %v", err)
    }
    defer pc.Close()

    omniAlloc, err := omni.NewOmniAllocator([]int{256, 1024}, parent, nil, pc)
    if err != nil {
        t.Fatalf("NewOmniAllocator failed: %v", err)
    }
    defer omniAlloc.Close()

    smallId, smallOff, err := omniAlloc.Allocate(200)
    if err != nil {
        t.Fatalf("Allocate small failed: %v", err)
    }

    mediumId, mediumOff, err := omniAlloc.Allocate(900)
    if err != nil {
        t.Fatalf("Allocate medium failed: %v", err)
    }

    largeId, largeOff, err := omniAlloc.Allocate(5000)
    if err != nil {
        t.Fatalf("Allocate large failed: %v", err)
    }

    if smallId == mediumId || smallId == largeId || mediumId == largeId {
        t.Fatalf("expected unique ObjectIds, got duplicates")
    }

    off, size, err := omniAlloc.GetObjectInfo(smallId)
    if err != nil {
        t.Fatalf("GetObjectInfo small failed: %v", err)
    }
    if off != smallOff || size != types.FileSize(256) {
        t.Fatalf("small info mismatch off=%d size=%d", off, size)
    }

    off, size, err = omniAlloc.GetObjectInfo(mediumId)
    if err != nil {
        t.Fatalf("GetObjectInfo medium failed: %v", err)
    }
    if off != mediumOff || size != types.FileSize(1024) {
        t.Fatalf("medium info mismatch off=%d size=%d", off, size)
    }

    off, size, err = omniAlloc.GetObjectInfo(largeId)
    if err != nil {
        t.Fatalf("GetObjectInfo large failed: %v", err)
    }
    if off != largeOff || size != types.FileSize(5000) {
        t.Fatalf("large info mismatch off=%d size=%d", off, size)
    }
}
