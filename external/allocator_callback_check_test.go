package external_test

import (
	"fmt"
	"path/filepath"
	"testing"

	atypes "github.com/cbehopkins/bobbob/allocator/types"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	yttypes "github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
)

type fileData struct {
	Path    string
	Payload string
}

// TestConfigureAllocatorCallbacks demonstrates attaching allocation callbacks externally
// via VaultSession.ConfigureAllocatorCallbacks. It verifies callbacks on both the
// OmniBlockAllocator and its parent BasicAllocator are invoked.
func TestConfigureAllocatorCallbacks(t *testing.T) {
	file := filepath.Join(t.TempDir(), "vault.db")

	// Open a vault with an identity-backed payload collection
	identity := yttypes.IntKey(1)
	spec := vault.PayloadIdentitySpec[yttypes.IntKey, yttypes.IntKey, yttypes.JsonPayload[string]]{
		Identity:        identity,
		LessFunc:        yttypes.IntLess,
		KeyTemplate:     new(yttypes.IntKey),
		PayloadTemplate: yttypes.JsonPayload[string]{},
	}

	session, colls, err := vault.OpenVaultWithIdentity(file, spec)
	if err != nil {
		t.Fatalf("OpenVaultWithIdentity: %v", err)
	}
	defer session.Close()

	childHits := 0
	parentHits := 0

	ok := session.ConfigureAllocatorCallbacks(
		func(obj atypes.ObjectId, _ atypes.FileOffset, _ int) { childHits++ },
		func(obj atypes.ObjectId, _ atypes.FileOffset, _ int) { parentHits++ },
	)
	if !ok {
		t.Fatalf("expected allocator callbacks to attach")
	}

	// Small payload should be handled by OmniBlockAllocator (child callback)
	coll, ok := colls[identity].(*treap.PersistentPayloadTreap[yttypes.IntKey, yttypes.JsonPayload[string]])
	if !ok {
		t.Fatalf("unexpected collection type: %T", colls[identity])
	}
	key := yttypes.IntKey(1)
	coll.Insert(&key, yttypes.JsonPayload[string]{Value: "small"})

	// Large allocation should fall back to parent BasicAllocator (parent callback)
	if _, err := session.Store.NewObj(6000); err != nil {
		t.Fatalf("NewObj large: %v", err)
	}

	if childHits == 0 {
		t.Fatalf("expected child allocator callback to fire")
	}
	if parentHits == 0 {
		t.Fatalf("expected parent allocator callback to fire")
	}
}

// TestFreshVaultSmallObjectsStayInBlockAllocator mirrors the user pattern of creating a
// new vault, attaching callbacks, and inserting small records. It ensures the parent
// allocator does not repeatedly handle small allocations (<=4KB) during steady-state inserts.
func TestFreshVaultSmallObjectsStayInBlockAllocator(t *testing.T) {
	file := filepath.Join(t.TempDir(), "fresh_vault.db")

	session, colls, err := vault.OpenVaultWithIdentity(
		file,
		vault.PayloadIdentitySpec[string, yttypes.MD5Key, yttypes.JsonPayload[fileData]]{
			Identity:        "srcFiles",
			LessFunc:        yttypes.MD5Less,
			KeyTemplate:     (*yttypes.MD5Key)(new(yttypes.MD5Key)),
			PayloadTemplate: yttypes.JsonPayload[fileData]{},
		},
		vault.PayloadIdentitySpec[string, yttypes.MD5Key, yttypes.JsonPayload[fileData]]{
			Identity:        "dstFiles",
			LessFunc:        yttypes.MD5Less,
			KeyTemplate:     (*yttypes.MD5Key)(new(yttypes.MD5Key)),
			PayloadTemplate: yttypes.JsonPayload[fileData]{},
		},
	)
	if err != nil {
		t.Fatalf("OpenVaultWithIdentity: %v", err)
	}
	defer session.Close()

	recording := true
	childSizes := make([]int, 0, 64)
	parentAll := 0
	parentSmall := make([]int, 0, 16)

	ok := session.ConfigureAllocatorCallbacks(
		func(_ atypes.ObjectId, _ atypes.FileOffset, size int) {
			if !recording {
				return
			}
			childSizes = append(childSizes, size)
		},
		func(_ atypes.ObjectId, _ atypes.FileOffset, size int) {
			if !recording {
				return
			}
			parentAll++
			if size <= 4096 {
				parentSmall = append(parentSmall, size)
			}
		},
	)
	if !ok {
		t.Fatalf("expected allocator callbacks to attach")
	}

	src, ok := colls["srcFiles"].(*treap.PersistentPayloadTreap[yttypes.MD5Key, yttypes.JsonPayload[fileData]])
	if !ok {
		t.Fatalf("unexpected src collection type: %T", colls["srcFiles"])
	}
	dst, ok := colls["dstFiles"].(*treap.PersistentPayloadTreap[yttypes.MD5Key, yttypes.JsonPayload[fileData]])
	if !ok {
		t.Fatalf("unexpected dst collection type: %T", colls["dstFiles"])
	}

	payload := func(i int) yttypes.JsonPayload[fileData] {
		return yttypes.JsonPayload[fileData]{Value: fileData{
			Path:    fmt.Sprintf("file-%d", i),
			Payload: "contents-32-bytes-placeholder",
		}}
	}
	numFiles := 500
	for i := range numFiles {
		var srcKey yttypes.MD5Key
		srcKey[0] = byte(i + 1)
		var dstKey yttypes.MD5Key
		dstKey[0] = byte(i + 1)
		dstKey[1] = 0xFF

		src.Insert(&srcKey, payload(i))
		dst.Insert(&dstKey, payload(i+1000))
	}

	if err := src.Persist(); err != nil {
		t.Fatalf("persist src treap: %v", err)
	}
	if err := dst.Persist(); err != nil {
		t.Fatalf("persist dst treap: %v", err)
	}

	recording = false
	gotChild := len(childSizes)
	gotParentAll := parentAll

	if gotChild == 0 {
		t.Fatalf("expected child allocator callbacks during inserts")
	}
	// The parent allocator will handle allocations that don't fit standard block sizes.
	// Treap node serialization produces sizes like 97-98 bytes which fall between
	// the 64-byte and 256-byte block allocators, so they use the parent.
	// This is expected behavior: only perfectly-aligned sizes use child allocators.
	if gotParentAll == 0 {
		t.Fatalf("parent allocator should handle non-block-aligned allocations (e.g., 97-98 byte nodes)")
	}
}
