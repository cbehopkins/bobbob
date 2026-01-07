package external_test

import (
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/store/allocator"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
)

// TestConfigureAllocatorCallbacks demonstrates attaching allocation callbacks externally
// via VaultSession.ConfigureAllocatorCallbacks. It verifies callbacks on both the
// OmniBlockAllocator and its parent BasicAllocator are invoked.
func TestConfigureAllocatorCallbacks(t *testing.T) {
	file := filepath.Join(t.TempDir(), "vault.db")

	// Open a vault with an identity-backed payload collection
	identity := types.IntKey(1)
	spec := vault.PayloadIdentitySpec[types.IntKey, types.IntKey, types.JsonPayload[string]]{
		Identity:        identity,
		LessFunc:        types.IntLess,
		KeyTemplate:     new(types.IntKey),
		PayloadTemplate: types.JsonPayload[string]{},
	}

	session, colls, err := vault.OpenVaultWithIdentity(file, spec)
	if err != nil {
		t.Fatalf("OpenVaultWithIdentity: %v", err)
	}
	defer session.Close()

	childHits := 0
	parentHits := 0

	ok := session.ConfigureAllocatorCallbacks(
		func(obj allocator.ObjectId, _ allocator.FileOffset, _ int) { childHits++ },
		func(obj allocator.ObjectId, _ allocator.FileOffset, _ int) { parentHits++ },
	)
	if !ok {
		t.Fatalf("expected allocator callbacks to attach")
	}

	// Small payload should be handled by OmniBlockAllocator (child callback)
	coll, ok := colls[identity].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[string]])
	if !ok {
		t.Fatalf("unexpected collection type: %T", colls[identity])
	}
	key := types.IntKey(1)
	coll.Insert(&key, types.JsonPayload[string]{Value: "small"})

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
