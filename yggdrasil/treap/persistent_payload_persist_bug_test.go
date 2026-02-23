package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func TestPayloadTreapEmbeddedPersistFails(t *testing.T) {
	_, store, cleanup := testutil.SetupTestStore(t)
	defer cleanup()

	treap := NewPersistentPayloadTreap[
		types.IntKey,
		types.JsonPayload[int],
	](types.IntLess, (*types.IntKey)(new(types.IntKey)), store)

	key := types.IntKey(42)
	treap.Insert(&key, types.JsonPayload[int]{Value: 123})

	// This should fail: calling Persist() on the embedded PersistentTreap
	err := treap.PersistentTreap.Persist()
	if err == nil || err.Error() != "root is not a PersistentTreapNode" {
		t.Errorf("Expected error 'root is not a PersistentTreapNode', got: %v", err)
	}
}
