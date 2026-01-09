package treap_test

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	collections "github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// fixedPayload is a simple constant-size payload for batch persistence tests.
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

func TestPersistentPayloadTreapBatchPersistWithMultiStore(t *testing.T) {
	ms, err := collections.NewMultiStore(filepath.Join(t.TempDir(), "multi_payload_batch.bin"), 0)
	if err != nil {
		t.Fatalf("failed to create multistore: %v", err)
	}
	defer ms.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))
	pt := treap.NewPersistentPayloadTreap[types.IntKey, fixedPayload](types.IntLess, keyTemplate, ms)

	keys := []*types.IntKey{(*types.IntKey)(new(int32)), (*types.IntKey)(new(int32)), (*types.IntKey)(new(int32))}
	*keys[0] = 1
	*keys[1] = 2
	*keys[2] = 3

	payloads := []fixedPayload{
		{A: 1, B: 10},
		{A: 2, B: 20},
		{A: 3, B: 30},
	}

	for i, k := range keys {
		pt.Insert(k, payloads[i])
	}

	if err := pt.BatchPersist(); err != nil {
		t.Fatalf("BatchPersist failed: %v", err)
	}

	rootNode, ok := pt.Root().(treap.PersistentTreapNodeInterface[types.IntKey])
	if !ok {
		t.Fatalf("root is not persistent node")
	}
	rootId, err := rootNode.ObjectId()
	if err != nil {
		t.Fatalf("root ObjectId failed: %v", err)
	}

	reloaded := treap.NewPersistentPayloadTreap[types.IntKey, fixedPayload](types.IntLess, keyTemplate, ms)
	if err := reloaded.Load(rootId); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	for i, k := range keys {
		n := reloaded.Search(k)
		if n == nil {
			t.Fatalf("expected to find key %d after reload", *k)
		}
		payloadNode, ok := n.(treap.PersistentPayloadNodeInterface[types.IntKey, fixedPayload])
		if !ok {
			t.Fatalf("node is not payload node")
		}
		got := payloadNode.GetPayload()
		if got != payloads[i] {
			t.Fatalf("payload mismatch for key %d: got %+v want %+v", *k, got, payloads[i])
		}
	}
}
