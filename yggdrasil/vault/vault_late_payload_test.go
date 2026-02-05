package vault

import (
	"bytes"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

var instrumentedLateMarshalCalls atomic.Int32
var instrumentedLateUnmarshalCalls atomic.Int32
var instrumentedLastUnmarshalMu sync.Mutex
var instrumentedLastUnmarshalValue string

func resetInstrumentedPayloadTracking() {
	instrumentedLateMarshalCalls.Store(0)
	instrumentedLateUnmarshalCalls.Store(0)
	instrumentedLastUnmarshalMu.Lock()
	instrumentedLastUnmarshalValue = ""
	instrumentedLastUnmarshalMu.Unlock()
}

func getInstrumentedLastUnmarshalValue() string {
	instrumentedLastUnmarshalMu.Lock()
	defer instrumentedLastUnmarshalMu.Unlock()
	return instrumentedLastUnmarshalValue
}

// InstrumentedStringPayload wraps StringPayload and tracks Late method calls.
type InstrumentedStringPayload struct {
	Value types.StringPayload
}

func (p InstrumentedStringPayload) Marshal() ([]byte, error) {
	return p.Value.Marshal()
}

func (p InstrumentedStringPayload) Unmarshal(data []byte) (types.UntypedPersistentPayload, error) {
	var sp types.StringPayload
	if err := sp.Unmarshal(data); err != nil {
		return nil, err
	}
	return InstrumentedStringPayload{Value: sp}, nil
}

func (p InstrumentedStringPayload) SizeInBytes() int {
	return p.Value.SizeInBytes()
}

// LateMarshal increments the call counter and delegates to StringPayload.
func (p InstrumentedStringPayload) LateMarshal(s bobbob.Storer) (bobbob.ObjectId, bobbob.Finisher) {
	instrumentedLateMarshalCalls.Add(1)
	return p.Value.LateMarshal(s)
}

// LateUnmarshal increments the call counter and delegates to StringPayload.
func (p InstrumentedStringPayload) LateUnmarshal(id bobbob.ObjectId, s bobbob.Storer) bobbob.Finisher {
	instrumentedLateUnmarshalCalls.Add(1)
	var sp types.StringPayload
	finisher := sp.LateUnmarshal(id, s)
	return func() error {
		if err := finisher(); err != nil {
			return err
		}
		instrumentedLastUnmarshalMu.Lock()
		instrumentedLastUnmarshalValue = string(bytes.TrimRight([]byte(sp), "\x00"))
		instrumentedLastUnmarshalMu.Unlock()
		return nil
	}
}

func TestVaultUsesLateMarshalLateUnmarshalForInstrumentedPayload(t *testing.T) {
	resetInstrumentedPayloadTracking()

	tmpFile := filepath.Join(t.TempDir(), "vault_late_payload.db")

	session, collections, err := OpenVault(
		tmpFile,
		PayloadCollectionSpec[types.IntKey, InstrumentedStringPayload]{
			Name:            "instrumented_payloads",
			LessFunc:        types.IntLess,
			KeyTemplate:     (*types.IntKey)(new(int32)),
			PayloadTemplate: InstrumentedStringPayload{Value: types.StringPayload("")},
		},
	)
	if err != nil {
		t.Fatalf("failed to open vault: %v", err)
	}

	coll := collections[0].(*treap.PersistentPayloadTreap[types.IntKey, InstrumentedStringPayload])
	key := types.IntKey(7)
	payload := InstrumentedStringPayload{Value: types.StringPayload("payload-7")}
	coll.Insert(&key, payload)

	stored := coll.Search(&key)
	if stored == nil || stored.IsNil() {
		_ = session.Close()
		t.Fatalf("expected to find inserted payload")
	}

	if err := session.Vault.PersistCollection("instrumented_payloads"); err != nil {
		_ = session.Close()
		t.Fatalf("failed to persist collection: %v", err)
	}

	if got := instrumentedLateMarshalCalls.Load(); got != 1 {
		_ = session.Close()
		t.Fatalf("expected 1 LateMarshal call after persist, got %d", got)
	}
	if got := instrumentedLateUnmarshalCalls.Load(); got != 0 {
		_ = session.Close()
		t.Fatalf("expected 0 LateUnmarshal calls before reload, got %d", got)
	}

	if err := session.Close(); err != nil {
		t.Fatalf("failed to close vault session: %v", err)
	}

	reloadSession, reloadCollections, err := OpenVault(
		tmpFile,
		PayloadCollectionSpec[types.IntKey, InstrumentedStringPayload]{
			Name:            "instrumented_payloads",
			LessFunc:        types.IntLess,
			KeyTemplate:     (*types.IntKey)(new(int32)),
			PayloadTemplate: InstrumentedStringPayload{Value: types.StringPayload("")},
		},
	)
	if err != nil {
		t.Fatalf("failed to reload vault: %v", err)
	}
	defer reloadSession.Close()

	_ = reloadCollections[0].(*treap.PersistentPayloadTreap[types.IntKey, InstrumentedStringPayload])

	if got := instrumentedLateUnmarshalCalls.Load(); got != 1 {
		t.Fatalf("expected 1 LateUnmarshal call after reload, got %d", got)
	}
	if got := getInstrumentedLastUnmarshalValue(); got != "payload-7" {
		t.Fatalf("expected LateUnmarshal to read %q, got %q", "payload-7", got)
	}
}
