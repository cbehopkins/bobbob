package yggdrasil

import (
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
)

func TestOpenVaultWithIdentity_SimplePersistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "identity_persist.db")
	primaryID := types.StringKey("primary")
	backupID := types.StringKey("backup")

	session, colls, err := vault.OpenVaultWithIdentity(
		dbPath,
		vault.PayloadIdentitySpec[types.StringKey, types.IntKey, types.JsonPayload[UserProfile]]{
			Identity:        primaryID,
			LessFunc:        types.IntLess,
			KeyTemplate:     (*types.IntKey)(new(int32)),
			PayloadTemplate: types.JsonPayload[UserProfile]{},
		},
		vault.PayloadIdentitySpec[types.StringKey, types.IntKey, types.JsonPayload[UserProfile]]{
			Identity:        backupID,
			LessFunc:        types.IntLess,
			KeyTemplate:     (*types.IntKey)(new(int32)),
			PayloadTemplate: types.JsonPayload[UserProfile]{},
		},
	)
	if err != nil {
		t.Fatalf("OpenVaultWithIdentity: %v", err)
	}

	primary := colls[primaryID].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]])
	backup := colls[backupID].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]])

	key1, key2 := types.IntKey(1), types.IntKey(2)
	primary.Insert(&key1, types.JsonPayload[UserProfile]{Value: UserProfile{Username: "alice", Email: "a@ex.com", Credits: 100}})
	backup.Insert(&key2, types.JsonPayload[UserProfile]{Value: UserProfile{Username: "bob", Email: "b@ex.com", Credits: 50}})

	if err := session.Close(); err != nil {
		t.Fatalf("session.Close: %v", err)
	}

	reopen, recolls, err := vault.OpenVaultWithIdentity(
		dbPath,
		vault.PayloadIdentitySpec[types.StringKey, types.IntKey, types.JsonPayload[UserProfile]]{
			Identity:        primaryID,
			LessFunc:        types.IntLess,
			KeyTemplate:     (*types.IntKey)(new(int32)),
			PayloadTemplate: types.JsonPayload[UserProfile]{},
		},
		vault.PayloadIdentitySpec[types.StringKey, types.IntKey, types.JsonPayload[UserProfile]]{
			Identity:        backupID,
			LessFunc:        types.IntLess,
			KeyTemplate:     (*types.IntKey)(new(int32)),
			PayloadTemplate: types.JsonPayload[UserProfile]{},
		},
	)
	if err != nil {
		t.Fatalf("OpenVaultWithIdentity (reopen): %v", err)
	}
	t.Cleanup(func() { _ = reopen.Close() })

	reprimary := recolls[primaryID].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]])
	rebackup := recolls[backupID].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]])

	primaryNode := reprimary.Search(&key1)
	if primaryNode == nil || primaryNode.IsNil() {
		t.Fatalf("reloaded primary missing key1")
	}
	backupNode := rebackup.Search(&key2)
	if backupNode == nil || backupNode.IsNil() {
		t.Fatalf("reloaded backup missing key2")
	}
}
