package yggdrasil

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
)

type compareFixture struct {
	primaryID types.StringKey
	backupID  types.StringKey
	primary   *treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]]
	backup    *treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]]
}

func openCompareFixture(t *testing.T) (*vault.VaultSession, compareFixture) {
	t.Helper()
	primaryID := types.StringKey("users_primary")
	backupID := types.StringKey("users_backup")
	
	// Use temporary directory to minimize disk I/O while keeping real store
	// (vault identity testing requires actual vault reconstruction)
	dbPath := filepath.Join(t.TempDir(), "compare.db")
	
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

	fixture := compareFixture{primaryID: primaryID, backupID: backupID}

	var ok bool
	fixture.primary, ok = colls[primaryID].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]])
	if !ok {
		t.Fatalf("primary collection type assertion failed")
	}
	fixture.backup, ok = colls[backupID].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]])
	if !ok {
		t.Fatalf("backup collection type assertion failed")
	}

	return session, fixture
}

func seedCompareData(f compareFixture) (key1, key2, key3, key4 types.IntKey, err error) {
	key1, key2, key3, key4 = types.IntKey(1), types.IntKey(2), types.IntKey(3), types.IntKey(4)
	f.primary.Insert(&key1, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "alice", Email: "alice@example.com", Credits: 100},
	})
	f.primary.Insert(&key2, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "bob", Email: "bob@example.com", Credits: 50},
	})
	f.primary.Insert(&key3, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "charlie", Email: "charlie@example.com", Credits: 75},
	})

	f.backup.Insert(&key2, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "bob", Email: "bob@example.com", Credits: 50},
	})
	f.backup.Insert(&key3, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "charlie", Email: "charlie.new@example.com", Credits: 80},
	})
	f.backup.Insert(&key4, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "david", Email: "david@example.com", Credits: 120},
	})

	return key1, key2, key3, key4, nil
}

func TestVaultCompareCollections_Differences(t *testing.T) {
	session, fixture := openCompareFixture(t)
	t.Cleanup(func() { _ = session.Close() })

	session.Vault.SetMemoryBudgetWithPercentile(1000, 25)

	if _, _, _, _, err := seedCompareData(fixture); err != nil {
		t.Fatalf("seedCompareData error: %v", err)
	}

	var onlyPrimary, onlyBackup, identical []int
	modified := make(map[int]struct {
		emailPrimary string
		emailBackup  string
		creditsA     int
		creditsB     int
	})

	err := fixture.primary.Compare(fixture.backup,
		func(node treap.TreapNodeInterface[types.IntKey]) error {
			onlyPrimary = append(onlyPrimary, int(*node.GetKey().(*types.IntKey)))
			return nil
		},
		func(nodeA, nodeB treap.TreapNodeInterface[types.IntKey]) error {
			pa := nodeA.(treap.PersistentPayloadNodeInterface[types.IntKey, types.JsonPayload[UserProfile]]).GetPayload().Value
			pb := nodeB.(treap.PersistentPayloadNodeInterface[types.IntKey, types.JsonPayload[UserProfile]]).GetPayload().Value
			id := int(*nodeA.GetKey().(*types.IntKey))
			if pa.Email == pb.Email && pa.Credits == pb.Credits {
				identical = append(identical, id)
			} else {
				modified[id] = struct {
					emailPrimary string
					emailBackup  string
					creditsA     int
					creditsB     int
				}{emailPrimary: pa.Email, emailBackup: pb.Email, creditsA: pa.Credits, creditsB: pb.Credits}
			}
			return nil
		},
		func(node treap.TreapNodeInterface[types.IntKey]) error {
			onlyBackup = append(onlyBackup, int(*node.GetKey().(*types.IntKey)))
			return nil
		},
	)
	if err != nil {
		t.Fatalf("Compare returned error: %v", err)
	}

	sort.Ints(onlyPrimary)
	sort.Ints(onlyBackup)
	sort.Ints(identical)

	if len(onlyPrimary) != 1 || onlyPrimary[0] != 1 {
		t.Fatalf("onlyPrimary mismatch: %v", onlyPrimary)
	}
	if len(identical) != 1 || identical[0] != 2 {
		t.Fatalf("identical mismatch: %v", identical)
	}
	if len(onlyBackup) != 1 || onlyBackup[0] != 4 {
		t.Fatalf("onlyBackup mismatch: %v", onlyBackup)
	}
	modifiedEntry, ok := modified[3]
	if !ok {
		t.Fatalf("modified entry for key 3 missing; got %v", modified)
	}
	if modifiedEntry.emailPrimary != "charlie@example.com" || modifiedEntry.emailBackup != "charlie.new@example.com" {
		t.Fatalf("modified emails mismatch: %+v", modifiedEntry)
	}
	if modifiedEntry.creditsA != 75 || modifiedEntry.creditsB != 80 {
		t.Fatalf("modified credits mismatch: %+v", modifiedEntry)
	}
}

func TestVaultCompareCollections_MemoryStats(t *testing.T) {
	session, fixture := openCompareFixture(t)
	t.Cleanup(func() { _ = session.Close() })
	session.Vault.SetMemoryBudgetWithPercentile(1000, 25)
	if _, _, _, _, err := seedCompareData(fixture); err != nil {
		t.Fatalf("seedCompareData error: %v", err)
	}

	stats := session.Vault.GetMemoryStats()
	if stats.TotalInMemoryNodes == 0 {
		t.Fatalf("expected in-memory nodes after inserts, got 0")
	}
	primaryName := string(fixture.primaryID)
	backupName := string(fixture.backupID)
	if stats.CollectionNodes[primaryName] == 0 {
		t.Fatalf("missing or zero nodes for primary collection")
	}
	if stats.CollectionNodes[backupName] == 0 {
		t.Fatalf("missing or zero nodes for backup collection")
	}
}
