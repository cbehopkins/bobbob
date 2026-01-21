package vault

import (
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func benchmarkInsertUsersWithStore(b *testing.B, useMock bool) {
	var s store.Storer
	var cleanup func()

	if useMock {
		s = testutil.NewMockStore()
		cleanup = func() { _ = s.Close() }
	} else {
		tempDir := b.TempDir()
		storePath := filepath.Join(tempDir, "bench_insert.db")
		stre, err := store.NewBasicStore(storePath)
		if err != nil {
			b.Fatalf("Failed to create store: %v", err)
		}
		s = stre
		cleanup = func() { _ = s.Close() }
	}
	defer cleanup()

	v, err := LoadVault(s)
	if err != nil {
		b.Fatalf("Failed to load vault: %v", err)
	}
	defer v.Close()

	// Register types
	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[testutil.TestUserData]{})

	users, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[testutil.TestUserData]](
		v, "users", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		b.Fatalf("Failed to create users collection: %v", err)
	}

	payload := types.JsonPayload[testutil.TestUserData]{
		Value: testutil.TestUserData{Username: "bench", Email: "bench@example.com", Age: 25},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := types.IntKey(i)
		users.Insert(&key, payload)
	}
}

func BenchmarkInsertUsers_MockStore(b *testing.B) {
	benchmarkInsertUsersWithStore(b, true)
}

func BenchmarkInsertUsers_BasicStore(b *testing.B) {
	benchmarkInsertUsersWithStore(b, false)
}
