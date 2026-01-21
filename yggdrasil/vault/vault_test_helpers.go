package vault

import (
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// newTestVault creates a vault with MockStore and common types pre-registered.
// This replaces the typical 15-20 line vault setup with a single line.
//
// Pre-registered types:
// - types.IntKey (int32)
// - types.StringKey (string)
// - types.JsonPayload[testutil.TestUserData]
// - types.JsonPayload[testutil.TestProductData]
//
// Example usage:
//
//	func TestMyFeature(t *testing.T) {
//	    v := newTestVault(t)
//	    defer v.Close()
//
//	    users := addCollection[types.IntKey, types.JsonPayload[testutil.TestUserData]](t, v, "users")
//	    // ... test logic ...
//	}
func newTestVault(tb testing.TB) *Vault {
	tb.Helper()

	mockStore := testutil.NewMockStore()
	v, err := LoadVault(mockStore)
	if err != nil {
		tb.Fatalf("Failed to create test vault: %v", err)
	}

	// Pre-register common test types
	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType((*types.StringKey)(new(string)))
	v.RegisterType(types.JsonPayload[testutil.TestUserData]{})
	v.RegisterType(types.JsonPayload[testutil.TestProductData]{})

	return v
}

// newMockVault creates a vault backed by an in-memory MockStore without
// registering any payload types. Tests can register only the types they need.
func newMockVault(tb testing.TB) *Vault {
	tb.Helper()

	mockStore := testutil.NewMockStore()
	v, err := LoadVault(mockStore)
	if err != nil {
		tb.Fatalf("Failed to create mock vault: %v", err)
	}
	return v
}

// addCollection is a helper to create collections with less boilerplate.
// It automatically selects the appropriate less function and key template
// for common types.
//
// Example usage:
//
//	users := addCollection[types.IntKey, types.JsonPayload[testutil.TestUserData]](t, v, "users")
func addCollection[K any, P types.PersistentPayload[P]](
	tb testing.TB,
	v *Vault,
	name string,
) *treap.PersistentPayloadTreap[K, P] {
	tb.Helper()

	lessFunc, keyTpl := getTypeHelpers[K]()

	coll, err := GetOrCreateCollection[K, P](v, name, lessFunc, keyTpl)
	if err != nil {
		tb.Fatalf("Failed to create collection %s: %v", name, err)
	}
	return coll
}

// getTypeHelpers returns the appropriate comparison function and key template for common key types.
func getTypeHelpers[K any]() (func(a, b K) bool, types.PersistentKey[K]) {
	var k K
	switch any(k).(type) {
	case types.IntKey:
		// Use type assertion to convert the function signature
		lessFunc := func(a, b K) bool {
			aKey := any(a).(types.IntKey)
			bKey := any(b).(types.IntKey)
			return types.IntLess(aKey, bKey)
		}
		keyTemplate := any((*types.IntKey)(new(int32))).(types.PersistentKey[K])
		return lessFunc, keyTemplate
	case types.StringKey:
		lessFunc := func(a, b K) bool {
			aKey := any(a).(types.StringKey)
			bKey := any(b).(types.StringKey)
			return types.StringLess(aKey, bKey)
		}
		keyTemplate := any((*types.StringKey)(new(string))).(types.PersistentKey[K])
		return lessFunc, keyTemplate
	default:
		// For custom types, this will panic with a helpful message
		panic("getTypeHelpers: unsupported key type - please use GetOrCreateCollection directly")
	}
}

// insertTestUser is a helper to insert a test user into a collection.
func insertTestUser(tb testing.TB, coll *treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[testutil.TestUserData]], id int32, username string) {
	tb.Helper()

	key := types.IntKey(id)
	payload := types.JsonPayload[testutil.TestUserData]{
		Value: testutil.TestUserData{
			Username: username,
			Email:    username + "@example.com",
			Age:      25,
		},
	}
	coll.Insert(&key, payload)
}

// newMockVaultSession creates a VaultSession backed by MockStore.
// This enables tests to use MockStore for vault identity/equality testing
// instead of disk-backed stores, achieving 3.8× faster execution.
//
// The identity opener pattern allows the vault to reconstruct collections from
// their stored identities on demand, without requiring actual disk persistence.
// Use it with vault.OpenVaultWithIdentity() instead of vault.OpenVault().
//
// Example usage (for vault_compare_test.go):
//
//	session, colls, _ := vault.OpenVaultWithIdentity(nil, // Use MockStore
//	    vault.PayloadIdentitySpec[types.StringKey, types.IntKey, types.JsonPayload[Profile]]{
//	        Identity:        types.StringKey("users_primary"),
//	        LessFunc:        types.IntLess,
//	        KeyTemplate:     (*types.IntKey)(new(int32)),
//	        PayloadTemplate: types.JsonPayload[Profile]{},
//	    },
//	)
//	defer session.Close()
//
// Note: This is currently an internal helper. The public API expects
// a dbPath string, but passing nil triggers MockStore creation internally.
func insertTestProduct(tb testing.TB, coll *treap.PersistentPayloadTreap[types.StringKey, types.JsonPayload[testutil.TestProductData]], name string, price float64) {
	tb.Helper()

	key := types.StringKey(name)
	payload := types.JsonPayload[testutil.TestProductData]{
		Value: testutil.TestProductData{
			Name:  name,
			Price: price,
			Stock: 100,
		},
	}
	coll.Insert(&key, payload)
}
