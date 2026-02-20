package types

import (
	"testing"

	"github.com/cbehopkins/bobbob/store"
)

// TestPersistentKeyInterface verifies that all key types implement the complete PersistentKey interface,
// including the new DeleteDependents method.
func TestPersistentKeyInterface(t *testing.T) {
	mockStore := store.NewMockStore()
	defer mockStore.Close()

	// Test IntKey
	t.Run("IntKey", func(t *testing.T) {
		var key IntKey = 42
		if err := key.DeleteDependents(mockStore); err != nil {
			t.Errorf("IntKey.DeleteDependents failed: %v", err)
		}
	})

	// Test ShortUIntKey
	t.Run("ShortUIntKey", func(t *testing.T) {
		var key ShortUIntKey = 42
		if err := key.DeleteDependents(mockStore); err != nil {
			t.Errorf("ShortUIntKey.DeleteDependents failed: %v", err)
		}
	})

	// Test StringKey
	t.Run("StringKey", func(t *testing.T) {
		key := StringKey("test")
		if err := key.DeleteDependents(mockStore); err != nil {
			t.Errorf("StringKey.DeleteDependents failed: %v", err)
		}
	})

	// Test MD5Key
	t.Run("MD5Key", func(t *testing.T) {
		key := MD5Key{}
		if err := key.DeleteDependents(mockStore); err != nil {
			t.Errorf("MD5Key.DeleteDependents failed: %v", err)
		}
	})
}

// TestPersistentKeyInterfaceCompleteness is a compile-time check that ensures
// our key types implement the full PersistentKey interface.
func TestPersistentKeyInterfaceCompleteness(t *testing.T) {
	// These assignments will fail to compile if the types don't fully implement PersistentKey
	var _ PersistentKey[IntKey] = (*IntKey)(nil)
	var _ PersistentKey[ShortUIntKey] = (*ShortUIntKey)(nil)
	var _ PersistentKey[StringKey] = (*StringKey)(nil)
	var _ PersistentKey[MD5Key] = (*MD5Key)(nil)
}
