package treap

import (
	"crypto/md5"
	"encoding/binary"
	"testing"

	"bobbob/internal/testutil"
)

// TestMD5KeyPriorityProvider verifies that MD5Key implements PriorityProvider
// and that its priority is derived from the hash value.
func TestMD5KeyPriorityProvider(t *testing.T) {
	// Create an MD5 hash from some data
	data := []byte("test data for MD5 hashing")
	hash := md5.Sum(data)
	key := MD5Key(hash)

	// Verify it implements PriorityProvider
	var _ PriorityProvider = key

	// Verify the priority matches the first 4 bytes of the hash
	expectedPriority := Priority(binary.LittleEndian.Uint32(hash[0:4]))
	actualPriority := key.Priority()

	if actualPriority != expectedPriority {
		t.Errorf("Priority mismatch: expected %d, got %d", expectedPriority, actualPriority)
	}
}

// TestTreapWithPriorityProvider verifies that Insert uses the key's Priority method
// when the key implements PriorityProvider.
func TestTreapWithPriorityProvider(t *testing.T) {
	treap := NewTreap[MD5Key](MD5Less)

	// Create a few MD5 keys
	keys := make([]MD5Key, 3)
	for i := range keys {
		data := []byte{byte(i), byte(i * 2), byte(i * 3)}
		keys[i] = md5.Sum(data)
	}

	// Insert the keys
	for _, key := range keys {
		treap.Insert(key)
	}

	// Verify each key was inserted and its priority matches what Priority() returns
	for _, key := range keys {
		node := treap.Search(key)
		if node.IsNil() {
			t.Errorf("Key not found: %v", key)
			continue
		}

		expectedPriority := key.Priority()
		actualPriority := node.GetPriority()

		if actualPriority != expectedPriority {
			t.Errorf("Priority mismatch for key %v: expected %d, got %d",
				key, expectedPriority, actualPriority)
		}
	}
}

// TestPersistentTreapWithPriorityProvider verifies that persistent treaps
// also use the key's Priority method.
func TestPersistentTreapWithPriorityProvider(t *testing.T) {
	_, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()

	keyTemplate := MD5Key{}
	treap := NewPersistentTreap[MD5Key](MD5Less, &keyTemplate, stre)

	// Create MD5 keys
	data1 := []byte("first file content")
	data2 := []byte("second file content")
	data3 := []byte("third file content")

	key1 := md5.Sum(data1)
	key2 := md5.Sum(data2)
	key3 := md5.Sum(data3)

	md5Key1 := MD5Key(key1)
	md5Key2 := MD5Key(key2)
	md5Key3 := MD5Key(key3)

	// Insert the keys
	treap.Insert(&md5Key1)
	treap.Insert(&md5Key2)
	treap.Insert(&md5Key3)

	// Verify each key's priority
	testCases := []struct {
		key      *MD5Key
		expected Priority
	}{
		{&md5Key1, md5Key1.Priority()},
		{&md5Key2, md5Key2.Priority()},
		{&md5Key3, md5Key3.Priority()},
	}

	for _, tc := range testCases {
		node := treap.Search(tc.key)
		if node.IsNil() {
			t.Errorf("Key not found: %v", tc.key)
			continue
		}

		actualPriority := node.GetPriority()
		if actualPriority != tc.expected {
			t.Errorf("Priority mismatch for key %v: expected %d, got %d",
				tc.key, tc.expected, actualPriority)
		}
	}
}

// TestIntKeyWithoutPriorityProvider verifies that keys without PriorityProvider
// still get random priorities as before.
func TestIntKeyWithoutPriorityProvider(t *testing.T) {
	treap := NewTreap[IntKey](IntLess)

	// Insert multiple nodes
	key1 := IntKey(10)
	key2 := IntKey(20)
	key3 := IntKey(30)

	treap.Insert(key1)
	treap.Insert(key2)
	treap.Insert(key3)

	// Verify all keys exist
	for _, key := range []IntKey{key1, key2, key3} {
		node := treap.Search(key)
		if node.IsNil() {
			t.Errorf("Key not found: %v", key)
		}
	}

	// Verify that priorities are different (highly likely with random generation)
	// This isn't a guarantee, but with 32-bit random numbers, collision is very unlikely
	node1 := treap.Search(key1)
	node2 := treap.Search(key2)
	node3 := treap.Search(key3)

	priorities := map[Priority]bool{
		node1.GetPriority(): true,
		node2.GetPriority(): true,
		node3.GetPriority(): true,
	}

	// If all three are different, we have 3 unique priorities
	if len(priorities) < 2 {
		t.Logf("Warning: Found duplicate priorities (may be rare random occurrence)")
		t.Logf("Priorities: %d, %d, %d",
			node1.GetPriority(), node2.GetPriority(), node3.GetPriority())
	}
}

// TestInsertComplexOverridesPriorityProvider verifies that InsertComplex
// can override the priority even when PriorityProvider is implemented.
func TestInsertComplexOverridesPriorityProvider(t *testing.T) {
	treap := NewTreap[MD5Key](MD5Less)

	data := []byte("test data")
	hash := md5.Sum(data)
	key := MD5Key(hash)

	// Insert with a specific priority using InsertComplex
	customPriority := Priority(99999)
	treap.InsertComplex(key, customPriority)

	// Verify the custom priority was used, not the one from Priority()
	node := treap.Search(key)
	if node.IsNil() {
		t.Fatal("Key not found")
	}

	actualPriority := node.GetPriority()
	if actualPriority != customPriority {
		t.Errorf("Expected custom priority %d, got %d", customPriority, actualPriority)
	}

	// Verify it's different from what Priority() would return
	keyPriority := key.Priority()
	if actualPriority == keyPriority {
		t.Errorf("Custom priority should differ from key.Priority(), but both are %d", actualPriority)
	}
}
