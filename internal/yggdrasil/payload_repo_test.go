package yggdrasil

import (
	"fmt"
	"testing"
)

type dummyPayload struct {
	val int
}

func (dp dummyPayload) Value() int {
	return dp.val
}

func (dp dummyPayload) SizeInBytes() int {
	return 4
}

func (dp dummyPayload) Marshal() ([]byte, error) {
	return []byte{byte(dp.val >> 24), byte(dp.val >> 16), byte(dp.val >> 8), byte(dp.val)}, nil
}

func (dp dummyPayload) Unmarshal(data []byte) (UntypedPersistentPayload, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short to unmarshal dummyPayload")
	}
	dp.val = int(data[0])<<24 | int(data[1])<<16 | int(data[2])<<8 | int(data[3])
	return dp, nil
}

func (dp dummyPayload) Equals(other dummyPayload) bool {
	return dp.val == other.val
}

func TestPayloadRepo_Get_IntKey(t *testing.T) {
	// Create a store using setupTestStore
	tmpStore := setupTestStore(t)
	defer tmpStore.Close()
	tm := NewTypeMap()
	// Add IntKey type to the TypeMap
	repo := NewPayloadRepo(tm, tmpStore)

	key := IntKey(123)
	dummyPayloadConstructor := func() (PersistentPayloadTreapInterface[IntKey, dummyPayload], error) {
		var ttmp *PersistentPayloadTreap[IntKey, dummyPayload]
		ttmp = NewPersistentPayloadTreap[IntKey, dummyPayload](IntLess, (*IntKey)(new(int32)), tmpStore)
		return ttmp, nil
	}

	// First Get should create a new payload
	treap1, err := PayloadRepoGet[IntKey, dummyPayload](repo, key, dummyPayloadConstructor)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if treap1 == nil {
		t.Fatalf("Expected non-nil payload from Get")
	}
	treap1.Insert(&key, Priority(0), dummyPayload{val: 42})
	result1 := treap1.Search(&key)
	if result1 == nil {
		t.Errorf("Expected to find the key in the payload1, but it was not found")
	}
	payload1Result := result1.GetPayload()

	if payload1Result.val != 42 {
		t.Errorf("Expected payload value to be 42, got %d", payload1Result.val)
	}

	// Second Get should return the same payload (pointer equality)
	treap2, err := PayloadRepoGet[IntKey, dummyPayload](repo, key, dummyPayloadConstructor)
	if err != nil {
		t.Fatalf("Second Get failed: %v", err)
	}
	if treap2 == nil {
		t.Fatalf("Expected non-nil payload from second Get")
	}
	if treap1 != treap2 {
		t.Errorf("Expected same payload instance for repeated Get, got different instances")
	}

	result := treap2.Search(&key)
	if result == nil {
		t.Errorf("Expected to find the key in the payload, but it was not found")
	}
	finalPayload := result.GetPayload()

	if finalPayload.val != 42 {
		t.Errorf("Expected payload value to be 42, got %d", finalPayload.val)
	}
}
