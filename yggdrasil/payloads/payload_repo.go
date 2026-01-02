package payloads

import (
	"fmt"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"

	"github.com/cbehopkins/bobbob/store"
)

// FIXME is this a dead file - I think so...
type PayloadRepo struct {
	PersistentPayloadTreap treap.PersistentPayloadTreapInterface[vault.ShortCodeType, treap.UntypedPersistentPayload]
	TypeMap                *vault.TypeMap
}

func NewPayloadRepo(tm *vault.TypeMap, store store.Storer) *PayloadRepo {
	keyTemplate := vault.ShortCodeType(0)
	ppt := treap.NewPersistentPayloadTreap[vault.ShortCodeType, treap.UntypedPersistentPayload](vault.ShortCodeLess, &keyTemplate, store)
	return &PayloadRepo{
		PersistentPayloadTreap: ppt,
		TypeMap:                tm,
	}
}

type PayloadConstructor[T, P any] func() (treap.PersistentPayloadTreapInterface[T, P], error)

func PayloadRepoGet[T, P any](pr *PayloadRepo, key treap.Key[T], pc PayloadConstructor[T, P]) (treap.PersistentPayloadTreapInterface[T, P], error) {
	if pr.TypeMap == nil {
		return nil, fmt.Errorf("type map not initialized")
	}
	shortCode, err := pr.TypeMap.GetShortCode(key)
	if err != nil {
		return nil, err
	}
	if node := pr.PersistentPayloadTreap.Search(&shortCode); node != nil {
		if payload, ok := node.GetPayload().(treap.PersistentPayloadTreapInterface[T, P]); ok {
			return payload, nil
		} else {
			return nil, fmt.Errorf("payload found, but type mismatch")
		}
	}
	newPayload, err := pc()
	if err != nil {
		return nil, err
	}
	bob, ok := newPayload.(treap.PersistentPayload[treap.UntypedPersistentPayload])
	if !ok {
		return nil, fmt.Errorf("payload does not implement treap.PersistentPayload[treap.UntypedPersistentPayload]")
	}
	pr.PersistentPayloadTreap.Insert(&shortCode, bob)
	return newPayload, nil
}
