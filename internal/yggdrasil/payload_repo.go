package yggdrasil

import (
	"fmt"
	"math/rand"

	"bobbob/internal/store"
)

type PayloadRepo struct {
	PersistentPayloadTreap PersistentPayloadTreapInterface[ShortCodeType, UntypedPersistentPayload]
	TypeMap                *TypeMap
}

func NewPayloadRepo(tm *TypeMap, store store.Storer) *PayloadRepo {
	keyTemplate := ShortCodeType(0)
	ppt := NewPersistentPayloadTreap[ShortCodeType, UntypedPersistentPayload](ShortCodeLess, &keyTemplate, store)
	return &PayloadRepo{
		PersistentPayloadTreap: ppt,
		TypeMap:                tm,
	}
}

type PayloadConstructor[T, P any] func() (PersistentPayloadTreapInterface[T, P], error)

func PayloadRepoGet[T, P any](pr *PayloadRepo, key Key[T], pc PayloadConstructor[T, P]) (PersistentPayloadTreapInterface[T, P], error) {
	if pr.TypeMap == nil {
		return nil, fmt.Errorf("type map not initialized")
	}
	shortCode, err := pr.TypeMap.getShortCode(key)
	if err != nil {
		return nil, err
	}
	if node := pr.PersistentPayloadTreap.Search(&shortCode); node != nil {
		if payload, ok := node.GetPayload().(PersistentPayloadTreapInterface[T, P]); ok {
			return payload, nil
		} else {
			return nil, fmt.Errorf("payload found, but type mismatch")
		}
	}
	newPayload, err := pc()
	if err != nil {
		return nil, err
	}
	bob, ok := newPayload.(PersistentPayload[UntypedPersistentPayload])
	if !ok {
		return nil, fmt.Errorf("payload does not implement PersistentPayload[UntypedPersistentPayload]")
	}
	pr.PersistentPayloadTreap.Insert(&shortCode, Priority(rand.Intn(100)), bob)
	return newPayload, nil
}
