package treap

import (
	"fmt"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
)

type persistSyncNode[T any] interface {
	PersistentNodeWalker[T]
	getLeftObjectId() store.ObjectId
	setLeftObjectId(store.ObjectId)
	getRightObjectId() store.ObjectId
	setRightObjectId(store.ObjectId)
	SetObjectId(store.ObjectId)
	persistSelf() error
}

func persistLockedTreeCommon[T any, N persistSyncNode[T]](
	root N,
	rangePostOrder func(N, func(N) error) ([]N, error),
) error {
	if any(root) == nil || root.IsNil() {
		return nil
	}

	_, err := rangePostOrder(root, func(node N) error {
		if any(node) == nil || node.IsNil() {
			return nil
		}

		leftChild := node.GetLeftChild()
		if leftChild != nil && !leftChild.IsNil() {
			leftNode, ok := leftChild.(persistSyncNode[T])
			if !ok {
				return fmt.Errorf("left child is not a persistent node")
			}
			leftObjId := leftNode.GetObjectIdNoAlloc()
			if store.IsValidObjectId(leftObjId) && leftObjId != node.getLeftObjectId() {
				node.setLeftObjectId(leftObjId)
				node.SetObjectId(bobbob.ObjNotAllocated)
			}
		}

		rightChild := node.GetRightChild()
		if rightChild != nil && !rightChild.IsNil() {
			rightNode, ok := rightChild.(persistSyncNode[T])
			if !ok {
				return fmt.Errorf("right child is not a persistent node")
			}
			rightObjId := rightNode.GetObjectIdNoAlloc()
			if store.IsValidObjectId(rightObjId) && rightObjId != node.getRightObjectId() {
				node.setRightObjectId(rightObjId)
				node.SetObjectId(bobbob.ObjNotAllocated)
			}
		}

		return node.persistSelf()
	})
	return err
}
