package yggdrasil

import (
	"fmt"

	"bobbob/internal/store"
)

type PersistentObjectId store.ObjectId

func (id PersistentObjectId) New() PersistentKey[PersistentObjectId] {
	v := PersistentObjectId(store.ObjectId(-1))
	return &v
}

func (id PersistentObjectId) Equals(other PersistentObjectId) bool {
	return id == other
}

func (id PersistentObjectId) MarshalToObjectId(stre store.Storer) (store.ObjectId, error) {
	return store.ObjectId(id), nil
}

func (id *PersistentObjectId) UnmarshalFromObjectId(id_src store.ObjectId, stre store.Storer) error {
	*id = PersistentObjectId(id_src)
	return nil
}

func (id PersistentObjectId) SizeInBytes() int {
	return store.ObjectId(0).SizeInBytes()
}

func (id PersistentObjectId) Value() PersistentObjectId {
	return id
}

type PersistentTreapNodeInterface[T any] interface {
	TreapNodeInterface[T]
	ObjectId() store.ObjectId
	SetObjectId(store.ObjectId)
}

// PersistentTreapNode represents a node in the persistent treap.
type PersistentTreapNode[T any] struct {
	TreapNode[T]  // Embed TreapNode to reuse its logic
	objectId      store.ObjectId
	leftObjectId  store.ObjectId
	rightObjectId store.ObjectId
	Store         store.Storer
	parent        *PersistentTreap[T]
}

func (n *PersistentTreapNode[T]) newFromObjectId(objId store.ObjectId) (*PersistentTreapNode[T], error) {
	tmp := NewPersistentTreapNode(n.parent.keyTemplate.New(), 0, n.Store, n.parent)
	err := store.ReadGeneric(n.Store, tmp, objId)
	if err != nil {
		return nil, err
	}
	return tmp, nil
}

func NewFromObjectId[T any](objId store.ObjectId, parent *PersistentTreap[T], stre store.Storer) (*PersistentTreapNode[T], error) {
	tmp := NewPersistentTreapNode[T](parent.keyTemplate.New(), 0, stre, parent)
	err := store.ReadGeneric(stre, tmp, objId)
	if err != nil {
		return nil, err
	}
	return tmp, nil
}

func asPersistentTreapNode[T any](node TreapNodeInterface[T]) (*PersistentTreapNode[T], error) {
	if node == nil {
		return nil, nil
	}
	if persistentNode, ok := node.(PersistentTreapNodeInterface[T]); ok {
		return persistentNode.(*PersistentTreapNode[T]), nil // Cast to concrete type
	}
	return nil, fmt.Errorf("node is not of type PersistentTreapNode")
}

// GetKey returns the key of the node.
func (n *PersistentTreapNode[T]) GetKey() Key[T] {
	return n.TreapNode.key // Explicitly access the key field from the embedded TreapNode
}

// GetPriority returns the priority of the node.
func (n *PersistentTreapNode[T]) GetPriority() Priority {
	return n.TreapNode.priority
}

// SetPriority sets the priority of the node.
func (n *PersistentTreapNode[T]) SetPriority(p Priority) {
	n.objectId = store.ObjNotAllocated
	n.TreapNode.priority = p
}

// GetLeft returns the left child of the node.
func (n *PersistentTreapNode[T]) GetLeft() TreapNodeInterface[T] {
	if n.TreapNode.left == nil && store.IsValidObjectId(n.leftObjectId) {
		tmp, err := n.newFromObjectId(n.leftObjectId)
		if err != nil {
			return nil
		}
		n.TreapNode.left = tmp
	}
	return n.TreapNode.left
}

// GetRight returns the right child of the node.
func (n *PersistentTreapNode[T]) GetRight() TreapNodeInterface[T] {
	if n.TreapNode.right == nil && store.IsValidObjectId(n.rightObjectId) {
		tmp, err := n.newFromObjectId(n.rightObjectId)
		if err != nil {
			return nil
		}
		n.TreapNode.right = tmp
	}
	return n.TreapNode.right
}

// SetLeft sets the left child of the node.
func (n *PersistentTreapNode[T]) SetLeft(left TreapNodeInterface[T]) {
	n.TreapNode.left = left
	n.objectId = store.ObjNotAllocated
}

// SetRight sets the right child of the node.
func (n *PersistentTreapNode[T]) SetRight(right TreapNodeInterface[T]) {
	n.TreapNode.right = right
	n.objectId = store.ObjNotAllocated
}

// IsNil checks if the node is nil.
func (n *PersistentTreapNode[T]) IsNil() bool {
	return n == nil
}

func (n *PersistentTreapNode[T]) sizeInBytes() int {
	objectIdSize := n.objectId.SizeInBytes()
	keySize := objectIdSize
	prioritySize := n.priority.SizeInBytes()
	leftSize := objectIdSize
	rightSize := objectIdSize
	selfSize := objectIdSize
	return keySize + prioritySize + leftSize + rightSize + selfSize
}

func PersistentTreapObjectSizes() []int {
	n := &PersistentTreapNode[any]{}
	return []int{
		n.objectId.SizeInBytes(),
		n.sizeInBytes(),
	}
}

func (n *PersistentTreapNode[T]) ObjectId() store.ObjectId {
	if n == nil {
		return store.ObjNotAllocated
	}
	if n.objectId < 0 {
		// FIXME ignored error - update to return error
		n.objectId, _ = n.Store.NewObj(n.sizeInBytes())
	}
	return n.objectId
}

// SetObjectId sets the object ID of the node.
func (n *PersistentTreapNode[T]) SetObjectId(id store.ObjectId) {
	n.objectId = id
}

func (n *PersistentTreapNode[T]) Persist() error {
	if n == nil {
		return nil
	}
	if n.TreapNode.left != nil && !store.IsValidObjectId(n.leftObjectId) {
		leftNode, err := asPersistentTreapNode[T](n.TreapNode.left)
		if err != nil {
			return err
		}
		if leftNode != nil {
			err := leftNode.Persist()
			if err != nil {
				return err
			}
		}
		n.leftObjectId = leftNode.ObjectId()
		n.TreapNode.left = nil
	}
	if n.TreapNode.right != nil && !store.IsValidObjectId(n.rightObjectId) {
		rightNode, err := asPersistentTreapNode[T](n.TreapNode.right)
		if err != nil {
			return err
		}
		if rightNode != nil {
			err := rightNode.Persist()
			if err != nil {
				return err
			}
		}
		n.rightObjectId = rightNode.ObjectId()
		n.TreapNode.right = nil
	}
	return n.persist()
}

func (n *PersistentTreapNode[T]) persist() error {
	buf, err := n.Marshal()
	if err != nil {
		return err
	}
	return store.WriteBytesToObj(n.Store, buf, n.ObjectId())
}

func (n *PersistentTreapNode[T]) Marshal() ([]byte, error) {
	buf := make([]byte, n.sizeInBytes())
	offset := 0
	keyAsObjectId, err := n.key.(PersistentKey[T]).MarshalToObjectId(n.Store)
	if err != nil {
		return nil, err
	}

	if store.IsValidObjectId(n.leftObjectId) {
		if n.TreapNode.left != nil {
			leftNode, err := asPersistentTreapNode[T](n.TreapNode.left)
			if err != nil {
				return nil, err
			}
			newLeftObjectId := leftNode.ObjectId()
			if newLeftObjectId != n.leftObjectId {
				n.leftObjectId = newLeftObjectId
				n.objectId = store.ObjNotAllocated
			}
		}
	} else {
		if n.TreapNode.left != nil {
			leftNode, err := asPersistentTreapNode[T](n.TreapNode.left)
			if err != nil {
				return nil, err
			}
			n.leftObjectId = leftNode.ObjectId()
			n.objectId = store.ObjNotAllocated
		}
	}
	if store.IsValidObjectId(n.rightObjectId) {
		if n.TreapNode.right != nil {
			rightNode, err := asPersistentTreapNode[T](n.TreapNode.right)
			if err != nil {
				return nil, err
			}
			newRightObjectId := rightNode.ObjectId()
			if newRightObjectId != n.rightObjectId {
				n.rightObjectId = newRightObjectId
				n.objectId = store.ObjNotAllocated
			}
		}
	} else {
		if n.TreapNode.right != nil {
			rightNode, err := asPersistentTreapNode[T](n.TreapNode.right)
			if err != nil {
				return nil, err
			}
			n.rightObjectId = rightNode.ObjectId()
			n.objectId = store.ObjNotAllocated
		}
	}
	marshalables := []interface {
		Marshal() ([]byte, error)
	}{
		keyAsObjectId,
		n.priority,
		n.leftObjectId,
		n.rightObjectId,
		n.ObjectId(),
	}

	for _, m := range marshalables {
		data, err := m.Marshal()
		if err != nil {
			return nil, err
		}
		copy(buf[offset:], data)
		offset += len(data)
	}

	return buf, nil
}

func (n *PersistentTreapNode[T]) unmarshal(data []byte, key PersistentKey[T]) error {
	offset := 0

	keyAsObjectId := store.ObjectId(0)
	err := keyAsObjectId.Unmarshal(data[offset:])
	if err != nil {
		return err
	}
	offset += keyAsObjectId.SizeInBytes()
	tmpKey := key.New()
	err = tmpKey.UnmarshalFromObjectId(keyAsObjectId, n.Store)
	if err != nil {
		return err
	}
	n.key = tmpKey.(Key[T])

	err = n.priority.Unmarshal(data[offset:])
	if err != nil {
		return err
	}
	offset += n.priority.SizeInBytes()

	leftObjectId := store.ObjectId(store.ObjNotAllocated)
	err = leftObjectId.Unmarshal(data[offset:])
	if err != nil {
		return err
	}
	offset += leftObjectId.SizeInBytes()
	n.leftObjectId = leftObjectId

	rightObjectId := store.ObjectId(store.ObjNotAllocated)
	err = rightObjectId.Unmarshal(data[offset:])
	if err != nil {
		return err
	}
	offset += rightObjectId.SizeInBytes()
	n.rightObjectId = rightObjectId

	selfObjectId := store.ObjectId(store.ObjNotAllocated)
	err = selfObjectId.Unmarshal(data[offset:])
	if err != nil {
		return err
	}
	offset += selfObjectId.SizeInBytes()
	n.objectId = selfObjectId

	return nil
}

func (n *PersistentTreapNode[T]) Unmarshal(data []byte) error {
	return n.unmarshal(data, n.parent.keyTemplate)
}

// PersistentTreap represents a persistent treap data structure.
type PersistentTreap[T any] struct {
	Treap[T]
	keyTemplate PersistentKey[T]
	Store       store.Storer
}

// NewPersistentTreapNode creates a new PersistentTreapNode with the given key, priority, and store reference.
func NewPersistentTreapNode[T any](key PersistentKey[T], priority Priority, stre store.Storer, parent *PersistentTreap[T]) *PersistentTreapNode[T] {
	return &PersistentTreapNode[T]{
		TreapNode: TreapNode[T]{
			key:      key,
			priority: priority,
			left:     nil,
			right:    nil,
		},
		objectId: store.ObjNotAllocated,
		Store:    stre,
		parent:   parent,
	}
}

// NewPersistentTreap creates a new PersistentTreap with the given comparison function and store reference.
func NewPersistentTreap[T any](lessFunc func(a, b T) bool, keyTemplate PersistentKey[T], store store.Storer) *PersistentTreap[T] {
	return &PersistentTreap[T]{
		Treap: Treap[T]{
			root: nil,
			Less: func(a, b T) bool {
				return lessFunc(a, b)
			},
		},
		keyTemplate: keyTemplate,
		Store:       store,
	}
}

// rotateRight performs a right rotation on the given node.
func (t *PersistentTreap[T]) rotateRight(node TreapNodeInterface[T]) *PersistentTreapNode[T] {
	newRoot := t.Treap.rotateRight(node).(*PersistentTreapNode[T])
	nodeCast := node.(*PersistentTreapNode[T])

	if nodeCast.objectId > store.ObjNotAllocated {
		t.Store.DeleteObj(store.ObjectId(nodeCast.objectId))
	}
	if newRoot.objectId > store.ObjNotAllocated {
		t.Store.DeleteObj(store.ObjectId(newRoot.objectId))
	}
	nodeCast.objectId = store.ObjNotAllocated
	newRoot.objectId = store.ObjNotAllocated
	return newRoot
}

// rotateLeft performs a left rotation on the given node.
func (t *PersistentTreap[T]) rotateLeft(node TreapNodeInterface[T]) *PersistentTreapNode[T] {
	newRoot := t.Treap.rotateLeft(node).(*PersistentTreapNode[T])
	nodeCast := node.(*PersistentTreapNode[T])
	if nodeCast.objectId > store.ObjNotAllocated {
		t.Store.DeleteObj(store.ObjectId(nodeCast.objectId))
	}
	if newRoot.objectId > store.ObjNotAllocated {
		t.Store.DeleteObj(store.ObjectId(newRoot.objectId))
	}
	nodeCast.objectId = store.ObjNotAllocated
	newRoot.objectId = store.ObjNotAllocated
	return newRoot
}

// insert is a helper function that inserts a new node into the persistent treap.
func (t *PersistentTreap[T]) insert(node TreapNodeInterface[T], newNode TreapNodeInterface[T]) TreapNodeInterface[T] {
	// Call the insert method of the embedded Treap
	result := t.Treap.insert(node, newNode)

	nodeCast := result.(PersistentTreapNodeInterface[T])
	objId := nodeCast.ObjectId()
	if objId > store.ObjNotAllocated {
		t.Store.DeleteObj(store.ObjectId(objId))
	}
	nodeCast.SetObjectId(store.ObjNotAllocated)

	return result
}

// delete removes the node with the given key from the persistent treap.
func (t *PersistentTreap[T]) delete(node TreapNodeInterface[T], key T) TreapNodeInterface[T] {
	// Call the delete method of the embedded Treap
	result := t.Treap.delete(node, key)

	if result != nil && !result.IsNil() {
		nodeCast := result.(PersistentTreapNodeInterface[T])
		objId := nodeCast.ObjectId()
		if objId > store.ObjNotAllocated {
			t.Store.DeleteObj(objId)
		}
		nodeCast.SetObjectId(store.ObjNotAllocated)
	}

	return result
}

// Insert inserts a new node with the given key and priority into the persistent treap.
func (t *PersistentTreap[T]) Insert(key PersistentKey[T], priority Priority) {
	newNode := NewPersistentTreapNode(key, priority, t.Store, t)
	t.root = t.insert(t.root, newNode)
}

// Delete removes the node with the given key from the persistent treap.
func (t *PersistentTreap[T]) Delete(key PersistentKey[T]) {
	t.root = t.delete(t.root, key.Value())
}

// Search searches for the node with the given key in the persistent treap.
func (t *PersistentTreap[T]) Search(key PersistentKey[T]) TreapNodeInterface[T] {
	return t.search(t.root, key.Value())
}

// UpdatePriority updates the priority of the node with the given key.
func (t *PersistentTreap[T]) UpdatePriority(key PersistentKey[T], newPriority Priority) {
	node := t.Search(key)
	if node != nil && !node.IsNil() {
		node.SetPriority(newPriority)
		t.Delete(key)
		t.Insert(key, newPriority)
	}
}

func (t *PersistentTreap[T]) Persist() error {
	if t.root != nil {
		rootNode := t.root.(*PersistentTreapNode[T])
		return rootNode.Persist()
	}
	return nil
}

func (t *PersistentTreap[T]) Load(objId store.ObjectId) error {
	var err error
	t.root, err = NewFromObjectId(objId, t, t.Store)
	return err
}
