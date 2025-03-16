package yggdrasil

import "github.com/cbehopkins/bobbob/internal/store"

type PersistentKey interface {
	Key
    New() PersistentKey
	MarshalToObjectId() (store.ObjectId, error)
	UnmarshalFromObjectId(store.ObjectId) error
}
// PersistentTreapNode represents a node in the persistent treap.
type PersistentTreapNode struct {
    TreapNode
	left     *PersistentTreapNode
    right    *PersistentTreapNode
    objectId  store.ObjectId
    leftObjectId store.ObjectId
    rightObjectId store.ObjectId
    Store    store.Storer
}

// GetKey returns the key of the node.
func (n *PersistentTreapNode) GetKey() Key {
    return n.key
}

// GetPriority returns the priority of the node.
func (n *PersistentTreapNode) GetPriority() Priority {
    return n.priority
}
func (n *PersistentTreapNode) SetPriority(p Priority) {
    n.objectId = store.ObjNotAllocated
    n.priority = p
}
// GetLeft returns the left child of the node.
func (n *PersistentTreapNode) GetLeft() TreapNodeInterface {
    return n.left
}

// GetRight returns the right child of the node.
func (n *PersistentTreapNode) GetRight() TreapNodeInterface {
    return n.right
}

// SetLeft sets the left child of the node.
func (n *PersistentTreapNode) SetLeft(left TreapNodeInterface) {
    tmp := left.(*PersistentTreapNode)
    // FIXME ignored error - update to return error
    if tmp != n.left {
        // Object ID is invalidated if the left child is changed
        n.left = tmp
        n.objectId = store.ObjNotAllocated
    }
}

// SetRight sets the right child of the node.
func (n *PersistentTreapNode) SetRight(right TreapNodeInterface) {
    tmp := right.(*PersistentTreapNode)
    // FIXME ignored error - update to return error
    if tmp != n.right {
        // Object ID is invalidated if the right child is changed
        n.right = tmp
        n.objectId = store.ObjNotAllocated
    }
}

// IsNil checks if the node is nil.
func (n *PersistentTreapNode) IsNil() bool {
    return n == nil
}

func (n *PersistentTreapNode) sizeInBytes() int {
    objectIdSize := n.objectId.SizeInBytes()
    keySize := objectIdSize
    prioritySize := n.priority.SizeInBytes()
    leftSize := objectIdSize
    rightSize := objectIdSize
    selfSize := objectIdSize
    return keySize + prioritySize + leftSize + rightSize + selfSize
}

func (n *PersistentTreapNode) ObjectId() store.ObjectId {
    if n == nil {
        return store.ObjNotAllocated
    }
    if n.objectId < 0 {
        // FIXME ignored error - update to return error
        n.objectId, _ = n.Store.NewObj(n.sizeInBytes())
    }
    return n.objectId
}

// Persist self to disk
func (n *PersistentTreapNode) Persist() error {
    if n == nil {
        return nil
    }
    buf, err := n.Marshal()
    if err != nil {
        return err
    }
    return n.Store.WriteBytesToObj(buf, n.ObjectId())
}

func (n *PersistentTreapNode) Marshal() ([]byte, error) {
    buf := make([]byte, n.sizeInBytes())
    offset := 0
    keyAsObjectId, err := n.key.(PersistentKey).MarshalToObjectId()
    if err != nil {
        return nil, err
    }
    newLeftObjectId := n.left.ObjectId()
    newRightObjectId := n.right.ObjectId()
    if newLeftObjectId != n.leftObjectId  || newRightObjectId != n.rightObjectId {
        n.leftObjectId = newLeftObjectId
        n.rightObjectId = newRightObjectId
        n.objectId = store.ObjNotAllocated
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

func (n *PersistentTreapNode) Unmarshal(data []byte, key PersistentKey) error {
    offset := 0

    keyAsObjectId := store.ObjectId(0)
    err := keyAsObjectId.Unmarshal(data[offset:])
    if err != nil {
        return err
    }
    offset += keyAsObjectId.SizeInBytes()
    tmpKey := key.New()
    err = tmpKey.UnmarshalFromObjectId(keyAsObjectId)
    if err != nil {
        return err
    }
    n.key = tmpKey

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

    rightObjectId := store.ObjectId(store.ObjNotAllocated)
    err = rightObjectId.Unmarshal(data[offset:])
    if err != nil {
        return err
    }
    offset += rightObjectId.SizeInBytes()

    selfObjectId := store.ObjectId(store.ObjNotAllocated)
    err = selfObjectId.Unmarshal(data[offset:])
    if err != nil {
        return err
    }
    offset += selfObjectId.SizeInBytes()
    n.objectId = selfObjectId

    return nil
}

// PersistentTreap represents a persistent treap data structure.
type PersistentTreap struct {
    Treap
    Store store.Storer
}

// NewPersistentTreapNode creates a new PersistentTreapNode with the given key, priority, and store reference.
func NewPersistentTreapNode(key PersistentKey, priority Priority, stre store.Storer) *PersistentTreapNode {
    return &PersistentTreapNode{
        TreapNode: TreapNode{
            key:      key,
            priority: priority,
            left:     nil,
            right:    nil,
        },
        objectId: store.ObjNotAllocated,
        Store:    stre,
    }
}

// NewPersistentTreap creates a new PersistentTreap with the given comparison function and store reference.
func NewPersistentTreap(lessFunc func(a, b any) bool, store store.Storer) *PersistentTreap {
    return &PersistentTreap{
        Treap: Treap{
            root: nil,
            Less: lessFunc,
        },
        Store: store,
    }
}

// rotateRight performs a right rotation on the given node.
func (t *PersistentTreap) rotateRight(node TreapNodeInterface) *PersistentTreapNode {
    newRoot := t.Treap.rotateRight(node).(*PersistentTreapNode)
    nodeCast := node.(*PersistentTreapNode)

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
func (t *PersistentTreap) rotateLeft(node TreapNodeInterface) *PersistentTreapNode {
    newRoot := t.Treap.rotateLeft(node).(*PersistentTreapNode)
    nodeCast := node.(*PersistentTreapNode)
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
func (t *PersistentTreap) insert(node TreapNodeInterface, newNode TreapNodeInterface) TreapNodeInterface {
    if node == nil || node.IsNil() {
        return newNode
    }

    if t.Less(newNode.GetKey(), node.GetKey()) {
        node.SetLeft(t.insert(node.GetLeft(), newNode))
        if node.GetLeft() != nil && node.GetLeft().GetPriority() > node.GetPriority() {
            node = t.rotateRight(node)
        }
    } else {
        node.SetRight(t.insert(node.GetRight(), newNode))
        if node.GetRight() != nil && node.GetRight().GetPriority() > node.GetPriority() {
            node = t.rotateLeft(node)
        }
    }
    nodeCast := node.(*PersistentTreapNode)

    if nodeCast.objectId > store.ObjNotAllocated {
        t.Store.DeleteObj(store.ObjectId(nodeCast.objectId))
    }
    nodeCast.objectId = store.ObjNotAllocated
    return node
}

// delete removes the node with the given key from the persistent treap.
func (t *PersistentTreap) delete(node TreapNodeInterface, key any) TreapNodeInterface {
    if node == nil || node.IsNil() {
        return nil
    }

    if t.Less(key, node.GetKey()) {
        node.SetLeft(t.delete(node.GetLeft().(*PersistentTreapNode), key))
    } else if t.Less(node.GetKey(), key) {
        node.SetRight(t.delete(node.GetRight().(*PersistentTreapNode), key))
    } else {
        left := node.GetLeft().(*PersistentTreapNode)
        right := node.GetRight().(*PersistentTreapNode)
        if left == nil {
            return right
        } 
        if right == nil {
            return left
        } 
        if left.GetPriority() > right.GetPriority() {
            node = t.rotateRight(node)
            node.SetRight(t.delete(node.GetRight(), key))
        } else {
            node = t.rotateLeft(node)
            node.SetLeft(t.delete(node.GetLeft(), key))
        }
        
    }
    nodeCast := node.(*PersistentTreapNode)
    if nodeCast.objectId > store.ObjNotAllocated {
        t.Store.DeleteObj(store.ObjectId(nodeCast.objectId))
    }
    nodeCast.objectId = store.ObjNotAllocated
    return node
}

// Insert inserts a new node with the given key and priority into the persistent treap.
func (t *PersistentTreap) Insert(key PersistentKey, priority Priority) {
    newNode := NewPersistentTreapNode(key, priority, t.Store)
    t.root = t.insert(t.root, newNode)
}

// Delete removes the node with the given key from the persistent treap.
func (t *PersistentTreap) Delete(key PersistentKey) {
    t.root = t.delete(t.root, key)
}

// Search searches for the node with the given key in the persistent treap.
func (t *PersistentTreap) Search(key PersistentKey) TreapNodeInterface {
    return t.search(t.root, key)
}

// UpdatePriority updates the priority of the node with the given key.
func (t *PersistentTreap) UpdatePriority(key PersistentKey, newPriority Priority) {
	node := t.Search(key)
	if node != nil && !node.IsNil() {
		node.SetPriority(newPriority)
		t.Delete(key)
		t.Insert(key, newPriority)
	}
}

