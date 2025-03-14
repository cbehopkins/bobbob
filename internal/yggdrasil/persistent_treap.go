package yggdrasil

import "github.com/cbehopkins/bobbob/internal/store"

// PersistentTreapNode represents a node in the persistent treap.
type PersistentTreapNode struct {
    TreapNode
	left     *PersistentTreapNode
    right    *PersistentTreapNode
    objectId  store.ObjectId
    Store    *store.Store
}
// GetKey returns the key of the node.
func (n *PersistentTreapNode) GetKey() Key {
    return n.key
}

// GetPriority returns the priority of the node.
func (n *PersistentTreapNode) GetPriority() Priority {
    return n.priority
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
    n.left = left.(*PersistentTreapNode)
}

// SetRight sets the right child of the node.
func (n *PersistentTreapNode) SetRight(right TreapNodeInterface) {
    n.right = right.(*PersistentTreapNode)
}

// IsNil checks if the node is nil.
func (n *PersistentTreapNode) IsNil() bool {
    return n == nil
}
func (n *PersistentTreapNode) sizeInBytes() int {
    objectIdSize:= n.objectId.SizeInBytes()
    keySize:= objectIdSize
    prioritySize:= n.priority.SizeInBytes()
    leftSize:= objectIdSize
    rightSize:= objectIdSize
    selfSize:= objectIdSize
    return keySize + prioritySize + leftSize + rightSize + selfSize
}
func (n *PersistentTreapNode) ObjectId() store.ObjectId {
    if n.objectId < 0 {
        // FIXME ignored error
        n.objectId, _ = n.Store.NewObj(n.sizeInBytes())
    }
    return n.objectId
}


func (n *PersistentTreapNode) Marshal() ([]byte, error) {
    buf := make([]byte, n.sizeInBytes())
    offset := 0

    marshalables := []interface {
        Marshal() ([]byte, error)
    }{
        n.key,
        n.priority,
        n.left.ObjectId(),
        n.right.ObjectId(),
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
// PersistentTreap represents a persistent treap data structure.
type PersistentTreap struct {
    Treap
    Store *store.Store
}

// NewPersistentTreapNode creates a new PersistentTreapNode with the given key, priority, and store reference.
func NewPersistentTreapNode(key Key, priority Priority, store *store.Store) *PersistentTreapNode {
    return &PersistentTreapNode{
        TreapNode: TreapNode{
            key:      key,
            priority: priority,
            left:     nil,
            right:    nil,
        },
        objectId: -1,
        Store:    store,
    }
}

// NewPersistentTreap creates a new PersistentTreap with the given comparison function and store reference.
func NewPersistentTreap(lessFunc func(a, b any) bool, store *store.Store) *PersistentTreap {
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

    if nodeCast.objectId > 0 {
        t.Store.DeleteObj(store.ObjectId(nodeCast.objectId))
    }
    if newRoot.objectId > 0 {
        t.Store.DeleteObj(store.ObjectId(newRoot.objectId))
    }
    nodeCast.objectId = -1
    newRoot.objectId = -1
    return newRoot
}

// rotateLeft performs a left rotation on the given node.
func (t *PersistentTreap) rotateLeft(node TreapNodeInterface) *PersistentTreapNode {
    newRoot := t.Treap.rotateLeft(node).(*PersistentTreapNode)
	nodeCast := node.(*PersistentTreapNode)
    if nodeCast.objectId > 0 {
        t.Store.DeleteObj(store.ObjectId(nodeCast.objectId))
    }
    if newRoot.objectId > 0 {
        t.Store.DeleteObj(store.ObjectId(newRoot.objectId))
    }
    nodeCast.objectId = -1
    newRoot.objectId = -1
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

    if nodeCast.objectId > 0 {
        t.Store.DeleteObj(store.ObjectId(nodeCast.objectId))
    }
    nodeCast.objectId = -1
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
        if node.GetLeft() == nil {
            return node.GetRight().(*PersistentTreapNode)
        } else if node.GetRight() == nil {
            return node.GetLeft().(*PersistentTreapNode)
        } else {
            if node.GetLeft().GetPriority() > node.GetRight().GetPriority() {
                node = t.rotateRight(node)
                node.SetRight(t.delete(node.GetRight().(*PersistentTreapNode), key))
            } else {
                node = t.rotateLeft(node)
                node.SetLeft(t.delete(node.GetLeft().(*PersistentTreapNode), key))
            }
        }
    }
	nodeCast := node.(*PersistentTreapNode)
    if nodeCast.objectId > 0 {
        t.Store.DeleteObj(store.ObjectId(nodeCast.objectId))
    }
    nodeCast.objectId = -1
    return node
}

// Insert inserts a new node with the given key and priority into the persistent treap.
func (t *PersistentTreap) Insert(key Key, priority Priority) {
    newNode := NewPersistentTreapNode(key, priority, t.Store)
    t.root = t.insert(t.root, newNode)
}

// Delete removes the node with the given key from the persistent treap.
func (t *PersistentTreap) Delete(key Key) {
    t.root = t.delete(t.root, key)
}

// Search searches for the node with the given key in the persistent treap.
func (t *PersistentTreap) Search(key Key) TreapNodeInterface {
    return t.search(t.root, key)
}