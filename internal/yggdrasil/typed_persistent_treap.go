package yggdrasil

// import "bobbob/internal/store"

// type TypedPersistentTreap struct {
// 	// Ability to store multiple treaps, one per type
// 	PersistentTreap *PersistentTreap[any]
// 	// Repository of payloads by type
// 	payloadRepo PayloadRepo
// 	// Type map to manage type registrations
// 	typeMap *TypeMap
// 	// There is one teap per Key and payload pair type
// 	// The key type is always a PersistentKey[T]
// 	// The payload type is always a PersistentPayload[P]
// }

// func NewTypedPersistentTreap(store store.Storer) (*TypedPersistentTreap, error) {
// 	tm := NewTypeMap()
// 	tpt := &TypedPersistentTreap{
// 		PersistentTreap: nil,
// 		payloadRepo:     PayloadRepo{TypeMap: tm},
// 		typeMap:         tm,
// 	}

// 	return tpt, nil
// }

// // RegisterType is called to register a type with the treap's type map
// // That is, you are telling the treap about a type that you will be using
// // You can then use this to load and save objects of this type
// func (tpt *TypedPersistentTreap) RegisterType(t any) {
// 	tpt.typeMap.AddType(t)
// }

// // Insert inserts a new node with the given key and priority into the appropriate persistent treap.
// func (tpt *TypedPersistentTreap) Insert(key any, priority Priority, payload any) {
// 	// figure out the short code for the type of key

// 	// Find the Treap to use for this type
// 	// If it doesn't exist, create it and add it to the map

// 	// We are provided with a payload
// 	// However that payload needs to include the short code for the type
// 	// so that when we load it back we know what type to create

// 	// Insert into that treap
// }

// // Delete removes the node with the given key from the persistent treap.
// func (t *TypedPersistentTreap) Delete(key PersistentKey[T]) {
// }

// // Search searches for the node with the given key in the persistent treap.
// func (t *TypedPersistentTreap) Search(key PersistentKey[T]) TreapNodeInterface[T] {
// }

// // UpdatePriority updates the priority of the node with the given key.
// func (tpt *TypedPersistentTreap) UpdatePriority(key PersistentKey[T], newPriority Priority) {
// }

// func (tpt *TypedPersistentTreap) Persist() error {
// 	return nil
// }

// func (tpt *TypedPersistentTreap) Load(objId store.ObjectId) error {
// 	return nil
// }
