package store

//TBD
// * Add a collection table <- i.e. one can name an object by a string, then search for that string and get the object
// * Add a way to list all objects in the store
// * Add a way to list all collections
// * delete collections

type managedStore struct {
	concurrentStore
	collectionMap map[string]ObjectId
	allocatorList []Allocator
}

// A Managed store attempts to tidy up all the concepts we've been working on
// One can have multiple collections
// There is range of Allocators that can be setup
func NewManagedStore(filePath string) (*managedStore, error) {
	// Route the baseStore Allocator to the managedStore Allocator
	// Define and load the Collection Map
	// Load the Allocator Setup
	return nil, nil
}

func (s *managedStore) CreateCollection(collectionName string) error {
	// Collection names that start with __ are reserved for internal use
	return nil
}
func (s *managedStore) DeleteCollection(collectionName string) error {
	return nil

}
func (s *managedStore) ListCollections() ([]string, error) {
	return nil, nil

}
func (s *managedStore) Sync() error {
	return nil

}
func (s *managedStore) Close() error {
	return nil
}
