package vault

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/cbehopkins/bobbob"
	atypes "github.com/cbehopkins/bobbob/allocator/types"
	"github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/collections"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	yttypes "github.com/cbehopkins/bobbob/yggdrasil/types"
)

const identityMapCollectionName = "__vault_identity_map__reserved"

// identityMapping holds the collection name associated to a user-provided identity key.
// Stored as JsonPayload to avoid defining a custom PersistentPayload.
type identityMapping struct {
	CollectionName string `json:"collectionName"`
}

// VaultMetadata contains the ObjectIds of the types.TypeMap and CollectionRegistry.
type VaultMetadata struct {
	TypeMapObjectId            store.ObjectId
	CollectionRegistryObjectId store.ObjectId
}

// Marshal serializes the metadata to bytes.
func (vm *VaultMetadata) Marshal() []byte {
	buf := make([]byte, 16) // 2 ObjectIds * 8 bytes each
	binary.LittleEndian.PutUint64(buf[0:8], uint64(vm.TypeMapObjectId))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(vm.CollectionRegistryObjectId))
	return buf
}

// Unmarshal deserializes the metadata from bytes.
func (vm *VaultMetadata) Unmarshal(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("metadata too short: %d bytes", len(data))
	}
	vm.TypeMapObjectId = store.ObjectId(binary.LittleEndian.Uint64(data[0:8]))
	vm.CollectionRegistryObjectId = store.ObjectId(binary.LittleEndian.Uint64(data[8:16]))
	return nil
}

// CollectionInterface defines the methods required for collections stored in a vault.
type CollectionInterface interface {
	Persist() error
	GetRootObjectId() (store.ObjectId, error)
}

// MemoryStats provides current memory usage information across all collections.
type MemoryStats struct {
	// TotalInMemoryNodes is the total count of nodes currently loaded in memory
	TotalInMemoryNodes int

	// CollectionNodes maps collection name to its in-memory node count
	CollectionNodes map[string]int

	// OperationsSinceLastFlush tracks operations since the last flush
	OperationsSinceLastFlush int

	// MemoryBreakdown provides detailed memory usage estimates
	MemoryBreakdown MemoryBreakdown
}

// MemoryBreakdown provides detailed memory usage estimates by component.
type MemoryBreakdown struct {
	// NodeStructSize is the size of a PersistentTreapNode[T] in bytes (approximate)
	NodeStructSize int

	// EstimatedNodeMemory is NodeStructSize * TotalInMemoryNodes
	EstimatedNodeMemory int64

	// NumCollections is the number of active collections
	NumCollections int

	// NumObjectsInStore is the total number of objects tracked by the allocator
	NumObjectsInStore int

	// EstimatedAllocatorMemory is the memory used by the allocator's tracking structures
	// (varies by allocator type: BasicAllocator ~48B/obj, BlockAllocator ~1B/obj)
	EstimatedAllocatorMemory int64

	// VaultOverhead estimates the Vault struct and maps overhead
	VaultOverhead int64

	// TotalEstimated is the sum of all estimated memory usage
	TotalEstimated int64
}

// MemoryMonitor tracks memory usage and triggers cleanup when thresholds are exceeded.
type MemoryMonitor struct {
	// ShouldFlush is called to determine if flushing should occur.
	// Returns true if flushing should be triggered based on the current stats.
	ShouldFlush func(stats MemoryStats) bool

	// OnFlush is called to perform the actual flushing when triggered.
	// Receives current stats and returns the number of nodes flushed and any error.
	OnFlush func(stats MemoryStats) (int, error)

	// CheckInterval controls how often to check (number of operations).
	// Default: 100 (check every 100 operations)
	CheckInterval int

	// operationCount tracks operations since last check
	operationCount int

	// mu protects operationCount from concurrent access
	mu sync.Mutex
}

// Vault is the top-level abstraction for working with multiple collections
// (treaps) in a single persistent store. It manages:
// - A single persistent store file
// - A types.TypeMap for efficient type serialization
// - A CollectionRegistry for tracking all collections
//
// Usage pattern:
//  1. Create/open a vault
//  2. Register all your types (in a consistent order)
//  3. Get or create collections by name
//  4. Work with collections (insert, search, delete)
//  5. Close the vault to persist everything
type Vault struct {
	// Store is the underlying persistent storage
	Store store.Storer

	// TypeMap manages type-to-short-code mappings
	TypeMap *TypeMap

	// CollectionRegistry tracks all collections in this vault
	CollectionRegistry *collections.CollectionRegistry

	// ActiveCollections caches loaded collections
	// Maps collection name to the actual treap instance
	// All cached collections must implement GetRootObjectId() and Persist()
	activeCollections map[string]CollectionInterface

	// memoryMonitor manages automatic memory cleanup (optional)
	memoryMonitor *MemoryMonitor

	// monitorStopChan signals the background monitor goroutine to stop
	monitorStopChan chan struct{}

	// monitorWg waits for the background monitor goroutine to exit
	monitorWg sync.WaitGroup

	// backgroundMonitoringEnabled controls whether monitoring auto-starts when a budget is set
	backgroundMonitoringEnabled bool

	// mu protects concurrent access to activeCollections
	mu sync.RWMutex
}

// Allocator exposes the underlying allocator when the Store implements
// store.AllocatorProvider. External callers can use this to set allocation
// callbacks on the OmniBlockAllocator and its parent BasicAllocator.
func (v *Vault) Allocator() atypes.Allocator {
	if provider, ok := v.Store.(interface{ Allocator() atypes.Allocator }); ok {
		return provider.Allocator()
	}
	return nil
}

// Note: NewVault has been removed. Use LoadVault for both new and existing vaults.

// LoadVault loads an existing vault from a store.
// It reads the types.TypeMap and CollectionRegistry from the store's reserved objects.
// After loading, you should register your types in the same order as when the vault was created.
// The loaded types.TypeMap will be merged with your registered types.
func LoadVault(stre store.Storer) (*Vault, error) {
	vault := &Vault{
		Store:                       stre,
		TypeMap:                     NewTypeMap(),
		CollectionRegistry:          collections.NewCollectionRegistry(),
		activeCollections:           make(map[string]CollectionInterface),
		backgroundMonitoringEnabled: true,
	}

	// Get the prime object where vault metadata is stored
	primeObjectId, err := stre.PrimeObject(16)
	if err != nil {
		return nil, fmt.Errorf("failed to get prime object: %w", err)
	}

	// Try to load the metadata object
	metadataBytes, err := store.ReadBytesFromObj(stre, primeObjectId)
	if err != nil {
		// No metadata found - this is a new vault
		return vault, nil
	}

	var metadata VaultMetadata
	if err := metadata.Unmarshal(metadataBytes); err != nil {
		return vault, nil // Ignore errors, treat as new vault
	}

	// Load types.TypeMap
	if store.IsValidObjectId(metadata.TypeMapObjectId) {
		typeMapData, err := store.ReadBytesFromObj(stre, metadata.TypeMapObjectId)
		if err == nil {
			loadedTypeMap := NewTypeMap()
			if err := loadedTypeMap.Unmarshal(typeMapData); err == nil {
				// Use the loaded types.TypeMap
				vault.TypeMap = loadedTypeMap
			}
		}
	}

	// Load CollectionRegistry
	if store.IsValidObjectId(metadata.CollectionRegistryObjectId) {
		registryData, err := store.ReadBytesFromObj(stre, metadata.CollectionRegistryObjectId)
		if err == nil {
			loadedRegistry := collections.NewCollectionRegistry()
			if err := loadedRegistry.Unmarshal(registryData); err == nil {
				vault.CollectionRegistry = loadedRegistry
			}
		}
	}

	return vault, nil
}

// RegisterType adds a type to the vault's type map.
// You should call this for all types you'll use in collections,
// in a consistent order across sessions.
func (v *Vault) RegisterType(t any) {
	v.TypeMap.AddType(t)
}

// GetOrCreateCollection retrieves or creates a collection with the given name.
// The collection stores keys of type K and payloads of type P.
// If the collection already exists, it loads it from the store.
// If it doesn't exist, it creates a new empty collection.
//
// Important: You must register the key and payload types before calling this.
//
// Example:
//
//	v.RegisterType(StringKey(""))
//	v.RegisterType(UserData{})
//	users := GetOrCreateCollection[string, JsonPayload[UserData]](v, "users", StringLess, (*StringKey)(new(string)))
func GetOrCreateCollection[K any, P yttypes.PersistentPayload[P]](
	v *Vault,
	collectionName string,
	lessFunc func(a, b K) bool,
	keyTemplate yttypes.PersistentKey[K],
) (*treap.PersistentPayloadTreap[K, P], error) {
	// Check if we already have this collection loaded (with read lock)
	v.mu.RLock()
	if cached, exists := v.activeCollections[collectionName]; exists {
		v.mu.RUnlock()
		if treap, ok := cached.(*treap.PersistentPayloadTreap[K, P]); ok {
			return treap, nil
		}
		return nil, fmt.Errorf("collection %s exists but has wrong type", collectionName)
	}
	v.mu.RUnlock()

	// Acquire write lock for potential creation
	v.mu.Lock()
	defer v.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have created it)
	if cached, exists := v.activeCollections[collectionName]; exists {
		if treap, ok := cached.(*treap.PersistentPayloadTreap[K, P]); ok {
			return treap, nil
		}
		return nil, fmt.Errorf("collection %s exists but has wrong type", collectionName)
	}

	// Check if the collection exists in the registry
	collInfo, exists := v.CollectionRegistry.GetCollection(collectionName)
	if exists {
		// Validate type codes match what we expect
		var zeroP P
		expectedKeyShortCode, err := v.TypeMap.GetShortCode(keyTemplate)
		if err != nil {
			return nil, fmt.Errorf("key type not registered: %w", err)
		}
		expectedPayloadShortCode, err := v.TypeMap.GetShortCode(zeroP)
		if err != nil {
			return nil, fmt.Errorf("payload type not registered: %w", err)
		}

		if collInfo.KeyTypeShortCode != expectedKeyShortCode {
			return nil, fmt.Errorf("collection %s has key type mismatch: stored=%d, expected=%d", collectionName, collInfo.KeyTypeShortCode, expectedKeyShortCode)
		}
		if collInfo.PayloadTypeShortCode != expectedPayloadShortCode {
			return nil, fmt.Errorf("collection %s has payload type mismatch: stored=%d, expected=%d", collectionName, collInfo.PayloadTypeShortCode, expectedPayloadShortCode)
		}

		// Load the existing collection from the store
		treap := treap.NewPersistentPayloadTreap[K, P](lessFunc, keyTemplate, v.Store)
		if store.IsValidObjectId(collInfo.RootObjectId) {
			err := treap.Load(collInfo.RootObjectId)
			if err != nil {
				return nil, fmt.Errorf("failed to load collection %s: %w", collectionName, err)
			}
		}
		// Cache it
		v.activeCollections[collectionName] = treap
		return treap, nil
	}

	// Create a new collection
	treap := treap.NewPersistentPayloadTreap[K, P](lessFunc, keyTemplate, v.Store)

	// Get the type short codes
	var zeroP P

	// Use the template to get the short code (not the generic K)
	keyShortCode, err := v.TypeMap.GetShortCode(keyTemplate)
	if err != nil {
		return nil, fmt.Errorf("key type not registered: %w", err)
	}

	payloadShortCode, err := v.TypeMap.GetShortCode(zeroP)
	if err != nil {
		return nil, fmt.Errorf("payload type not registered: %w", err)
	}

	// Register in the collection registry
	_, err = v.CollectionRegistry.RegisterCollection(
		collectionName,
		bobbob.ObjNotAllocated, // No root yet
		keyShortCode,
		payloadShortCode,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register collection: %w", err)
	}

	// Cache it
	v.activeCollections[collectionName] = treap
	return treap, nil
}

// GetOrCreateKeyOnlyCollection retrieves or creates a collection that stores only keys (no payloads).
// This is useful for sets or indexes.
func GetOrCreateKeyOnlyCollection[K any](
	v *Vault,
	collectionName string,
	lessFunc func(a, b K) bool,
	keyTemplate yttypes.PersistentKey[K],
) (*treap.PersistentTreap[K], error) {
	// Check if we already have this collection loaded (with read lock)
	v.mu.RLock()
	if cached, exists := v.activeCollections[collectionName]; exists {
		v.mu.RUnlock()
		if treap, ok := cached.(*treap.PersistentTreap[K]); ok {
			return treap, nil
		}
		return nil, fmt.Errorf("collection %s exists but has wrong type", collectionName)
	}
	v.mu.RUnlock()

	// Acquire write lock for potential creation
	v.mu.Lock()
	defer v.mu.Unlock()

	// Double-check after acquiring write lock
	if cached, exists := v.activeCollections[collectionName]; exists {
		if treap, ok := cached.(*treap.PersistentTreap[K]); ok {
			return treap, nil
		}
		return nil, fmt.Errorf("collection %s exists but has wrong type", collectionName)
	}

	// Check if the collection exists in the registry
	collInfo, exists := v.CollectionRegistry.GetCollection(collectionName)
	if exists {
		// Validate type codes match what we expect
		expectedKeyShortCode, err := v.TypeMap.GetShortCode(keyTemplate)
		if err != nil {
			return nil, fmt.Errorf("key type not registered: %w", err)
		}

		if collInfo.KeyTypeShortCode != expectedKeyShortCode {
			return nil, fmt.Errorf("collection %s has key type mismatch: stored=%d, expected=%d", collectionName, collInfo.KeyTypeShortCode, expectedKeyShortCode)
		}
		if collInfo.PayloadTypeShortCode != 0 {
			return nil, fmt.Errorf("collection %s is not a key-only collection (has payload type %d)", collectionName, collInfo.PayloadTypeShortCode)
		}

		// Load the existing collection from the store
		treap := treap.NewPersistentTreap[K](lessFunc, keyTemplate, v.Store)
		if store.IsValidObjectId(collInfo.RootObjectId) {
			err := treap.Load(collInfo.RootObjectId)
			if err != nil {
				return nil, fmt.Errorf("failed to load collection %s: %w", collectionName, err)
			}
		}
		// Cache it
		v.activeCollections[collectionName] = treap
		return treap, nil
	}

	// Create a new collection
	treap := treap.NewPersistentTreap[K](lessFunc, keyTemplate, v.Store)

	// Get the type short code for the key (use the template, not the generic K)
	keyShortCode, err := v.TypeMap.GetShortCode(keyTemplate)
	if err != nil {
		return nil, fmt.Errorf("key type not registered: %w", err)
	}

	// Register in the collection registry (payload short code is 0 for key-only)
	_, err = v.CollectionRegistry.RegisterCollection(
		collectionName,
		bobbob.ObjNotAllocated, // No root yet
		keyShortCode,
		0, // No payload type
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register collection: %w", err)
	}

	// Cache it
	v.activeCollections[collectionName] = treap
	return treap, nil
}

// PersistCollection saves a collection's current root to the registry.
// Call this after making changes to a collection and before closing the vault.
func (v *Vault) PersistCollection(collectionName string) error {
	// Get the collection from the cache
	v.mu.RLock()
	cached, exists := v.activeCollections[collectionName]
	v.mu.RUnlock()

	if !exists {
		return fmt.Errorf("collection %s not loaded", collectionName)
	}

	// Persist the collection and get its root ObjectId
	err := cached.Persist()
	if err != nil {
		return fmt.Errorf("failed to persist collection %s: %w", collectionName, err)
	}
	rootObjectId, err := cached.GetRootObjectId()
	if err != nil {
		return fmt.Errorf("failed to get root object ID for collection %s: %w", collectionName, err)
	}

	// Update the registry
	return v.CollectionRegistry.UpdateRootObjectId(collectionName, rootObjectId)
}

// Close persists all active collections and closes the underlying store.
// You should call this when you're done with the vault.
// If any collections fail to persist, it continues and returns all errors.
func (v *Vault) Close() (err error) {
	// Stop background memory monitoring if it's running
	if v.monitorStopChan != nil {
		close(v.monitorStopChan)
		v.monitorWg.Wait()
		v.monitorStopChan = nil
	}

	// Always attempt to close the store, even if we encounter errors earlier.
	if v.Store != nil {
		defer func() {
			if cerr := v.Store.Close(); cerr != nil {
				if err != nil {
					err = fmt.Errorf("%w; store close: %v", err, cerr)
				} else {
					err = fmt.Errorf("store close: %w", cerr)
				}
			}
		}()
	}

	// Persist all active collections - accumulate errors instead of failing fast
	var persistErrors []error
	v.mu.RLock()
	collectionNames := make([]string, 0, len(v.activeCollections))
	for name := range v.activeCollections {
		collectionNames = append(collectionNames, name)
	}
	v.mu.RUnlock()

	for _, name := range collectionNames {
		err := v.PersistCollection(name)
		if err != nil {
			persistErrors = append(persistErrors, fmt.Errorf("failed to persist collection %s: %w", name, err))
		}
	}

	// If we had persist errors, return them now before trying to save metadata
	if len(persistErrors) > 0 {
		return fmt.Errorf("failed to persist %d collections: %v", len(persistErrors), persistErrors)
	}

	// Save types.TypeMap to a new object
	typeMapData, err := v.TypeMap.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal TypeMap: %w", err)
	}
	typeMapObjId, err := store.WriteNewObjFromBytes(v.Store, typeMapData)
	if err != nil {
		return fmt.Errorf("failed to write TypeMap: %w", err)
	}

	// Save CollectionRegistry to a new object
	registryData, err := v.CollectionRegistry.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal CollectionRegistry: %w", err)
	}
	registryObjId, err := store.WriteNewObjFromBytes(v.Store, registryData)
	if err != nil {
		return fmt.Errorf("failed to write CollectionRegistry: %w", err)
	}

	// Get the prime object (ObjectId 1) where we'll store vault metadata
	primeObjectId, err := v.Store.PrimeObject(16)
	if err != nil {
		return fmt.Errorf("failed to get prime object: %w", err)
	}

	// Create and save metadata pointing to types.TypeMap and CollectionRegistry
	metadata := VaultMetadata{
		TypeMapObjectId:            typeMapObjId,
		CollectionRegistryObjectId: registryObjId,
	}
	metadataBytes := metadata.Marshal()
	err = store.WriteBytesToObj(v.Store, metadataBytes, primeObjectId)
	if err != nil {
		return fmt.Errorf("failed to write metadata to prime object: %w", err)
	}

	return nil
}

// ListCollections returns the names of all collections in the vault.
func (v *Vault) ListCollections() []string {
	return v.CollectionRegistry.ListCollections()
}

// EnableMemoryMonitoring activates automatic memory management for the vault.
// The shouldFlush callback determines when flushing should occur based on current stats.
// The onFlush callback performs the actual flushing and returns the number of nodes flushed.
// A background goroutine is started to periodically check memory usage.
//
// Default behavior:
//   - Background monitoring auto-starts when a memory budget is configured
//     (via EnableMemoryMonitoring or SetMemoryBudget*). Use SetBackgroundMonitoring(false)
//     to disable for deterministic tests, then call checkMemoryAndFlush() manually.
//
// Example - auto background monitoring (node budget):
//
//	vault.EnableMemoryMonitoring(
//	    func(stats MemoryStats) bool { return stats.TotalInMemoryNodes > 1000 },
//	    func(stats MemoryStats) (int, error) {
//	        cutoff := time.Now().Unix() - 10 // 10 seconds old
//	        return vault.FlushOlderThan(cutoff)
//	    },
//	)
//	// Background monitoring runs automatically
//
// Example - disable background monitoring for tests:
//
//	vault.SetBackgroundMonitoring(false)
//	vault.SetMemoryBudget(1000, 10)
//	// Manually trigger checks when desired
//	_ = vault.checkMemoryAndFlush()
//
// Example - flush based on node count:
//
//	vault.EnableMemoryMonitoring(
//	    func(stats MemoryStats) bool {
//	        return stats.TotalInMemoryNodes > 1000
//	    },
//	    func(stats MemoryStats) (int, error) {
//	        cutoff := time.Now().Unix() - 10 // 10 seconds old
//	        return vault.FlushOlderThan(cutoff)
//	    },
//	)
func (v *Vault) EnableMemoryMonitoring(shouldFlush func(MemoryStats) bool, onFlush func(MemoryStats) (int, error)) {
	v.memoryMonitor = &MemoryMonitor{
		ShouldFlush:   shouldFlush,
		OnFlush:       onFlush,
		CheckInterval: store.MemoryMonitorCheckInterval, // default: check every N operations
	}

	// Auto-start background monitoring if enabled by default
	if v.backgroundMonitoringEnabled {
		v.StartBackgroundMonitoring()
	}
}

// StartBackgroundMonitoring starts a background goroutine that periodically checks memory usage
// (every 100ms). This must be called after EnableMemoryMonitoring() or SetMemoryBudget*().
// Call Vault.Close() to stop the background goroutine.
func (v *Vault) StartBackgroundMonitoring() {
	if v.monitorStopChan == nil {
		v.monitorStopChan = make(chan struct{})
		v.monitorWg.Add(1)
		go v.backgroundMemoryMonitor()
	}
}

// StopBackgroundMonitoring stops the background monitoring goroutine if running.
func (v *Vault) StopBackgroundMonitoring() {
	if v.monitorStopChan != nil {
		close(v.monitorStopChan)
		v.monitorWg.Wait()
		v.monitorStopChan = nil
	}
}

// SetBackgroundMonitoring enables or disables auto background monitoring.
// When enabling, it starts the monitor if a memory budget has been set.
// When disabling, it stops the monitor if running.
func (v *Vault) SetBackgroundMonitoring(enabled bool) {
	v.backgroundMonitoringEnabled = enabled
	if enabled {
		// Start if monitoring has been configured
		if v.memoryMonitor != nil {
			v.StartBackgroundMonitoring()
		}
	} else {
		v.StopBackgroundMonitoring()
	}
}

// SetMemoryBudget is a convenience function that automatically flushes nodes to stay
// under a maximum node count. This is simpler than EnableMemoryMonitoring for common cases.
//
// Parameters:
//   - maxNodes: maximum number of nodes to keep in memory across all collections
//   - flushAgeSeconds: nodes older than this (in seconds) will be flushed
//
// Example:
//
//	// Auto background monitoring enabled by default
//	vault.SetMemoryBudget(1000, 10) // Keep max 1000 nodes, flush nodes >10 sec old
//
//	// Disable background monitoring (e.g., for deterministic tests)
//	vault.SetBackgroundMonitoring(false)
//	vault.SetMemoryBudget(1000, 10)
//	// Then call vault.checkMemoryAndFlush() explicitly as needed
func (v *Vault) SetMemoryBudget(maxNodes int, flushAgeSeconds int64) {
	v.EnableMemoryMonitoring(
		func(stats MemoryStats) bool {
			return stats.TotalInMemoryNodes > maxNodes
		},
		func(stats MemoryStats) (int, error) {
			// Calculate cutoff time
			cutoff := getCurrentTimeSeconds() - flushAgeSeconds
			return v.FlushOlderThan(cutoff)
		},
	)
}

// SetMemoryBudgetWithPercentile automatically manages memory by flushing the oldest
// percentage of nodes when the maximum node count is exceeded.
//
// Parameters:
//   - maxNodes: maximum number of nodes to keep in memory across all collections
//   - flushPercentage: percentage (0-100) of oldest nodes to flush when limit is exceeded
//
// Example:
//
//	// Auto background monitoring enabled by default
//	vault.SetMemoryBudgetWithPercentile(1000, 25) // Keep max 1000 nodes, flush oldest 25% when exceeded
//
//	// Disable background monitoring (e.g., for deterministic tests)
//	vault.SetBackgroundMonitoring(false)
//	vault.SetMemoryBudgetWithPercentile(1000, 25)
//	// Then call vault.checkMemoryAndFlush() explicitly as needed
//
// This is useful when you want to aggressively reduce memory without relying on
// time-based flushing. When the limit is hit, it flushes the oldest N% of nodes
// to bring memory usage back down.
func (v *Vault) SetMemoryBudgetWithPercentile(maxNodes int, flushPercentage int) {
	v.SetMemoryBudgetWithPercentileWithCallbacks(maxNodes, flushPercentage, nil, nil)
}
func (v *Vault) SetMemoryBudgetWithPercentileWithCallbacks(maxNodes int, flushPercentage int, shouldFlushDebug func(MemoryStats, bool), onFlushDebug func(MemoryStats, int)) {
	shouldFlushLocal := func(stats MemoryStats) bool {
		sf := stats.TotalInMemoryNodes > maxNodes
		if shouldFlushDebug != nil {
			shouldFlushDebug(stats, sf)
		}
		return sf
	}
	onFlushLocal := func(stats MemoryStats) (int, error) {
		count, err := v.FlushOldestPercentile(flushPercentage)
		if onFlushDebug != nil {
			onFlushDebug(stats, count)
		}
		return count, err
	}
	v.EnableMemoryMonitoring(
		shouldFlushLocal,
		onFlushLocal,
	)
}

// SetCheckInterval changes how often the memory monitor checks for flushing.
// Default is 100 operations. Lower values check more frequently but have more overhead.
func (v *Vault) SetCheckInterval(interval int) {
	if v.memoryMonitor != nil {
		v.memoryMonitor.CheckInterval = interval
	}
}

// GetMemoryStats returns current memory usage statistics across all collections.
func (v *Vault) GetMemoryStats() MemoryStats {
	stats := MemoryStats{
		CollectionNodes: make(map[string]int),
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	for name, coll := range v.activeCollections {
		nodeCount := getInMemoryNodeCount(coll)
		stats.CollectionNodes[name] = nodeCount
		stats.TotalInMemoryNodes += nodeCount
	}

	if v.memoryMonitor != nil {
		v.memoryMonitor.mu.Lock()
		stats.OperationsSinceLastFlush = v.memoryMonitor.operationCount
		v.memoryMonitor.mu.Unlock()
	}

	// Calculate memory breakdown
	stats.MemoryBreakdown = v.calculateMemoryBreakdown(stats.TotalInMemoryNodes, len(v.activeCollections))

	return stats
}

// calculateMemoryBreakdown estimates memory usage by component.
// This provides a theoretical calculation based on struct sizes to compare
// against actual heap usage reported by runtime.MemStats.
func (v *Vault) calculateMemoryBreakdown(totalNodes int, numCollections int) MemoryBreakdown {
	breakdown := MemoryBreakdown{
		NumCollections: numCollections,
	}

	// Estimate node size:
	// PersistentTreapNode[T] contains:
	// - TreapNode[T]: key (interface), priority (uint32), left/right (pointers) ~ 32-48 bytes
	// - objectId, leftObjectId, rightObjectId: 3 * 8 bytes = 24 bytes
	// - Store (interface): 2 * 8 bytes = 16 bytes
	// - parent (pointer): 8 bytes
	// - lastAccessTime (int64): 8 bytes
	// Plus payload: varies by type, assume ~64 bytes average
	// Plus Go runtime overhead: ~32 bytes per allocation
	// Approximate: 200 bytes per node (conservative estimate)
	breakdown.NodeStructSize = 200

	breakdown.EstimatedNodeMemory = int64(totalNodes) * int64(breakdown.NodeStructSize)

	// Get store allocator size
	if bs, ok := v.Store.(interface{ GetObjectCount() int }); ok {
		breakdown.NumObjectsInStore = bs.GetObjectCount()
		// Allocator overhead depends on type:
		// - BasicAllocator: map header (~48 bytes) + per entry (~48 bytes)
		// - BlockAllocator: ~1 byte per object (bitmap-based)
		// - OmniBlockAllocator: mixed (small objects ~1B, large objects ~48B)
		// For multiStore (default), conservatively estimate 2 bytes/object:
		breakdown.EstimatedAllocatorMemory = 48 + int64(breakdown.NumObjectsInStore)*2
	}

	// Vault overhead:
	// - Vault struct itself: ~128 bytes
	// - activeCollections map: ~48 bytes + entries
	// - Map entries: numCollections * (key string + value interface) ~ 80 bytes each
	// - PersistentTreap structs: numCollections * ~200 bytes
	breakdown.VaultOverhead = 128 + 48 + int64(numCollections)*(80+200)

	breakdown.TotalEstimated = breakdown.EstimatedNodeMemory + breakdown.EstimatedAllocatorMemory + breakdown.VaultOverhead

	return breakdown
}

// FlushOlderThan flushes nodes older than the given timestamp across all collections.
// Returns the total number of nodes flushed and any error encountered.
//
// The cutoffTime should be a Unix timestamp (seconds since epoch). Nodes with
// a last access time older than this will be flushed from memory to disk.
func (v *Vault) FlushOlderThan(cutoffTime int64) (int, error) {
	totalFlushed := 0

	v.mu.RLock()
	collections := make([]CollectionInterface, 0, len(v.activeCollections))
	for _, coll := range v.activeCollections {
		collections = append(collections, coll)
	}
	v.mu.RUnlock()

	for _, coll := range collections {
		flushed, err := flushCollectionOlderThan(coll, cutoffTime)
		if err != nil {
			return totalFlushed, err
		}
		totalFlushed += flushed
	}

	// Release the collections slice to help GC
	for i := range collections {
		collections[i] = nil
	}
	collections = nil

	if v.memoryMonitor != nil {
		v.memoryMonitor.operationCount = 0
	}

	return totalFlushed, nil
}

// FlushOldestPercentile flushes the oldest percentage of nodes across all collections.
// Returns the total number of nodes flushed and any error encountered.
//
// Parameters:
//   - percentage: percentage (0-100) of oldest nodes to flush
//
// This determines the cutoff time by examining current node access times and
// calculating a timestamp that would flush approximately the requested percentage.
func (v *Vault) FlushOldestPercentile(percentage int) (int, error) {
	if percentage <= 0 || percentage > 100 {
		return 0, fmt.Errorf("percentage must be between 1 and 100, got %d", percentage)
	}

	totalFlushed := 0

	v.mu.RLock()
	collections := make([]CollectionInterface, 0, len(v.activeCollections))
	for _, coll := range v.activeCollections {
		collections = append(collections, coll)
	}
	v.mu.RUnlock()

	for _, coll := range collections {
		flushed, err := flushCollectionOldestPercentile(coll, percentage)
		if err != nil {
			return totalFlushed, err
		}
		totalFlushed += flushed
	}

	// Release the collections slice to help GC
	for i := range collections {
		collections[i] = nil
	}
	collections = nil

	if v.memoryMonitor != nil {
		v.memoryMonitor.operationCount = 0
	}

	return totalFlushed, nil
}

// checkMemoryAndFlush is called internally after operations to check if flushing is needed.
func (v *Vault) checkMemoryAndFlush() error {
	if v.memoryMonitor == nil {
		return nil
	}

	v.memoryMonitor.mu.Lock()
	v.memoryMonitor.operationCount++
	count := v.memoryMonitor.operationCount
	v.memoryMonitor.mu.Unlock()

	if count < v.memoryMonitor.CheckInterval {
		return nil
	}

	stats := v.GetMemoryStats()

	if v.memoryMonitor.ShouldFlush(stats) {
		// Recover from any panics in flush to avoid stack overflows crashing tests
		var flushErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Fallback: attempt a small age-based flush to reduce pressure
					// Use 1 second age window as a safe minimal flush
					cutoff := getCurrentTimeSeconds() - 1
					_, _ = v.FlushOlderThan(cutoff)
				}
			}()
			_, flushErr = v.memoryMonitor.OnFlush(stats)
		}()
		v.memoryMonitor.operationCount = 0
		return flushErr
	}

	return nil
}

// backgroundMemoryMonitor runs in a background goroutine and periodically checks memory usage.
// It bypasses the operationCount interval mechanism and checks on a timer basis.
// It exits when monitorStopChan is signaled (during vault close).
func (v *Vault) backgroundMemoryMonitor() {
	defer v.monitorWg.Done()

	// Check memory usage at configured interval
	ticker := time.NewTicker(store.MemoryMonitorBackgroundTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-v.monitorStopChan:
			// Vault is closing, stop monitoring
			return
		case <-ticker.C:
			// Perform a memory check directly without waiting for operationCount interval
			if v.memoryMonitor == nil {
				continue
			}

			// Recover from any panics in the flush logic to prevent crashing the vault
			func() {
				defer func() {
					if r := recover(); r != nil {
						// Log recovery but continue monitoring
						_ = r
					}
				}()

				stats := v.GetMemoryStats()
				if v.memoryMonitor.ShouldFlush(stats) {
					if _, err := v.memoryMonitor.OnFlush(stats); err != nil {
						// Log error but continue monitoring
						_ = err
					}
					v.memoryMonitor.mu.Lock()
					v.memoryMonitor.operationCount = 0
					v.memoryMonitor.mu.Unlock()
				}
			}()
		}
	}
}

// Helper function to get current time in seconds (can be mocked in tests)
var getCurrentTimeSeconds = func() int64 {
	return time.Now().Unix()
}

// getInMemoryNodeCount returns the number of nodes in memory for a collection.
func getInMemoryNodeCount(coll CollectionInterface) int {
	// Try to get count from collections that support it
	if counter, ok := coll.(interface{ CountInMemoryNodes() int }); ok {
		return counter.CountInMemoryNodes()
	}
	return 0
}

// flushCollectionOlderThan flushes old nodes from a collection.
func flushCollectionOlderThan(coll CollectionInterface, cutoffTime int64) (int, error) {
	// Try persistent payload treaps first
	if ppt, ok := coll.(interface{ FlushOlderThan(int64) (int, error) }); ok {
		return ppt.FlushOlderThan(cutoffTime)
	}
	// Try persistent treaps
	if pt, ok := coll.(interface{ FlushOlderThan(int64) (int, error) }); ok {
		return pt.FlushOlderThan(cutoffTime)
	}
	return 0, nil
}

// flushCollectionOldestPercentile flushes the oldest percentage of nodes from a collection.
func flushCollectionOldestPercentile(coll CollectionInterface, percentage int) (int, error) {
	// Try persistent payload treaps first
	if ppt, ok := coll.(interface{ FlushOldestPercentile(int) (int, error) }); ok {
		return ppt.FlushOldestPercentile(percentage)
	}
	// Try persistent treaps
	if pt, ok := coll.(interface{ FlushOldestPercentile(int) (int, error) }); ok {
		return pt.FlushOldestPercentile(percentage)
	}
	return 0, nil
}

// VaultSession manages a vault with pre-configured collections.
// Call Close() when done to persist all changes and close the underlying store.
//
// Deprecated: VaultSession is a legacy wrapper kept for backward compatibility.
// Use Vault directly instead via LoadVault() for cleaner, more explicit code.
// VaultSession will be removed in a future version.
//
// Migration guide:
//
//	Old: session, collections, _ := OpenVault(filename, specs...)
//	New: use OpenVault which returns VaultSession for now, but switch to:
//	     vault, _ := LoadVault(store); coll, _ := GetOrCreateCollection(vault, ...)
//
// Note: VaultSession.Close() does close the underlying store via Vault.Close(),
// but the explicit Store field can be confusing. Direct Vault usage is clearer.
type VaultSession struct {
	Vault *Vault
	Store store.Storer
}

// Close persists all changes and closes the vault and its underlying store.
// This closes both the vault metadata and the store itself.
func (vs *VaultSession) Close() error {
	return vs.Vault.Close()
}

// Allocator returns the allocator backing this session (if exposed by the store).
// This enables external users of OpenVault/OpenVaultWithIdentity to attach
// allocation callbacks without direct access to the store implementation.
func (vs *VaultSession) Allocator() atypes.Allocator {
	if vs == nil || vs.Vault == nil {
		return nil
	}
	return vs.Vault.Allocator()
}

// ConfigureAllocatorCallbacks attaches callbacks to the session's allocator and, when present,
// its parent (e.g., BasicAllocator behind OmniBlockAllocator). Returns true if at least one
// callback was attached.
func (vs *VaultSession) ConfigureAllocatorCallbacks(
	childCb func(bobbob.ObjectId, bobbob.FileOffset, int),
	parentCb func(bobbob.ObjectId, bobbob.FileOffset, int),
) bool {
	alloc := vs.Allocator()
	if alloc == nil {
		return false
	}

	type callbackSetter interface {
		SetOnAllocate(func(bobbob.ObjectId, bobbob.FileOffset, int))
	}

	succeeded := false
	if childCb != nil {
		if setter, ok := alloc.(callbackSetter); ok {
			setter.SetOnAllocate(childCb)
			succeeded = true
		}
	}

	if parentCb != nil {
		if provider, ok := alloc.(interface{ Parent() atypes.Allocator }); ok {
			if parent := provider.Parent(); parent != nil {
				if setter, ok := parent.(callbackSetter); ok {
					setter.SetOnAllocate(parentCb)
					succeeded = true
				}
			}
		}
	}

	return succeeded
}

// CollectionSpec defines the configuration for a collection to be opened.
// This interface allows type-safe collection specifications.
type CollectionSpec interface {
	// getName returns the collection name
	getName() string
	// getTypes returns the key and payload types in order [keyType, payloadType]
	getTypes() []any
	// openCollection creates or loads the collection in the given vault
	openCollection(v *Vault) (any, error)
}

// IdentitySpec is a collection specification tagged with a caller-provided identity.
// Identity must be comparable (so it can be used as a map key). Its string form via fmt.Sprint
// is used for human-readable naming and deterministic persistence.
type IdentitySpec[I comparable] interface {
	CollectionSpec
	getIdentity() I
}

// PayloadIdentitySpec wraps a payload collection spec with an identity value.
// The identity string representation is used as the collection name; the identity itself
// is returned to the caller so indexing does not rely on position.
type PayloadIdentitySpec[I comparable, K any, P yttypes.PersistentPayload[P]] struct {
	Identity        I
	LessFunc        func(a, b K) bool
	KeyTemplate     yttypes.PersistentKey[K]
	PayloadTemplate P
}

func (s PayloadIdentitySpec[I, K, P]) getIdentity() I { return s.Identity }
func (s PayloadIdentitySpec[I, K, P]) getName() string {
	return fmt.Sprint(s.Identity)
}

func (s PayloadIdentitySpec[I, K, P]) getTypes() []any {
	return []any{s.KeyTemplate, s.PayloadTemplate}
}

func (s PayloadIdentitySpec[I, K, P]) openCollection(v *Vault) (any, error) {
	return GetOrCreateCollection[K, P](v, s.getName(), s.LessFunc, s.KeyTemplate)
}

// PayloadCollectionSpec specifies a collection with both keys and payloads.
type PayloadCollectionSpec[K any, P yttypes.PersistentPayload[P]] struct {
	Name        string
	LessFunc    func(a, b K) bool
	KeyTemplate yttypes.PersistentKey[K]
	// PayloadTemplate is used only for type extraction, not stored
	PayloadTemplate P
}

func (s PayloadCollectionSpec[K, P]) getName() string {
	return s.Name
}

func (s PayloadCollectionSpec[K, P]) getTypes() []any {
	return []any{s.KeyTemplate, s.PayloadTemplate}
}

// returning any is not ideal here but Go generics limitations leave us no choice
func (s PayloadCollectionSpec[K, P]) openCollection(v *Vault) (any, error) {
	return GetOrCreateCollection[K, P](v, s.Name, s.LessFunc, s.KeyTemplate)
}

// OpenVault is a unified convenience function that:
// 1. Creates or loads a store from the given filename
// 2. Loads the vault
// 3. Automatically extracts and registers types from collection specs (deterministically)
// 4. Opens all specified collections
// 5. Returns the session and collections
//
// Type registration is deterministic: types are extracted from collections in the order
// provided, with key types before payload types for each collection.
//
// Example:
//
//	session, collections, err := OpenVault(
//	    "mydata.db",
//	    PayloadCollectionSpec[types.StringKey, types.JsonPayload[UserProfile]]{
//	        Name: "users",
//	        LessFunc: types.StringLess,
//	        KeyTemplate: (*types.StringKey)(new(string)),
//	        PayloadTemplate: types.JsonPayload[UserProfile]{},
//	    },
//	)
//	if err != nil { ... }
//	defer session.Close()
//
//	users := collections[0].(*treap.PersistentPayloadTreap[types.StringKey, types.JsonPayload[UserProfile]])
func OpenVault(filename string, specs ...CollectionSpec) (*VaultSession, []any, error) {
	// Try to load existing store, or create new one
	var s store.Storer
	var err error
	// Use ConcurrentMultiStore for thread-safe access with optimized allocator routing
	ms, mErr := multistore.LoadConcurrentMultiStore(filename, 0)
	if mErr != nil {
		// New vault: create concurrent multi-store
		ms, mErr = multistore.NewConcurrentMultiStore(filename, 0)
		if mErr != nil {
			return nil, nil, fmt.Errorf("failed to create concurrent multi-store: %w", mErr)
		}
	}
	s = ms

	// Load vault (works for both new and existing)
	vault, err := LoadVault(s)
	if err != nil {
		_ = s.Close() // Best effort cleanup
		return nil, nil, fmt.Errorf("failed to load vault: %w", err)
	}

	// Extract and register types in deterministic order
	// For each collection spec, register key type then payload type
	for _, spec := range specs {
		types := spec.getTypes()
		for _, t := range types {
			vault.RegisterType(t)
		}
	}

	// Create session
	session := &VaultSession{
		Vault: vault,
		Store: s,
	}

	// Open all collections
	collections := make([]any, len(specs))
	for i, spec := range specs {
		coll, err := spec.openCollection(vault)
		if err != nil {
			_ = session.Close() // Best effort cleanup
			return nil, nil, fmt.Errorf("failed to open collection %s: %w", spec.getName(), err)
		}
		collections[i] = coll
	}

	return session, collections, nil
}

// OpenVaultWithIdentity works like OpenVault but returns a map keyed by the provided identities
// (instead of a positional slice). Identity must be comparable. The identity's fmt.Sprint form is
// used as the collection name, and a reserved identity-map collection is maintained for deterministic reloads.
func OpenVaultWithIdentity[I comparable](filename string, specs ...IdentitySpec[I]) (*VaultSession, map[I]any, error) {
	baseSpecs := make([]CollectionSpec, len(specs))
	for i, spec := range specs {
		baseSpecs[i] = spec
	}

	session, colls, err := OpenVault(filename, baseSpecs...)
	if err != nil {
		return nil, nil, err
	}

	// Ensure identity map exists
	if _, err := session.Vault.ensureIdentityMap(); err != nil {
		_ = session.Close() // Best effort cleanup
		return nil, nil, fmt.Errorf("failed to ensure identity map: %w", err)
	}

	result := make(map[I]any, len(specs))
	for i, spec := range specs {
		id := spec.getIdentity()
		result[id] = colls[i]
		idStr := fmt.Sprint(id)
		if err := session.Vault.setIdentityMapping(idStr, spec.getName()); err != nil {
			_ = session.Close() // Best effort cleanup
			return nil, nil, fmt.Errorf("failed to set identity mapping for %s: %w", idStr, err)
		}
	}

	return session, result, nil
}

// GetOrCreateCollectionWithIdentity dynamically creates or loads a collection tied to an identity.
// It registers key/payload types as needed, ensures the identity map exists, and records the mapping.
// This enables treating the vault as a collection-of-collections that can grow at runtime.
func GetOrCreateCollectionWithIdentity[I comparable, K any, P yttypes.PersistentPayload[P]](
	v *Vault,
	identity I,
	lessFunc func(a, b K) bool,
	keyTemplate yttypes.PersistentKey[K],
	payloadTemplate P,
) (*treap.PersistentPayloadTreap[K, P], error) {
	// Ensure types are registered so the collection registry can validate them.
	v.RegisterType(keyTemplate)
	v.RegisterType(payloadTemplate)

	if _, err := v.ensureIdentityMap(); err != nil {
		return nil, fmt.Errorf("failed to ensure identity map: %w", err)
	}

	collectionName := fmt.Sprint(identity)

	coll, err := GetOrCreateCollection[K, P](v, collectionName, lessFunc, keyTemplate)
	if err != nil {
		return nil, err
	}

	if err := v.setIdentityMapping(collectionName, collectionName); err != nil {
		return nil, err
	}

	return coll, nil
}

// ensureIdentityMap lazily creates/loads the reserved identity-map collection.
func (v *Vault) ensureIdentityMap() (*treap.PersistentPayloadTreap[yttypes.StringKey, yttypes.JsonPayload[identityMapping]], error) {
	// Register key/payload types to keep TypeMap consistent.
	v.RegisterType((*yttypes.StringKey)(new(string)))
	v.RegisterType(yttypes.JsonPayload[identityMapping]{})

	// Cached?
	if cached, ok := v.activeCollections[identityMapCollectionName]; ok {
		if treap, ok := cached.(*treap.PersistentPayloadTreap[yttypes.StringKey, yttypes.JsonPayload[identityMapping]]); ok {
			return treap, nil
		}
		return nil, fmt.Errorf("identity map cached with unexpected type")
	}

	coll, err := GetOrCreateCollection[yttypes.StringKey, yttypes.JsonPayload[identityMapping]](
		v,
		identityMapCollectionName,
		yttypes.StringLess,
		(*yttypes.StringKey)(new(string)),
	)
	if err != nil {
		return nil, err
	}
	// Cache it explicitly
	v.activeCollections[identityMapCollectionName] = coll
	return coll, nil
}

// setIdentityMapping records identity -> collection name in the reserved identity map.
func (v *Vault) setIdentityMapping(identityStr, collectionName string) error {
	imap, err := v.ensureIdentityMap()
	if err != nil {
		return err
	}
	key := yttypes.StringKey(identityStr)
	payload := yttypes.JsonPayload[identityMapping]{Value: identityMapping{CollectionName: collectionName}}
	imap.Insert(&key, payload)
	return nil
}
