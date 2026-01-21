# Test Utilities

This package provides shared test utilities for the BobBob project to reduce code duplication across test files.

## Store Setup Functions

### `SetupTestStore(tb testing.TB)`
Creates a temporary directory and a new basic store for testing.

**Returns:**
- `dir string` - temporary directory path
- `s store.Storer` - the created store
- `cleanup func()` - cleanup function to call with `defer`

**Example:**
```go
func TestMyFeature(t *testing.T) {
    _, store, cleanup := testutil.SetupTestStore(t)
    defer cleanup()
    
    // Your test code here
}
```

### `SetupConcurrentStore(tb testing.TB)`
Creates a temporary directory and a new concurrent store for testing.

**Returns:** Same as `SetupTestStore`

### `NewMockStore() store.Storer`
Creates an in-memory, disk-free store for unit tests. **Recommended for fast, logic-focused tests.**

**Returns:**
- `store.Storer` - an in-memory store backed by Go maps (thread-safe)

**Benefits:**
- ⚡ **3.8× faster** than disk-backed stores (no file I/O)
- 🧵 **Thread-safe** - built-in sync.RWMutex protection
- 🚫 **No disk files** - no temp directory cleanup needed
- ✅ **Full API** - implements complete Storer interface

**When to use MockStore:**
- Logic-focused tests (non-persistence, non-concurrency edge cases)
- Memory behavior and tree structure validation
- Fast iteration during development
- Memory/flushing behavior verification

**When to use real stores (BasicStore/MultiStore):**
- Persistence across sessions
- Concurrent reader/writer patterns with actual I/O contention
- Allocator behavior and block routing
- Disk corruption/recovery scenarios

**Example:**
```go
func TestMyFeature(t *testing.T) {
    // Fast, no disk I/O
    store := testutil.NewMockStore()
    defer store.Close()
    
    objId, _ := store.NewObj(100)
    // Your test code here
}
```

## Store Operation Helpers

### `WriteObject(tb testing.TB, s store.Storer, data []byte) store.ObjectId`
Writes data to a new object in the store. Automatically handles the finisher and error checking.

**Example:**
```go
data := []byte("test data")
objId := testutil.WriteObject(t, store, data)
```

### `ReadObject(tb testing.TB, s store.Storer, objId store.ObjectId) []byte`
Reads and returns data from an object in the store.

**Example:**
```go
data := testutil.ReadObject(t, store, objId)
```

### `VerifyObject(tb testing.TB, s store.Storer, objId store.ObjectId, expected []byte)`
Reads an object and verifies its contents match the expected data. Fails the test if they don't match.

**Example:**
```go
expectedData := []byte("test data")
testutil.VerifyObject(t, store, objId, expectedData)
```

### `UpdateObject(tb testing.TB, s store.Storer, objId store.ObjectId, data []byte)`
Overwrites an existing object with new data.

**Example:**
```go
newData := []byte("updated data")
testutil.UpdateObject(t, store, objId, newData)
```

### `DeleteObject(tb testing.TB, s store.Storer, objId store.ObjectId)`
Deletes an object from the store.

**Example:**
```go
testutil.DeleteObject(t, store, objId)
```

### `AllocateObject(tb testing.TB, s store.Storer, size int) store.ObjectId`
Allocates an object of the specified size without writing to it.

**Example:**
```go
objId := testutil.AllocateObject(t, store, 100)
```

## Data Generation Functions

### `RandomBytes(tb testing.TB, length int) []byte`
Generates a slice of cryptographically random bytes of the specified length.

**Example:**
```go
data := testutil.RandomBytes(t, 1024)
```

### `RandomBytesSeeded(length int, seed byte) []byte`
Generates deterministic random bytes for reproducible tests. Same seed always produces the same output.

**Example:**
```go
data1 := testutil.RandomBytesSeeded(100, 42)
data2 := testutil.RandomBytesSeeded(100, 42) // identical to data1
```

## File Utilities

### `CreateTempFile(tb testing.TB, pattern string) (filePath string, cleanup func())`
Creates a temporary file for testing.

**Example:**
```go
filePath, cleanup := testutil.CreateTempFile(t, "test.bin")
defer cleanup()
```

## Complete Example

```go
package mypackage_test

import (
    "testing"
    "bobbob/internal/testutil"
)

func TestCompleteWorkflow(t *testing.T) {
    // Setup store with automatic cleanup
    _, store, cleanup := testutil.SetupTestStore(t)
    defer cleanup()
    
    // Generate test data
    data := testutil.RandomBytesSeeded(100, 1)
    
    // Write object
    objId := testutil.WriteObject(t, store, data)
    
    // Verify it was written correctly
    testutil.VerifyObject(t, store, objId, data)
    
    // Update the object
    newData := testutil.RandomBytesSeeded(100, 2)
    testutil.UpdateObject(t, store, objId, newData)
    
    // Verify the update
    testutil.VerifyObject(t, store, objId, newData)
    
    // Clean up
    testutil.DeleteObject(t, store, objId)
}
```

## Benefits

- **Less Boilerplate**: Common test setup and operations are handled by utilities
- **Consistent Error Handling**: All helpers use `testing.TB.Fatalf` for consistent error reporting
- **Automatic Cleanup**: Setup functions return cleanup callbacks to ensure resources are freed
- **Easier to Read**: Tests focus on what's being tested, not setup details
- **Reusable**: Same utilities work across all packages in the project
