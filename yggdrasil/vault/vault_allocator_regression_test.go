package vault_test

import (
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
)

type testData struct {
	Value int
}

func (t testData) Marshal() ([]byte, error) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(int64(t.Value)))
	return buf, nil
}

func (t testData) Unmarshal(data []byte) (types.UntypedPersistentPayload, error) {
	if len(data) < 8 {
		return testData{}, io.ErrUnexpectedEOF
	}
	val := int64(binary.LittleEndian.Uint64(data[:8]))
	return testData{Value: int(val)}, nil
}

func (t testData) SizeInBytes() int {
	return 8 // size of int
}

// Regression test: concurrent persistions used to trigger allocator slice bounds panic.
func TestConcurrentPersistAllocatorNoPanic(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vault_regression.db")

	session, colls, err := vault.OpenVaultWithIdentity(
		dbPath,
		vault.PayloadIdentitySpec[string, types.MD5Key, testData]{
			Identity:        "test",
			LessFunc:        types.MD5Less,
			KeyTemplate:     (*types.MD5Key)(new(types.MD5Key)),
			PayloadTemplate: testData{},
		},
	)
	if err != nil {
		t.Fatalf("failed to open vault: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	collection := colls["test"].(*treap.PersistentPayloadTreap[types.MD5Key, testData])

	// Enable background memory monitoring (previously triggered the panic path).
	session.Vault.SetMemoryBudgetWithPercentileWithCallbacks(1000, 20, nil, nil)
	session.Vault.SetCheckInterval(10)

	const numWorkers = 20
	const opsPerWorker = 50

	var wg sync.WaitGroup
	errs := make(chan error, numWorkers*opsPerWorker)

	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := range opsPerWorker {
				key := types.MD5Key{}
				copy(key[:], fmt.Sprintf("worker-%02d-op-%03d", workerID, i))

				collection.Insert(&key, testData{Value: i})

				// Persist the collection periodically to exercise concurrent persist
				if i%10 == 0 {
					if err := collection.Persist(); err != nil {
						errs <- fmt.Errorf("worker %d op %d: %w", workerID, i, err)
						return
					}
				}
			}
		}(w)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("persist error: %v", err)
	}
}
