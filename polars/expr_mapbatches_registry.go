package polars

import (
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/isesword/polars-go-bridge/bridge"
)

type exprMapBatchesFunc func(batch arrow.RecordBatch) (arrow.RecordBatch, error)

var exprMapBatchesRegistry = struct {
	mu     sync.RWMutex
	nextID uint64
	funcs  map[uint64]exprMapBatchesFunc
}{
	nextID: 1,
	funcs:  make(map[uint64]exprMapBatchesFunc),
}

var exprMapBatchesBridgeState = struct {
	mu         sync.Mutex
	registered map[*bridge.Bridge]bool
}{
	registered: make(map[*bridge.Bridge]bool),
}

func registerExprMapBatches(fn exprMapBatchesFunc) uint64 {
	exprMapBatchesRegistry.mu.Lock()
	defer exprMapBatchesRegistry.mu.Unlock()

	id := exprMapBatchesRegistry.nextID
	exprMapBatchesRegistry.nextID++
	exprMapBatchesRegistry.funcs[id] = fn
	return id
}

func lookupExprMapBatches(id uint64) (exprMapBatchesFunc, bool) {
	exprMapBatchesRegistry.mu.RLock()
	defer exprMapBatchesRegistry.mu.RUnlock()
	fn, ok := exprMapBatchesRegistry.funcs[id]
	return fn, ok
}

func hasRegisteredExprMapBatches() bool {
	exprMapBatchesRegistry.mu.RLock()
	defer exprMapBatchesRegistry.mu.RUnlock()
	return len(exprMapBatchesRegistry.funcs) > 0
}

func ensureExprMapBatchesBridge(brg *bridge.Bridge) error {
	if brg == nil {
		return fmt.Errorf("bridge is nil")
	}
	if !hasRegisteredExprMapBatches() {
		return nil
	}

	exprMapBatchesBridgeState.mu.Lock()
	defer exprMapBatchesBridgeState.mu.Unlock()

	if exprMapBatchesBridgeState.registered[brg] {
		return nil
	}
	if err := registerExprMapBatchesBridgeCallback(brg); err != nil {
		return err
	}
	exprMapBatchesBridgeState.registered[brg] = true
	return nil
}
