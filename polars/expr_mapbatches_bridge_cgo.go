//go:build cgo
// +build cgo

package polars

import (
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/ebitengine/purego"
	"github.com/isesword/polars-go-bridge/bridge"
)

var exprMapBatchesCallbackPtr = purego.NewCallback(goExprMapBatchesCallback)

func registerExprMapBatchesBridgeCallback(brg *bridge.Bridge) error {
	return brg.RegisterGoExprMapBatchesCallback(exprMapBatchesCallbackPtr)
}

func goExprMapBatchesCallback(
	udgID uint64,
	inputSchema *bridge.ArrowSchema,
	inputArray *bridge.ArrowArray,
	outputSchema *bridge.ArrowSchema,
	outputArray *bridge.ArrowArray,
) uintptr {
	fn, ok := lookupExprMapBatches(udgID)
	if !ok {
		return 1
	}
	if fn == nil {
		return 1
	}
	if inputSchema == nil || inputArray == nil || outputSchema == nil || outputArray == nil {
		return 1
	}

	inputBatch, err := cdata.ImportCRecordBatch(
		(*cdata.CArrowArray)(unsafe.Pointer(inputArray)),
		(*cdata.CArrowSchema)(unsafe.Pointer(inputSchema)),
	)
	if err != nil {
		return 1
	}

	outputBatch, err := fn(inputBatch)
	if err != nil {
		inputBatch.Release()
		return 1
	}
	if outputBatch == nil {
		inputBatch.Release()
		return 1
	}

	var cSchema cdata.CArrowSchema
	var cArray cdata.CArrowArray
	cdata.ExportArrowRecordBatch(outputBatch, &cArray, &cSchema)

	*(*cdata.CArrowSchema)(unsafe.Pointer(outputSchema)) = cSchema
	*(*cdata.CArrowArray)(unsafe.Pointer(outputArray)) = cArray

	if outputBatch != inputBatch {
		inputBatch.Release()
	}
	outputBatch.Release()
	return 0
}
