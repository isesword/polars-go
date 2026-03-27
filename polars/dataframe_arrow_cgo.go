//go:build cgo
// +build cgo

package polars

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/isesword/polars-go/bridge"
)

// NewEagerFrameFromArrowRecordBatch creates an eager DataFrame from an Arrow RecordBatch.
//
// This is the preferred Arrow import path. The batch is exported through the
// Arrow C Data Interface and passed to Rust without going through the JSON
// bridge.
//
// Use this API when the caller already has Arrow data or wants a more
// column-oriented and higher-throughput exchange path than the JSON APIs.
// Prefer NewDataFrameFromArrowBatch for managed application-facing usage.
func NewEagerFrameFromArrowRecordBatch(brg *bridge.Bridge, recordBatch arrow.RecordBatch) (*EagerFrame, error) {
	if brg == nil {
		return nil, fmt.Errorf("bridge is nil")
	}
	if recordBatch == nil {
		return nil, fmt.Errorf("record batch is nil")
	}

	var cSchema cdata.CArrowSchema
	var cArray cdata.CArrowArray
	cdata.ExportArrowRecordBatch(recordBatch, &cArray, &cSchema)

	dfHandle, err := brg.CreateDataFrameFromArrow(
		(*bridge.ArrowSchema)(unsafe.Pointer(&cSchema)),
		(*bridge.ArrowArray)(unsafe.Pointer(&cArray)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create dataframe from arrow record batch: %w", err)
	}

	return newDataFrame(dfHandle, brg), nil
}
