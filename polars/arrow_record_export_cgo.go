//go:build cgo
// +build cgo

package polars

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/isesword/polars-go-bridge/bridge"
)

func exportDataFrameToArrowRecordBatch(brg *bridge.Bridge, handle uint64) (arrow.RecordBatch, error) {
	schema, array, err := brg.ExportDataFrameToArrow(handle)
	if err != nil {
		return nil, err
	}

	recordBatch, err := cdata.ImportCRecordBatch(
		(*cdata.CArrowArray)(unsafe.Pointer(array)),
		(*cdata.CArrowSchema)(unsafe.Pointer(schema)),
	)
	if err != nil {
		bridge.ReleaseArrowArray(array)
		bridge.ReleaseArrowSchema(schema)
		return nil, fmt.Errorf("failed to import arrow record batch: %w", err)
	}
	return recordBatch, nil
}
