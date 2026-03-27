//go:build !cgo
// +build !cgo

package polars

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/isesword/polars-go/bridge"
)

func zeroCopySupported() bool {
	return false
}

func buildArrowInput() (*bridge.ArrowSchema, *bridge.ArrowArray, func(), error) {
	return nil, nil, func() {}, fmt.Errorf("zero-copy requires cgo")
}

func importArrowRecordBatch(
	_ *bridge.ArrowSchema,
	_ *bridge.ArrowArray,
) (arrow.RecordBatch, error) {
	return nil, fmt.Errorf("zero-copy requires cgo")
}
