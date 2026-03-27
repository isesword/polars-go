//go:build !cgo
// +build !cgo

package polars

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/isesword/polars-go/bridge"
)

func exportDataFrameToArrowRecordBatch(_ *bridge.Bridge, _ uint64) (arrow.RecordBatch, error) {
	return nil, fmt.Errorf("exportDataFrameToArrowRecordBatch requires cgo (set CGO_ENABLED=1)")
}
