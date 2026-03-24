//go:build !cgo
// +build !cgo

package polars

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/isesword/polars-go-bridge/bridge"
)

// NewEagerFrameFromArrowRecordBatch 从 Arrow RecordBatch 创建 EagerFrame。
//
// 无 cgo 时不可用，因为 Arrow C Data Interface 需要 cgo 绑定。
func NewEagerFrameFromArrowRecordBatch(_ *bridge.Bridge, _ arrow.RecordBatch) (*EagerFrame, error) {
	return nil, fmt.Errorf("NewEagerFrameFromArrowRecordBatch requires cgo (set CGO_ENABLED=1)")
}
