//go:build !cgo
// +build !cgo

package polars

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// NewArrowRecordBatchFromRowsWithSchema 将行式数据按显式 schema 转为 Arrow RecordBatch。
//
// 无 cgo 时不可用，因为当前高性能 Arrow 导入链依赖 cgo。
func NewArrowRecordBatchFromRowsWithSchema(
	_ []map[string]any,
	_ map[string]DataType,
) (arrow.RecordBatch, error) {
	return nil, fmt.Errorf("NewArrowRecordBatchFromRowsWithSchema requires cgo (set CGO_ENABLED=1)")
}

// NewArrowRecordBatchFromRowsWithArrowSchema 将行式数据按显式 Arrow schema 转为 Arrow RecordBatch。
func NewArrowRecordBatchFromRowsWithArrowSchema(
	_ []map[string]any,
	_ *arrow.Schema,
) (arrow.RecordBatch, error) {
	return nil, fmt.Errorf("NewArrowRecordBatchFromRowsWithArrowSchema requires cgo (set CGO_ENABLED=1)")
}
