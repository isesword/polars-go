//go:build cgo
// +build cgo

package polars

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
	"github.com/isesword/polars-go/bridge"
)

func zeroCopySupported() bool {
	return true
}

func buildArrowInput() (*bridge.ArrowSchema, *bridge.ArrowArray, func(), error) {
	alloc := mallocator.NewMallocator()
	nameBuilder := array.NewStringBuilder(alloc)
	ageBuilder := array.NewInt64Builder(alloc)
	defer nameBuilder.Release()
	defer ageBuilder.Release()

	names := []string{"alice", "bob", "carl"}
	ages := []int64{34, 21, 45}
	for i := range names {
		nameBuilder.Append(names[i])
		ageBuilder.Append(ages[i])
	}

	nameArr := nameBuilder.NewArray()
	ageArr := ageBuilder.NewArray()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	rb := array.NewRecordBatch(schema, []arrow.Array{nameArr, ageArr}, int64(len(names)))
	nameArr.Release()
	ageArr.Release()

	var cSchema cdata.CArrowSchema
	var cArray cdata.CArrowArray
	cdata.ExportArrowRecordBatch(rb, &cArray, &cSchema)

	cleanup := func() {
		rb.Release()
	}

	return (*bridge.ArrowSchema)(unsafe.Pointer(&cSchema)),
		(*bridge.ArrowArray)(unsafe.Pointer(&cArray)),
		cleanup,
		nil
}

func importArrowRecordBatch(
	schema *bridge.ArrowSchema,
	arr *bridge.ArrowArray,
) (arrow.RecordBatch, error) {
	if schema == nil || arr == nil {
		return nil, fmt.Errorf("nil schema/array")
	}
	return cdata.ImportCRecordBatch(
		(*cdata.CArrowArray)(unsafe.Pointer(arr)),
		(*cdata.CArrowSchema)(unsafe.Pointer(schema)),
	)
}
