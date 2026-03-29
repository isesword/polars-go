package polars

import (
	"fmt"
	"runtime"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func exportArrayColumn(
	op string,
	f sliceExportFrame,
	column string,
	normalize func(arrow.Array) (arrow.Array, error),
) (arrow.Array, error) {
	if f == nil {
		return nil, wrapOp(op, ErrNilDataFrame)
	}

	recordBatch, err := f.ToArrow()
	if err != nil {
		runtime.KeepAlive(f)
		return nil, wrapOp(op, err)
	}
	defer recordBatch.Release()

	colIdx, colName, err := resolveSliceColumn(op, column, schemaFieldNames(recordBatch.Schema().Fields()))
	if err != nil {
		runtime.KeepAlive(f)
		return nil, err
	}

	out, err := normalize(recordBatch.Column(colIdx))
	runtime.KeepAlive(f)
	if err != nil {
		return nil, &ValidationError{
			Op:      op,
			Field:   colName,
			Message: err.Error(),
			Err:     err,
		}
	}
	return out, nil
}

func normalizeStringArrowArray(arr arrow.Array) (arrow.Array, error) {
	switch col := arr.(type) {
	case *array.String:
		col.Retain()
		return col, nil
	case *array.LargeString:
		builder := array.NewStringBuilder(memory.DefaultAllocator)
		defer builder.Release()
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				builder.AppendNull()
				continue
			}
			builder.Append(col.Value(i))
		}
		return builder.NewStringArray(), nil
	case *array.StringView:
		builder := array.NewStringBuilder(memory.DefaultAllocator)
		defer builder.Release()
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				builder.AppendNull()
				continue
			}
			builder.Append(col.Value(i))
		}
		return builder.NewStringArray(), nil
	default:
		return nil, fmt.Errorf("expected Arrow string column, got %T", arr)
	}
}

func normalizeInt32ArrowArray(arr arrow.Array) (arrow.Array, error) {
	col, ok := arr.(*array.Int32)
	if !ok {
		return nil, fmt.Errorf("expected Arrow int32 column, got %T", arr)
	}
	col.Retain()
	return col, nil
}

func normalizeInt64ArrowArray(arr arrow.Array) (arrow.Array, error) {
	col, ok := arr.(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("expected Arrow int64 column, got %T", arr)
	}
	col.Retain()
	return col, nil
}

func normalizeUint32ArrowArray(arr arrow.Array) (arrow.Array, error) {
	col, ok := arr.(*array.Uint32)
	if !ok {
		return nil, fmt.Errorf("expected Arrow uint32 column, got %T", arr)
	}
	col.Retain()
	return col, nil
}

func normalizeUint64ArrowArray(arr arrow.Array) (arrow.Array, error) {
	col, ok := arr.(*array.Uint64)
	if !ok {
		return nil, fmt.Errorf("expected Arrow uint64 column, got %T", arr)
	}
	col.Retain()
	return col, nil
}

func normalizeFloat32ArrowArray(arr arrow.Array) (arrow.Array, error) {
	col, ok := arr.(*array.Float32)
	if !ok {
		return nil, fmt.Errorf("expected Arrow float32 column, got %T", arr)
	}
	col.Retain()
	return col, nil
}

func normalizeFloat64ArrowArray(arr arrow.Array) (arrow.Array, error) {
	col, ok := arr.(*array.Float64)
	if !ok {
		return nil, fmt.Errorf("expected Arrow float64 column, got %T", arr)
	}
	col.Retain()
	return col, nil
}

func normalizeBoolArrowArray(arr arrow.Array) (arrow.Array, error) {
	col, ok := arr.(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("expected Arrow bool column, got %T", arr)
	}
	col.Retain()
	return col, nil
}

func (f *DataFrame) ToArrayString(column ...string) (*array.String, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToArrayString", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("DataFrame.ToArrayString", f, name, normalizeStringArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.String), nil
}

func (df *EagerFrame) ToArrayString(column ...string) (*array.String, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToArrayString", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("EagerFrame.ToArrayString", df, name, normalizeStringArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.String), nil
}

func (f *DataFrame) ToArrayInt32(column ...string) (*array.Int32, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToArrayInt32", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("DataFrame.ToArrayInt32", f, name, normalizeInt32ArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Int32), nil
}

func (df *EagerFrame) ToArrayInt32(column ...string) (*array.Int32, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToArrayInt32", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("EagerFrame.ToArrayInt32", df, name, normalizeInt32ArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Int32), nil
}

func (f *DataFrame) ToArrayInt64(column ...string) (*array.Int64, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToArrayInt64", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("DataFrame.ToArrayInt64", f, name, normalizeInt64ArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Int64), nil
}

func (df *EagerFrame) ToArrayInt64(column ...string) (*array.Int64, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToArrayInt64", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("EagerFrame.ToArrayInt64", df, name, normalizeInt64ArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Int64), nil
}

func (f *DataFrame) ToArrayUInt32(column ...string) (*array.Uint32, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToArrayUInt32", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("DataFrame.ToArrayUInt32", f, name, normalizeUint32ArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Uint32), nil
}

func (df *EagerFrame) ToArrayUInt32(column ...string) (*array.Uint32, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToArrayUInt32", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("EagerFrame.ToArrayUInt32", df, name, normalizeUint32ArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Uint32), nil
}

func (f *DataFrame) ToArrayUInt64(column ...string) (*array.Uint64, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToArrayUInt64", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("DataFrame.ToArrayUInt64", f, name, normalizeUint64ArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Uint64), nil
}

func (df *EagerFrame) ToArrayUInt64(column ...string) (*array.Uint64, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToArrayUInt64", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("EagerFrame.ToArrayUInt64", df, name, normalizeUint64ArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Uint64), nil
}

func (f *DataFrame) ToArrayFloat32(column ...string) (*array.Float32, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToArrayFloat32", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("DataFrame.ToArrayFloat32", f, name, normalizeFloat32ArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Float32), nil
}

func (df *EagerFrame) ToArrayFloat32(column ...string) (*array.Float32, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToArrayFloat32", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("EagerFrame.ToArrayFloat32", df, name, normalizeFloat32ArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Float32), nil
}

func (f *DataFrame) ToArrayFloat64(column ...string) (*array.Float64, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToArrayFloat64", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("DataFrame.ToArrayFloat64", f, name, normalizeFloat64ArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Float64), nil
}

func (df *EagerFrame) ToArrayFloat64(column ...string) (*array.Float64, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToArrayFloat64", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("EagerFrame.ToArrayFloat64", df, name, normalizeFloat64ArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Float64), nil
}

func (f *DataFrame) ToArrayBool(column ...string) (*array.Boolean, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToArrayBool", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("DataFrame.ToArrayBool", f, name, normalizeBoolArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Boolean), nil
}

func (df *EagerFrame) ToArrayBool(column ...string) (*array.Boolean, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToArrayBool", column)
	if err != nil {
		return nil, err
	}
	arr, err := exportArrayColumn("EagerFrame.ToArrayBool", df, name, normalizeBoolArrowArray)
	if err != nil {
		return nil, err
	}
	return arr.(*array.Boolean), nil
}
