package polars

import (
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

type sliceExportFrame interface {
	ToArrow() (arrow.RecordBatch, error)
	ToMaps() ([]map[string]interface{}, error)
}

func normalizeUntypedSliceColumnArg(op string, column []string) (string, error) {
	if len(column) > 1 {
		return "", &ValidationError{
			Op:      op,
			Message: fmt.Sprintf("expected at most one column name, got %d", len(column)),
			Err:     ErrInvalidInput,
		}
	}
	if len(column) == 0 {
		return "", nil
	}
	return column[0], nil
}

// ToSlice materializes a single dataframe column into a typed Go slice.
//
// This is the most direct counterpart to Python's Series.to_list() for callers
// that already selected a single column, for example after Select(...).Unique().
// When column is empty, the dataframe must contain exactly one column.
func ToSlice[T any](f sliceExportFrame, column string) ([]T, error) {
	if f == nil {
		return nil, wrapOp("polars.ToSlice", ErrNilDataFrame)
	}

	recordBatch, err := f.ToArrow()
	if err == nil {
		defer recordBatch.Release()
		values, sliceErr := recordBatchToSlice[T]("polars.ToSlice", recordBatch, column)
		runtime.KeepAlive(f)
		return values, sliceErr
	}
	if !bridgeArrowExportSupported() {
		rows, mapErr := f.ToMaps()
		if mapErr != nil {
			runtime.KeepAlive(f)
			return nil, wrapOp("polars.ToSlice", mapErr)
		}
		values, sliceErr := rowsColumnToSlice[T]("polars.ToSlice", rows, column)
		runtime.KeepAlive(f)
		return values, sliceErr
	}
	runtime.KeepAlive(f)
	return nil, wrapOp("polars.ToSlice", err)
}

// ToSlice materializes a single dataframe column into a []any.
//
// When no column name is provided, the dataframe must contain exactly one
// column. This is a convenience wrapper around the generic ToSlice[any].
func (f *DataFrame) ToSlice(column ...string) ([]any, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToSlice", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[any](f, name)
}

// ToSlice materializes a single eager dataframe column into a []any.
//
// When no column name is provided, the dataframe must contain exactly one
// column. This is a convenience wrapper around the generic ToSlice[any].
func (df *EagerFrame) ToSlice(column ...string) ([]any, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToSlice", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[any](df, name)
}

// ToList is kept as a compatibility alias for ToSlice.
func (f *DataFrame) ToList(column ...string) ([]any, error) {
	return f.ToSlice(column...)
}

// ToList is kept as a compatibility alias for ToSlice.
func (df *EagerFrame) ToList(column ...string) ([]any, error) {
	return df.ToSlice(column...)
}

// ToSliceString materializes a single dataframe column into a []string.
func (f *DataFrame) ToSliceString(column ...string) ([]string, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToSliceString", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[string](f, name)
}

// ToSliceString materializes a single eager dataframe column into a []string.
func (df *EagerFrame) ToSliceString(column ...string) ([]string, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToSliceString", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[string](df, name)
}

// ToSliceInt64 materializes a single dataframe column into a []int64.
func (f *DataFrame) ToSliceInt64(column ...string) ([]int64, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToSliceInt64", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[int64](f, name)
}

// ToSliceInt64 materializes a single eager dataframe column into a []int64.
func (df *EagerFrame) ToSliceInt64(column ...string) ([]int64, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToSliceInt64", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[int64](df, name)
}

// ToSliceUInt64 materializes a single dataframe column into a []uint64.
func (f *DataFrame) ToSliceUInt64(column ...string) ([]uint64, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToSliceUInt64", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[uint64](f, name)
}

// ToSliceUInt64 materializes a single eager dataframe column into a []uint64.
func (df *EagerFrame) ToSliceUInt64(column ...string) ([]uint64, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToSliceUInt64", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[uint64](df, name)
}

// ToSliceInt32 materializes a single dataframe column into a []int32.
func (f *DataFrame) ToSliceInt32(column ...string) ([]int32, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToSliceInt32", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[int32](f, name)
}

// ToSliceInt32 materializes a single eager dataframe column into a []int32.
func (df *EagerFrame) ToSliceInt32(column ...string) ([]int32, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToSliceInt32", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[int32](df, name)
}

// ToSliceUInt32 materializes a single dataframe column into a []uint32.
func (f *DataFrame) ToSliceUInt32(column ...string) ([]uint32, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToSliceUInt32", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[uint32](f, name)
}

// ToSliceUInt32 materializes a single eager dataframe column into a []uint32.
func (df *EagerFrame) ToSliceUInt32(column ...string) ([]uint32, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToSliceUInt32", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[uint32](df, name)
}

// ToSliceFloat64 materializes a single dataframe column into a []float64.
func (f *DataFrame) ToSliceFloat64(column ...string) ([]float64, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToSliceFloat64", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[float64](f, name)
}

// ToSliceFloat64 materializes a single eager dataframe column into a []float64.
func (df *EagerFrame) ToSliceFloat64(column ...string) ([]float64, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToSliceFloat64", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[float64](df, name)
}

// ToSliceFloat32 materializes a single dataframe column into a []float32.
func (f *DataFrame) ToSliceFloat32(column ...string) ([]float32, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToSliceFloat32", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[float32](f, name)
}

// ToSliceFloat32 materializes a single eager dataframe column into a []float32.
func (df *EagerFrame) ToSliceFloat32(column ...string) ([]float32, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToSliceFloat32", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[float32](df, name)
}

// ToSliceBool materializes a single dataframe column into a []bool.
func (f *DataFrame) ToSliceBool(column ...string) ([]bool, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToSliceBool", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[bool](f, name)
}

// ToSliceBool materializes a single eager dataframe column into a []bool.
func (df *EagerFrame) ToSliceBool(column ...string) ([]bool, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToSliceBool", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[bool](df, name)
}

// ToSliceTime materializes a single dataframe column into a []time.Time.
func (f *DataFrame) ToSliceTime(column ...string) ([]time.Time, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToSliceTime", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[time.Time](f, name)
}

// ToSliceTime materializes a single eager dataframe column into a []time.Time.
func (df *EagerFrame) ToSliceTime(column ...string) ([]time.Time, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToSliceTime", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[time.Time](df, name)
}

// ToSliceBytes materializes a single dataframe column into a [][]byte.
func (f *DataFrame) ToSliceBytes(column ...string) ([][]byte, error) {
	name, err := normalizeUntypedSliceColumnArg("DataFrame.ToSliceBytes", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[[]byte](f, name)
}

// ToSliceBytes materializes a single eager dataframe column into a [][]byte.
func (df *EagerFrame) ToSliceBytes(column ...string) ([][]byte, error) {
	name, err := normalizeUntypedSliceColumnArg("EagerFrame.ToSliceBytes", column)
	if err != nil {
		return nil, err
	}
	return ToSlice[[]byte](df, name)
}

func recordBatchToSlice[T any](op string, recordBatch arrow.RecordBatch, column string) ([]T, error) {
	colIdx, colName, err := resolveSliceColumn(op, column, schemaFieldNames(recordBatch.Schema().Fields()))
	if err != nil {
		return nil, err
	}

	decoder, err := newArrowColumnDecoder(recordBatch.Column(colIdx))
	if err != nil {
		return nil, &ValidationError{
			Op:      op,
			Field:   colName,
			Message: err.Error(),
			Err:     err,
		}
	}

	nRows := int(recordBatch.NumRows())
	out := make([]T, nRows)
	for i := 0; i < nRows; i++ {
		raw, err := decoder(i)
		if err != nil {
			return nil, &ValidationError{
				Op:      op,
				Row:     i,
				Field:   colName,
				Message: err.Error(),
				Err:     err,
			}
		}
		if err := assignSliceElementValue(out[i:], 0, raw); err != nil {
			return nil, &ValidationError{
				Op:      op,
				Row:     i,
				Field:   colName,
				Message: err.Error(),
				Err:     err,
			}
		}
	}
	return out, nil
}

func rowsColumnToSlice[T any](op string, rows []map[string]interface{}, column string) ([]T, error) {
	colName, err := resolveSliceColumnFromRows(op, column, rows)
	if err != nil {
		return nil, err
	}

	out := make([]T, len(rows))
	for i, row := range rows {
		raw, ok := row[colName]
		if !ok {
			return nil, &ValidationError{
				Op:      op,
				Row:     i,
				Field:   colName,
				Message: "column not found",
				Err:     ErrInvalidInput,
			}
		}
		if err := assignSliceElementValue(out[i:], 0, raw); err != nil {
			return nil, &ValidationError{
				Op:      op,
				Row:     i,
				Field:   colName,
				Message: err.Error(),
				Err:     err,
			}
		}
	}
	return out, nil
}

func assignSliceElementValue[T any](dest []T, idx int, raw any) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Slice || idx < 0 || idx >= value.Len() {
		return fmt.Errorf("destination slice index %d out of range", idx)
	}
	return assignStructFieldValue(value.Index(idx), raw)
}

func resolveSliceColumn(op string, column string, columns []string) (int, string, error) {
	if column == "" {
		if len(columns) == 1 {
			return 0, columns[0], nil
		}
		return -1, "", &ValidationError{
			Op:      op,
			Message: fmt.Sprintf("column name is required when dataframe has %d columns", len(columns)),
			Err:     ErrInvalidInput,
		}
	}
	for i, name := range columns {
		if name == column {
			return i, name, nil
		}
	}
	return -1, "", &ValidationError{
		Op:      op,
		Field:   column,
		Message: "column not found",
		Err:     ErrInvalidInput,
	}
}

func resolveSliceColumnFromRows(op string, column string, rows []map[string]interface{}) (string, error) {
	if column != "" {
		if len(rows) == 0 {
			return column, nil
		}
		if _, ok := rows[0][column]; ok {
			return column, nil
		}
		return "", &ValidationError{
			Op:      op,
			Field:   column,
			Message: "column not found",
			Err:     ErrInvalidInput,
		}
	}
	if len(rows) == 0 {
		return "", &ValidationError{
			Op:      op,
			Message: "column name is required when dataframe has no rows",
			Err:     ErrInvalidInput,
		}
	}
	if len(rows[0]) == 1 {
		for name := range rows[0] {
			return name, nil
		}
	}
	return "", &ValidationError{
		Op:      op,
		Message: fmt.Sprintf("column name is required when dataframe has %d columns", len(rows[0])),
		Err:     ErrInvalidInput,
	}
}

func schemaFieldNames(fields []arrow.Field) []string {
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.Name
	}
	return names
}
