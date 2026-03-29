package polars

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
)

type sliceExportFrame interface {
	ToArrow() (arrow.RecordBatch, error)
	ToMaps() ([]map[string]interface{}, error)
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
		return recordBatchToSlice[T]("polars.ToSlice", recordBatch, column)
	}
	if !bridgeArrowExportSupported() {
		rows, mapErr := f.ToMaps()
		if mapErr != nil {
			return nil, wrapOp("polars.ToSlice", mapErr)
		}
		return rowsColumnToSlice[T]("polars.ToSlice", rows, column)
	}
	return nil, wrapOp("polars.ToSlice", err)
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
