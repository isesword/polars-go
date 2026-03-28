package polars

import (
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// WriteParquetFile writes the dataframe to a Parquet file at path.
func (f *DataFrame) WriteParquetFile(path string) error {
	if err := invalidDataFrameError(f); err != nil {
		return wrapOp("DataFrame.WriteParquetFile", err)
	}
	return f.df.WriteParquetFile(path)
}

// WriteParquetFile writes the eager dataframe to a Parquet file at path.
func (df *EagerFrame) WriteParquetFile(path string) error {
	if err := invalidEagerFrameError(df); err != nil {
		return wrapOp("EagerFrame.WriteParquetFile", err)
	}
	if path == "" {
		return wrapOp("EagerFrame.WriteParquetFile", fmt.Errorf("path is empty"))
	}

	recordBatch, err := df.ToArrow()
	if err != nil {
		return wrapOp("EagerFrame.WriteParquetFile", err)
	}
	defer recordBatch.Release()

	recordBatch, err = normalizeParquetRecordBatch(recordBatch)
	if err != nil {
		return wrapOp("EagerFrame.WriteParquetFile", err)
	}
	defer recordBatch.Release()

	file, err := os.Create(path)
	if err != nil {
		return wrapOp("EagerFrame.WriteParquetFile", err)
	}
	defer file.Close()

	writer, err := pqarrow.NewFileWriter(
		recordBatch.Schema(),
		file,
		parquet.NewWriterProperties(),
		pqarrow.DefaultWriterProps(),
	)
	if err != nil {
		return wrapOp("EagerFrame.WriteParquetFile", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = writer.Close()
		}
	}()

	if err := writer.Write(recordBatch); err != nil {
		return wrapOp("EagerFrame.WriteParquetFile", err)
	}
	if err := writer.Close(); err != nil {
		return wrapOp("EagerFrame.WriteParquetFile", err)
	}
	closed = true
	return nil
}

// SinkParquetFile collects the lazy query and writes the result to a Parquet file.
func (lf *LazyFrame) SinkParquetFile(path string) error {
	if err := invalidLazyFrameError(lf); err != nil {
		return wrapOp("LazyFrame.SinkParquetFile", err)
	}
	if lf.err != nil {
		return wrapOp("LazyFrame.SinkParquetFile", lf.err)
	}
	if path == "" {
		return wrapOp("LazyFrame.SinkParquetFile", fmt.Errorf("path is empty"))
	}

	df, err := lf.Collect()
	if err != nil {
		return wrapOp("LazyFrame.SinkParquetFile", err)
	}
	defer df.Free()

	if err := df.WriteParquetFile(path); err != nil {
		return wrapOp("LazyFrame.SinkParquetFile", err)
	}
	return nil
}

func normalizeParquetRecordBatch(recordBatch arrow.RecordBatch) (arrow.RecordBatch, error) {
	schema := recordBatch.Schema()
	fields := schema.Fields()
	cols := recordBatch.Columns()

	normalizedFields := make([]arrow.Field, len(fields))
	normalizedCols := make([]arrow.Array, len(cols))
	changed := false

	for i, field := range fields {
		normalizedFields[i] = field
		col, nextField, err := normalizeParquetArray(cols[i], field)
		if err != nil {
			return nil, err
		}
		normalizedCols[i] = col
		normalizedFields[i] = nextField
		if col != cols[i] || nextField.Type != field.Type {
			changed = true
		}
	}

	if !changed {
		recordBatch.Retain()
		return recordBatch, nil
	}

	meta := schema.Metadata()
	out := array.NewRecord(
		arrow.NewSchema(normalizedFields, &meta),
		normalizedCols,
		recordBatch.NumRows(),
	)
	for _, col := range normalizedCols {
		col.Release()
	}
	return out, nil
}

func normalizeParquetArray(arr arrow.Array, field arrow.Field) (arrow.Array, arrow.Field, error) {
	switch col := arr.(type) {
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
		next := builder.NewArray()
		field.Type = arrow.BinaryTypes.String
		return next, field, nil
	case *array.BinaryView:
		builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		defer builder.Release()
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				builder.AppendNull()
				continue
			}
			builder.Append(col.Value(i))
		}
		next := builder.NewArray()
		field.Type = arrow.BinaryTypes.Binary
		return next, field, nil
	default:
		arr.Retain()
		return arr, field, nil
	}
}
