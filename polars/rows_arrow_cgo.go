//go:build cgo
// +build cgo

package polars

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
	pb "github.com/isesword/polars-go-bridge/proto"
)

var recordBatchAllocator = mallocator.NewMallocator()

// NewArrowRecordBatchFromRowsWithSchema converts row-oriented Go data into an
// Arrow RecordBatch using an explicit primitive schema.
func NewArrowRecordBatchFromRowsWithSchema(
	rows []map[string]any,
	schema map[string]pb.DataType,
) (arrow.RecordBatch, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("rows is empty")
	}
	if len(schema) == 0 {
		return nil, fmt.Errorf("schema is empty")
	}

	columns, err := rowsToColumnJSON(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rows: %w", err)
	}

	fields := make([]arrow.Field, 0, len(columns))
	for _, col := range columns {
		dataType, ok := schema[col.Name]
		if !ok {
			return nil, fmt.Errorf("missing schema for column %s", col.Name)
		}
		arrowType, err := arrowDataTypeFromPrimitive(dataType)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		fields = append(fields, arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: true,
		})
	}

	return NewArrowRecordBatchFromRowsWithArrowSchema(rows, arrow.NewSchema(fields, nil))
}

// NewArrowRecordBatchFromRowsWithArrowSchema converts row-oriented Go data into an
// Arrow RecordBatch using an explicit Arrow schema.
func NewArrowRecordBatchFromRowsWithArrowSchema(
	rows []map[string]any,
	schema *arrow.Schema,
) (arrow.RecordBatch, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("rows is empty")
	}
	if schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}

	builder := array.NewRecordBuilder(recordBatchAllocator, schema)
	defer builder.Release()

	fields := schema.Fields()
	for rowIdx, row := range rows {
		for fieldIdx, field := range fields {
			value, exists := row[field.Name]
			if !exists {
				value = nil
			}
			if err := appendValueToArrowBuilder(builder.Field(fieldIdx), value, field.Type, field.Name); err != nil {
				return nil, fmt.Errorf("row %d, field %s: %w", rowIdx, field.Name, err)
			}
		}
	}

	return builder.NewRecordBatch(), nil
}

func arrowDataTypeFromPrimitive(dataType pb.DataType) (arrow.DataType, error) {
	switch dataType {
	case pb.DataType_INT64:
		return arrow.PrimitiveTypes.Int64, nil
	case pb.DataType_INT32:
		return arrow.PrimitiveTypes.Int32, nil
	case pb.DataType_INT16:
		return arrow.PrimitiveTypes.Int16, nil
	case pb.DataType_INT8:
		return arrow.PrimitiveTypes.Int8, nil
	case pb.DataType_UINT64:
		return arrow.PrimitiveTypes.Uint64, nil
	case pb.DataType_UINT32:
		return arrow.PrimitiveTypes.Uint32, nil
	case pb.DataType_UINT16:
		return arrow.PrimitiveTypes.Uint16, nil
	case pb.DataType_UINT8:
		return arrow.PrimitiveTypes.Uint8, nil
	case pb.DataType_FLOAT64:
		return arrow.PrimitiveTypes.Float64, nil
	case pb.DataType_FLOAT32:
		return arrow.PrimitiveTypes.Float32, nil
	case pb.DataType_BOOL:
		return arrow.FixedWidthTypes.Boolean, nil
	case pb.DataType_UTF8:
		return arrow.BinaryTypes.String, nil
	case pb.DataType_DATE:
		return arrow.FixedWidthTypes.Date32, nil
	case pb.DataType_DATETIME:
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case pb.DataType_TIME:
		return &arrow.Time64Type{Unit: arrow.Microsecond}, nil
	default:
		return nil, fmt.Errorf("unsupported Arrow schema data type: %s", dataType.String())
	}
}

func appendValueToArrowBuilder(builder array.Builder, value any, dataType arrow.DataType, path string) error {
	if value == nil {
		return appendNullToArrowBuilder(builder, dataType, path)
	}

	switch b := builder.(type) {
	case *array.Int64Builder:
		v, err := toInt64(value)
		if err != nil {
			return fmt.Errorf("%s: expected int64-compatible value, got %T: %w", path, value, err)
		}
		b.Append(v)
	case *array.Int32Builder:
		v, err := toInt64(value)
		if err != nil {
			return fmt.Errorf("%s: expected int32-compatible value, got %T: %w", path, value, err)
		}
		b.Append(int32(v))
	case *array.Int16Builder:
		v, err := toInt64(value)
		if err != nil {
			return fmt.Errorf("%s: expected int16-compatible value, got %T: %w", path, value, err)
		}
		b.Append(int16(v))
	case *array.Int8Builder:
		v, err := toInt64(value)
		if err != nil {
			return fmt.Errorf("%s: expected int8-compatible value, got %T: %w", path, value, err)
		}
		b.Append(int8(v))
	case *array.Uint64Builder:
		v, err := toUint64(value)
		if err != nil {
			return fmt.Errorf("%s: expected uint64-compatible value, got %T: %w", path, value, err)
		}
		b.Append(v)
	case *array.Uint32Builder:
		v, err := toUint64(value)
		if err != nil {
			return fmt.Errorf("%s: expected uint32-compatible value, got %T: %w", path, value, err)
		}
		b.Append(uint32(v))
	case *array.Uint16Builder:
		v, err := toUint64(value)
		if err != nil {
			return fmt.Errorf("%s: expected uint16-compatible value, got %T: %w", path, value, err)
		}
		b.Append(uint16(v))
	case *array.Uint8Builder:
		v, err := toUint64(value)
		if err != nil {
			return fmt.Errorf("%s: expected uint8-compatible value, got %T: %w", path, value, err)
		}
		b.Append(uint8(v))
	case *array.Float64Builder:
		v, err := toFloat64(value)
		if err != nil {
			return fmt.Errorf("%s: expected float64-compatible value, got %T: %w", path, value, err)
		}
		b.Append(v)
	case *array.Float32Builder:
		v, err := toFloat64(value)
		if err != nil {
			return fmt.Errorf("%s: expected float32-compatible value, got %T: %w", path, value, err)
		}
		b.Append(float32(v))
	case *array.BooleanBuilder:
		v, ok := value.(bool)
		if !ok {
			return fmt.Errorf("%s: expected bool, got %T", path, value)
		}
		b.Append(v)
	case *array.StringBuilder:
		b.Append(fmt.Sprint(value))
	case *array.BinaryBuilder:
		v, err := toBytes(value)
		if err != nil {
			return fmt.Errorf("%s: expected binary-compatible value, got %T: %w", path, value, err)
		}
		b.Append(v)
	case *array.FixedSizeBinaryBuilder:
		v, err := toBytes(value)
		if err != nil {
			return fmt.Errorf("%s: expected fixed-size-binary-compatible value, got %T: %w", path, value, err)
		}
		if len(v) != b.Type().(*arrow.FixedSizeBinaryType).ByteWidth {
			return fmt.Errorf("%s: expected %d bytes, got %d", path, b.Type().(*arrow.FixedSizeBinaryType).ByteWidth, len(v))
		}
		b.Append(v)
	case *array.Date32Builder:
		v, err := toArrowDate32(value)
		if err != nil {
			return fmt.Errorf("%s: expected date-compatible value, got %T: %w", path, value, err)
		}
		b.Append(v)
	case *array.TimestampBuilder:
		v, err := toArrowTimestamp(value)
		if err != nil {
			return fmt.Errorf("%s: expected datetime-compatible value, got %T: %w", path, value, err)
		}
		b.AppendTime(v)
	case *array.Time64Builder:
		v, err := toArrowTime64Micro(value)
		if err != nil {
			return fmt.Errorf("%s: expected time-compatible value, got %T: %w", path, value, err)
		}
		b.Append(v)
	case *array.ListBuilder:
		values, pooled, err := toInterfaceSlice(value)
		if err != nil {
			return fmt.Errorf("%s: expected list-compatible value, got %T: %w", path, value, err)
		}
		if pooled {
			defer putAnySlice(values)
		}
		listType, ok := dataType.(*arrow.ListType)
		if !ok {
			return fmt.Errorf("expected *arrow.ListType, got %T", dataType)
		}
		b.Append(true)
		for idx, item := range values {
			if err := appendValueToArrowBuilder(b.ValueBuilder(), item, listType.Elem(), fmt.Sprintf("%s[%d]", path, idx)); err != nil {
				return err
			}
		}
	case *array.LargeListBuilder:
		values, pooled, err := toInterfaceSlice(value)
		if err != nil {
			return fmt.Errorf("%s: expected large-list-compatible value, got %T: %w", path, value, err)
		}
		if pooled {
			defer putAnySlice(values)
		}
		listType, ok := dataType.(*arrow.LargeListType)
		if !ok {
			return fmt.Errorf("expected *arrow.LargeListType, got %T", dataType)
		}
		b.Append(true)
		for idx, item := range values {
			if err := appendValueToArrowBuilder(b.ValueBuilder(), item, listType.Elem(), fmt.Sprintf("%s[%d]", path, idx)); err != nil {
				return err
			}
		}
	case *array.StructBuilder:
		structType, ok := dataType.(*arrow.StructType)
		if !ok {
			return fmt.Errorf("expected *arrow.StructType, got %T", dataType)
		}
		mapValue, err := toStringAnyMap(value)
		if err != nil {
			return fmt.Errorf("%s: expected struct-compatible value, got %T: %w", path, value, err)
		}
		b.Append(true)
		for idx, field := range structType.Fields() {
			if err := appendValueToArrowBuilder(b.FieldBuilder(idx), mapValue[field.Name], field.Type, path+"."+field.Name); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported builder type %T", builder)
	}

	return nil
}

func appendNullToArrowBuilder(builder array.Builder, dataType arrow.DataType, path string) error {
	switch b := builder.(type) {
	case *array.Int64Builder, *array.Int32Builder, *array.Int16Builder, *array.Int8Builder,
		*array.Uint64Builder, *array.Uint32Builder, *array.Uint16Builder, *array.Uint8Builder,
		*array.Float64Builder, *array.Float32Builder, *array.BooleanBuilder,
		*array.StringBuilder, *array.BinaryBuilder, *array.FixedSizeBinaryBuilder,
		*array.Date32Builder, *array.TimestampBuilder, *array.Time64Builder:
		builder.AppendNull()
		return nil
	case *array.ListBuilder:
		b.AppendNull()
		return nil
	case *array.LargeListBuilder:
		b.AppendNull()
		return nil
	case *array.StructBuilder:
		structType, ok := dataType.(*arrow.StructType)
		if !ok {
			return fmt.Errorf("expected *arrow.StructType for null struct at %s, got %T", path, dataType)
		}
		b.Append(false)
		for idx, field := range structType.Fields() {
			if err := appendNullToArrowBuilder(b.FieldBuilder(idx), field.Type, path+"."+field.Name); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported builder type %T", builder)
	}
}

func toInterfaceSlice(value any) ([]any, bool, error) {
	if items, ok := value.([]any); ok {
		return items, false, nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false, fmt.Errorf("value is not a slice")
	}

	items := getAnySlice(rv.Len())
	for i := 0; i < rv.Len(); i++ {
		items[i] = rv.Index(i).Interface()
	}
	return items, true, nil
}

func toStringAnyMap(value any) (map[string]any, error) {
	if typed, ok := value.(map[string]any); ok {
		return typed, nil
	}
	if typed, ok := value.(map[string]interface{}); ok {
		return typed, nil
	}
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil, fmt.Errorf("value is nil")
	}
	if rv.Kind() == reflect.Struct {
		out := make(map[string]any, rv.NumField())
		rt := rv.Type()
		for i := 0; i < rv.NumField(); i++ {
			field := rt.Field(i)
			if !field.IsExported() {
				continue
			}
			out[field.Name] = rv.Field(i).Interface()
		}
		return out, nil
	}
	if rv.Kind() == reflect.Map && rv.Type().Key().Kind() == reflect.String {
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			out[iter.Key().String()] = iter.Value().Interface()
		}
		return out, nil
	}
	return nil, fmt.Errorf("value is not a map[string]any or struct")
}

func toBytes(value any) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return append([]byte(nil), v...), nil
	case string:
		return []byte(v), nil
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil, fmt.Errorf("value is nil")
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("value is not a byte slice")
	}
	if rv.Type().Elem().Kind() != reflect.Uint8 {
		return nil, fmt.Errorf("value is not a byte slice")
	}
	buf := make([]byte, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		buf[i] = byte(rv.Index(i).Uint())
	}
	return buf, nil
}
