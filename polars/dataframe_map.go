package polars

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/isesword/polars-go-bridge/bridge"
	pb "github.com/isesword/polars-go-bridge/proto"
)

// NewEagerFrameFromMap creates an eager DataFrame from column-oriented Go data.
//
// This is the compatibility import path. The input is normalized in Go,
// serialized to JSON, and then sent to Rust over FFI for DataFrame creation.
//
// Prefer NewDataFrameFromColumns or NewDataFrame for application-facing code.
//
// The expected shape is:
//
//	map[string]interface{}{
//	    "col1": []int64{1, 2, 3},
//	    "col2": []string{"a", "b", "c"},
//	    "col3": []interface{}{1, nil, 3},
//	}
func NewEagerFrameFromMap(brg *bridge.Bridge, data map[string]interface{}) (*EagerFrame, error) {
	return NewEagerFrameFromMapWithSchema(brg, data, nil)
}

// NewEagerFrameFromRows creates an eager DataFrame from row-oriented Go data.
//
// This is the compatibility import path. Rows are converted to
// column-oriented data in Go, serialized to JSON, and then sent to Rust over
// FFI.
//
// This API is convenient for database query results and decoded JSON objects.
// Prefer NewDataFrameFromMaps or NewDataFrame for application-facing code.
func NewEagerFrameFromRows(brg *bridge.Bridge, rows []map[string]any) (*EagerFrame, error) {
	return NewEagerFrameFromRowsWithSchema(brg, rows, nil)
}

// NewEagerFrameFromRowsAuto creates an eager DataFrame from row-oriented Go data and
// automatically prefers the Arrow import path when possible.
//
// It prefers the Arrow import path when cgo is enabled, an explicit schema is
// provided, and every schema data type has an Arrow import implementation.
// Otherwise it falls back to the compatibility JSON import path.
//
// Prefer NewDataFrameFromRowsAuto for managed application-facing usage.
func NewEagerFrameFromRowsAuto(
	brg *bridge.Bridge,
	rows []map[string]any,
	schema map[string]pb.DataType,
) (*EagerFrame, error) {
	if shouldUseArrowRowsPath(schema) {
		recordBatch, err := NewArrowRecordBatchFromRowsWithSchema(rows, schema)
		if err == nil {
			defer recordBatch.Release()
			return NewEagerFrameFromArrowRecordBatch(brg, recordBatch)
		}
	}

	return NewEagerFrameFromRowsWithSchema(brg, rows, schema)
}

// NewEagerFrameFromRowsWithArrowSchema creates an eager DataFrame from
// row-oriented Go data using an explicit Arrow schema.
func NewEagerFrameFromRowsWithArrowSchema(
	brg *bridge.Bridge,
	rows []map[string]any,
	schema *arrow.Schema,
) (*EagerFrame, error) {
	recordBatch, err := NewArrowRecordBatchFromRowsWithArrowSchema(rows, schema)
	if err != nil {
		return nil, err
	}
	defer recordBatch.Release()
	return NewEagerFrameFromArrowRecordBatch(brg, recordBatch)
}

// NewEagerFrameFromMapWithSchema creates an eager DataFrame from column-oriented Go
// data and applies an explicit schema.
//
// This is the compatibility import path. Values are normalized in Go
// according to schema, serialized to JSON, and then sent to Rust over FFI.
//
// Columns missing from schema continue to use Rust-side inference.
// Prefer NewDataFrameFromColumnsWithSchema or NewDataFrame for managed usage.
func NewEagerFrameFromMapWithSchema(
	brg *bridge.Bridge,
	data map[string]interface{},
	schema map[string]pb.DataType,
) (*EagerFrame, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data is empty")
	}

	columns, err := mapToColumnJSON(data)
	if err != nil {
		return nil, fmt.Errorf("failed to convert data: %w", err)
	}

	return newDataFrameFromColumns(brg, columns, schema)
}

// NewEagerFrameFromMapWithArrowSchema creates an eager DataFrame from
// column-oriented Go data using an explicit Arrow schema.
func NewEagerFrameFromMapWithArrowSchema(
	brg *bridge.Bridge,
	data map[string]interface{},
	schema *arrow.Schema,
) (*EagerFrame, error) {
	rows, err := columnMapToRows(data)
	if err != nil {
		return nil, fmt.Errorf("failed to convert columns to rows: %w", err)
	}
	return NewEagerFrameFromRowsWithArrowSchema(brg, rows, schema)
}

// NewEagerFrameFromRowsWithSchema creates an eager DataFrame from row-oriented Go data
// and applies an explicit schema.
//
// This is the compatibility import path. Rows are reshaped into columns in
// Go, missing fields are padded with nil, and the payload is sent to Rust as
// JSON over FFI.
//
// Prefer NewDataFrameFromMapsWithSchema or NewDataFrame for managed usage.
func NewEagerFrameFromRowsWithSchema(
	brg *bridge.Bridge,
	rows []map[string]any,
	schema map[string]pb.DataType,
) (*EagerFrame, error) {
	columns, err := rowsToColumnJSON(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rows: %w", err)
	}

	return newDataFrameFromColumns(brg, columns, schema)
}

// columnData 表示单列数据
type columnData struct {
	Name   string        `json:"name"`
	Values []interface{} `json:"values"`
}

type dataframeColumnsPayload struct {
	Columns []columnData      `json:"columns"`
	Schema  map[string]string `json:"schema,omitempty"`
}

func newDataFrameFromColumns(
	brg *bridge.Bridge,
	columns []columnData,
	schema map[string]pb.DataType,
) (*EagerFrame, error) {
	if len(schema) > 0 {
		var err error
		columns, err = applySchemaToColumns(columns, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to apply schema: %w", err)
		}
	}

	payload := dataframeColumnsPayload{
		Columns: columns,
		Schema:  schemaToJSON(schema),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	dfHandle, err := brg.CreateDataFrameFromColumns(jsonData)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	return newDataFrame(dfHandle, brg), nil
}

func schemaToJSON(schema map[string]pb.DataType) map[string]string {
	if len(schema) == 0 {
		return nil
	}

	result := make(map[string]string, len(schema))
	for name, dataType := range schema {
		result[name] = dataType.String()
	}
	return result
}

// mapToColumnJSON 将 map 转换为列数据格式
func mapToColumnJSON(data map[string]interface{}) ([]columnData, error) {
	columns := make([]columnData, 0, len(data))

	for colName, colValues := range data {
		// 获取列数据
		values, err := convertColumnValues(colValues)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", colName, err)
		}

		columns = append(columns, columnData{
			Name:   colName,
			Values: values,
		})
	}

	return columns, nil
}

// convertColumnValues 转换列数据为 []interface{}
func convertColumnValues(colValues interface{}) ([]interface{}, error) {
	v := reflect.ValueOf(colValues)

	// 如果已经是 []interface{}，直接返回
	if slice, ok := colValues.([]interface{}); ok {
		return slice, nil
	}

	// 处理其他切片类型
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("column data must be a slice, got %T", colValues)
	}

	// 转换为 []interface{}
	length := v.Len()
	result := make([]interface{}, length)

	for i := 0; i < length; i++ {
		val := v.Index(i)

		// 处理指针类型（nil 值）
		if val.Kind() == reflect.Ptr {
			if val.IsNil() {
				result[i] = nil
			} else {
				result[i] = val.Elem().Interface()
			}
		} else {
			result[i] = val.Interface()
		}
	}

	return result, nil
}

func columnMapToRows(data map[string]interface{}) ([]map[string]any, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data is empty")
	}

	columns := make(map[string][]interface{}, len(data))
	expectedLen := -1
	for name, colValues := range data {
		values, err := convertColumnValues(colValues)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", name, err)
		}
		if expectedLen == -1 {
			expectedLen = len(values)
		} else if len(values) != expectedLen {
			return nil, fmt.Errorf("column %s has length %d, expected %d", name, len(values), expectedLen)
		}
		columns[name] = values
	}

	rows := make([]map[string]any, expectedLen)
	for rowIdx := 0; rowIdx < expectedLen; rowIdx++ {
		row := make(map[string]any, len(columns))
		for name, values := range columns {
			row[name] = values[rowIdx]
		}
		rows[rowIdx] = row
	}
	return rows, nil
}

func rowsToColumnJSON(rows []map[string]any) ([]columnData, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("rows is empty")
	}

	columnOrder := make([]string, 0)
	columnIndex := make(map[string]int)

	for _, row := range rows {
		for name := range row {
			if _, exists := columnIndex[name]; exists {
				continue
			}
			columnIndex[name] = len(columnOrder)
			columnOrder = append(columnOrder, name)
		}
	}

	if len(columnOrder) == 0 {
		return nil, fmt.Errorf("rows contain no columns")
	}

	columns := make([]columnData, len(columnOrder))
	for i, name := range columnOrder {
		columns[i] = columnData{
			Name:   name,
			Values: make([]interface{}, 0, len(rows)),
		}
	}

	for _, row := range rows {
		for i, name := range columnOrder {
			value, exists := row[name]
			if !exists {
				columns[i].Values = append(columns[i].Values, nil)
				continue
			}

			normalized, err := normalizeRowValue(value)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", name, err)
			}
			columns[i].Values = append(columns[i].Values, normalized)
		}
	}

	return columns, nil
}

func normalizeRowValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, nil
		}
		return normalizeRowValue(v.Elem().Interface())
	}

	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return cloneBytes(v), nil
		}
		items := getAnySlice(v.Len())
		for i := 0; i < v.Len(); i++ {
			item, err := normalizeRowValue(v.Index(i).Interface())
			if err != nil {
				putAnySlice(items)
				return nil, fmt.Errorf("index %d: %w", i, err)
			}
			items[i] = item
		}
		return items, nil
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return value, nil
		}
		out := make(map[string]any, v.Len())
		iter := v.MapRange()
		for iter.Next() {
			normalized, err := normalizeRowValue(iter.Value().Interface())
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", iter.Key().String(), err)
			}
			out[iter.Key().String()] = normalized
		}
		return out, nil
	case reflect.Struct:
		if v.Type() == reflect.TypeOf(time.Time{}) {
			return value, nil
		}
		out := make(map[string]any, v.NumField())
		rt := v.Type()
		for i := 0; i < v.NumField(); i++ {
			field := rt.Field(i)
			if !field.IsExported() {
				continue
			}
			normalized, err := normalizeRowValue(v.Field(i).Interface())
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", field.Name, err)
			}
			out[field.Name] = normalized
		}
		return out, nil
	}

	return value, nil
}

func applySchemaToColumns(columns []columnData, schema map[string]pb.DataType) ([]columnData, error) {
	if len(schema) == 0 {
		return columns, nil
	}

	normalized := make([]columnData, len(columns))
	for i, col := range columns {
		normalized[i] = columnData{
			Name:   col.Name,
			Values: make([]interface{}, len(col.Values)),
		}

		dataType, hasSchema := schema[col.Name]
		if !hasSchema {
			copy(normalized[i].Values, col.Values)
			continue
		}

		for j, value := range col.Values {
			converted, err := normalizeValueForSchema(value, dataType)
			if err != nil {
				return nil, fmt.Errorf("column %s row %d: %w", col.Name, j, err)
			}
			normalized[i].Values[j] = converted
		}
	}

	return normalized, nil
}

func normalizeValueForSchema(value any, dataType pb.DataType) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch dataType {
	case pb.DataType_INT64, pb.DataType_INT32, pb.DataType_INT16, pb.DataType_INT8:
		v, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		return v, nil
	case pb.DataType_UINT64, pb.DataType_UINT32, pb.DataType_UINT16, pb.DataType_UINT8:
		v, err := toUint64(value)
		if err != nil {
			return nil, err
		}
		return v, nil
	case pb.DataType_FLOAT64, pb.DataType_FLOAT32:
		v, err := toFloat64(value)
		if err != nil {
			return nil, err
		}
		return v, nil
	case pb.DataType_BOOL:
		if b, ok := value.(bool); ok {
			return b, nil
		}
		return nil, fmt.Errorf("expected bool, got %T", value)
	case pb.DataType_UTF8:
		if s, ok := value.(string); ok {
			return s, nil
		}
		return fmt.Sprintf("%v", value), nil
	case pb.DataType_DATE, pb.DataType_DATETIME, pb.DataType_TIME:
		return normalizeTemporalValueForJSON(value, dataType)
	default:
		return value, nil
	}
}

func shouldUseArrowRowsPath(schema map[string]pb.DataType) bool {
	if !arrowRowsPathSupported() || len(schema) == 0 {
		return false
	}

	for _, dataType := range schema {
		if !isArrowRowsDataTypeSupported(dataType) {
			return false
		}
	}

	return true
}

func isArrowRowsDataTypeSupported(dataType pb.DataType) bool {
	switch dataType {
	case pb.DataType_INT64,
		pb.DataType_INT32,
		pb.DataType_INT16,
		pb.DataType_INT8,
		pb.DataType_UINT64,
		pb.DataType_UINT32,
		pb.DataType_UINT16,
		pb.DataType_UINT8,
		pb.DataType_FLOAT64,
		pb.DataType_FLOAT32,
		pb.DataType_BOOL,
		pb.DataType_UTF8,
		pb.DataType_DATE,
		pb.DataType_DATETIME,
		pb.DataType_TIME:
		return true
	default:
		return false
	}
}

func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case uint:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case json.Number:
		return v.Int64()
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

func toUint64(value any) (uint64, error) {
	switch v := value.(type) {
	case int:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int16:
		return uint64(v), nil
	case int8:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint64:
		return v, nil
	case uint32:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	case float32:
		return uint64(v), nil
	case json.Number:
		parsed, err := strconv.ParseUint(v.String(), 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", value)
	}
}

func toFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case json.Number:
		return v.Float64()
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

func cloneBytes(v reflect.Value) []byte {
	buf := make([]byte, v.Len())
	for i := 0; i < v.Len(); i++ {
		buf[i] = byte(v.Index(i).Uint())
	}
	return buf
}
