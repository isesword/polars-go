package polars

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"time"

	"github.com/isesword/polars-go/bridge"
	pb "github.com/isesword/polars-go/proto"
	"google.golang.org/protobuf/proto"
)

func NewEagerFrameFromRowsWithOptions(
	brg *bridge.Bridge,
	rows []map[string]any,
	cfg importConfig,
) (*EagerFrame, error) {
	if err := cfg.validateForRows(); err != nil {
		return nil, err
	}

	profileEnabled := rowsImportProfileEnabled()

	encodeStarted := time.Now()
	payload, err := encodeRowsPayload(rows, cfg)
	if err != nil {
		return nil, err
	}
	encodeElapsed := time.Since(encodeStarted)

	marshalStarted := time.Now()
	payloadBytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal rows payload: %w", err)
	}
	marshalElapsed := time.Since(marshalStarted)

	ffiStarted := time.Now()
	dfHandle, err := brg.CreateDataFrameFromRows(payloadBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}
	ffiElapsed := time.Since(ffiStarted)

	if profileEnabled {
		fmt.Fprintf(
			os.Stderr,
			"[go-rows-import] rows=%d encode_rows_payload=%.3fms proto_marshal=%.3fms ffi_create_dataframe=%.3fms payload_bytes=%d\n",
			len(rows),
			float64(encodeElapsed.Microseconds())/1000.0,
			float64(marshalElapsed.Microseconds())/1000.0,
			float64(ffiElapsed.Microseconds())/1000.0,
			len(payloadBytes),
		)
	}

	return newDataFrame(dfHandle, brg), nil
}

func encodeRowsPayload(rows []map[string]any, cfg importConfig) (*pb.RowsPayload, error) {
	rowOrder, err := discoverRowColumnOrder(rows)
	if err != nil {
		return nil, err
	}
	rowIndex := make(map[string]int, len(rowOrder))
	for idx, name := range rowOrder {
		rowIndex[name] = idx
	}

	schemaFields := cfg.orderedSchemaFields(rowOrder)
	payloadRows := make([]*pb.RowRecord, 0, len(rows))
	for rowIdx, row := range rows {
		encoded, err := encodeTopLevelRecord(row, rowIndex, len(rowOrder))
		if err != nil {
			return nil, &ValidationError{
				Op:      "NewDataFrameFromMaps",
				Row:     rowIdx,
				Message: err.Error(),
				Err:     err,
			}
		}
		payloadRows = append(payloadRows, encoded)
	}

	options := &pb.RowsImportOptions{
		SchemaFields:    make([]*pb.RowsImportField, 0, len(schemaFields)),
		SchemaOverrides: cloneSchemaMap(cfg.schemaOverrides),
		Strict:          cfg.strictValue(),
	}
	if inferLen := cfg.inferSchemaLengthValue(); inferLen != nil {
		options.InferSchemaLength = inferLen
	}
	for _, field := range schemaFields {
		options.SchemaFields = append(options.SchemaFields, &pb.RowsImportField{
			Name:     field.Name,
			Dtype:    field.Type,
			HasDtype: true,
		})
	}
	if len(cfg.columnNames) > 0 {
		for i := range options.SchemaFields {
			options.SchemaFields[i].HasDtype = false
		}
	}

	return &pb.RowsPayload{
		RowOrder: append([]string(nil), rowOrder...),
		Rows:     payloadRows,
		Options:  options,
	}, nil
}

func discoverRowColumnOrder(rows []map[string]any) ([]string, error) {
	order := make([]string, 0)
	index := make(map[string]int)
	for rowIdx, row := range rows {
		if row == nil {
			return nil, &ValidationError{
				Op:      "NewDataFrameFromMaps",
				Row:     rowIdx,
				Message: "row must not be nil",
			}
		}
		for name := range row {
			if _, exists := index[name]; exists {
				continue
			}
			index[name] = len(order)
			order = append(order, name)
		}
	}
	return order, nil
}

func encodeTopLevelRecord(row map[string]any, rowIndex map[string]int, rowWidth int) (*pb.RowRecord, error) {
	values := make([]*pb.RowValue, rowWidth)
	for name, value := range row {
		normalized, err := normalizeRowValue(value)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", name, err)
		}
		encoded, err := encodeRowValue(normalized)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", name, err)
		}
		idx, ok := rowIndex[name]
		if !ok {
			return nil, fmt.Errorf("field %s: missing from row order", name)
		}
		values[idx] = encoded
	}
	for idx, value := range values {
		if value != nil {
			continue
		}
		values[idx] = nullRowValue()
	}
	return &pb.RowRecord{Values: values}, nil
}

func encodeRowValue(value any) (*pb.RowValue, error) {
	if value == nil {
		return nullRowValue(), nil
	}

	switch v := value.(type) {
	case bool:
		return &pb.RowValue{Kind: &pb.RowValue_BoolVal{BoolVal: v}}, nil
	case int:
		return &pb.RowValue{Kind: &pb.RowValue_IntVal{IntVal: int64(v)}}, nil
	case int8:
		return &pb.RowValue{Kind: &pb.RowValue_IntVal{IntVal: int64(v)}}, nil
	case int16:
		return &pb.RowValue{Kind: &pb.RowValue_IntVal{IntVal: int64(v)}}, nil
	case int32:
		return &pb.RowValue{Kind: &pb.RowValue_IntVal{IntVal: int64(v)}}, nil
	case int64:
		return &pb.RowValue{Kind: &pb.RowValue_IntVal{IntVal: v}}, nil
	case uint:
		return &pb.RowValue{Kind: &pb.RowValue_UintVal{UintVal: uint64(v)}}, nil
	case uint8:
		return &pb.RowValue{Kind: &pb.RowValue_UintVal{UintVal: uint64(v)}}, nil
	case uint16:
		return &pb.RowValue{Kind: &pb.RowValue_UintVal{UintVal: uint64(v)}}, nil
	case uint32:
		return &pb.RowValue{Kind: &pb.RowValue_UintVal{UintVal: uint64(v)}}, nil
	case uint64:
		return &pb.RowValue{Kind: &pb.RowValue_UintVal{UintVal: v}}, nil
	case float32:
		return &pb.RowValue{Kind: &pb.RowValue_FloatVal{FloatVal: float64(v)}}, nil
	case float64:
		return &pb.RowValue{Kind: &pb.RowValue_FloatVal{FloatVal: v}}, nil
	case string:
		return &pb.RowValue{Kind: &pb.RowValue_StringVal{StringVal: v}}, nil
	case []byte:
		return &pb.RowValue{Kind: &pb.RowValue_BytesVal{BytesVal: append([]byte(nil), v...)}}, nil
	case time.Time:
		return encodeTimeValue(v), nil
	case json.Number:
		return encodeJSONNumber(v)
	case []any:
		values := make([]*pb.RowValue, 0, len(v))
		for idx, item := range v {
			encoded, err := encodeRowValue(item)
			if err != nil {
				return nil, fmt.Errorf("index %d: %w", idx, err)
			}
			values = append(values, encoded)
		}
		return &pb.RowValue{Kind: &pb.RowValue_ListVal{ListVal: &pb.RowList{Values: values}}}, nil
	case map[string]any:
		return encodeStructValue(v)
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return nullRowValue(), nil
		}
		return encodeRowValue(rv.Elem().Interface())
	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return &pb.RowValue{Kind: &pb.RowValue_BytesVal{BytesVal: cloneBytes(rv)}}, nil
		}
		values := make([]*pb.RowValue, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			encoded, err := encodeRowValue(rv.Index(i).Interface())
			if err != nil {
				return nil, fmt.Errorf("index %d: %w", i, err)
			}
			values = append(values, encoded)
		}
		return &pb.RowValue{Kind: &pb.RowValue_ListVal{ListVal: &pb.RowList{Values: values}}}, nil
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("unsupported map key type %s", rv.Type().Key())
		}
		structValue := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			structValue[iter.Key().String()] = iter.Value().Interface()
		}
		return encodeStructValue(structValue)
	case reflect.Struct:
		if rv.Type() == reflect.TypeOf(time.Time{}) {
			return encodeTimeValue(value.(time.Time)), nil
		}
		normalized, err := normalizeRowValue(value)
		if err != nil {
			return nil, err
		}
		if structValue, ok := normalized.(map[string]any); ok {
			return encodeStructValue(structValue)
		}
		return nil, fmt.Errorf("unsupported struct value %T", value)
	}

	return nil, fmt.Errorf("unsupported row value type %T", value)
}

func nullRowValue() *pb.RowValue {
	return &pb.RowValue{Kind: &pb.RowValue_NullVal{NullVal: &pb.NullValue{}}}
}

func encodeStructValue(value map[string]any) (*pb.RowValue, error) {
	fields := make([]*pb.RowStructField, 0, len(value))
	for name, fieldValue := range value {
		encoded, err := encodeRowValue(fieldValue)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", name, err)
		}
		fields = append(fields, &pb.RowStructField{
			Name:  name,
			Value: encoded,
		})
	}
	return &pb.RowValue{Kind: &pb.RowValue_StructVal{StructVal: &pb.RowStruct{Fields: fields}}}, nil
}

func encodeJSONNumber(v json.Number) (*pb.RowValue, error) {
	if intVal, err := v.Int64(); err == nil {
		return &pb.RowValue{Kind: &pb.RowValue_IntVal{IntVal: intVal}}, nil
	}
	floatVal, err := v.Float64()
	if err != nil {
		return nil, fmt.Errorf("invalid json.Number %q: %w", v.String(), err)
	}
	return &pb.RowValue{Kind: &pb.RowValue_FloatVal{FloatVal: floatVal}}, nil
}

func encodeTimeValue(v time.Time) *pb.RowValue {
	if v.IsZero() {
		v = v.UTC()
	}
	return &pb.RowValue{
		Kind: &pb.RowValue_DatetimeVal{
			DatetimeVal: v.UTC().UnixMicro(),
		},
	}
}

func isEmptySliceValue(value any) bool {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return false
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return false
	}
	return rv.Len() == 0
}

func float64ToInt64(v float64) (int64, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	if math.Trunc(v) != v || v < math.MinInt64 || v > math.MaxInt64 {
		return 0, false
	}
	return int64(v), true
}

func rowsImportProfileEnabled() bool {
	switch os.Getenv("POLARS_GO_PROFILE_ROWS_IMPORT") {
	case "1", "true", "TRUE", "yes", "YES":
		return true
	default:
		return false
	}
}
