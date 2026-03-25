package polars

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// parseNDJSON 解析 NDJSON 格式（每行一个 JSON 对象）
func parseNDJSON(ndjson string) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	scanner := bufio.NewScanner(strings.NewReader(ndjson))

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var row map[string]interface{}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("failed to parse line: %w", err)
		}
		result = append(result, row)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

type arrowColumnDecoder func(i int) (interface{}, error)

func parseArrowRecordBatch(recordBatch arrow.RecordBatch) ([]map[string]interface{}, error) {
	fields := recordBatch.Schema().Fields()
	nRows := int(recordBatch.NumRows())
	nCols := int(recordBatch.NumCols())
	rows := make([]map[string]interface{}, nRows)
	for i := 0; i < nRows; i++ {
		rows[i] = make(map[string]interface{}, nCols)
	}

	fieldNames := getStringSlice(nCols)
	defer putStringSlice(fieldNames)
	decoders := getArrowColumnDecoders(nCols)
	defer putArrowColumnDecoders(decoders)
	for colIdx := 0; colIdx < nCols; colIdx++ {
		fieldNames[colIdx] = fields[colIdx].Name
		decoder, err := newArrowColumnDecoder(recordBatch.Column(colIdx))
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", fieldNames[colIdx], err)
		}
		decoders[colIdx] = decoder
	}

	for rowIdx := 0; rowIdx < nRows; rowIdx++ {
		row := rows[rowIdx]
		for colIdx := 0; colIdx < nCols; colIdx++ {
			fieldName := fieldNames[colIdx]
			decode := decoders[colIdx]
			value, err := decode(rowIdx)
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", fieldName, err)
			}
			row[fieldName] = value
		}
	}

	return rows, nil
}

func newArrowColumnDecoder(col arrow.Array) (arrowColumnDecoder, error) {
	switch c := col.(type) {
	case *array.Null:
		return func(i int) (interface{}, error) {
			return nil, nil
		}, nil
	case *array.Int64:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return c.Value(i), nil
		}, nil
	case *array.Int32:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return int64(c.Value(i)), nil
		}, nil
	case *array.Int16:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return int64(c.Value(i)), nil
		}, nil
	case *array.Int8:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return int64(c.Value(i)), nil
		}, nil
	case *array.Uint64:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return c.Value(i), nil
		}, nil
	case *array.Uint32:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return uint64(c.Value(i)), nil
		}, nil
	case *array.Uint16:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return uint64(c.Value(i)), nil
		}, nil
	case *array.Uint8:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return uint64(c.Value(i)), nil
		}, nil
	case *array.Float64:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return c.Value(i), nil
		}, nil
	case *array.Float32:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return float64(c.Value(i)), nil
		}, nil
	case *array.Boolean:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return c.Value(i), nil
		}, nil
	case *array.String:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return strings.Clone(c.Value(i)), nil
		}, nil
	case *array.Binary:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return append([]byte(nil), c.Value(i)...), nil
		}, nil
	case *array.LargeBinary:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return append([]byte(nil), c.Value(i)...), nil
		}, nil
	case *array.FixedSizeBinary:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return append([]byte(nil), c.Value(i)...), nil
		}, nil
	case *array.LargeString:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return strings.Clone(c.Value(i)), nil
		}, nil
	case *array.Date32:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return c.Value(i).ToTime(), nil
		}, nil
	case *array.Date64:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return c.Value(i).ToTime(), nil
		}, nil
	case *array.Timestamp:
		tsType := c.DataType().(*arrow.TimestampType)
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return c.Value(i).ToTime(tsType.Unit), nil
		}, nil
	case *array.BinaryView:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return string(c.Value(i)), nil
		}, nil
	case *array.StringView:
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return strings.Clone(c.Value(i)), nil
		}, nil
	case *array.Time32:
		timeType := c.DataType().(*arrow.Time32Type)
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return c.Value(i).ToTime(timeType.Unit).Format("15:04:05.999999999"), nil
		}, nil
	case *array.Time64:
		timeType := c.DataType().(*arrow.Time64Type)
		return func(i int) (interface{}, error) {
			if c.IsNull(i) {
				return nil, nil
			}
			return c.Value(i).ToTime(timeType.Unit).Format("15:04:05.999999999"), nil
		}, nil
	default:
		return func(i int) (interface{}, error) {
			return arrowValueAt(col, i)
		}, nil
	}
}

func arrowValueAt(col arrow.Array, i int) (interface{}, error) {
	if col.IsNull(i) {
		return nil, nil
	}

	switch c := col.(type) {
	case *array.Null:
		return nil, nil
	case *array.Int64:
		return c.Value(i), nil
	case *array.Int32:
		return int64(c.Value(i)), nil
	case *array.Int16:
		return int64(c.Value(i)), nil
	case *array.Int8:
		return int64(c.Value(i)), nil
	case *array.Uint64:
		return c.Value(i), nil
	case *array.Uint32:
		return uint64(c.Value(i)), nil
	case *array.Uint16:
		return uint64(c.Value(i)), nil
	case *array.Uint8:
		return uint64(c.Value(i)), nil
	case *array.Float64:
		return c.Value(i), nil
	case *array.Float32:
		return float64(c.Value(i)), nil
	case *array.Boolean:
		return c.Value(i), nil
	case *array.String:
		return strings.Clone(c.Value(i)), nil
	case *array.Binary:
		return append([]byte(nil), c.Value(i)...), nil
	case *array.LargeBinary:
		return append([]byte(nil), c.Value(i)...), nil
	case *array.FixedSizeBinary:
		return append([]byte(nil), c.Value(i)...), nil
	case *array.LargeString:
		return strings.Clone(c.Value(i)), nil
	case *array.Date32:
		return c.Value(i).ToTime(), nil
	case *array.Date64:
		return c.Value(i).ToTime(), nil
	case *array.Timestamp:
		tsType := c.DataType().(*arrow.TimestampType)
		return c.Value(i).ToTime(tsType.Unit), nil
	case *array.BinaryView:
		return string(c.Value(i)), nil
	case *array.StringView:
		return strings.Clone(c.Value(i)), nil
	case *array.Time32:
		timeType := c.DataType().(*arrow.Time32Type)
		return c.Value(i).ToTime(timeType.Unit).Format("15:04:05.999999999"), nil
	case *array.Time64:
		timeType := c.DataType().(*arrow.Time64Type)
		return c.Value(i).ToTime(timeType.Unit).Format("15:04:05.999999999"), nil
	case *array.List:
		start, end := c.ValueOffsets(i)
		return arrowListValueAt(c.ListValues(), start, end)
	case *array.LargeList:
		start, end := c.ValueOffsets(i)
		return arrowListValueAt(c.ListValues(), start, end)
	case *array.Struct:
		return arrowStructValueAt(c, i)
	case *array.Dictionary:
		return arrowValueAt(c.Dictionary(), c.GetValueIndex(i))
	default:
		return nil, fmt.Errorf("unsupported Arrow type %T", col)
	}
}

func arrowListValueAt(values arrow.Array, start, end int64) ([]interface{}, error) {
	items := make([]interface{}, 0, end-start)
	for idx := start; idx < end; idx++ {
		value, err := arrowValueAt(values, int(idx))
		if err != nil {
			return nil, fmt.Errorf("list item %d: %w", idx-start, err)
		}
		items = append(items, value)
	}
	return items, nil
}

func arrowStructValueAt(col *array.Struct, i int) (map[string]interface{}, error) {
	structType, ok := col.DataType().(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("expected struct type, got %T", col.DataType())
	}

	row := make(map[string]interface{}, len(structType.Fields()))
	for fieldIdx, field := range structType.Fields() {
		value, err := arrowValueAt(col.Field(fieldIdx), i)
		if err != nil {
			return nil, fmt.Errorf("struct field %s: %w", field.Name, err)
		}
		row[field.Name] = value
	}
	return row, nil
}
