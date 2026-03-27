package polars

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ToStructs materializes the dataframe into a slice of structs.
func ToStructs[T any](f *DataFrame) ([]T, error) {
	if err := invalidDataFrameError(f); err != nil {
		return nil, wrapOp("polars.ToStructs", err)
	}
	targetType, err := targetStructType[T]("polars.ToStructs")
	if err != nil {
		return nil, err
	}

	recordBatch, err := f.ToArrow()
	if err == nil {
		defer recordBatch.Release()
		return recordBatchToStructs[T]("polars.ToStructs", recordBatch, targetType)
	}
	if !bridgeArrowExportSupported() {
		rows, mapErr := f.ToMaps()
		if mapErr != nil {
			return nil, wrapOp("polars.ToStructs", mapErr)
		}
		return rowsToStructsWithType[T]("polars.ToStructs", rows, targetType)
	}
	return nil, wrapOp("polars.ToStructs", err)
}

// ToStructPointers materializes the dataframe into a slice of struct pointers.
func ToStructPointers[T any](f *DataFrame) ([]*T, error) {
	values, err := ToStructs[T](f)
	if err != nil {
		return nil, err
	}
	out := make([]*T, len(values))
	for i := range values {
		v := values[i]
		out[i] = &v
	}
	return out, nil
}

func rowsToStructs[T any](op string, rows []map[string]interface{}) ([]T, error) {
	targetType, err := targetStructType[T](op)
	if err != nil {
		return nil, err
	}
	return rowsToStructsWithType[T](op, rows, targetType)
}

func rowsToStructsWithType[T any](op string, rows []map[string]interface{}, targetType reflect.Type) ([]T, error) {
	meta, err := getStructTypeMeta(targetType)
	if err != nil {
		return nil, wrapStructMetaError(op, err)
	}

	out := make([]T, len(rows))
	for i, row := range rows {
		value := reflect.New(targetType).Elem()
		for _, field := range meta.fields {
			raw, ok := row[field.name]
			if !ok {
				continue
			}
			dest := value.FieldByIndex(field.index)
			if err := assignStructFieldValue(dest, raw); err != nil {
				return nil, &ValidationError{
					Op:      op,
					Row:     i,
					Field:   field.name,
					Message: err.Error(),
					Err:     err,
				}
			}
		}
		out[i] = value.Interface().(T)
	}
	return out, nil
}

func targetStructType[T any](op string) (reflect.Type, error) {
	targetType := reflect.TypeOf((*T)(nil)).Elem()
	if targetType.Kind() == reflect.Ptr {
		return nil, &ValidationError{Op: op, Message: "target type must be a struct, not a pointer"}
	}
	if targetType.Kind() != reflect.Struct {
		return nil, &ValidationError{Op: op, Message: "target type must be a struct"}
	}
	return targetType, nil
}

func recordBatchToStructs[T any](op string, recordBatch arrow.RecordBatch, targetType reflect.Type) ([]T, error) {
	meta, err := getStructTypeMeta(targetType)
	if err != nil {
		return nil, wrapStructMetaError(op, err)
	}

	nRows := int(recordBatch.NumRows())
	nCols := int(recordBatch.NumCols())
	schemaFields := recordBatch.Schema().Fields()
	columnIndex := make(map[string]int, nCols)
	for i, field := range schemaFields {
		columnIndex[field.Name] = i
	}

	bindings := make([]structColumnBinding, 0, len(meta.fields))
	for _, field := range meta.fields {
		colIdx, ok := columnIndex[field.name]
		if !ok {
			continue
		}
		binding, err := newStructColumnBinding(field, recordBatch.Column(colIdx))
		if err != nil {
			return nil, &ValidationError{
				Op:      op,
				Field:   field.name,
				Message: err.Error(),
				Err:     err,
			}
		}
		bindings = append(bindings, binding)
	}

	out := make([]T, nRows)
	for i := 0; i < nRows; i++ {
		value := reflect.New(targetType).Elem()
		for _, binding := range bindings {
			if err := binding.assign(value, i); err != nil {
				fieldName := "<unknown>"
				switch b := binding.(type) {
				case directStructColumnBinding:
					fieldName = b.field.name
				case decoderStructColumnBinding:
					fieldName = b.field.name
				}
				return nil, &ValidationError{
					Op:      op,
					Row:     i,
					Field:   fieldName,
					Message: err.Error(),
					Err:     err,
				}
			}
		}
		out[i] = value.Interface().(T)
	}
	return out, nil
}

func assignStructFieldValue(dest reflect.Value, raw any) error {
	if !dest.CanSet() {
		return fmt.Errorf("destination field cannot be set")
	}
	if raw == nil {
		dest.Set(reflect.Zero(dest.Type()))
		return nil
	}

	if dest.Kind() == reflect.Ptr {
		elem := reflect.New(dest.Type().Elem())
		if err := assignStructFieldValue(elem.Elem(), raw); err != nil {
			return err
		}
		dest.Set(elem)
		return nil
	}

	if dest.Type() == reflect.TypeOf(time.Time{}) {
		t, err := toDateTime(raw)
		if err != nil {
			return fmt.Errorf("cannot assign %s to %s: %w", describeValueType(raw), dest.Type(), err)
		}
		dest.Set(reflect.ValueOf(t))
		return nil
	}

	switch dest.Kind() {
	case reflect.String:
		if b, ok := raw.([]byte); ok {
			dest.SetString(string(b))
			return nil
		}
		dest.SetString(fmt.Sprint(raw))
		return nil
	case reflect.Bool:
		v, ok := raw.(bool)
		if !ok {
			return fmt.Errorf("cannot convert %s to %s (value=%s); hint: provide a Go bool value", describeValueType(raw), dest.Type(), describeValue(raw))
		}
		dest.SetBool(v)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if dest.Type().PkgPath() == "time" && dest.Type().Name() == "Duration" {
			d, err := toDuration(raw)
			if err != nil {
				return fmt.Errorf("cannot assign %s to %s: %w", describeValueType(raw), dest.Type(), err)
			}
			dest.SetInt(int64(d))
			return nil
		}
		v, err := toInt64(raw)
		if err != nil {
			return fmt.Errorf("cannot assign %s to %s: %w", describeValueType(raw), dest.Type(), err)
		}
		dest.SetInt(v)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err := toUint64(raw)
		if err != nil {
			return fmt.Errorf("cannot assign %s to %s: %w", describeValueType(raw), dest.Type(), err)
		}
		dest.SetUint(v)
		return nil
	case reflect.Float32, reflect.Float64:
		v, err := toFloat64(raw)
		if err != nil {
			return fmt.Errorf("cannot assign %s to %s: %w", describeValueType(raw), dest.Type(), err)
		}
		dest.SetFloat(v)
		return nil
	case reflect.Slice:
		if dest.Type().Elem().Kind() == reflect.Uint8 {
			v, err := toBytes(raw)
			if err != nil {
				return fmt.Errorf("cannot assign %s to %s: %w", describeValueType(raw), dest.Type(), err)
			}
			dest.SetBytes(v)
			return nil
		}
		return assignSliceValue(dest, raw)
	case reflect.Struct:
		rowMap, err := toStringAnyMap(raw)
		if err != nil {
			return err
		}
		meta, err := getStructTypeMeta(dest.Type())
		if err != nil {
			return err
		}
		for _, field := range meta.fields {
			child, ok := rowMap[field.name]
			if !ok {
				continue
			}
			if err := assignStructFieldValue(dest.FieldByIndex(field.index), child); err != nil {
				return fmt.Errorf("%s: %w", field.name, err)
			}
		}
		return nil
	default:
		value := reflect.ValueOf(raw)
		if value.IsValid() && value.Type().AssignableTo(dest.Type()) {
			dest.Set(value)
			return nil
		}
		if value.IsValid() && value.Type().ConvertibleTo(dest.Type()) {
			dest.Set(value.Convert(dest.Type()))
			return nil
		}
		return fmt.Errorf("unsupported assignment from %T to %s", raw, dest.Type())
	}
}

func assignSliceValue(dest reflect.Value, raw any) error {
	rv := reflect.ValueOf(raw)
	if !rv.IsValid() {
		dest.Set(reflect.Zero(dest.Type()))
		return nil
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return fmt.Errorf("cannot convert %T to %s", raw, dest.Type())
	}
	out := reflect.MakeSlice(dest.Type(), rv.Len(), rv.Len())
	for i := 0; i < rv.Len(); i++ {
		if err := assignStructFieldValue(out.Index(i), rv.Index(i).Interface()); err != nil {
			return fmt.Errorf("index %d: %w", i, err)
		}
	}
	dest.Set(out)
	return nil
}

func toDuration(value any) (time.Duration, error) {
	switch v := value.(type) {
	case time.Duration:
		return v, nil
	case string:
		return time.ParseDuration(v)
	case []byte:
		return time.ParseDuration(string(v))
	case int8:
		return time.Duration(v), nil
	case int16:
		return time.Duration(v), nil
	case int32:
		return time.Duration(v), nil
	case int64:
		return time.Duration(v), nil
	case int:
		return time.Duration(v), nil
	case uintptr:
		return time.Duration(v), nil
	case uint8:
		return time.Duration(v), nil
	case uint16:
		return time.Duration(v), nil
	case uint32:
		return time.Duration(v), nil
	case uint64:
		return time.Duration(v), nil
	case uint:
		return time.Duration(v), nil
	case float32:
		return time.Duration(v), nil
	case float64:
		return time.Duration(v), nil
	case jsonNumberLike:
		iv, err := strconv.ParseInt(v.String(), 10, 64)
		if err != nil {
			return 0, err
		}
		return time.Duration(iv), nil
	default:
		return 0, fmt.Errorf(
			"cannot convert %s to duration (value=%s); hint: provide time.Duration, an integer nanosecond value, or a duration string such as 1500ms",
			describeValueType(value),
			describeValue(value),
		)
	}
}

type jsonNumberLike interface {
	String() string
}

type structColumnBinding interface {
	assign(rowValue reflect.Value, rowIdx int) error
}

type directStructColumnBinding struct {
	field structFieldMeta
	fn    func(dest reflect.Value, rowIdx int) error
}

func (b directStructColumnBinding) assign(rowValue reflect.Value, rowIdx int) error {
	return b.fn(rowValue.FieldByIndex(b.field.index), rowIdx)
}

type decoderStructColumnBinding struct {
	field   structFieldMeta
	decoder arrowColumnDecoder
}

func (b decoderStructColumnBinding) assign(rowValue reflect.Value, rowIdx int) error {
	raw, err := b.decoder(rowIdx)
	if err != nil {
		return err
	}
	return assignStructFieldValue(rowValue.FieldByIndex(b.field.index), raw)
}

type nestedStructColumnBinding struct {
	field    structFieldMeta
	column   *array.Struct
	bindings []structColumnBinding
}

func (b nestedStructColumnBinding) assign(rowValue reflect.Value, rowIdx int) error {
	dest := rowValue.FieldByIndex(b.field.index)
	if b.column.IsNull(rowIdx) {
		dest.Set(reflect.Zero(dest.Type()))
		return nil
	}

	target := dest
	if dest.Kind() == reflect.Ptr {
		target = reflect.New(dest.Type().Elem()).Elem()
	} else {
		target.Set(reflect.Zero(target.Type()))
	}

	for _, binding := range b.bindings {
		if err := binding.assign(target, rowIdx); err != nil {
			return err
		}
	}

	if dest.Kind() == reflect.Ptr {
		ptr := reflect.New(dest.Type().Elem())
		ptr.Elem().Set(target)
		dest.Set(ptr)
	}
	return nil
}

func newStructColumnBinding(field structFieldMeta, col arrow.Array) (structColumnBinding, error) {
	if binding, ok := newDirectStructColumnBinding(field, col); ok {
		return binding, nil
	}
	decoder, err := newArrowColumnDecoder(col)
	if err != nil {
		return nil, err
	}
	return decoderStructColumnBinding{field: field, decoder: decoder}, nil
}

func newDirectStructColumnBinding(field structFieldMeta, col arrow.Array) (structColumnBinding, bool) {
	targetType := field.typ
	isPtr := targetType.Kind() == reflect.Ptr
	if isPtr {
		targetType = targetType.Elem()
	}

	if binding, ok := newNestedStructColumnBinding(field, targetType, col); ok {
		return binding, true
	}
	if binding, ok := newDirectListColumnBinding(field, targetType, col); ok {
		return binding, true
	}

	switch c := col.(type) {
	case *array.Int64:
		switch targetType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setIntValue(dest, c.Value(rowIdx))
				},
			}, true
		}
	case *array.Int32:
		switch targetType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setIntValue(dest, int64(c.Value(rowIdx)))
				},
			}, true
		}
	case *array.Int16:
		switch targetType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setIntValue(dest, int64(c.Value(rowIdx)))
				},
			}, true
		}
	case *array.Int8:
		switch targetType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setIntValue(dest, int64(c.Value(rowIdx)))
				},
			}, true
		}
	case *array.Uint64:
		switch targetType.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setUintValue(dest, c.Value(rowIdx))
				},
			}, true
		}
	case *array.Uint32:
		switch targetType.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setUintValue(dest, uint64(c.Value(rowIdx)))
				},
			}, true
		}
	case *array.Uint16:
		switch targetType.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setUintValue(dest, uint64(c.Value(rowIdx)))
				},
			}, true
		}
	case *array.Uint8:
		switch targetType.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setUintValue(dest, uint64(c.Value(rowIdx)))
				},
			}, true
		}
	case *array.Float64:
		switch targetType.Kind() {
		case reflect.Float32, reflect.Float64:
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setFloatValue(dest, c.Value(rowIdx))
				},
			}, true
		}
	case *array.Float32:
		switch targetType.Kind() {
		case reflect.Float32, reflect.Float64:
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setFloatValue(dest, float64(c.Value(rowIdx)))
				},
			}, true
		}
	case *array.Boolean:
		if targetType.Kind() == reflect.Bool {
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setBoolValue(dest, c.Value(rowIdx))
				},
			}, true
		}
	case *array.String:
		if targetType.Kind() == reflect.String {
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setStringValue(dest, c.Value(rowIdx))
				},
			}, true
		}
	case *array.LargeString:
		if targetType.Kind() == reflect.String {
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setStringValue(dest, c.Value(rowIdx))
				},
			}, true
		}
	case *array.StringView:
		if targetType.Kind() == reflect.String {
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setStringValue(dest, c.Value(rowIdx))
				},
			}, true
		}
	case *array.BinaryView:
		switch {
		case targetType.Kind() == reflect.String:
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setStringValue(dest, string(c.Value(rowIdx)))
				},
			}, true
		case targetType.Kind() == reflect.Slice && targetType.Elem().Kind() == reflect.Uint8:
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setBytesValue(dest, []byte(c.Value(rowIdx)))
				},
			}, true
		}
	case *array.Binary:
		if targetType.Kind() == reflect.Slice && targetType.Elem().Kind() == reflect.Uint8 {
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setBytesValue(dest, c.Value(rowIdx))
				},
			}, true
		}
	case *array.LargeBinary:
		if targetType.Kind() == reflect.Slice && targetType.Elem().Kind() == reflect.Uint8 {
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setBytesValue(dest, c.Value(rowIdx))
				},
			}, true
		}
	case *array.FixedSizeBinary:
		if targetType.Kind() == reflect.Slice && targetType.Elem().Kind() == reflect.Uint8 {
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setBytesValue(dest, c.Value(rowIdx))
				},
			}, true
		}
	case *array.Date32:
		if targetType == reflect.TypeOf(time.Time{}) {
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setTimeValue(dest, c.Value(rowIdx).ToTime())
				},
			}, true
		}
	case *array.Date64:
		if targetType == reflect.TypeOf(time.Time{}) {
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setTimeValue(dest, c.Value(rowIdx).ToTime())
				},
			}, true
		}
	case *array.Timestamp:
		if targetType == reflect.TypeOf(time.Time{}) {
			tsType := c.DataType().(*arrow.TimestampType)
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setTimeValue(dest, c.Value(rowIdx).ToTime(tsType.Unit))
				},
			}, true
		}
	case *array.Time64:
		if targetType.Kind() == reflect.String {
			timeType := c.DataType().(*arrow.Time64Type)
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setStringValue(dest, c.Value(rowIdx).ToTime(timeType.Unit).Format("15:04:05.999999999"))
				},
			}, true
		}
	case *array.Time32:
		if targetType.Kind() == reflect.String {
			timeType := c.DataType().(*arrow.Time32Type)
			return directStructColumnBinding{
				field: field,
				fn: func(dest reflect.Value, rowIdx int) error {
					if c.IsNull(rowIdx) {
						dest.Set(reflect.Zero(dest.Type()))
						return nil
					}
					return setStringValue(dest, c.Value(rowIdx).ToTime(timeType.Unit).Format("15:04:05.999999999"))
				},
			}, true
		}
	}

	return nil, false
}

func newNestedStructColumnBinding(field structFieldMeta, targetType reflect.Type, col arrow.Array) (structColumnBinding, bool) {
	structCol, ok := col.(*array.Struct)
	if !ok || targetType.Kind() != reflect.Struct {
		return nil, false
	}

	meta, err := getStructTypeMeta(targetType)
	if err != nil {
		return nil, false
	}
	structType, ok := structCol.DataType().(*arrow.StructType)
	if !ok {
		return nil, false
	}

	childIndex := make(map[string]int, len(structType.Fields()))
	for i, child := range structType.Fields() {
		childIndex[child.Name] = i
	}

	bindings := make([]structColumnBinding, 0, len(meta.fields))
	for _, childField := range meta.fields {
		idx, ok := childIndex[childField.name]
		if !ok {
			continue
		}
		binding, err := newStructColumnBinding(childField, structCol.Field(idx))
		if err != nil {
			return nil, false
		}
		bindings = append(bindings, binding)
	}

	return nestedStructColumnBinding{
		field:    field,
		column:   structCol,
		bindings: bindings,
	}, true
}

func newDirectListColumnBinding(field structFieldMeta, targetType reflect.Type, col arrow.Array) (structColumnBinding, bool) {
	if targetType.Kind() != reflect.Slice || targetType.Elem().Kind() == reflect.Uint8 {
		return nil, false
	}

	var (
		valueArray arrow.Array
		offsets    func(rowIdx int) (int64, int64)
	)
	switch c := col.(type) {
	case *array.List:
		valueArray = c.ListValues()
		offsets = c.ValueOffsets
	case *array.LargeList:
		valueArray = c.ListValues()
		offsets = c.ValueOffsets
	default:
		return nil, false
	}

	switch values := valueArray.(type) {
	case *array.String:
		if targetType.Elem().Kind() != reflect.String {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setStringSliceValue(dest, values, start, end)
			},
		}, true
	case *array.LargeString:
		if targetType.Elem().Kind() != reflect.String {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setLargeStringSliceValue(dest, values, start, end)
			},
		}, true
	case *array.StringView:
		if targetType.Elem().Kind() != reflect.String {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setStringViewSliceValue(dest, values, start, end)
			},
		}, true
	case *array.Int64:
		if !isIntKind(targetType.Elem().Kind()) {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setIntSliceValue(dest, values, start, end)
			},
		}, true
	case *array.Int32:
		if !isIntKind(targetType.Elem().Kind()) {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setInt32SliceValue(dest, values, start, end)
			},
		}, true
	case *array.Int16:
		if !isIntKind(targetType.Elem().Kind()) {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setInt16SliceValue(dest, values, start, end)
			},
		}, true
	case *array.Int8:
		if !isIntKind(targetType.Elem().Kind()) {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setInt8SliceValue(dest, values, start, end)
			},
		}, true
	case *array.Uint64:
		if !isUintKind(targetType.Elem().Kind()) {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setUintSliceValue(dest, values, start, end)
			},
		}, true
	case *array.Uint32:
		if !isUintKind(targetType.Elem().Kind()) {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setUint32SliceValue(dest, values, start, end)
			},
		}, true
	case *array.Uint16:
		if !isUintKind(targetType.Elem().Kind()) {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setUint16SliceValue(dest, values, start, end)
			},
		}, true
	case *array.Uint8:
		if !isUintKind(targetType.Elem().Kind()) {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setUint8SliceValue(dest, values, start, end)
			},
		}, true
	case *array.Float64:
		if !isFloatKind(targetType.Elem().Kind()) {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setFloatSliceValue(dest, values, start, end)
			},
		}, true
	case *array.Float32:
		if !isFloatKind(targetType.Elem().Kind()) {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setFloat32SliceValue(dest, values, start, end)
			},
		}, true
	case *array.Boolean:
		if targetType.Elem().Kind() != reflect.Bool {
			return nil, false
		}
		return directStructColumnBinding{
			field: field,
			fn: func(dest reflect.Value, rowIdx int) error {
				start, end := offsets(rowIdx)
				return setBoolSliceValue(dest, values, start, end)
			},
		}, true
	default:
		return nil, false
	}
}

func setIntValue(dest reflect.Value, v int64) error {
	if dest.Kind() == reflect.Ptr {
		elem := reflect.New(dest.Type().Elem())
		elem.Elem().SetInt(v)
		dest.Set(elem)
		return nil
	}
	dest.SetInt(v)
	return nil
}

func setUintValue(dest reflect.Value, v uint64) error {
	if dest.Kind() == reflect.Ptr {
		elem := reflect.New(dest.Type().Elem())
		elem.Elem().SetUint(v)
		dest.Set(elem)
		return nil
	}
	dest.SetUint(v)
	return nil
}

func setFloatValue(dest reflect.Value, v float64) error {
	if dest.Kind() == reflect.Ptr {
		elem := reflect.New(dest.Type().Elem())
		elem.Elem().SetFloat(v)
		dest.Set(elem)
		return nil
	}
	dest.SetFloat(v)
	return nil
}

func setBoolValue(dest reflect.Value, v bool) error {
	if dest.Kind() == reflect.Ptr {
		elem := reflect.New(dest.Type().Elem())
		elem.Elem().SetBool(v)
		dest.Set(elem)
		return nil
	}
	dest.SetBool(v)
	return nil
}

func setStringValue(dest reflect.Value, v string) error {
	if dest.Kind() == reflect.Ptr {
		elem := reflect.New(dest.Type().Elem())
		elem.Elem().SetString(v)
		dest.Set(elem)
		return nil
	}
	dest.SetString(v)
	return nil
}

func setBytesValue(dest reflect.Value, v []byte) error {
	cloned := append([]byte(nil), v...)
	if dest.Kind() == reflect.Ptr {
		elem := reflect.New(dest.Type().Elem())
		elem.Elem().SetBytes(cloned)
		dest.Set(elem)
		return nil
	}
	dest.SetBytes(cloned)
	return nil
}

func setTimeValue(dest reflect.Value, v time.Time) error {
	if dest.Kind() == reflect.Ptr {
		elem := reflect.New(dest.Type().Elem())
		elem.Elem().Set(reflect.ValueOf(v))
		dest.Set(elem)
		return nil
	}
	dest.Set(reflect.ValueOf(v))
	return nil
}

func isIntKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

func isUintKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

func isFloatKind(kind reflect.Kind) bool {
	return kind == reflect.Float32 || kind == reflect.Float64
}

func prepareSliceDest(dest reflect.Value, length int) (reflect.Value, bool) {
	if dest.Kind() == reflect.Ptr {
		elem := reflect.New(dest.Type().Elem())
		elem.Elem().Set(reflect.MakeSlice(dest.Type().Elem(), length, length))
		return elem, true
	}
	dest.Set(reflect.MakeSlice(dest.Type(), length, length))
	return dest, false
}

func finishSliceDest(dest reflect.Value, holder reflect.Value, isPtr bool) {
	if isPtr {
		dest.Set(holder)
	}
}

func setStringSliceValue(dest reflect.Value, values *array.String, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetString(values.Value(int(i)))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}

func setLargeStringSliceValue(dest reflect.Value, values *array.LargeString, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetString(values.Value(int(i)))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}

func setStringViewSliceValue(dest reflect.Value, values *array.StringView, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetString(values.Value(int(i)))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}

func setIntSliceValue(dest reflect.Value, values *array.Int64, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetInt(values.Value(int(i)))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}

func setInt32SliceValue(dest reflect.Value, values *array.Int32, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetInt(int64(values.Value(int(i))))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}

func setInt16SliceValue(dest reflect.Value, values *array.Int16, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetInt(int64(values.Value(int(i))))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}

func setInt8SliceValue(dest reflect.Value, values *array.Int8, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetInt(int64(values.Value(int(i))))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}

func setUintSliceValue(dest reflect.Value, values *array.Uint64, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetUint(values.Value(int(i)))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}

func setUint32SliceValue(dest reflect.Value, values *array.Uint32, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetUint(uint64(values.Value(int(i))))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}

func setUint16SliceValue(dest reflect.Value, values *array.Uint16, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetUint(uint64(values.Value(int(i))))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}

func setUint8SliceValue(dest reflect.Value, values *array.Uint8, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetUint(uint64(values.Value(int(i))))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}

func setFloatSliceValue(dest reflect.Value, values *array.Float64, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetFloat(values.Value(int(i)))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}

func setFloat32SliceValue(dest reflect.Value, values *array.Float32, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetFloat(float64(values.Value(int(i))))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}

func setBoolSliceValue(dest reflect.Value, values *array.Boolean, start, end int64) error {
	holder, isPtr := prepareSliceDest(dest, int(end-start))
	slice := holder
	if isPtr {
		slice = holder.Elem()
	}
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("list item %d is null", i-start)
		}
		slice.Index(int(i - start)).SetBool(values.Value(int(i)))
	}
	finishSliceDest(dest, holder, isPtr)
	return nil
}
