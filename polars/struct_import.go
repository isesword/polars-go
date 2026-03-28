package polars

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

type structFieldMeta struct {
	name  string
	index []int
	typ   reflect.Type
}

type structTypeMeta struct {
	fields       []structFieldMeta
	fieldsByName map[string]structFieldMeta
}

var structTypeCache sync.Map

// NewDataFrameFromStructs creates a managed dataframe from a slice of structs or struct pointers.
func NewDataFrameFromStructs[T any](rows []T, opts ...ImportOption) (*DataFrame, error) {
	if len(rows) == 0 {
		return NewDataFrameFromMaps(nil, opts...)
	}
	maps, err := structRowsToMaps(rows)
	if err != nil {
		return nil, err
	}
	return NewDataFrameFromMaps(maps, opts...)
}

func structRowsToMaps[T any](rows []T) ([]map[string]any, error) {
	if len(rows) == 0 {
		return nil, &ValidationError{Op: "NewDataFrameFromStructs", Message: "rows is empty"}
	}
	out := make([]map[string]any, len(rows))
	var meta *structTypeMeta
	for i, row := range rows {
		rowValue := reflect.ValueOf(row)
		resolved, err := derefStructValue(rowValue)
		if err != nil {
			return nil, &ValidationError{Op: "NewDataFrameFromStructs", Row: i, Message: err.Error(), Err: err}
		}
		if meta == nil {
			meta, err = getStructTypeMeta(resolved.Type())
			if err != nil {
				return nil, wrapStructMetaError("NewDataFrameFromStructs", err)
			}
		}
		mapped, err := structValueToMap(resolved, meta)
		if err != nil {
			return nil, &ValidationError{Op: "NewDataFrameFromStructs", Row: i, Message: err.Error(), Err: err}
		}
		out[i] = mapped
	}
	return out, nil
}

func structSliceToMaps(rows any) ([]map[string]any, error) {
	rv := reflect.ValueOf(rows)
	if !rv.IsValid() {
		return nil, &ValidationError{Op: "NewDataFrame", Message: "rows is nil"}
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, &ValidationError{Op: "NewDataFrame", Message: "rows must be a slice or array"}
	}
	if rv.Len() == 0 {
		return nil, &ValidationError{Op: "NewDataFrame", Message: "rows is empty"}
	}
	out := make([]map[string]any, rv.Len())
	var meta *structTypeMeta
	for i := 0; i < rv.Len(); i++ {
		resolved, err := derefStructValue(rv.Index(i))
		if err != nil {
			return nil, &ValidationError{Op: "NewDataFrame", Row: i, Message: err.Error(), Err: err}
		}
		if meta == nil {
			meta, err = getStructTypeMeta(resolved.Type())
			if err != nil {
				return nil, wrapStructMetaError("NewDataFrame", err)
			}
		}
		mapped, err := structValueToMap(resolved, meta)
		if err != nil {
			return nil, &ValidationError{Op: "NewDataFrame", Row: i, Message: err.Error(), Err: err}
		}
		out[i] = mapped
	}
	return out, nil
}

func looksLikeStructSlice(rows any) bool {
	rv := reflect.ValueOf(rows)
	if !rv.IsValid() {
		return false
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return false
	}
	elemType := rv.Type().Elem()
	for elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}
	return elemType.Kind() == reflect.Struct
}

func getStructTypeMeta(typ reflect.Type) (*structTypeMeta, error) {
	if cached, ok := structTypeCache.Load(typ); ok {
		return cached.(*structTypeMeta), nil
	}

	meta := &structTypeMeta{}
	seen := make(map[string]struct{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		name, skip := parseStructFieldName(field)
		if skip {
			continue
		}
		if _, exists := seen[name]; exists {
			return nil, &ValidationError{
				Op:      "NewDataFrameFromStructs",
				Field:   name,
				Message: "duplicate column name from struct fields",
			}
		}
		seen[name] = struct{}{}
		meta.fields = append(meta.fields, structFieldMeta{
			name:  name,
			index: field.Index,
			typ:   field.Type,
		})
	}
	if len(meta.fields) == 0 {
		return nil, &ValidationError{Op: "NewDataFrameFromStructs", Message: "struct type has no exported dataframe fields"}
	}
	meta.fieldsByName = make(map[string]structFieldMeta, len(meta.fields))
	for _, field := range meta.fields {
		meta.fieldsByName[field.name] = field
	}
	structTypeCache.Store(typ, meta)
	return meta, nil
}

func structValueToMap(v reflect.Value, meta *structTypeMeta) (map[string]any, error) {
	out := make(map[string]any, len(meta.fields))
	for _, field := range meta.fields {
		value := v.FieldByIndex(field.index)
		normalized, err := normalizeStructFieldValue(value)
		if err != nil {
			return nil, &ValidationError{Field: field.name, Message: err.Error(), Err: err}
		}
		out[field.name] = normalized
	}
	return out, nil
}

func normalizeStructFieldValue(v reflect.Value) (any, error) {
	if !v.IsValid() {
		return nil, nil
	}
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, nil
		}
		v = v.Elem()
	}
	if v.Type() == reflect.TypeOf(time.Time{}) {
		return v.Interface(), nil
	}
	normalized, err := normalizeRowValue(v.Interface())
	if err != nil {
		return nil, err
	}
	return normalized, nil
}

func derefStructValue(v reflect.Value) (reflect.Value, error) {
	if !v.IsValid() {
		return reflect.Value{}, fmt.Errorf("row is nil")
	}
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return reflect.Value{}, fmt.Errorf("row pointer is nil")
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("row must be a struct or struct pointer, got %s", v.Kind())
	}
	return v, nil
}

func parseStructFieldName(field reflect.StructField) (string, bool) {
	tag := field.Tag.Get("polars")
	if tag == "-" {
		return "", true
	}
	if tag == "" {
		return field.Name, false
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "" {
		return field.Name, false
	}
	return name, false
}

func wrapStructMetaError(op string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*ValidationError); ok {
		return wrapOp(op, err)
	}
	return wrapOp(op, &ValidationError{Op: op, Message: err.Error(), Err: err})
}
