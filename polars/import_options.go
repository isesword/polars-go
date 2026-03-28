package polars

import (
	"fmt"
	"slices"
)

// SchemaField defines an ordered schema field for row-oriented constructors.
type SchemaField struct {
	Name string
	Type DataType
}

// WithSchemaOverrides overrides inferred or declared dtypes for selected columns.
func WithSchemaOverrides(schema map[string]DataType) ImportOption {
	return func(cfg *importConfig) {
		cfg.schemaOverrides = cloneSchemaMap(schema)
	}
}

// WithStrict configures whether row-oriented import should error on mismatched values.
func WithStrict(strict bool) ImportOption {
	return func(cfg *importConfig) {
		cfg.strict = &strict
	}
}

// WithInferSchemaLength limits schema inference to the first n rows.
func WithInferSchemaLength(n int) ImportOption {
	return func(cfg *importConfig) {
		cfg.inferSchemaLength = &n
	}
}

// WithInferSchemaAll configures row-oriented import to scan all rows for schema inference.
func WithInferSchemaAll() ImportOption {
	return func(cfg *importConfig) {
		n := 0
		cfg.inferSchemaLength = &n
	}
}

// WithColumnNames sets an ordered list of row-oriented columns to import.
func WithColumnNames(names []string) ImportOption {
	return func(cfg *importConfig) {
		cfg.columnNames = slices.Clone(names)
	}
}

// WithSchemaFields sets an ordered typed schema for row-oriented import.
func WithSchemaFields(fields []SchemaField) ImportOption {
	return func(cfg *importConfig) {
		cfg.schemaFields = slices.Clone(fields)
	}
}

func (cfg importConfig) strictValue() bool {
	if cfg.strict == nil {
		return true
	}
	return *cfg.strict
}

func (cfg importConfig) validateForRows() error {
	if err := cfg.validateCommon(); err != nil {
		return err
	}
	return nil
}

func (cfg importConfig) validateForColumns() error {
	if err := cfg.validateCommon(); err != nil {
		return err
	}
	if len(cfg.schemaOverrides) > 0 || cfg.strict != nil || cfg.inferSchemaLength != nil || len(cfg.columnNames) > 0 || len(cfg.schemaFields) > 0 {
		return &ValidationError{
			Op:      "NewDataFrameFromColumns",
			Message: "row-oriented import options are not supported for column-oriented constructors",
		}
	}
	return nil
}

func (cfg importConfig) validateCommon() error {
	schemaKinds := 0
	if len(cfg.schema) > 0 {
		schemaKinds++
	}
	if len(cfg.columnNames) > 0 {
		schemaKinds++
	}
	if len(cfg.schemaFields) > 0 {
		schemaKinds++
	}
	if schemaKinds > 1 {
		return &ValidationError{
			Op:      "ImportOptions",
			Message: "only one of WithSchema, WithColumnNames, or WithSchemaFields may be specified",
		}
	}
	if cfg.inferSchemaLength != nil && *cfg.inferSchemaLength < 0 {
		return &ValidationError{
			Op:      "ImportOptions",
			Message: "infer schema length must be non-negative",
		}
	}
	if err := validateColumnNames(cfg.columnNames, "WithColumnNames"); err != nil {
		return err
	}
	if err := validateSchemaFields(cfg.schemaFields); err != nil {
		return err
	}
	return nil
}

func validateColumnNames(names []string, op string) error {
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			return &ValidationError{Op: op, Message: "column name must not be empty"}
		}
		if _, exists := seen[name]; exists {
			return &ValidationError{Op: op, Field: name, Message: "duplicate column name"}
		}
		seen[name] = struct{}{}
	}
	return nil
}

func validateSchemaFields(fields []SchemaField) error {
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field.Name == "" {
			return &ValidationError{Op: "WithSchemaFields", Message: "column name must not be empty"}
		}
		if _, exists := seen[field.Name]; exists {
			return &ValidationError{Op: "WithSchemaFields", Field: field.Name, Message: "duplicate column name"}
		}
		seen[field.Name] = struct{}{}
	}
	return nil
}

func cloneSchemaMap(schema map[string]DataType) map[string]DataType {
	if len(schema) == 0 {
		return nil
	}
	cloned := make(map[string]DataType, len(schema))
	for name, dtype := range schema {
		cloned[name] = dtype
	}
	return cloned
}

func (cfg importConfig) inferSchemaLengthValue() *uint64 {
	if cfg.inferSchemaLength == nil {
		return nil
	}
	n := uint64(*cfg.inferSchemaLength)
	return &n
}

func (cfg importConfig) orderedSchemaFields(rowOrder []string) []SchemaField {
	switch {
	case len(cfg.schemaFields) > 0:
		return slices.Clone(cfg.schemaFields)
	case len(cfg.columnNames) > 0:
		fields := make([]SchemaField, 0, len(cfg.columnNames))
		for _, name := range cfg.columnNames {
			fields = append(fields, SchemaField{Name: name})
		}
		return fields
	case len(cfg.schema) > 0:
		return orderedSchemaFieldsFromMap(cfg.schema, rowOrder)
	default:
		return nil
	}
}

func orderedSchemaFieldsFromMap(schema map[string]DataType, rowOrder []string) []SchemaField {
	fields := make([]SchemaField, 0, len(schema))
	seen := make(map[string]struct{}, len(schema))
	for _, name := range rowOrder {
		dtype, ok := schema[name]
		if !ok {
			continue
		}
		fields = append(fields, SchemaField{Name: name, Type: dtype})
		seen[name] = struct{}{}
	}

	remaining := make([]string, 0, len(schema)-len(seen))
	for name := range schema {
		if _, ok := seen[name]; ok {
			continue
		}
		remaining = append(remaining, name)
	}
	slices.Sort(remaining)
	for _, name := range remaining {
		fields = append(fields, SchemaField{Name: name, Type: schema[name]})
	}
	return fields
}

func (cfg importConfig) hasRowsSchemaDefinition() bool {
	return len(cfg.schema) > 0 || len(cfg.columnNames) > 0 || len(cfg.schemaFields) > 0
}

func unexpectedRowsOptionError(constructor string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", constructor, err)
}
