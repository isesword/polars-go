package polars

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNilDataFrame    = errors.New("dataframe is nil")
	ErrClosedDataFrame = errors.New("dataframe is closed")
	ErrNilLazyFrame    = errors.New("lazyframe is nil")
	ErrNilSQLContext   = errors.New("sql context is nil")
	ErrInvalidInput    = errors.New("invalid input")
)

type ValidationError struct {
	Op      string
	Row     int
	Field   string
	Message string
	Err     error
}

func (e *ValidationError) Error() string {
	if e == nil {
		return ErrInvalidInput.Error()
	}
	msg := e.Message
	if msg == "" && e.Err != nil {
		msg = e.Err.Error()
	}
	prefix := e.Op
	if prefix == "" {
		prefix = "validation"
	}
	switch {
	case e.Row >= 0 && e.Field != "":
		return fmt.Sprintf("%s: row %d field %s: %s", prefix, e.Row, e.Field, msg)
	case e.Row >= 0:
		return fmt.Sprintf("%s: row %d: %s", prefix, e.Row, msg)
	case e.Field != "":
		return fmt.Sprintf("%s: field %s: %s", prefix, e.Field, msg)
	default:
		return fmt.Sprintf("%s: %s", prefix, msg)
	}
}

func (e *ValidationError) Unwrap() error {
	if e == nil {
		return ErrInvalidInput
	}
	if e.Err == nil {
		return ErrInvalidInput
	}
	return errors.Join(ErrInvalidInput, e.Err)
}

func invalidDataFrameError(df *DataFrame) error {
	if df == nil {
		return ErrNilDataFrame
	}
	if df.df == nil {
		return ErrClosedDataFrame
	}
	return nil
}

func invalidEagerFrameError(df *EagerFrame) error {
	if df == nil {
		return ErrNilDataFrame
	}
	if df.handle == 0 || df.brg == nil {
		return ErrClosedDataFrame
	}
	return nil
}

func invalidLazyFrameError(lf *LazyFrame) error {
	if lf == nil {
		return ErrNilLazyFrame
	}
	return nil
}

func invalidSQLContextError(ctx *SQLContext) error {
	if ctx == nil {
		return ErrNilSQLContext
	}
	return nil
}

func wrapOp(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", op, err)
}

func describeValueType(value any) string {
	if value == nil {
		return "nil"
	}
	return fmt.Sprintf("%T", value)
}

func describeValue(value any) string {
	if value == nil {
		return "nil"
	}

	const maxLen = 48
	switch v := value.(type) {
	case string:
		if len(v) > maxLen {
			return fmt.Sprintf("%q...", v[:maxLen])
		}
		return fmt.Sprintf("%q", v)
	case []byte:
		s := string(v)
		if len(s) > maxLen {
			return fmt.Sprintf("%q...", s[:maxLen])
		}
		return fmt.Sprintf("%q", s)
	default:
		s := fmt.Sprint(value)
		if len(s) > maxLen {
			return fmt.Sprintf("%s...", s[:maxLen])
		}
		return s
	}
}

func schemaValueHint(dataType DataType) string {
	switch dataType {
	case DataTypeInt64, DataTypeInt32, DataTypeInt16, DataTypeInt8:
		return "provide an integer-like Go value such as int/int64, json.Number, or a base-10 numeric string"
	case DataTypeUInt64, DataTypeUInt32, DataTypeUInt16, DataTypeUInt8:
		return "provide a non-negative integer-like Go value such as uint64, json.Number, or a base-10 numeric string"
	case DataTypeFloat64, DataTypeFloat32:
		return "provide a numeric Go value such as float64/int64, json.Number, or a decimal string"
	case DataTypeBool:
		return "provide a Go bool value"
	case DataTypeDate:
		return "provide time.Time or a date/datetime string such as RFC3339 or 2006-01-02"
	case DataTypeDatetime:
		return "provide time.Time or a datetime string such as RFC3339"
	case DataTypeTime:
		return "provide time.Time or a time string such as 15:04:05"
	default:
		return ""
	}
}

func enrichSchemaValueError(dataType DataType, value any, err error) error {
	msg := fmt.Sprintf(
		"schema expects %s but got %s (value=%s)",
		dataType.String(),
		describeValueType(value),
		describeValue(value),
	)
	if hint := schemaValueHint(dataType); hint != "" {
		msg = fmt.Sprintf("%s; hint: %s", msg, hint)
	}
	if err == nil {
		return errors.New(msg)
	}
	return fmt.Errorf("%s: %w", msg, err)
}

func temporalValueHint(kind string, layouts []string) string {
	return fmt.Sprintf(
		"provide time.Time or a string matching one of: %s",
		strings.Join(layouts, ", "),
	)
}

func summarizeQuery(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return `""`
	}
	query = strings.Join(strings.Fields(query), " ")
	const maxLen = 96
	if len(query) > maxLen {
		query = query[:maxLen] + "..."
	}
	return fmt.Sprintf("%q", query)
}
