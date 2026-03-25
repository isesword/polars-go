package polars

import (
	"errors"
	"fmt"
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
