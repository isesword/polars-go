package polars

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// WithDataFrame runs fn with df and closes df when fn returns.
func WithDataFrame(df *DataFrame, fn func(*DataFrame) error) error {
	if err := invalidDataFrameError(df); err != nil {
		return err
	}
	defer df.Close()
	if fn == nil {
		return nil
	}
	return fn(df)
}

// WithCollect collects lf into an eager frame, runs fn, and frees the eager
// frame when fn returns.
func WithCollect(lf *LazyFrame, fn func(*EagerFrame) error) error {
	df, err := lf.Collect()
	if err != nil {
		return err
	}
	defer df.Free()
	if fn == nil {
		return nil
	}
	return fn(df)
}

// Releaser helps aggregate DataFrame/EagerFrame/Arrow cleanup in complex
// functions that would otherwise need multiple defer statements.
type Releaser struct {
	releasers []func()
}

func NewReleaser() *Releaser {
	return &Releaser{}
}

func (r *Releaser) Add(fn func()) {
	if r == nil || fn == nil {
		return
	}
	r.releasers = append(r.releasers, fn)
}

func (r *Releaser) DataFrame(df *DataFrame) *DataFrame {
	if r == nil || df == nil {
		return df
	}
	r.Add(func() { df.Close() })
	return df
}

func (r *Releaser) EagerFrame(df *EagerFrame) *EagerFrame {
	if r == nil || df == nil {
		return df
	}
	r.Add(func() { df.Free() })
	return df
}

func (r *Releaser) RecordBatch(batch arrow.RecordBatch) arrow.RecordBatch {
	if r == nil || batch == nil {
		return batch
	}
	r.Add(func() { batch.Release() })
	return batch
}

func (r *Releaser) Array(arr arrow.Array) arrow.Array {
	if r == nil || arr == nil {
		return arr
	}
	r.Add(func() { arr.Release() })
	return arr
}

// Release releases resources in reverse order.
func (r *Releaser) Release() {
	if r == nil {
		return
	}
	for i := len(r.releasers) - 1; i >= 0; i-- {
		r.releasers[i]()
	}
	r.releasers = nil
}
