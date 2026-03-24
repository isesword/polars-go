package polars

import (
	"fmt"
	"runtime"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/isesword/polars-go-bridge/bridge"
	pb "github.com/isesword/polars-go-bridge/proto"
)

// EagerFrame represents a low-level eager Polars DataFrame held by the Rust
// engine.
//
// Most callers should prefer the high-level NewDataFrame(...) /
// NewDataFrameFromMaps(...) / NewDataFrameFromArrow(...) constructors, which
// return the managed DataFrame wrapper around this eager type.
//
// EagerFrame is intended for advanced control paths that need direct access to
// the Rust-side handle lifecycle. In normal application code, prefer the
// managed DataFrame wrapper and use Close instead of Free.
type EagerFrame struct {
	handle uint64
	brg    *bridge.Bridge
}

func newDataFrame(handle uint64, brg *bridge.Bridge) *EagerFrame {
	df := &EagerFrame{handle: handle, brg: brg}
	runtime.SetFinalizer(df, func(d *EagerFrame) {
		if d != nil && d.handle != 0 && d.brg != nil {
			d.brg.FreeDataFrame(d.handle)
		}
	})
	return df
}

// Free releases the Rust-side DataFrame handle.
// It's safe to call multiple times. After calling Free, the DataFrame becomes invalid.
func (df *EagerFrame) Free() {
	if df == nil || df.brg == nil {
		return
	}
	// Store handle locally and clear it atomically to prevent double-free
	handle := df.handle
	if handle == 0 {
		return
	}
	df.handle = 0
	// Clear finalizer before freeing to prevent race
	runtime.SetFinalizer(df, nil)
	df.brg.FreeDataFrame(handle)
}

// ToMaps exports the DataFrame through Arrow C Data Interface and parses it
// into Go maps.
func (df *EagerFrame) ToMaps() ([]map[string]interface{}, error) {
	if df == nil || df.handle == 0 || df.brg == nil {
		return nil, fmt.Errorf("dataframe is nil")
	}
	if !bridgeArrowExportSupported() {
		return nil, fmt.Errorf("ToMaps requires cgo (set CGO_ENABLED=1)")
	}

	recordBatch, err := exportDataFrameToArrowRecordBatch(df.brg, df.handle)
	if err != nil {
		return nil, fmt.Errorf("failed to export dataframe: %w", err)
	}
	defer recordBatch.Release()

	rows, err := parseArrowRecordBatch(recordBatch)
	if err != nil {
		return nil, fmt.Errorf("failed to parse arrow record batch: %w", err)
	}
	return rows, nil
}

// ToArrow exports the DataFrame through Arrow C Data Interface and returns
// an Arrow RecordBatch.
//
// Callers own the returned RecordBatch and must call Release when finished.
func (df *EagerFrame) ToArrow() (arrow.RecordBatch, error) {
	if df == nil || df.handle == 0 || df.brg == nil {
		return nil, fmt.Errorf("dataframe is nil")
	}
	if !bridgeArrowExportSupported() {
		return nil, fmt.Errorf("ToArrow requires cgo (set CGO_ENABLED=1)")
	}

	recordBatch, err := exportDataFrameToArrowRecordBatch(df.brg, df.handle)
	if err != nil {
		return nil, fmt.Errorf("failed to export dataframe: %w", err)
	}
	return recordBatch, nil
}

// Print outputs the DataFrame using Polars' Display implementation.
func (df *EagerFrame) Print() error {
	if df == nil || df.handle == 0 || df.brg == nil {
		return fmt.Errorf("dataframe is nil")
	}
	return df.brg.DataFramePrint(df.handle)
}

// Lazy converts the DataFrame into a LazyFrame for further operations.
func (df *EagerFrame) Lazy() *LazyFrame {
	if df == nil || df.handle == 0 {
		return nil
	}
	return &LazyFrame{
		root: &pb.Node{
			Id: 1,
			Kind: &pb.Node_MemoryScan{
				MemoryScan: &pb.MemoryScan{
					ColumnNames: []string{},
					SourceId:    0,
				},
			},
		},
		nodeID:        1,
		memorySources: []*EagerFrame{df},
	}
}

// Filter applies a filter operation and returns a LazyFrame for further chaining.
func (df *EagerFrame) Filter(predicate Expr) *LazyFrame {
	return df.Lazy().Filter(predicate)
}

// Select selects columns and returns a LazyFrame for further chaining.
func (df *EagerFrame) Select(exprs ...Expr) *LazyFrame {
	return df.Lazy().Select(exprs...)
}

// WithColumns adds or modifies columns and returns a LazyFrame for further chaining.
func (df *EagerFrame) WithColumns(exprs ...Expr) *LazyFrame {
	return df.Lazy().WithColumns(exprs...)
}

// Limit limits the number of rows and returns a LazyFrame for further chaining.
func (df *EagerFrame) Limit(n uint64) *LazyFrame {
	return df.Lazy().Limit(n)
}

// GroupBy groups rows by one or more column names.
func (df *EagerFrame) GroupBy(columns ...string) *GroupByFrame {
	return df.Lazy().GroupBy(columns...)
}

// GroupByExprs groups rows by one or more expressions.
func (df *EagerFrame) GroupByExprs(exprs ...Expr) *GroupByFrame {
	return df.Lazy().GroupByExprs(exprs...)
}

// Join joins the eager frame with another eager frame using Python-Polars-style options.
func (df *EagerFrame) Join(other *EagerFrame, opts JoinOptions) *LazyFrame {
	if other == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("join dataframe is nil"))
	}
	return df.Lazy().Join(other.Lazy(), opts)
}

// Sort sorts the eager frame using Python-Polars-style options.
func (df *EagerFrame) Sort(opts SortOptions) *LazyFrame {
	return df.Lazy().Sort(opts)
}

// Unique removes duplicate rows using Python-Polars-style options.
func (df *EagerFrame) Unique(opts UniqueOptions) *LazyFrame {
	return df.Lazy().Unique(opts)
}

// Concat concatenates the eager frame with additional lazy inputs.
func (df *EagerFrame) Concat(others ...*LazyFrame) *LazyFrame {
	return df.Lazy().Concat(others...)
}
