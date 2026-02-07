package polars

import (
	"fmt"
	"runtime"

	"github.com/isesword/polars-go-bridge/bridge"
	pb "github.com/isesword/polars-go-bridge/proto"
)

// DataFrame represents an eager Polars DataFrame held by the Rust engine.
type DataFrame struct {
	handle uint64
	brg    *bridge.Bridge
}

func newDataFrame(handle uint64, brg *bridge.Bridge) *DataFrame {
	df := &DataFrame{handle: handle, brg: brg}
	runtime.SetFinalizer(df, func(d *DataFrame) {
		if d != nil && d.handle != 0 && d.brg != nil {
			d.brg.FreeDataFrame(d.handle)
		}
	})
	return df
}

// Free releases the Rust-side DataFrame handle.
// It's safe to call multiple times. After calling Free, the DataFrame becomes invalid.
func (df *DataFrame) Free() {
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

// Rows exports the DataFrame to Arrow IPC and parses it into rows.
func (df *DataFrame) Rows() ([]map[string]interface{}, error) {
	if df == nil || df.handle == 0 || df.brg == nil {
		return nil, fmt.Errorf("dataframe is nil")
	}
	ipcBytes, err := df.brg.DataFrameToIPC(df.handle)
	if err != nil {
		return nil, fmt.Errorf("failed to export dataframe: %w", err)
	}
	return parseArrowIPC(ipcBytes)
}

// Print outputs the DataFrame using Polars' Display implementation.
func (df *DataFrame) Print() error {
	if df == nil || df.handle == 0 || df.brg == nil {
		return fmt.Errorf("dataframe is nil")
	}
	return df.brg.DataFramePrint(df.handle)
}

// Lazy converts the DataFrame into a LazyFrame for further operations.
func (df *DataFrame) Lazy() *LazyFrame {
	if df == nil || df.handle == 0 {
		return nil
	}
	return &LazyFrame{
		root: &pb.Node{
			Id: 1,
			Kind: &pb.Node_MemoryScan{
				MemoryScan: &pb.MemoryScan{
					ColumnNames: []string{},
				},
			},
		},
		nodeID:  1,
		inputDF: df,
	}
}

// Filter applies a filter operation and returns a LazyFrame for further chaining.
func (df *DataFrame) Filter(predicate Expr) *LazyFrame {
	return df.Lazy().Filter(predicate)
}

// Select selects columns and returns a LazyFrame for further chaining.
func (df *DataFrame) Select(exprs ...Expr) *LazyFrame {
	return df.Lazy().Select(exprs...)
}

// WithColumns adds or modifies columns and returns a LazyFrame for further chaining.
func (df *DataFrame) WithColumns(exprs ...Expr) *LazyFrame {
	return df.Lazy().WithColumns(exprs...)
}

// Limit limits the number of rows and returns a LazyFrame for further chaining.
func (df *DataFrame) Limit(n uint64) *LazyFrame {
	return df.Lazy().Limit(n)
}
