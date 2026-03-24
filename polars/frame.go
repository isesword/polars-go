package polars

import (
	"fmt"
	"runtime"

	"github.com/apache/arrow-go/v18/arrow"
	pb "github.com/isesword/polars-go-bridge/proto"
)

// DataFrame is a higher-level wrapper around EagerFrame.
//
// It keeps the low-level EagerFrame lifetime management inside the library's
// object lifecycle so most callers do not need to interact with Free
// directly. Close remains available for deterministic cleanup in long-lived
// processes.
type DataFrame struct {
	df *EagerFrame
}

// ImportOption configures high-level import constructors such as
// NewDataFrameFromMaps and NewDataFrameFromColumns.
type ImportOption func(*importConfig)

type importConfig struct {
	schema      map[string]pb.DataType
	arrowSchema *arrow.Schema
}

// MapRowsOptions configures DataFrame.MapRows, the first UDG (User-defined Go
// function) entrypoint aligned with Python Polars map_rows.
type MapRowsOptions struct {
	OutputSchema *arrow.Schema
}

// MapBatchesOptions configures DataFrame.MapBatches, the batch-oriented UDG
// entrypoint aligned with Python Polars map_batches naming.
type MapBatchesOptions struct{}

func newDataFrameWrapper(df *EagerFrame) *DataFrame {
	dataFrame := &DataFrame{df: df}
	runtime.SetFinalizer(dataFrame, func(df *DataFrame) {
		if df != nil {
			df.Close()
		}
	})
	return dataFrame
}

// WithSchema applies an explicit schema to high-level import constructors.
func WithSchema(schema map[string]pb.DataType) ImportOption {
	return func(cfg *importConfig) {
		cfg.schema = schema
	}
}

// WithArrowSchema applies an explicit Arrow schema to row-oriented import constructors.
func WithArrowSchema(schema *arrow.Schema) ImportOption {
	return func(cfg *importConfig) {
		cfg.arrowSchema = schema
	}
}

// NewDataFrame creates a managed DataFrame from supported Go in-memory data.
//
// This is the recommended high-level constructor for most callers.
//
// It accepts:
// - `[]map[string]any` for row-oriented maps
// - `map[string]interface{}` for column-oriented data
//
// For Arrow RecordBatch inputs, use NewDataFrameFromArrowBatch.
func NewDataFrame(data any, opts ...ImportOption) (*DataFrame, error) {
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}

	switch v := data.(type) {
	case []map[string]any:
		_ = brg
		return NewDataFrameFromMaps(v, opts...)
	case map[string]interface{}:
		return NewDataFrameFromColumns(v, opts...)
	default:
		return nil, fmt.Errorf("unsupported dataframe input type %T", data)
	}
}

// NewDataFrameFromMaps creates a managed DataFrame from row-oriented Go data.
//
// Prefer NewDataFrame when you do not need an explicit row-oriented constructor.
//
// Schema inference/building is delegated to the Rust engine unless an
// explicit schema is supplied. Resource cleanup is managed by DataFrame
// itself, with Close available for deterministic release.
func NewDataFrameFromMaps(rows []map[string]any, opts ...ImportOption) (*DataFrame, error) {
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}
	cfg := applyImportOptions(opts...)
	_ = brg
	if cfg.arrowSchema != nil {
		return NewDataFrameFromRowsWithArrowSchema(rows, cfg.arrowSchema)
	}
	return NewDataFrameFromRowsWithSchema(rows, cfg.schema)
}

// NewDataFrameFromMapsWithSchema creates a managed DataFrame from row-oriented
// Go data using an explicit schema.
func NewDataFrameFromMapsWithSchema(
	rows []map[string]any,
	schema map[string]pb.DataType,
) (*DataFrame, error) {
	return NewDataFrameFromMaps(rows, WithSchema(schema))
}

// NewDataFrameFromColumns creates a managed DataFrame from column-oriented Go
// data.
//
// Prefer NewDataFrame when you do not need an explicit column-oriented constructor.
//
// Like NewDataFrameFromMaps, this lets Rust infer/build the DataFrame unless an
// explicit schema is supplied.
func NewDataFrameFromColumns(data map[string]interface{}, opts ...ImportOption) (*DataFrame, error) {
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}
	cfg := applyImportOptions(opts...)
	_ = brg
	if cfg.arrowSchema != nil {
		return NewDataFrameFromColumnsWithArrowSchema(data, cfg.arrowSchema)
	}
	return NewDataFrameFromMapWithSchema(data, cfg.schema)
}

// NewDataFrameFromColumnsWithSchema creates a managed DataFrame from
// column-oriented Go data using an explicit schema.
func NewDataFrameFromColumnsWithSchema(
	data map[string]interface{},
	schema map[string]pb.DataType,
) (*DataFrame, error) {
	return NewDataFrameFromColumns(data, WithSchema(schema))
}

// NewDataFrameFromColumnsWithArrowSchema creates a managed DataFrame from
// column-oriented Go data using an explicit Arrow schema.
func NewDataFrameFromColumnsWithArrowSchema(
	data map[string]interface{},
	schema *arrow.Schema,
) (*DataFrame, error) {
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}
	df, err := NewEagerFrameFromMapWithArrowSchema(brg, data, schema)
	if err != nil {
		return nil, err
	}
	return newDataFrameWrapper(df), nil
}

// NewDataFrameFromArrow creates a managed DataFrame from an Arrow RecordBatch
// and takes ownership of the input batch.
//
// Use this constructor when you already have Arrow data and want the explicit
// Arrow import path.
func NewDataFrameFromArrow(recordBatch arrow.RecordBatch) (*DataFrame, error) {
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}
	_ = brg
	return NewDataFrameFromArrowRecordBatch(recordBatch)
}

// NewDataFrameFromRowsAuto creates a managed DataFrame from row-oriented Go
// data.
//
// It uses the same automatic path selection as NewEagerFrameFromRowsAuto, but
// returns a higher-level DataFrame wrapper so callers usually do not need to call
// EagerFrame.Free directly.
//
// Prefer NewDataFrameFromMaps or NewDataFrameFromMapsWithSchema for new code.
func NewDataFrameFromRowsAuto(
	rows []map[string]any,
	schema map[string]pb.DataType,
) (*DataFrame, error) {
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}
	df, err := NewEagerFrameFromRowsAuto(brg, rows, schema)
	if err != nil {
		return nil, err
	}
	return newDataFrameWrapper(df), nil
}

// NewDataFrameFromRowsWithSchema creates a managed DataFrame from row-oriented Go data
// using the compatibility JSON import path.
//
// Prefer NewDataFrameFromMapsWithSchema for new code.
func NewDataFrameFromRowsWithSchema(
	rows []map[string]any,
	schema map[string]pb.DataType,
) (*DataFrame, error) {
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}
	df, err := NewEagerFrameFromRowsWithSchema(brg, rows, schema)
	if err != nil {
		return nil, err
	}
	return newDataFrameWrapper(df), nil
}

// NewDataFrameFromRowsWithArrowSchema creates a managed DataFrame from
// row-oriented Go data using an explicit Arrow schema.
func NewDataFrameFromRowsWithArrowSchema(
	rows []map[string]any,
	schema *arrow.Schema,
) (*DataFrame, error) {
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}
	df, err := NewEagerFrameFromRowsWithArrowSchema(brg, rows, schema)
	if err != nil {
		return nil, err
	}
	return newDataFrameWrapper(df), nil
}

// NewDataFrameFromMapWithSchema creates a managed DataFrame from column-oriented Go
// data using the compatibility JSON import path.
//
// Prefer NewDataFrameFromColumnsWithSchema for new code.
func NewDataFrameFromMapWithSchema(
	data map[string]interface{},
	schema map[string]pb.DataType,
) (*DataFrame, error) {
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}
	df, err := NewEagerFrameFromMapWithSchema(brg, data, schema)
	if err != nil {
		return nil, err
	}
	return newDataFrameWrapper(df), nil
}

// NewDataFrameFromArrowRecordBatch creates a managed DataFrame from an Arrow
// RecordBatch and takes ownership of the input batch.
//
// The input Arrow RecordBatch is released automatically after import, so callers do
// not need to call recordBatch.Release after a successful call.
//
// Prefer NewDataFrameFromArrow for new code.
func NewDataFrameFromArrowRecordBatch(recordBatch arrow.RecordBatch) (*DataFrame, error) {
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}
	if recordBatch == nil {
		return nil, fmt.Errorf("record batch is nil")
	}

	df, err := NewEagerFrameFromArrowRecordBatch(brg, recordBatch)
	recordBatch.Release()
	if err != nil {
		return nil, err
	}
	return newDataFrameWrapper(df), nil
}

// Close releases the underlying DataFrame. It is safe to call multiple times.
func (f *DataFrame) Close() {
	if f == nil || f.df == nil {
		return
	}
	runtime.SetFinalizer(f, nil)
	f.df.Free()
	f.df = nil
}

// ToMaps materializes the DataFrame into Go maps.
func (f *DataFrame) ToMaps() ([]map[string]interface{}, error) {
	if f == nil || f.df == nil {
		return nil, fmt.Errorf("dataframe is nil")
	}
	return f.df.ToMaps()
}

// ToArrow exports the DataFrame to an Arrow RecordBatch.
//
// Callers own the returned RecordBatch and must call Release when finished.
func (f *DataFrame) ToArrow() (arrow.RecordBatch, error) {
	if f == nil || f.df == nil {
		return nil, fmt.Errorf("dataframe is nil")
	}
	return f.df.ToArrow()
}

// MapRows applies a User-defined Go function (UDG) to each row and returns a
// new managed DataFrame.
//
// This method is convenient and migration-friendly, but it materializes the
// DataFrame in Go and is slower than native Polars expressions. Prefer native
// expressions whenever possible.
func (f *DataFrame) MapRows(
	fn func(row map[string]any) (map[string]any, error),
	opts MapRowsOptions,
) (*DataFrame, error) {
	if f == nil || f.df == nil {
		return nil, fmt.Errorf("dataframe is nil")
	}
	if fn == nil {
		return nil, fmt.Errorf("MapRows function is nil")
	}

	rows, err := f.df.ToMaps()
	if err != nil {
		return nil, err
	}

	out := make([]map[string]any, 0, len(rows))
	for i, row := range rows {
		mapped, err := fn(row)
		if err != nil {
			return nil, fmt.Errorf("MapRows row %d: %w", i, err)
		}
		if mapped == nil {
			return nil, fmt.Errorf("MapRows row %d returned nil", i)
		}
		out = append(out, mapped)
	}

	if opts.OutputSchema != nil {
		return NewDataFrame(out, WithArrowSchema(opts.OutputSchema))
	}
	return NewDataFrame(out)
}

// MapBatches applies a batch-oriented User-defined Go function (UDG) to an
// Arrow RecordBatch exported from the DataFrame and returns a new managed
// DataFrame.
//
// This method aligns with Python Polars map_batches naming, but currently runs
// eagerly on a materialized DataFrame. Prefer native Polars expressions when
// possible.
func (f *DataFrame) MapBatches(
	fn func(batch arrow.RecordBatch) (arrow.RecordBatch, error),
	_ MapBatchesOptions,
) (*DataFrame, error) {
	if f == nil || f.df == nil {
		return nil, fmt.Errorf("dataframe is nil")
	}
	if fn == nil {
		return nil, fmt.Errorf("MapBatches function is nil")
	}

	inputBatch, err := f.df.ToArrow()
	if err != nil {
		return nil, err
	}

	outputBatch, err := fn(inputBatch)
	if err != nil {
		inputBatch.Release()
		return nil, fmt.Errorf("MapBatches failed: %w", err)
	}
	if outputBatch == nil {
		inputBatch.Release()
		return nil, fmt.Errorf("MapBatches returned nil")
	}
	if outputBatch == inputBatch {
		return NewDataFrameFromArrow(outputBatch)
	}

	inputBatch.Release()
	return NewDataFrameFromArrow(outputBatch)
}

// Print outputs the DataFrame using Polars' Display implementation.
func (f *DataFrame) Print() error {
	if f == nil || f.df == nil {
		return fmt.Errorf("dataframe is nil")
	}
	return f.df.Print()
}

// Lazy converts the DataFrame into a LazyFrame for further operations.
func (f *DataFrame) Lazy() *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Lazy()
}

// Filter applies a filter operation and returns a LazyFrame.
func (f *DataFrame) Filter(predicate Expr) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Filter(predicate)
}

// Select selects columns and returns a LazyFrame.
func (f *DataFrame) Select(exprs ...Expr) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Select(exprs...)
}

// WithColumns adds or modifies columns and returns a LazyFrame.
func (f *DataFrame) WithColumns(exprs ...Expr) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.WithColumns(exprs...)
}

// Limit limits the number of rows and returns a LazyFrame.
func (f *DataFrame) Limit(n uint64) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Limit(n)
}

// GroupBy groups rows by one or more column names.
func (f *DataFrame) GroupBy(columns ...string) *GroupByFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.GroupBy(columns...)
}

// GroupByExprs groups rows by one or more expressions.
func (f *DataFrame) GroupByExprs(exprs ...Expr) *GroupByFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.GroupByExprs(exprs...)
}

// Join joins the dataframe with another dataframe using Python-Polars-style options.
func (f *DataFrame) Join(other *DataFrame, opts JoinOptions) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	if other == nil || other.df == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("join dataframe is nil"))
	}
	return f.df.Join(other.df, opts)
}

// Sort sorts the dataframe using Python-Polars-style options.
func (f *DataFrame) Sort(opts SortOptions) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Sort(opts)
}

// Unique removes duplicate rows using Python-Polars-style options.
func (f *DataFrame) Unique(opts UniqueOptions) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Unique(opts)
}

// Concat concatenates the dataframe with additional lazy inputs.
func (f *DataFrame) Concat(others ...*LazyFrame) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Concat(others...)
}

// QueryMaps imports rows into a managed DataFrame, executes the provided query
// builder, materializes maps, and releases internal resources automatically.
func QueryMaps(
	rows []map[string]any,
	schema map[string]pb.DataType,
	build func(*DataFrame) *LazyFrame,
) ([]map[string]interface{}, error) {
	frame, err := NewDataFrameFromRowsWithSchema(rows, schema)
	if err != nil {
		return nil, err
	}
	defer frame.Close()

	var lf *LazyFrame
	if build != nil {
		lf = build(frame)
	} else {
		lf = frame.Lazy()
	}
	if lf == nil {
		return nil, fmt.Errorf("query builder returned nil lazyframe")
	}
	df, err := lf.Collect()
	if err != nil {
		runtime.KeepAlive(frame)
		return nil, err
	}
	defer df.Free()

	result, err := df.ToMaps()
	runtime.KeepAlive(frame)
	return result, err
}

func applyImportOptions(opts ...ImportOption) importConfig {
	cfg := importConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}
