package polars

import (
	"encoding/json"
	"fmt"
	"runtime"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/isesword/polars-go/bridge"
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
	schema            map[string]DataType
	schemaOverrides   map[string]DataType
	strict            *bool
	inferSchemaLength *int
	columnNames       []string
	schemaFields      []SchemaField
	arrowSchema       *arrow.Schema
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
//
// For row-oriented constructors, this follows Python from_dicts-style partial
// schema semantics: only declared columns are loaded, extra input columns are
// dropped, and schema-only columns are null-filled.
func WithSchema(schema map[string]DataType) ImportOption {
	return func(cfg *importConfig) {
		cfg.schema = cloneSchemaMap(schema)
	}
}

// WithArrowSchema applies an explicit Arrow schema to import constructors.
//
// This opts the constructor into the explicit Arrow import path and is most
// useful for nested List/Struct schemas or when the caller already wants
// Arrow-backed import semantics.
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
		if looksLikeStructSlice(data) {
			if rows, err := structSliceToMaps(data); err == nil {
				return NewDataFrameFromMaps(rows, opts...)
			} else if isEmptySliceValue(data) {
				return NewDataFrameFromMaps(nil, opts...)
			} else {
				return nil, err
			}
		}
		if rows, err := structSliceToMaps(data); err == nil {
			return NewDataFrameFromMaps(rows, opts...)
		}
		return nil, fmt.Errorf("unsupported dataframe input type %T", data)
	}
}

// NewDataFrameFromMaps creates a managed DataFrame from row-oriented Go data.
//
// Prefer NewDataFrame when you do not need an explicit row-oriented constructor.
//
// Row-oriented maps and struct slices use the Rust-side rows importer aligned
// with Python pl.from_dicts semantics. WithArrowSchema opts into the explicit
// Arrow path instead.
//
// Resource cleanup is managed by DataFrame itself, with Close available for
// deterministic release.
func NewDataFrameFromMaps(rows []map[string]any, opts ...ImportOption) (*DataFrame, error) {
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}
	cfg := applyImportOptions(opts...)
	if err := cfg.validateForRows(); err != nil {
		return nil, unexpectedRowsOptionError("NewDataFrameFromMaps", err)
	}
	_ = brg
	if cfg.arrowSchema != nil {
		return NewDataFrameFromRowsWithArrowSchema(rows, cfg.arrowSchema)
	}
	return NewDataFrameFromRowsWithOptions(rows, cfg)
}

// NewDataFrameFromMapsWithSchema creates a managed DataFrame from row-oriented
// Go data using WithSchema-style partial schema semantics.
func NewDataFrameFromMapsWithSchema(
	rows []map[string]any,
	schema map[string]DataType,
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
	if err := cfg.validateForColumns(); err != nil {
		return nil, unexpectedRowsOptionError("NewDataFrameFromColumns", err)
	}
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
	schema map[string]DataType,
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
	schema map[string]DataType,
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

func NewDataFrameFromRowsWithOptions(
	rows []map[string]any,
	cfg importConfig,
) (*DataFrame, error) {
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}
	df, err := NewEagerFrameFromRowsWithOptions(brg, rows, cfg)
	if err != nil {
		return nil, err
	}
	return newDataFrameWrapper(df), nil
}

// NewDataFrameFromRowsWithSchema creates a managed DataFrame from row-oriented Go data
// using the Python-from_dicts compatible rows import path.
//
// Prefer NewDataFrameFromMapsWithSchema for new code.
func NewDataFrameFromRowsWithSchema(
	rows []map[string]any,
	schema map[string]DataType,
) (*DataFrame, error) {
	return NewDataFrameFromRowsWithOptions(rows, importConfig{schema: cloneSchemaMap(schema)})
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
	schema map[string]DataType,
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

// Closed reports whether the managed dataframe has been closed.
func (f *DataFrame) Closed() bool {
	return f == nil || f.df == nil
}

// ToMaps materializes the DataFrame into Go maps.
func (f *DataFrame) ToMaps() ([]map[string]interface{}, error) {
	if err := invalidDataFrameError(f); err != nil {
		return nil, wrapOp("DataFrame.ToMaps", err)
	}
	return f.df.ToMaps()
}

// ToArrow exports the DataFrame to an Arrow RecordBatch.
//
// Callers own the returned RecordBatch and must call Release when finished.
func (f *DataFrame) ToArrow() (arrow.RecordBatch, error) {
	if err := invalidDataFrameError(f); err != nil {
		return nil, wrapOp("DataFrame.ToArrow", err)
	}
	return f.df.ToArrow()
}

// Describe returns a summary dataframe.
func (f *DataFrame) Describe() (*DataFrame, error) {
	if err := invalidDataFrameError(f); err != nil {
		return nil, wrapOp("DataFrame.Describe", err)
	}
	return f.df.Describe()
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
	if err := invalidDataFrameError(f); err != nil {
		return nil, wrapOp("DataFrame.MapRows", err)
	}
	if fn == nil {
		return nil, fmt.Errorf("DataFrame.MapRows: function is nil")
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
	if err := invalidDataFrameError(f); err != nil {
		return nil, wrapOp("DataFrame.MapBatches", err)
	}
	if fn == nil {
		return nil, fmt.Errorf("DataFrame.MapBatches: function is nil")
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
	if err := invalidDataFrameError(f); err != nil {
		return wrapOp("DataFrame.Print", err)
	}
	return f.df.Print()
}

// Explain returns the logical or optimized plan description.
func (f *DataFrame) Explain(optimized ...bool) (string, error) {
	if err := invalidDataFrameError(f); err != nil {
		return "", wrapOp("DataFrame.Explain", err)
	}
	return f.df.Explain(optimized...)
}

// LogicalPlan returns the unoptimized logical plan for debugging.
func (f *DataFrame) LogicalPlan() (string, error) {
	return f.Explain(false)
}

// OptimizedPlan returns the optimized logical plan for debugging.
func (f *DataFrame) OptimizedPlan() (string, error) {
	return f.Explain(true)
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

// Drop removes columns and returns a LazyFrame.
func (f *DataFrame) Drop(columns ...string) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Drop(columns...)
}

// Rename renames columns and returns a LazyFrame.
func (f *DataFrame) Rename(mapping map[string]string, strict ...bool) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Rename(mapping, strict...)
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

// Slice slices rows and returns a LazyFrame.
func (f *DataFrame) Slice(offset int64, length uint64) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Slice(offset, length)
}

// Head returns the first n rows.
func (f *DataFrame) Head(n uint64) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Head(n)
}

// Tail returns the last n rows.
func (f *DataFrame) Tail(n uint64) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Tail(n)
}

// FFill forward-fills null values across all columns.
func (f *DataFrame) FFill(limit ...uint64) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.FFill(limit...)
}

// BFill backward-fills null values across all columns.
func (f *DataFrame) BFill(limit ...uint64) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.BFill(limit...)
}

// FillNull fills null values across all columns.
func (f *DataFrame) FillNull(value interface{}) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.FillNull(value)
}

// FillNan fills NaN values across all columns.
func (f *DataFrame) FillNan(value interface{}) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.FillNan(value)
}

// DropNulls drops rows containing nulls in the provided subset.
func (f *DataFrame) DropNulls(subset ...string) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.DropNulls(subset...)
}

// DropNans drops rows containing NaN values in the provided subset.
func (f *DataFrame) DropNans(subset ...string) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.DropNans(subset...)
}

// Explode explodes list-like columns.
func (f *DataFrame) Explode(columns ...string) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Explode(columns...)
}

// Unnest unnests struct columns into their fields.
func (f *DataFrame) Unnest(columns ...string) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Unnest(columns...)
}

// Reverse reverses row order.
func (f *DataFrame) Reverse() *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Reverse()
}

// SampleN samples n rows.
func (f *DataFrame) SampleN(n uint64, opts ...SampleOptions) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.SampleN(n, opts...)
}

// SampleFrac samples a fraction of rows.
func (f *DataFrame) SampleFrac(frac float64, opts ...SampleOptions) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.SampleFrac(frac, opts...)
}

// Unpivot reshapes wide data to long format.
func (f *DataFrame) Unpivot(opts UnpivotOptions) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Unpivot(opts)
}

// Melt is an alias for Unpivot for pandas users.
func (f *DataFrame) Melt(opts UnpivotOptions) *LazyFrame {
	if f == nil || f.df == nil {
		return nil
	}
	return f.df.Melt(opts)
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

// Pivot performs an eager pivot operation aligned with Polars' eager-only pivot semantics.
func (f *DataFrame) Pivot(opts PivotOptions) (*DataFrame, error) {
	if err := invalidDataFrameError(f); err != nil {
		return nil, wrapOp("DataFrame.Pivot", err)
	}
	df, err := f.df.Pivot(opts)
	if err != nil {
		return nil, err
	}
	return newDataFrameWrapper(df), nil
}

// PivotLazy performs a lazy pivot that requires explicit OnColumns, aligned
// with Python Polars LazyFrame.pivot semantics.
func (f *DataFrame) PivotLazy(opts LazyPivotOptions) *LazyFrame {
	if err := invalidDataFrameError(f); err != nil {
		return (&LazyFrame{}).withErr(wrapOp("DataFrame.PivotLazy", err))
	}
	return f.df.PivotLazy(opts)
}

func pivotEagerFrame(brg *bridge.Bridge, handle uint64, opts PivotOptions) (*EagerFrame, error) {
	if len(opts.On) == 0 {
		return nil, fmt.Errorf("pivot options require at least one column in On; hint: set PivotOptions.On to one or more column names")
	}
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to encode pivot options: %w", err)
	}
	dfHandle, err := brg.PivotDataFrame(handle, payload)
	if err != nil {
		return nil, fmt.Errorf(
			"pivot failed (on=%v, index=%v, values=%v, aggregate=%q): %w",
			opts.On,
			opts.Index,
			opts.Values,
			opts.Aggregate,
			err,
		)
	}
	return newDataFrame(dfHandle, brg), nil
}

// QueryMaps imports rows into a managed DataFrame, executes the provided query
// builder, materializes maps, and releases internal resources automatically.
func QueryMaps(
	rows []map[string]any,
	schema map[string]DataType,
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
