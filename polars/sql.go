package polars

import (
	"fmt"
	"sort"

	pb "github.com/isesword/polars-go/proto"
)

// SQLContext is the Go-facing SQL entrypoint aligned with Polars SQLContext.
//
// It accepts registered Go-side DataFrame/LazyFrame values and executes SQL
// through the Rust Polars SQL engine.
type SQLContext struct {
	tables map[string]any
	err    error
}

// NewSQLContext creates a new SQL execution context.
//
// Optional initial tables can be provided to get closer to the Python Polars
// SQLContext constructor style.
func NewSQLContext(initialTables ...map[string]any) *SQLContext {
	ctx := &SQLContext{
		tables: make(map[string]any),
	}
	for _, tables := range initialTables {
		ctx.RegisterMany(tables)
	}
	return ctx
}

// Register adds a table to the SQLContext.
//
// Supported table types:
// - *DataFrame
// - *EagerFrame
// - *LazyFrame
func (ctx *SQLContext) Register(name string, table any) *SQLContext {
	if err := invalidSQLContextError(ctx); err != nil {
		return ctx
	}
	if ctx.err != nil {
		return ctx
	}
	if name == "" {
		ctx.err = fmt.Errorf("table name is empty")
		return ctx
	}
	if table == nil {
		ctx.err = fmt.Errorf("table %q is nil", name)
		return ctx
	}
	ctx.tables[name] = table
	return ctx
}

// RegisterMany adds multiple tables to the SQLContext.
func (ctx *SQLContext) RegisterMany(tables map[string]any) *SQLContext {
	if err := invalidSQLContextError(ctx); err != nil {
		return ctx
	}
	if ctx.err != nil {
		return ctx
	}
	for name, table := range tables {
		ctx.Register(name, table)
		if ctx.err != nil {
			return ctx
		}
	}
	return ctx
}

// Unregister removes a table from the SQLContext.
func (ctx *SQLContext) Unregister(name string) *SQLContext {
	if err := invalidSQLContextError(ctx); err != nil {
		return ctx
	}
	delete(ctx.tables, name)
	return ctx
}

// Tables returns the currently registered table names in sorted order.
func (ctx *SQLContext) Tables() []string {
	if err := invalidSQLContextError(ctx); err != nil {
		return nil
	}
	names := make([]string, 0, len(ctx.tables))
	for name := range ctx.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (ctx *SQLContext) buildLazyQuery(query string) (*LazyFrame, error) {
	if err := invalidSQLContextError(ctx); err != nil {
		return nil, wrapOp("SQLContext.buildLazyQuery", err)
	}
	if ctx.err != nil {
		return nil, wrapOp("SQLContext.buildLazyQuery", ctx.err)
	}
	if query == "" {
		return nil, fmt.Errorf("SQLContext.buildLazyQuery: query is empty")
	}
	names := ctx.Tables()
	if len(names) == 0 {
		return nil, fmt.Errorf("SQLContext.buildLazyQuery: sql context has no registered tables")
	}
	inputs := make([]*pb.Node, 0, len(names))
	var (
		memorySources []*EagerFrame
		maxNodeID     uint32
	)
	for _, name := range names {
		lf, err := resolveSQLTableLazy(ctx.tables[name])
		if err != nil {
			return nil, fmt.Errorf("SQLContext.buildLazyQuery: sql table %q: %w", name, err)
		}
		var sourceIDs map[*EagerFrame]uint32
		memorySources, sourceIDs, err = mergeMemorySources(memorySources, lf.memorySources)
		if err != nil {
			return nil, fmt.Errorf("SQLContext.buildLazyQuery: sql table %q: %w", name, err)
		}
		root, err := remapNodeSources(lf.root, lf.memorySources, sourceIDs)
		if err != nil {
			return nil, fmt.Errorf("SQLContext.buildLazyQuery: sql table %q: %w", name, err)
		}
		inputs = append(inputs, root)
		if lf.nodeID > maxNodeID {
			maxNodeID = lf.nodeID
		}
	}

	return &LazyFrame{
		root: &pb.Node{
			Id: maxNodeID + 1,
			Kind: &pb.Node_SqlQuery{
				SqlQuery: &pb.SqlQuery{
					Query:      query,
					TableNames: names,
					Inputs:     inputs,
				},
			},
		},
		nodeID:        maxNodeID + 1,
		memorySources: memorySources,
	}, nil
}

// ExecuteLazy runs a SQL query and returns the resulting LazyFrame.
//
// This mirrors Python Polars SQLContext.execute(..., eager=False) semantics.
func (ctx *SQLContext) ExecuteLazy(query string) (*LazyFrame, error) {
	return ctx.buildLazyQuery(query)
}

// Execute runs a SQL query and returns the resulting managed DataFrame.
//
// This mirrors Python Polars SQLContext.execute(..., eager=True) semantics.
func (ctx *SQLContext) Execute(query string) (*DataFrame, error) {
	lf, err := ctx.buildLazyQuery(query)
	if err != nil {
		return nil, err
	}
	df, err := lf.Collect()
	if err != nil {
		return nil, err
	}
	return newDataFrameWrapper(df), nil
}

// SQL is a convenience helper that creates a SQLContext, registers the given
// tables, and executes the query.
func SQL(query string, tables map[string]any) (*DataFrame, error) {
	return NewSQLContext(tables).Execute(query)
}

// SQLLazy is a convenience helper that creates a SQLContext, registers the
// given tables, and returns a LazyFrame result.
func SQLLazy(query string, tables map[string]any) (*LazyFrame, error) {
	return NewSQLContext(tables).ExecuteLazy(query)
}

func resolveSQLTableLazy(table any) (*LazyFrame, error) {
	switch v := table.(type) {
	case *DataFrame:
		if v == nil || v.df == nil {
			return nil, fmt.Errorf("dataframe is nil")
		}
		return v.Lazy(), nil
	case *EagerFrame:
		if v == nil || v.handle == 0 {
			return nil, fmt.Errorf("eager dataframe is nil")
		}
		return v.Lazy(), nil
	case *LazyFrame:
		if v == nil {
			return nil, fmt.Errorf("lazyframe is nil")
		}
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported SQL table type %T", table)
	}
}
