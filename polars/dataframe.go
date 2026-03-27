package polars

import (
	"fmt"
	"io"
	"os"
	"reflect"

	pb "github.com/isesword/polars-go/proto"
	"google.golang.org/protobuf/proto"
)

// LazyFrame 惰性数据框架（延迟执行）
type LazyFrame struct {
	root          *pb.Node
	nodeID        uint32
	memorySources []*EagerFrame
	err           error
}

type GroupByFrame struct {
	input *LazyFrame
	keys  []Expr
	err   error
}

type JoinType = pb.JoinType

const (
	JoinInner JoinType = pb.JoinType_JOIN_INNER
	JoinLeft  JoinType = pb.JoinType_JOIN_LEFT
	JoinRight JoinType = pb.JoinType_JOIN_RIGHT
	JoinFull  JoinType = pb.JoinType_JOIN_FULL
	JoinSemi  JoinType = pb.JoinType_JOIN_SEMI
	JoinAnti  JoinType = pb.JoinType_JOIN_ANTI
	JoinCross JoinType = pb.JoinType_JOIN_CROSS
)

type UniqueKeepStrategy = pb.UniqueKeepStrategy

const (
	UniqueKeepFirst UniqueKeepStrategy = pb.UniqueKeepStrategy_UNIQUE_KEEP_FIRST
	UniqueKeepLast  UniqueKeepStrategy = pb.UniqueKeepStrategy_UNIQUE_KEEP_LAST
	UniqueKeepAny   UniqueKeepStrategy = pb.UniqueKeepStrategy_UNIQUE_KEEP_ANY
	UniqueKeepNone  UniqueKeepStrategy = pb.UniqueKeepStrategy_UNIQUE_KEEP_NONE
)

type JoinOptions struct {
	On      []string
	LeftOn  []Expr
	RightOn []Expr
	How     JoinType
	Suffix  string
}

type SortOptions struct {
	By         []Expr
	Descending []bool
	NullsLast  []bool
}

type UniqueOptions struct {
	Subset        []string
	Keep          string
	MaintainOrder bool
}

type ConcatOptions struct {
	Rechunk       bool
	ToSupertypes  bool
	Parallel      bool
	Diagonal      bool
	MaintainOrder bool
}

type PivotOptions struct {
	On            []string `json:"on"`
	Index         []string `json:"index"`
	Values        []string `json:"values"`
	Aggregate     string   `json:"aggregate"`
	SortColumns   bool     `json:"sort_columns"`
	MaintainOrder bool     `json:"maintain_order"`
	Separator     string   `json:"separator"`
}

type LazyPivotOptions struct {
	On            []string
	OnColumns     []any
	Index         []string
	Values        []string
	Aggregate     string
	MaintainOrder bool
	Separator     string
}

type UnpivotOptions struct {
	On           []string
	Index        []string
	VariableName string
	ValueName    string
}

type SampleOptions struct {
	WithReplacement bool
	Shuffle         bool
	Seed            *uint64
}

type CSVScanOptions struct {
	HasHeader         *bool
	Separator         *byte
	SkipRows          *uint64
	InferSchemaLength *uint64
	NullValue         *string
	TryParseDates     *bool
	QuoteChar         *byte
	CommentPrefix     *string
	Schema            map[string]DataType
	Encoding          *CsvEncoding
	IgnoreErrors      *bool
}

type ParquetScanOptions struct {
	Rechunk *bool
}

// NewLazyFrame 从内存数据创建 LazyFrame
func NewLazyFrame(data []map[string]interface{}) *LazyFrame {
	return &LazyFrame{
		root: &pb.Node{
			Id: 1,
			Kind: &pb.Node_MemoryScan{
				MemoryScan: &pb.MemoryScan{
					ColumnNames: []string{}, // 空表示所有列
					SourceId:    0,
				},
			},
		},
		nodeID: 1,
		err:    nil,
	}
}

// ScanCSV 从 CSV 文件路径创建 LazyFrame（懒加载）
func ScanCSV(path string) *LazyFrame {
	return ScanCSVWithOptions(path, CSVScanOptions{})
}

// ScanCSVWithOptions 从 CSV 文件路径创建带选项的 LazyFrame（懒加载）。
func ScanCSVWithOptions(path string, opts CSVScanOptions) *LazyFrame {
	scan := &pb.CsvScan{
		Path: path,
	}
	if opts.HasHeader != nil {
		scan.HasHeader = opts.HasHeader
	}
	if opts.Separator != nil {
		sep := uint32(*opts.Separator)
		scan.Separator = &sep
	}
	if opts.SkipRows != nil {
		scan.SkipRows = opts.SkipRows
	}
	if opts.InferSchemaLength != nil {
		scan.InferSchemaLength = opts.InferSchemaLength
	}
	if opts.NullValue != nil {
		scan.NullValue = opts.NullValue
	}
	if opts.TryParseDates != nil {
		scan.TryParseDates = opts.TryParseDates
	}
	if opts.QuoteChar != nil {
		quoteChar := uint32(*opts.QuoteChar)
		scan.QuoteChar = &quoteChar
	}
	if opts.CommentPrefix != nil {
		scan.CommentPrefix = opts.CommentPrefix
	}
	if len(opts.Schema) > 0 {
		scan.Schema = make(map[string]pb.DataType, len(opts.Schema))
		for name, dataType := range opts.Schema {
			scan.Schema[name] = dataType
		}
	}
	if opts.Encoding != nil {
		scan.Encoding = opts.Encoding
	}
	if opts.IgnoreErrors != nil {
		scan.IgnoreErrors = opts.IgnoreErrors
	}
	return &LazyFrame{
		root: &pb.Node{
			Id: 1,
			Kind: &pb.Node_CsvScan{
				CsvScan: scan,
			},
		},
		nodeID: 1,
		err:    nil,
	}
}

// ScanParquet 从 Parquet 文件路径创建 LazyFrame（懒加载）
func ScanParquet(path string) *LazyFrame {
	return ScanParquetWithOptions(path, ParquetScanOptions{})
}

// ScanParquetWithOptions 从 Parquet 文件路径创建带选项的 LazyFrame（懒加载）。
func ScanParquetWithOptions(path string, opts ParquetScanOptions) *LazyFrame {
	scan := &pb.ParquetScan{
		Path: path,
	}
	if opts.Rechunk != nil {
		scan.Rechunk = opts.Rechunk
	}
	return &LazyFrame{
		root: &pb.Node{
			Id: 1,
			Kind: &pb.Node_ParquetScan{
				ParquetScan: scan,
			},
		},
		nodeID: 1,
		err:    nil,
	}
}

// Concat vertically concatenates multiple lazy frames.
func Concat(inputs []*LazyFrame, opts ConcatOptions) *LazyFrame {
	if len(inputs) == 0 {
		return (&LazyFrame{}).withErr(fmt.Errorf("concat requires at least one input"))
	}
	opts = normalizeConcatOptions(opts)
	var (
		memorySources []*EagerFrame
		sourceIDs     map[*EagerFrame]uint32
		nodes         = make([]*pb.Node, 0, len(inputs))
		maxNodeID     uint32
	)
	for _, input := range inputs {
		if input == nil {
			return (&LazyFrame{}).withErr(fmt.Errorf("concat input is nil"))
		}
		if input.err != nil {
			return input.withErr(input.err)
		}
		var err error
		memorySources, sourceIDs, err = mergeMemorySources(memorySources, input.memorySources)
		if err != nil {
			return (&LazyFrame{}).withErr(err)
		}
		root, err := remapNodeSources(input.root, input.memorySources, sourceIDs)
		if err != nil {
			return (&LazyFrame{}).withErr(err)
		}
		nodes = append(nodes, root)
		if input.nodeID > maxNodeID {
			maxNodeID = input.nodeID
		}
	}
	newNode := &pb.Node{
		Id: maxNodeID + 1,
		Kind: &pb.Node_Concat{
			Concat: &pb.Concat{
				Inputs:        nodes,
				Rechunk:       opts.Rechunk,
				ToSupertypes:  opts.ToSupertypes,
				Parallel:      opts.Parallel,
				Diagonal:      opts.Diagonal,
				MaintainOrder: opts.MaintainOrder,
			},
		},
	}
	return &LazyFrame{
		root:          newNode,
		nodeID:        maxNodeID + 1,
		memorySources: memorySources,
	}
}

// Concat concatenates the current lazy frame with additional inputs.
func (lf *LazyFrame) Concat(others ...*LazyFrame) *LazyFrame {
	inputs := make([]*LazyFrame, 0, len(others)+1)
	inputs = append(inputs, lf)
	inputs = append(inputs, others...)
	return Concat(inputs, ConcatOptions{
		Parallel:      true,
		MaintainOrder: true,
	})
}

// nextNodeID 获取下一个节点 ID
func (lf *LazyFrame) nextNodeID() uint32 {
	lf.nodeID++
	return lf.nodeID
}

func (lf *LazyFrame) withErr(err error) *LazyFrame {
	if lf == nil {
		return &LazyFrame{err: err}
	}
	return &LazyFrame{
		root:          lf.root,
		nodeID:        lf.nodeID,
		memorySources: lf.memorySources,
		err:           err,
	}
}

func (lf *LazyFrame) withRoot(root *pb.Node) *LazyFrame {
	if lf == nil {
		return &LazyFrame{err: fmt.Errorf("lazyframe is nil")}
	}
	return &LazyFrame{
		root:          root,
		nodeID:        lf.nodeID,
		memorySources: lf.memorySources,
		err:           lf.err,
	}
}

// Filter 过滤行
func (lf *LazyFrame) Filter(predicate Expr) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Filter{
			Filter: &pb.Filter{
				Input:     lf.root,
				Predicate: predicate.toProto(),
			},
		},
	}
	return lf.withRoot(newNode)
}

// Select 选择列
func (lf *LazyFrame) Select(exprs ...Expr) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	protoExprs := make([]*pb.Expr, len(exprs))
	for i, expr := range exprs {
		protoExprs[i] = expr.toProto()
	}

	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Project{
			Project: &pb.Project{
				Input:       lf.root,
				Expressions: protoExprs,
			},
		},
	}
	return lf.withRoot(newNode)
}

// Drop removes columns from the lazy frame.
func (lf *LazyFrame) Drop(columns ...string) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Drop{
			Drop: &pb.Drop{
				Input:   lf.root,
				Columns: columns,
			},
		},
	}
	return lf.withRoot(newNode)
}

// Rename renames columns using mapping semantics similar to Python Polars.
func (lf *LazyFrame) Rename(mapping map[string]string, strict ...bool) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	existing := make([]string, 0, len(mapping))
	newNames := make([]string, 0, len(mapping))
	for oldName, newName := range mapping {
		existing = append(existing, oldName)
		newNames = append(newNames, newName)
	}
	isStrict := true
	if len(strict) > 0 {
		isStrict = strict[0]
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Rename{
			Rename: &pb.Rename{
				Input:    lf.root,
				Existing: existing,
				New:      newNames,
				Strict:   isStrict,
			},
		},
	}
	return lf.withRoot(newNode)
}

// WithColumns 添加或修改列
func (lf *LazyFrame) WithColumns(exprs ...Expr) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	protoExprs := make([]*pb.Expr, len(exprs))
	for i, expr := range exprs {
		protoExprs[i] = expr.toProto()
	}

	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_WithColumns{
			WithColumns: &pb.WithColumns{
				Input:       lf.root,
				Expressions: protoExprs,
			},
		},
	}
	return lf.withRoot(newNode)
}

// Limit 限制行数
func (lf *LazyFrame) Limit(n uint64) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Limit{
			Limit: &pb.Limit{
				Input: lf.root,
				N:     n,
			},
		},
	}
	return lf.withRoot(newNode)
}

// Slice returns a slice of rows using offset and length semantics.
func (lf *LazyFrame) Slice(offset int64, length uint64) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Slice{
			Slice: &pb.Slice{
				Input:  lf.root,
				Offset: offset,
				Len:    length,
			},
		},
	}
	return lf.withRoot(newNode)
}

// Head returns the first n rows.
func (lf *LazyFrame) Head(n uint64) *LazyFrame {
	return lf.Slice(0, n)
}

// Tail returns the last n rows.
func (lf *LazyFrame) Tail(n uint64) *LazyFrame {
	return lf.Slice(-int64(n), n)
}

// FFill forward-fills null values across all columns.
func (lf *LazyFrame) FFill(limit ...uint64) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	return lf.WithColumns(All().FFill(limit...))
}

// BFill backward-fills null values across all columns.
func (lf *LazyFrame) BFill(limit ...uint64) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	return lf.WithColumns(All().BFill(limit...))
}

// FillNull fills null values across all columns with the provided value.
func (lf *LazyFrame) FillNull(value interface{}) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	return lf.WithColumns(All().FillNull(value))
}

// DropNulls drops rows that contain nulls in the provided subset.
func (lf *LazyFrame) DropNulls(subset ...string) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_DropNulls{
			DropNulls: &pb.DropNulls{
				Input:  lf.root,
				Subset: subset,
			},
		},
	}
	return lf.withRoot(newNode)
}

// DropNans drops rows that contain NaN values in the provided subset.
func (lf *LazyFrame) DropNans(subset ...string) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_DropNans{
			DropNans: &pb.DropNans{
				Input:  lf.root,
				Subset: subset,
			},
		},
	}
	return lf.withRoot(newNode)
}

// FillNan fills NaN values across all columns with the provided value.
func (lf *LazyFrame) FillNan(value interface{}) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	fillExpr := exprFromValue(value)
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_FillNan{
			FillNan: &pb.FillNanFrame{
				Input:     lf.root,
				FillValue: fillExpr.toProto(),
			},
		},
	}
	return lf.withRoot(newNode)
}

// Reverse reverses row order.
func (lf *LazyFrame) Reverse() *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Reverse{
			Reverse: &pb.Reverse{Input: lf.root},
		},
	}
	return lf.withRoot(newNode)
}

// SampleN samples n rows.
func (lf *LazyFrame) SampleN(n uint64, opts ...SampleOptions) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	cfg := SampleOptions{Shuffle: true}
	if len(opts) > 0 {
		cfg = opts[0]
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Sample{
			Sample: &pb.Sample{
				Input:           lf.root,
				IsFraction:      false,
				Value:           float64(n),
				WithReplacement: cfg.WithReplacement,
				Shuffle:         cfg.Shuffle,
				Seed:            cfg.Seed,
			},
		},
	}
	return lf.withRoot(newNode)
}

// SampleFrac samples a fraction of rows.
func (lf *LazyFrame) SampleFrac(frac float64, opts ...SampleOptions) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	cfg := SampleOptions{Shuffle: true}
	if len(opts) > 0 {
		cfg = opts[0]
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Sample{
			Sample: &pb.Sample{
				Input:           lf.root,
				IsFraction:      true,
				Value:           frac,
				WithReplacement: cfg.WithReplacement,
				Shuffle:         cfg.Shuffle,
				Seed:            cfg.Seed,
			},
		},
	}
	return lf.withRoot(newNode)
}

// Explode explodes list-like columns.
func (lf *LazyFrame) Explode(columns ...string) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Explode{
			Explode: &pb.Explode{
				Input:   lf.root,
				Columns: columns,
			},
		},
	}
	return lf.withRoot(newNode)
}

// Unnest unnests struct columns into their fields.
func (lf *LazyFrame) Unnest(columns ...string) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Unnest{
			Unnest: &pb.Unnest{
				Input:   lf.root,
				Columns: columns,
			},
		},
	}
	return lf.withRoot(newNode)
}

// Unpivot reshapes wide data to long format.
func (lf *LazyFrame) Unpivot(opts UnpivotOptions) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Unpivot{
			Unpivot: &pb.Unpivot{
				Input:        lf.root,
				On:           opts.On,
				Index:        opts.Index,
				VariableName: opts.VariableName,
				ValueName:    opts.ValueName,
			},
		},
	}
	return lf.withRoot(newNode)
}

// Melt is an alias for Unpivot for pandas users.
func (lf *LazyFrame) Melt(opts UnpivotOptions) *LazyFrame {
	return lf.Unpivot(opts)
}

func buildLazyPivotOnColumnRow(on []string, raw any) (*pb.PivotOnColumnRow, error) {
	if len(on) == 1 {
		return &pb.PivotOnColumnRow{
			Values: []*pb.Literal{literalFromValue(raw)},
		}, nil
	}

	if raw == nil {
		return nil, fmt.Errorf("on_columns row is nil; hint: provide a map keyed by On columns or a positional slice with %d values", len(on))
	}

	if values, ok := raw.([]any); ok {
		if len(values) != len(on) {
			return nil, fmt.Errorf("on_columns row length mismatch: expected %d values for On=%v, got %d", len(on), on, len(values))
		}
		row := &pb.PivotOnColumnRow{Values: make([]*pb.Literal, len(values))}
		for i, value := range values {
			row.Values[i] = literalFromValue(value)
		}
		return row, nil
	}

	if named, ok := raw.(map[string]any); ok {
		row := &pb.PivotOnColumnRow{Values: make([]*pb.Literal, len(on))}
		for i, name := range on {
			value, exists := named[name]
			if !exists {
				return nil, fmt.Errorf("on_columns row is missing key %q; expected keys matching On=%v", name, on)
			}
			row.Values[i] = literalFromValue(value)
		}
		return row, nil
	}

	rv := reflect.ValueOf(raw)
	if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
		if rv.Len() != len(on) {
			return nil, fmt.Errorf("on_columns row length mismatch: expected %d values for On=%v, got %d", len(on), on, rv.Len())
		}
		row := &pb.PivotOnColumnRow{Values: make([]*pb.Literal, rv.Len())}
		for i := 0; i < rv.Len(); i++ {
			row.Values[i] = literalFromValue(rv.Index(i).Interface())
		}
		return row, nil
	}

	return nil, fmt.Errorf(
		"unsupported on_columns row type %T for multi-column pivot; hint: use map[string]any or []any with values matching On=%v",
		raw,
		on,
	)
}

func buildLazyPivotOnColumns(on []string, rawRows []any) ([]*pb.PivotOnColumnRow, error) {
	rows := make([]*pb.PivotOnColumnRow, len(rawRows))
	for i, raw := range rawRows {
		row, err := buildLazyPivotOnColumnRow(on, raw)
		if err != nil {
			return nil, fmt.Errorf("invalid on_columns[%d]: %w", i, err)
		}
		rows[i] = row
	}
	return rows, nil
}

// Pivot reshapes the lazy frame using explicit on_columns, aligned with Python
// Polars LazyFrame.pivot semantics.
func (lf *LazyFrame) Pivot(opts LazyPivotOptions) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	if len(opts.On) == 0 {
		return lf.withErr(fmt.Errorf("pivot options require at least one column in On; hint: set LazyPivotOptions.On to one or more column names"))
	}
	if len(opts.OnColumns) == 0 {
		return lf.withErr(fmt.Errorf("lazy pivot requires explicit OnColumns; hint: pass LazyPivotOptions.OnColumns with the expected pivot column values"))
	}
	onColumns, err := buildLazyPivotOnColumns(opts.On, opts.OnColumns)
	if err != nil {
		return lf.withErr(fmt.Errorf("lazy pivot on_columns are invalid: %w", err))
	}

	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Pivot{
			Pivot: &pb.Pivot{
				Input:         lf.root,
				On:            opts.On,
				OnColumns:     onColumns,
				Index:         opts.Index,
				Values:        opts.Values,
				Aggregate:     opts.Aggregate,
				MaintainOrder: opts.MaintainOrder,
				Separator:     opts.Separator,
			},
		},
	}
	return lf.withRoot(newNode)
}

// GroupBy groups rows by one or more column names.
func (lf *LazyFrame) GroupBy(columns ...string) *GroupByFrame {
	if lf == nil {
		return &GroupByFrame{err: fmt.Errorf("lazyframe is nil")}
	}
	if lf.err != nil {
		return &GroupByFrame{input: lf, err: lf.err}
	}
	keys := make([]Expr, len(columns))
	for i, name := range columns {
		keys[i] = Col(name)
	}
	return &GroupByFrame{input: lf, keys: keys}
}

// GroupByExprs groups rows by one or more expressions.
func (lf *LazyFrame) GroupByExprs(exprs ...Expr) *GroupByFrame {
	if lf == nil {
		return &GroupByFrame{err: fmt.Errorf("lazyframe is nil")}
	}
	if lf.err != nil {
		return &GroupByFrame{input: lf, err: lf.err}
	}
	return &GroupByFrame{input: lf, keys: exprs}
}

// Join joins the current LazyFrame with another LazyFrame using Python-Polars-style options.
func (lf *LazyFrame) Join(other *LazyFrame, opts JoinOptions) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	if other == nil {
		return lf.withErr(fmt.Errorf("join lazyframe is nil"))
	}
	if other.err != nil {
		return lf.withErr(other.err)
	}
	leftOn, rightOn, how, suffix, err := normalizeJoinOptions(opts)
	if err != nil {
		return lf.withErr(err)
	}
	memorySources, sourceIDs, err := mergeMemorySources(lf.memorySources, other.memorySources)
	if err != nil {
		return lf.withErr(err)
	}
	leftRoot, err := remapNodeSources(lf.root, lf.memorySources, sourceIDs)
	if err != nil {
		return lf.withErr(err)
	}
	rightRoot, err := remapNodeSources(other.root, other.memorySources, sourceIDs)
	if err != nil {
		return lf.withErr(err)
	}

	leftExprs := make([]*pb.Expr, len(leftOn))
	for i, expr := range leftOn {
		leftExprs[i] = expr.toProto()
	}
	rightExprs := make([]*pb.Expr, len(rightOn))
	for i, expr := range rightOn {
		rightExprs[i] = expr.toProto()
	}

	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Join{
			Join: &pb.Join{
				Left:    leftRoot,
				Right:   rightRoot,
				LeftOn:  leftExprs,
				RightOn: rightExprs,
				How:     how,
				Suffix:  suffix,
			},
		},
	}

	return &LazyFrame{
		root:          newNode,
		nodeID:        lf.nodeID,
		memorySources: memorySources,
		err:           nil,
	}
}

// Sort sorts rows using Python-Polars-style options.
func (lf *LazyFrame) Sort(opts SortOptions) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	exprs, flags, nullsLast, err := normalizeSortOptions(opts)
	if err != nil {
		return lf.withErr(err)
	}
	protoExprs := make([]*pb.Expr, len(exprs))
	for i, expr := range exprs {
		protoExprs[i] = expr.toProto()
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Sort{
			Sort: &pb.Sort{
				Input:      lf.root,
				By:         protoExprs,
				Descending: flags,
				NullsLast:  nullsLast,
			},
		},
	}
	return lf.withRoot(newNode)
}

// Unique removes duplicate rows using Python-Polars-style options.
func (lf *LazyFrame) Unique(opts UniqueOptions) *LazyFrame {
	if lf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("lazyframe is nil"))
	}
	if lf.err != nil {
		return lf.withErr(lf.err)
	}
	keep, err := normalizeUniqueKeep(opts.Keep)
	if err != nil {
		return lf.withErr(err)
	}
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Unique{
			Unique: &pb.Unique{
				Input:         lf.root,
				Subset:        opts.Subset,
				Keep:          keep,
				MaintainOrder: opts.MaintainOrder,
			},
		},
	}
	return lf.withRoot(newNode)
}

// Collect 执行查询并返回 DataFrame（使用完请调用 Free）
func (lf *LazyFrame) Collect() (*EagerFrame, error) {
	if err := invalidLazyFrameError(lf); err != nil {
		return nil, wrapOp("LazyFrame.Collect", err)
	}
	if lf.err != nil {
		return nil, wrapOp("LazyFrame.Collect", lf.err)
	}
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}
	if err := ensureExprMapBatchesBridge(brg); err != nil {
		return nil, wrapOp("LazyFrame.Collect", err)
	}
	for i, src := range lf.memorySources {
		if src == nil || src.handle == 0 || src.brg == nil {
			return nil, fmt.Errorf("LazyFrame.Collect: memory source %d is nil", i)
		}
		if src.brg != brg {
			return nil, fmt.Errorf("LazyFrame.Collect: bridge mismatch for memory source %d", i)
		}
	}
	handle, err := lf.compilePlan(brg)
	if err != nil {
		return nil, fmt.Errorf(
			"LazyFrame.Collect: failed to compile plan (memory_sources=%d): %w",
			len(lf.memorySources),
			err,
		)
	}
	defer brg.FreePlan(handle)

	// 3. 执行查询并返回 DataFrame 句柄
	inputHandles := make([]uint64, len(lf.memorySources))
	for i, src := range lf.memorySources {
		inputHandles[i] = src.handle
	}
	dfHandle, err := brg.CollectPlanDF(handle, inputHandles)
	if err != nil {
		return nil, fmt.Errorf(
			"LazyFrame.Collect: failed during dataframe collection (memory_sources=%d): %w",
			len(lf.memorySources),
			err,
		)
	}

	return newDataFrame(dfHandle, brg), nil
}

// SinkJSON collects the lazy query and writes the resulting JSON to w.
func (lf *LazyFrame) SinkJSON(w io.Writer) error {
	if err := invalidLazyFrameError(lf); err != nil {
		return wrapOp("LazyFrame.SinkJSON", err)
	}
	if lf.err != nil {
		return wrapOp("LazyFrame.SinkJSON", lf.err)
	}
	df, err := lf.Collect()
	if err != nil {
		return wrapOp("LazyFrame.SinkJSON", err)
	}
	defer df.Free()
	if err := df.WriteJSON(w); err != nil {
		return wrapOp("LazyFrame.SinkJSON", err)
	}
	return nil
}

// SinkNDJSON writes the lazy query result as NDJSON to w.
//
// For generic writers this uses a temporary file-backed lazy sink internally,
// so it no longer needs to collect the full result into a dataframe first.
func (lf *LazyFrame) SinkNDJSON(w io.Writer, opts ...SinkNDJSONOptions) error {
	if err := invalidLazyFrameError(lf); err != nil {
		return wrapOp("LazyFrame.SinkNDJSON", err)
	}
	if lf.err != nil {
		return wrapOp("LazyFrame.SinkNDJSON", lf.err)
	}
	cfg := normalizeWriteNDJSONOptions(opts)
	if _, err := ndjsonCompressionCode(cfg.Compression); err != nil {
		return wrapOp("LazyFrame.SinkNDJSON", err)
	}
	if err := validateNDJSONCompressionLevel(cfg); err != nil {
		return wrapOp("LazyFrame.SinkNDJSON", err)
	}

	tmpFile, err := openNDJSONTempFile(cfg)
	if err != nil {
		return wrapOp("LazyFrame.SinkNDJSON", err)
	}
	tmpPath := tmpFile.Name()
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return wrapOp("LazyFrame.SinkNDJSON", fmt.Errorf("failed to prepare temporary NDJSON file: %w", err))
	}
	defer os.Remove(tmpPath)

	if err := lf.SinkNDJSONFile(tmpPath, cfg); err != nil {
		return wrapOp("LazyFrame.SinkNDJSON", err)
	}

	file, err := os.Open(tmpPath)
	if err != nil {
		return wrapOp("LazyFrame.SinkNDJSON", fmt.Errorf("failed to open temporary NDJSON file: %w", err))
	}
	defer file.Close()

	if _, err := io.Copy(w, file); err != nil {
		return wrapOp("LazyFrame.SinkNDJSON", err)
	}
	return nil
}

// SinkNDJSONFile streams the lazy query result directly to an NDJSON file
// without collecting it into a dataframe first.
func (lf *LazyFrame) SinkNDJSONFile(path string, opts ...SinkNDJSONOptions) error {
	if err := invalidLazyFrameError(lf); err != nil {
		return wrapOp("LazyFrame.SinkNDJSONFile", err)
	}
	if lf.err != nil {
		return wrapOp("LazyFrame.SinkNDJSONFile", lf.err)
	}
	if path == "" {
		return wrapOp("LazyFrame.SinkNDJSONFile", fmt.Errorf("path is empty"))
	}

	cfg := normalizeWriteNDJSONOptions(opts)
	compressionCode, err := ndjsonCompressionCode(cfg.Compression)
	if err != nil {
		return wrapOp("LazyFrame.SinkNDJSONFile", err)
	}
	if err := validateNDJSONCompressionLevel(cfg); err != nil {
		return wrapOp("LazyFrame.SinkNDJSONFile", err)
	}

	brg, err := resolveBridge(nil)
	if err != nil {
		return wrapOp("LazyFrame.SinkNDJSONFile", err)
	}
	for i, src := range lf.memorySources {
		if src == nil {
			return wrapOp("LazyFrame.SinkNDJSONFile", fmt.Errorf("memory source %d is nil", i))
		}
		if src.brg != brg {
			return wrapOp("LazyFrame.SinkNDJSONFile", fmt.Errorf("bridge mismatch for memory source %d", i))
		}
	}

	handle, err := lf.compilePlan(brg)
	if err != nil {
		return wrapOp(
			"LazyFrame.SinkNDJSONFile",
			fmt.Errorf("failed to compile plan (memory_sources=%d): %w", len(lf.memorySources), err),
		)
	}
	defer brg.FreePlan(handle)

	inputHandles := make([]uint64, len(lf.memorySources))
	for i, src := range lf.memorySources {
		inputHandles[i] = src.handle
	}

	if err := brg.SinkPlanNDJSON(
		handle,
		[]byte(path),
		inputHandles,
		compressionCode,
		int32(cfg.CompressionLevel),
	); err != nil {
		return wrapOp(
			"LazyFrame.SinkNDJSONFile",
			fmt.Errorf("failed during NDJSON sink (path=%q, memory_sources=%d): %w", path, len(lf.memorySources), err),
		)
	}
	return nil
}

// Print 执行查询并直接打印结果（使用 Polars 原生的 Display）
func (lf *LazyFrame) Print() error {
	if err := invalidLazyFrameError(lf); err != nil {
		return wrapOp("LazyFrame.Print", err)
	}
	if lf.err != nil {
		return wrapOp("LazyFrame.Print", lf.err)
	}
	brg, err := resolveBridge(nil)
	if err != nil {
		return err
	}
	if len(lf.memorySources) > 0 {
		df, err := lf.Collect()
		if err != nil {
			return err
		}
		defer df.Free()
		return df.Print()
	}

	handle, err := lf.compilePlan(brg)
	if err != nil {
		return fmt.Errorf(
			"LazyFrame.Print: failed to compile plan (memory_sources=%d): %w",
			len(lf.memorySources),
			err,
		)
	}
	defer brg.FreePlan(handle)

	// 3. 执行并打印（调用 Polars 原生的 Display）
	err = brg.ExecuteAndPrint(handle)
	if err != nil {
		return fmt.Errorf(
			"LazyFrame.Print: failed to execute and print (memory_sources=%d): %w",
			len(lf.memorySources),
			err,
		)
	}

	return nil
}

// Explain returns the logical or optimized plan description.
func (lf *LazyFrame) Explain(optimized ...bool) (string, error) {
	if err := invalidLazyFrameError(lf); err != nil {
		return "", wrapOp("LazyFrame.Explain", err)
	}
	if lf.err != nil {
		return "", wrapOp("LazyFrame.Explain", lf.err)
	}
	brg, err := resolveBridge(nil)
	if err != nil {
		return "", err
	}
	useOptimized := true
	if len(optimized) > 0 {
		useOptimized = optimized[0]
	}
	handle, err := lf.compilePlan(brg)
	if err != nil {
		return "", fmt.Errorf(
			"LazyFrame.Explain: failed to compile plan (optimized=%t, memory_sources=%d): %w",
			useOptimized,
			len(lf.memorySources),
			err,
		)
	}
	defer brg.FreePlan(handle)

	inputHandles := make([]uint64, len(lf.memorySources))
	for i, src := range lf.memorySources {
		if src == nil || src.handle == 0 {
			return "", fmt.Errorf("LazyFrame.Explain: memory source %d is nil", i)
		}
		inputHandles[i] = src.handle
	}
	plan, err := brg.ExplainPlan(handle, inputHandles, useOptimized)
	if err != nil {
		return "", fmt.Errorf(
			"LazyFrame.Explain: failed to explain plan (optimized=%t, memory_sources=%d): %w",
			useOptimized,
			len(lf.memorySources),
			err,
		)
	}
	return plan, nil
}

// LogicalPlan returns the unoptimized logical plan for debugging.
func (lf *LazyFrame) LogicalPlan() (string, error) {
	return lf.Explain(false)
}

// OptimizedPlan returns the optimized logical plan for debugging.
func (lf *LazyFrame) OptimizedPlan() (string, error) {
	return lf.Explain(true)
}

func (lf *LazyFrame) compilePlan(brg interface {
	CompilePlan([]byte) (uint64, error)
}) (uint64, error) {
	plan := &pb.Plan{
		PlanVersion: 1,
		Root:        lf.root,
	}
	planBytes, err := proto.Marshal(plan)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal plan: %w", err)
	}
	return brg.CompilePlan(planBytes)
}

// Agg finalizes a group-by pipeline and returns a LazyFrame.
func (gf *GroupByFrame) Agg(aggs ...Expr) *LazyFrame {
	if gf == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("groupby is nil"))
	}
	if gf.err != nil {
		return (&LazyFrame{}).withErr(gf.err)
	}
	if gf.input == nil {
		return (&LazyFrame{}).withErr(fmt.Errorf("groupby input is nil"))
	}
	if gf.input.err != nil {
		return gf.input.withErr(gf.input.err)
	}
	keys := make([]*pb.Expr, len(gf.keys))
	for i, expr := range gf.keys {
		keys[i] = expr.toProto()
	}
	protoAggs := make([]*pb.Expr, len(aggs))
	for i, expr := range aggs {
		protoAggs[i] = expr.toProto()
	}
	newNode := &pb.Node{
		Id: gf.input.nextNodeID(),
		Kind: &pb.Node_GroupBy{
			GroupBy: &pb.GroupBy{
				Input: gf.input.root,
				Keys:  keys,
				Aggs:  protoAggs,
			},
		},
	}
	return gf.input.withRoot(newNode)
}

func normalizeJoinOptions(opts JoinOptions) ([]Expr, []Expr, JoinType, string, error) {
	suffix := opts.Suffix
	if suffix == "" {
		suffix = "_right"
	}
	how := opts.How
	if how == JoinCross {
		if len(opts.On) > 0 || len(opts.LeftOn) > 0 || len(opts.RightOn) > 0 {
			return nil, nil, how, suffix, fmt.Errorf("cross join does not accept On or LeftOn/RightOn")
		}
		return nil, nil, how, suffix, nil
	}

	if len(opts.On) > 0 && (len(opts.LeftOn) > 0 || len(opts.RightOn) > 0) {
		return nil, nil, how, suffix, fmt.Errorf("join options On and LeftOn/RightOn are mutually exclusive")
	}
	if len(opts.On) > 0 {
		leftOn := make([]Expr, len(opts.On))
		rightOn := make([]Expr, len(opts.On))
		for i, name := range opts.On {
			leftOn[i] = Col(name)
			rightOn[i] = Col(name)
		}
		return leftOn, rightOn, how, suffix, nil
	}
	if len(opts.LeftOn) == 0 || len(opts.RightOn) == 0 {
		return nil, nil, how, suffix, fmt.Errorf("join options require On or both LeftOn and RightOn")
	}
	if len(opts.LeftOn) != len(opts.RightOn) {
		return nil, nil, how, suffix, fmt.Errorf("join options LeftOn and RightOn must have the same length")
	}
	return opts.LeftOn, opts.RightOn, how, suffix, nil
}

func normalizeSortOptions(opts SortOptions) ([]Expr, []bool, []bool, error) {
	if len(opts.By) == 0 {
		return nil, nil, nil, fmt.Errorf("sort options require at least one expression in By")
	}
	descending := make([]bool, len(opts.By))
	nullsLast := make([]bool, len(opts.By))
	for i := range descending {
		if i < len(opts.Descending) {
			descending[i] = opts.Descending[i]
		}
		if i < len(opts.NullsLast) {
			nullsLast[i] = opts.NullsLast[i]
		}
	}
	return opts.By, descending, nullsLast, nil
}

func normalizeUniqueKeep(keep string) (UniqueKeepStrategy, error) {
	switch keep {
	case "", "any":
		return UniqueKeepAny, nil
	case "first":
		return UniqueKeepFirst, nil
	case "last":
		return UniqueKeepLast, nil
	case "none":
		return UniqueKeepNone, nil
	default:
		return UniqueKeepAny, fmt.Errorf("unsupported unique keep strategy %q", keep)
	}
}

func normalizeConcatOptions(opts ConcatOptions) ConcatOptions {
	if !opts.Parallel && !opts.Rechunk && !opts.ToSupertypes && !opts.Diagonal && !opts.MaintainOrder {
		opts.Parallel = true
		opts.MaintainOrder = true
	}
	return opts
}

func mergeMemorySources(left []*EagerFrame, right []*EagerFrame) ([]*EagerFrame, map[*EagerFrame]uint32, error) {
	merged := make([]*EagerFrame, 0, len(left)+len(right))
	sourceIDs := make(map[*EagerFrame]uint32, len(left)+len(right))
	appendSource := func(src *EagerFrame) error {
		if src == nil || src.handle == 0 || src.brg == nil {
			return fmt.Errorf("memory source is nil")
		}
		if _, ok := sourceIDs[src]; ok {
			return nil
		}
		sourceIDs[src] = uint32(len(merged))
		merged = append(merged, src)
		return nil
	}
	for _, src := range left {
		if err := appendSource(src); err != nil {
			return nil, nil, err
		}
	}
	for _, src := range right {
		if err := appendSource(src); err != nil {
			return nil, nil, err
		}
	}
	return merged, sourceIDs, nil
}

func remapNodeSources(root *pb.Node, oldSources []*EagerFrame, sourceIDs map[*EagerFrame]uint32) (*pb.Node, error) {
	if root == nil {
		return nil, fmt.Errorf("node is nil")
	}
	cloned, ok := proto.Clone(root).(*pb.Node)
	if !ok {
		return nil, fmt.Errorf("failed to clone node")
	}
	if err := walkNodeSources(cloned, oldSources, sourceIDs); err != nil {
		return nil, err
	}
	return cloned, nil
}

func walkNodeSources(node *pb.Node, oldSources []*EagerFrame, sourceIDs map[*EagerFrame]uint32) error {
	if node == nil {
		return nil
	}
	switch kind := node.Kind.(type) {
	case *pb.Node_MemoryScan:
		sourceID := int(kind.MemoryScan.SourceId)
		if sourceID < 0 || sourceID >= len(oldSources) {
			return fmt.Errorf("memory source id %d out of range", sourceID)
		}
		src := oldSources[sourceID]
		remapped, ok := sourceIDs[src]
		if !ok {
			return fmt.Errorf("memory source id %d not found in merged sources", sourceID)
		}
		kind.MemoryScan.SourceId = remapped
		return nil
	case *pb.Node_Project:
		return walkNodeSources(kind.Project.Input, oldSources, sourceIDs)
	case *pb.Node_Filter:
		return walkNodeSources(kind.Filter.Input, oldSources, sourceIDs)
	case *pb.Node_WithColumns:
		return walkNodeSources(kind.WithColumns.Input, oldSources, sourceIDs)
	case *pb.Node_Limit:
		return walkNodeSources(kind.Limit.Input, oldSources, sourceIDs)
	case *pb.Node_GroupBy:
		return walkNodeSources(kind.GroupBy.Input, oldSources, sourceIDs)
	case *pb.Node_Join:
		if err := walkNodeSources(kind.Join.Left, oldSources, sourceIDs); err != nil {
			return err
		}
		return walkNodeSources(kind.Join.Right, oldSources, sourceIDs)
	case *pb.Node_Sort:
		return walkNodeSources(kind.Sort.Input, oldSources, sourceIDs)
	case *pb.Node_Unique:
		return walkNodeSources(kind.Unique.Input, oldSources, sourceIDs)
	case *pb.Node_Concat:
		for _, input := range kind.Concat.Inputs {
			if err := walkNodeSources(input, oldSources, sourceIDs); err != nil {
				return err
			}
		}
		return nil
	case *pb.Node_SqlQuery:
		for _, input := range kind.SqlQuery.Inputs {
			if err := walkNodeSources(input, oldSources, sourceIDs); err != nil {
				return err
			}
		}
		return nil
	default:
		return nil
	}
}
