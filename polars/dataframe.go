package polars

import (
	"fmt"

	pb "github.com/isesword/polars-go-bridge/proto"
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

type CSVScanOptions struct {
	HasHeader         *bool
	Separator         *byte
	SkipRows          *uint64
	InferSchemaLength *uint64
	NullValue         *string
	TryParseDates     *bool
	QuoteChar         *byte
	CommentPrefix     *string
	Schema            map[string]pb.DataType
	Encoding          *pb.CsvEncoding
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
	exprs, flags, err := normalizeSortOptions(opts)
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
	if lf == nil {
		return nil, fmt.Errorf("lazyframe is nil")
	}
	if lf.err != nil {
		return nil, lf.err
	}
	brg, err := resolveBridge(nil)
	if err != nil {
		return nil, err
	}
	if err := ensureExprMapBatchesBridge(brg); err != nil {
		return nil, err
	}
	for i, src := range lf.memorySources {
		if src == nil || src.handle == 0 || src.brg == nil {
			return nil, fmt.Errorf("memory source %d is nil", i)
		}
		if src.brg != brg {
			return nil, fmt.Errorf("bridge mismatch for memory source %d", i)
		}
	}
	// 1. 构建 Plan
	plan := &pb.Plan{
		PlanVersion: 1,
		Root:        lf.root,
	}

	// 2. 编译 Plan
	planBytes, err := proto.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal plan: %w", err)
	}

	handle, err := brg.CompilePlan(planBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to compile plan: %w", err)
	}
	defer brg.FreePlan(handle)

	// 3. 执行查询并返回 DataFrame 句柄
	inputHandles := make([]uint64, len(lf.memorySources))
	for i, src := range lf.memorySources {
		inputHandles[i] = src.handle
	}
	dfHandle, err := brg.CollectPlanDF(handle, inputHandles)
	if err != nil {
		return nil, fmt.Errorf("failed to collect dataframe: %w", err)
	}

	return newDataFrame(dfHandle, brg), nil
}

// Print 执行查询并直接打印结果（使用 Polars 原生的 Display）
func (lf *LazyFrame) Print() error {
	if lf == nil {
		return fmt.Errorf("lazyframe is nil")
	}
	if lf.err != nil {
		return lf.err
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

	// 1. 构建 Plan
	plan := &pb.Plan{
		PlanVersion: 1,
		Root:        lf.root,
	}

	// 2. 编译 Plan
	planBytes, err := proto.Marshal(plan)
	if err != nil {
		return fmt.Errorf("failed to marshal plan: %w", err)
	}

	handle, err := brg.CompilePlan(planBytes)
	if err != nil {
		return fmt.Errorf("failed to compile plan: %w", err)
	}
	defer brg.FreePlan(handle)

	// 3. 执行并打印（调用 Polars 原生的 Display）
	err = brg.ExecuteAndPrint(handle)
	if err != nil {
		return fmt.Errorf("failed to execute and print: %w", err)
	}

	return nil
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

func normalizeSortOptions(opts SortOptions) ([]Expr, []bool, error) {
	if len(opts.By) == 0 {
		return nil, nil, fmt.Errorf("sort options require at least one expression in By")
	}
	flags := make([]bool, len(opts.By))
	for i := range flags {
		if i < len(opts.Descending) {
			flags[i] = opts.Descending[i]
		}
	}
	return opts.By, flags, nil
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
