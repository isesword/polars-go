package polars

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	pb "github.com/isesword/polars-go/proto"
)

// 数据类型常量
var (
	Int64    = DataTypeInt64
	Int32    = DataTypeInt32
	Int16    = DataTypeInt16
	Int8     = DataTypeInt8
	UInt64   = DataTypeUInt64
	UInt32   = DataTypeUInt32
	UInt16   = DataTypeUInt16
	UInt8    = DataTypeUInt8
	Float64  = DataTypeFloat64
	Float32  = DataTypeFloat32
	Boolean  = DataTypeBool
	String   = DataTypeUTF8
	Date     = DataTypeDate
	Datetime = DataTypeDatetime
	Time     = DataTypeTime
)

// Expr 表达式构建器
type Expr struct {
	inner *pb.Expr
}

type ExprMapBatchesOptions struct {
	ReturnType DataType
}

type ValueCountsOptions struct {
	Sort      bool
	Parallel  bool
	Name      string
	Normalize bool
}

type RollingOptions struct {
	WindowSize uint64
	MinPeriods uint64
	Center     bool
}

type SortByOptions struct {
	Descending []bool
	NullsLast  []bool
}

type WhenBuilder struct {
	predicate Expr
}

type WhenThenBuilder struct {
	predicate Expr
	truthy    Expr
}

type RankMethod = pb.RankMethod

const (
	RankAverage = pb.RankMethod_RANK_AVERAGE
	RankMin     = pb.RankMethod_RANK_MIN
	RankMax     = pb.RankMethod_RANK_MAX
	RankDense   = pb.RankMethod_RANK_DENSE
	RankOrdinal = pb.RankMethod_RANK_ORDINAL
)

type RankOptions struct {
	Method     RankMethod
	Descending bool
	Seed       *uint64
}

type OverOptions struct {
	PartitionBy     []Expr
	OrderBy         []Expr
	Descending      bool
	NullsLast       bool
	MappingStrategy WindowMapping
}

type WindowMapping = pb.WindowMapping

const (
	WindowMappingGroupToRows = pb.WindowMapping_WINDOW_MAPPING_GROUP_TO_ROWS
	WindowMappingExplode     = pb.WindowMapping_WINDOW_MAPPING_EXPLODE
	WindowMappingJoin        = pb.WindowMapping_WINDOW_MAPPING_JOIN
)

type DiffNullBehavior = pb.DiffNullBehavior

const (
	DiffNullIgnore = pb.DiffNullBehavior_DIFF_NULL_IGNORE
	DiffNullDrop   = pb.DiffNullBehavior_DIFF_NULL_DROP
)

type QuantileMethod = pb.QuantileMethod

const (
	QuantileNearest      = pb.QuantileMethod_QUANTILE_NEAREST
	QuantileLower        = pb.QuantileMethod_QUANTILE_LOWER
	QuantileHigher       = pb.QuantileMethod_QUANTILE_HIGHER
	QuantileMidpoint     = pb.QuantileMethod_QUANTILE_MIDPOINT
	QuantileLinear       = pb.QuantileMethod_QUANTILE_LINEAR
	QuantileEquiprobable = pb.QuantileMethod_QUANTILE_EQUIPROBABLE
)

// Col 创建列引用表达式
func Col(name string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Col{
				Col: &pb.Column{
					Name: name,
				},
			},
		},
	}
}

// ColRegex creates a regex-based column selector, aligned with Polars regex projection semantics.
func ColRegex(pattern string) Expr {
	return Col(pattern)
}

// AsStruct collects multiple expressions into a single struct expression.
func AsStruct(exprs ...Expr) Expr {
	protoExprs := make([]*pb.Expr, len(exprs))
	for i, expr := range exprs {
		protoExprs[i] = expr.inner
	}
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_AsStruct{
				AsStruct: &pb.AsStruct{Exprs: protoExprs},
			},
		},
	}
}

// Cols 创建多列引用表达式（表达式展开）
func Cols(names ...string) []Expr {
	exprs := make([]Expr, len(names))
	for i, name := range names {
		exprs[i] = Col(name)
	}
	return exprs
}

// All 选择所有列（表达式展开）
func All() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Wildcard{
				Wildcard: &pb.Wildcard{},
			},
		},
	}
}

// Exclude excludes named columns from a wildcard/selector-style expression.
func (e Expr) Exclude(columns ...string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Exclude{
				Exclude: &pb.Exclude{
					Expr:    e.inner,
					Columns: columns,
				},
			},
		},
	}
}

// Lit 创建字面量表达式
func literalFromValue(value interface{}) *pb.Literal {
	var lit *pb.Literal
	switch v := value.(type) {
	case int:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: int64(v)},
		}
	case int8:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: int64(v)},
		}
	case int16:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: int64(v)},
		}
	case int32:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: int64(v)},
		}
	case int64:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: v},
		}
	case uint:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: int64(v)},
		}
	case uint8:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: int64(v)},
		}
	case uint16:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: int64(v)},
		}
	case uint32:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: int64(v)},
		}
	case uint64:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: int64(v)},
		}
	case float64:
		lit = &pb.Literal{
			Value: &pb.Literal_FloatVal{FloatVal: v},
		}
	case float32:
		lit = &pb.Literal{
			Value: &pb.Literal_FloatVal{FloatVal: float64(v)},
		}
	case bool:
		lit = &pb.Literal{
			Value: &pb.Literal_BoolVal{BoolVal: v},
		}
	case string:
		lit = &pb.Literal{
			Value: &pb.Literal_StringVal{StringVal: v},
		}
	case json.Number:
		if intVal, err := v.Int64(); err == nil {
			lit = &pb.Literal{
				Value: &pb.Literal_IntVal{IntVal: intVal},
			}
			break
		}
		floatVal, _ := v.Float64()
		lit = &pb.Literal{
			Value: &pb.Literal_FloatVal{FloatVal: floatVal},
		}
	case time.Time:
		lit = &pb.Literal{
			Value: &pb.Literal_StringVal{StringVal: v.Format(time.RFC3339Nano)},
		}
	case nil:
		lit = &pb.Literal{
			Value: &pb.Literal_NullVal{NullVal: &pb.NullValue{}},
		}
	default:
		lit = &pb.Literal{
			Value: &pb.Literal_StringVal{StringVal: fmt.Sprint(v)},
		}
	}

	return lit
}

func Lit(value interface{}) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Lit{
				Lit: literalFromValue(value),
			},
		},
	}
}

// When starts a conditional expression chain similar to Python Polars
// when(...).then(...).otherwise(...).
func When(predicate Expr) WhenBuilder {
	return WhenBuilder{predicate: predicate}
}

// Then sets the truthy branch for a conditional expression.
func (w WhenBuilder) Then(expr Expr) WhenThenBuilder {
	return WhenThenBuilder{
		predicate: w.predicate,
		truthy:    expr,
	}
}

// Otherwise finalizes a conditional expression.
func (w WhenThenBuilder) Otherwise(expr Expr) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_WhenThen{
				WhenThen: &pb.WhenThen{
					Predicate: w.predicate.inner,
					Truthy:    w.truthy.inner,
					Falsy:     expr.inner,
				},
			},
		},
	}
}

// MapBatches applies a batch-oriented User-defined Go function (UDG) to one or
// more expressions and returns a single expression result.
//
// This aligns with Python Polars map_batches naming. The current
// implementation is Go-side and Arrow-batch oriented, requires an explicit
// return type, and returns a single output column.
func MapBatches(
	inputs []Expr,
	fn func(batch arrow.RecordBatch) (arrow.RecordBatch, error),
	opts ExprMapBatchesOptions,
) Expr {
	protoExprs := make([]*pb.Expr, 0, len(inputs))
	for _, input := range inputs {
		protoExprs = append(protoExprs, input.inner)
	}
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_MapBatches{
				MapBatches: &pb.MapBatchesExpr{
					Exprs:      protoExprs,
					UdgId:      registerExprMapBatches(fn),
					OutputType: opts.ReturnType,
				},
			},
		},
	}
}

// MapBatches applies a batch-oriented User-defined Go function (UDG) to the
// materialized Arrow batch for this expression.
//
// This is the single-input shorthand for the package-level MapBatches helper.
func (e Expr) MapBatches(
	fn func(batch arrow.RecordBatch) (arrow.RecordBatch, error),
	opts ExprMapBatchesOptions,
) Expr {
	return MapBatches([]Expr{e}, fn, opts)
}

// 二元操作符辅助函数
func (e Expr) binaryOp(op pb.BinaryOperator, other Expr) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Binary{
				Binary: &pb.BinaryExpr{
					Left:  e.inner,
					Op:    op,
					Right: other.inner,
				},
			},
		},
	}
}

// Eq 等于
func (e Expr) Eq(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_EQ, other)
}

// Ne 不等于
func (e Expr) Ne(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_NE, other)
}

// Lt 小于
func (e Expr) Lt(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_LT, other)
}

// Le 小于等于
func (e Expr) Le(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_LE, other)
}

// Gt 大于
func (e Expr) Gt(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_GT, other)
}

// Ge 大于等于
func (e Expr) Ge(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_GE, other)
}

// Add 加法
func (e Expr) Add(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_ADD, other)
}

// Sub 减法
func (e Expr) Sub(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_SUB, other)
}

// Mul 乘法
func (e Expr) Mul(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_MUL, other)
}

// Div 除法
func (e Expr) Div(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_DIV, other)
}

// And 逻辑与
func (e Expr) And(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_AND, other)
}

// Or 逻辑或
func (e Expr) Or(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_OR, other)
}

// Mod 取模运算 (%)
func (e Expr) Mod(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_MOD, other)
}

// Pow 幂运算 (**)
func (e Expr) Pow(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_POW, other)
}

// Xor 异或运算 (^)
func (e Expr) Xor(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_XOR, other)
}

// Alias 设置别名
func (e Expr) Alias(name string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Alias{
				Alias: &pb.Alias{
					Expr: e.inner,
					Name: name,
				},
			},
		},
	}
}

// IsNull 检查是否为空
func (e Expr) IsNull() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_IsNull{
				IsNull: &pb.IsNull{
					Expr: e.inner,
				},
			},
		},
	}
}

// IsNotNull checks whether values are not null.
func (e Expr) IsNotNull() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_IsNotNull{
				IsNotNull: &pb.IsNotNull{
					Expr: e.inner,
				},
			},
		},
	}
}

// Not 逻辑取反 (~)
func (e Expr) Not() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Not{
				Not: &pb.Not{
					Expr: e.inner,
				},
			},
		},
	}
}

// Cast 类型转换（严格模式）
// 示例: Col("age").Cast(Int32, true)
func (e Expr) Cast(dataType DataType, strict bool) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Cast{
				Cast: &pb.Cast{
					Expr:     e.inner,
					DataType: dataType,
					Strict:   strict,
				},
			},
		},
	}
}

// StrictCast 严格模式类型转换（转换失败报错）
// 示例: Col("age").StrictCast(Int32)
func (e Expr) StrictCast(dataType DataType) Expr {
	return e.Cast(dataType, true)
}

// Sum 聚合求和
func (e Expr) Sum() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Sum{Sum: &pb.AggExpr{Expr: e.inner}},
		},
	}
}

// Mean 聚合求平均值
func (e Expr) Mean() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Mean{Mean: &pb.AggExpr{Expr: e.inner}},
		},
	}
}

// Min 聚合求最小值
func (e Expr) Min() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Min{Min: &pb.AggExpr{Expr: e.inner}},
		},
	}
}

// Max 聚合求最大值
func (e Expr) Max() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Max{Max: &pb.AggExpr{Expr: e.inner}},
		},
	}
}

// Count 聚合计数
func (e Expr) Count() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Count{Count: &pb.AggExpr{Expr: e.inner}},
		},
	}
}

// First 聚合取第一个值
func (e Expr) First() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_First{First: &pb.AggExpr{Expr: e.inner}},
		},
	}
}

// Last 聚合取最后一个值
func (e Expr) Last() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Last{Last: &pb.AggExpr{Expr: e.inner}},
		},
	}
}

// Len returns the number of values, including nulls.
func (e Expr) Len() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Len{Len: &pb.AggExpr{Expr: e.inner}},
		},
	}
}

// NullCount returns the number of null values.
func (e Expr) NullCount() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_NullCount{NullCount: &pb.NullCount{Expr: e.inner}},
		},
	}
}

// NUnique returns the number of unique values.
func (e Expr) NUnique() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_NUnique{NUnique: &pb.AggExpr{Expr: e.inner}},
		},
	}
}

// Median returns the median value.
func (e Expr) Median() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Median{Median: &pb.AggExpr{Expr: e.inner}},
		},
	}
}

// Std returns the sample standard deviation (ddof=1).
func (e Expr) Std() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Std{Std: &pb.AggExpr{Expr: e.inner}},
		},
	}
}

// Var returns the sample variance (ddof=1).
func (e Expr) Var() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Var{Var: &pb.AggExpr{Expr: e.inner}},
		},
	}
}

// Quantile 聚合分位数，默认使用 Linear 插值。
func (e Expr) Quantile(q float64) Expr {
	return e.QuantileWithMethod(q, QuantileLinear)
}

// QuantileWithMethod 聚合分位数，允许显式指定插值方法。
func (e Expr) QuantileWithMethod(q float64, method QuantileMethod) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Quantile{
				Quantile: &pb.Quantile{
					Expr:     e.inner,
					Quantile: q,
					Method:   method,
				},
			},
		},
	}
}

// Over applies the expression over the given partition columns.
func (e Expr) Over(partitionBy ...Expr) Expr {
	return e.OverWithOptions(OverOptions{PartitionBy: partitionBy})
}

// OverWithOptions applies the expression over the given window options.
func (e Expr) OverWithOptions(opts OverOptions) Expr {
	partitions := make([]*pb.Expr, len(opts.PartitionBy))
	for i, expr := range opts.PartitionBy {
		partitions[i] = expr.inner
	}
	orderBy := make([]*pb.Expr, len(opts.OrderBy))
	for i, expr := range opts.OrderBy {
		orderBy[i] = expr.inner
	}
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Over{
				Over: &pb.Over{
					Expr:            e.inner,
					PartitionBy:     partitions,
					OrderBy:         orderBy,
					Descending:      opts.Descending,
					NullsLast:       opts.NullsLast,
					MappingStrategy: opts.MappingStrategy,
				},
			},
		},
	}
}

// Rank assigns ranks to values.
func (e Expr) Rank(opts RankOptions) Expr {
	rank := &pb.Rank{
		Expr:       e.inner,
		Method:     opts.Method,
		Descending: opts.Descending,
	}
	if rank.Method == 0 {
		rank.Method = RankDense
	}
	if opts.Seed != nil {
		rank.Seed = opts.Seed
	}
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Rank{Rank: rank},
		},
	}
}

// CumSum returns the cumulative sum.
func (e Expr) CumSum(reverse bool) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_CumSum{
				CumSum: &pb.CumSum{
					Expr:    e.inner,
					Reverse: reverse,
				},
			},
		},
	}
}

// CumCount returns the cumulative count.
func (e Expr) CumCount(reverse bool) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_CumCount{
				CumCount: &pb.CumCount{
					Expr:    e.inner,
					Reverse: reverse,
				},
			},
		},
	}
}

// CumMin returns the cumulative minimum.
func (e Expr) CumMin(reverse bool) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_CumMin{
				CumMin: &pb.CumMin{
					Expr:    e.inner,
					Reverse: reverse,
				},
			},
		},
	}
}

// CumMax returns the cumulative maximum.
func (e Expr) CumMax(reverse bool) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_CumMax{
				CumMax: &pb.CumMax{
					Expr:    e.inner,
					Reverse: reverse,
				},
			},
		},
	}
}

// CumProd returns the cumulative product.
func (e Expr) CumProd(reverse bool) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_CumProd{
				CumProd: &pb.CumProd{
					Expr:    e.inner,
					Reverse: reverse,
				},
			},
		},
	}
}

// Shift shifts values by the given number of periods.
func (e Expr) Shift(periods int64) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Shift{
				Shift: &pb.Shift{
					Expr:    e.inner,
					Periods: periods,
				},
			},
		},
	}
}

// Diff calculates the discrete difference.
func (e Expr) Diff(periods int64, nullBehavior DiffNullBehavior) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Diff{
				Diff: &pb.Diff{
					Expr:         e.inner,
					Periods:      periods,
					NullBehavior: nullBehavior,
				},
			},
		},
	}
}

// FFill forward-fills null values with the last non-null value.
func (e Expr) FFill(limit ...uint64) Expr {
	var limitPtr *uint64
	if len(limit) > 0 {
		l := limit[0]
		limitPtr = &l
	}
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ForwardFill{
				ForwardFill: &pb.ForwardFill{
					Expr:  e.inner,
					Limit: limitPtr,
				},
			},
		},
	}
}

// BFill backward-fills null values with the next non-null value.
func (e Expr) BFill(limit ...uint64) Expr {
	var limitPtr *uint64
	if len(limit) > 0 {
		l := limit[0]
		limitPtr = &l
	}
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_BackwardFill{
				BackwardFill: &pb.BackwardFill{
					Expr:  e.inner,
					Limit: limitPtr,
				},
			},
		},
	}
}

// FillNull fills null values in the expression with the provided value or expression.
func (e Expr) FillNull(value interface{}) Expr {
	fillExpr := exprFromValue(value)
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_FillNull{
				FillNull: &pb.FillNull{
					Expr:      e.inner,
					FillValue: fillExpr.inner,
				},
			},
		},
	}
}

// FillNan fills NaN values with the provided value or expression.
func (e Expr) FillNan(value interface{}) Expr {
	fillExpr := exprFromValue(value)
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_FillNan{
				FillNan: &pb.FillNan{
					Expr:      e.inner,
					FillValue: fillExpr.inner,
				},
			},
		},
	}
}

// IsNan checks whether values are NaN.
func (e Expr) IsNan() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_IsNan{
				IsNan: &pb.IsNan{Expr: e.inner},
			},
		},
	}
}

// IsFinite checks whether values are finite.
func (e Expr) IsFinite() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_IsFinite{
				IsFinite: &pb.IsFinite{Expr: e.inner},
			},
		},
	}
}

// IsDuplicated returns a boolean mask marking duplicated values.
func (e Expr) IsDuplicated() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_IsDuplicated{
				IsDuplicated: &pb.IsDuplicated{Expr: e.inner},
			},
		},
	}
}

// Reverse reverses the expression values.
func (e Expr) Reverse() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Reverse{
				Reverse: &pb.ReverseExpr{Expr: e.inner},
			},
		},
	}
}

// Filter filters a single expression in aggregation context.
func (e Expr) Filter(predicate Expr) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_FilterExpr{
				FilterExpr: &pb.FilterExpr{
					Expr:      e.inner,
					Predicate: predicate.inner,
				},
			},
		},
	}
}

func normalizeSortFlags(flags []bool, n int) []bool {
	if n <= 0 {
		return nil
	}
	if len(flags) == 0 {
		return make([]bool, n)
	}
	if len(flags) == 1 && n > 1 {
		out := make([]bool, n)
		for i := range out {
			out[i] = flags[0]
		}
		return out
	}
	out := make([]bool, n)
	copy(out, flags)
	if len(flags) < n {
		last := flags[len(flags)-1]
		for i := len(flags); i < n; i++ {
			out[i] = last
		}
	}
	return out
}

// SortBy sorts this expression by one or more expressions, typically inside aggregation context.
func (e Expr) SortBy(by []Expr, opts ...SortByOptions) Expr {
	protoBy := make([]*pb.Expr, len(by))
	for i, expr := range by {
		protoBy[i] = expr.inner
	}
	var cfg SortByOptions
	if len(opts) > 0 {
		cfg = opts[0]
	}
	descending := normalizeSortFlags(cfg.Descending, len(by))
	nullsLast := normalizeSortFlags(cfg.NullsLast, len(by))
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_SortBy{
				SortBy: &pb.SortByExpr{
					Expr:       e.inner,
					By:         protoBy,
					Descending: descending,
					NullsLast:  nullsLast,
				},
			},
		},
	}
}

// Abs returns the absolute value of the expression.
func (e Expr) Abs() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Abs{
				Abs: &pb.Abs{Expr: e.inner},
			},
		},
	}
}

// Round rounds floating point values to the given decimal precision.
func (e Expr) Round(decimals uint32) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Round{
				Round: &pb.Round{
					Expr:     e.inner,
					Decimals: decimals,
				},
			},
		},
	}
}

// Sqrt computes the square root of the expression.
func (e Expr) Sqrt() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Sqrt{
				Sqrt: &pb.Sqrt{Expr: e.inner},
			},
		},
	}
}

// Log computes the logarithm using the provided base expression.
func (e Expr) Log(base interface{}) Expr {
	baseExpr := exprFromValue(base)
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Log{
				Log: &pb.Log{
					Expr: e.inner,
					Base: baseExpr.inner,
				},
			},
		},
	}
}

// Clip clips values to the provided inclusive min/max boundaries.
func (e Expr) Clip(min interface{}, max interface{}) Expr {
	minExpr := exprFromValue(min)
	maxExpr := exprFromValue(max)
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Clip{
				Clip: &pb.Clip{
					Expr: e.inner,
					Min:  minExpr.inner,
					Max:  maxExpr.inner,
				},
			},
		},
	}
}

// ValueCounts returns a struct expression containing values and counts.
func (e Expr) ValueCounts(opts ...ValueCountsOptions) Expr {
	cfg := ValueCountsOptions{Name: "count"}
	if len(opts) > 0 {
		cfg = opts[0]
		if cfg.Name == "" {
			cfg.Name = "count"
		}
	}
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ValueCounts{
				ValueCounts: &pb.ValueCounts{
					Expr:      e.inner,
					Sort:      cfg.Sort,
					Parallel:  cfg.Parallel,
					Name:      cfg.Name,
					Normalize: cfg.Normalize,
				},
			},
		},
	}
}

func normalizeRollingOptions(opts RollingOptions) RollingOptions {
	if opts.WindowSize == 0 {
		opts.WindowSize = 1
	}
	if opts.MinPeriods == 0 {
		opts.MinPeriods = 1
	}
	return opts
}

// RollingMin applies a fixed-size rolling minimum.
func (e Expr) RollingMin(opts RollingOptions) Expr {
	cfg := normalizeRollingOptions(opts)
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_RollingMin{
				RollingMin: &pb.Rolling{
					Expr:       e.inner,
					WindowSize: cfg.WindowSize,
					MinPeriods: cfg.MinPeriods,
					Center:     cfg.Center,
				},
			},
		},
	}
}

// RollingMax applies a fixed-size rolling maximum.
func (e Expr) RollingMax(opts RollingOptions) Expr {
	cfg := normalizeRollingOptions(opts)
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_RollingMax{
				RollingMax: &pb.Rolling{
					Expr:       e.inner,
					WindowSize: cfg.WindowSize,
					MinPeriods: cfg.MinPeriods,
					Center:     cfg.Center,
				},
			},
		},
	}
}

// RollingMean applies a fixed-size rolling mean.
func (e Expr) RollingMean(opts RollingOptions) Expr {
	cfg := normalizeRollingOptions(opts)
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_RollingMean{
				RollingMean: &pb.Rolling{
					Expr:       e.inner,
					WindowSize: cfg.WindowSize,
					MinPeriods: cfg.MinPeriods,
					Center:     cfg.Center,
				},
			},
		},
	}
}

// RollingSum applies a fixed-size rolling sum.
func (e Expr) RollingSum(opts RollingOptions) Expr {
	cfg := normalizeRollingOptions(opts)
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_RollingSum{
				RollingSum: &pb.Rolling{
					Expr:       e.inner,
					WindowSize: cfg.WindowSize,
					MinPeriods: cfg.MinPeriods,
					Center:     cfg.Center,
				},
			},
		},
	}
}

func exprFromValue(value interface{}) Expr {
	if expr, ok := value.(Expr); ok {
		return expr
	}
	return Lit(value)
}

// toProto 转换为 Protobuf 表达式
func (e Expr) toProto() *pb.Expr {
	return e.inner
}
