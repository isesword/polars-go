package polars

import (
	"github.com/apache/arrow-go/v18/arrow"
	pb "github.com/isesword/polars-go-bridge/proto"
)

// 数据类型常量
var (
	Int64    = pb.DataType_INT64
	Int32    = pb.DataType_INT32
	Int16    = pb.DataType_INT16
	Int8     = pb.DataType_INT8
	UInt64   = pb.DataType_UINT64
	UInt32   = pb.DataType_UINT32
	UInt16   = pb.DataType_UINT16
	UInt8    = pb.DataType_UINT8
	Float64  = pb.DataType_FLOAT64
	Float32  = pb.DataType_FLOAT32
	Boolean  = pb.DataType_BOOL
	String   = pb.DataType_UTF8
	Date     = pb.DataType_DATE
	Datetime = pb.DataType_DATETIME
	Time     = pb.DataType_TIME
)

// Expr 表达式构建器
type Expr struct {
	inner *pb.Expr
}

type ExprMapBatchesOptions struct {
	ReturnType pb.DataType
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
	PartitionBy []Expr
	OrderBy     []Expr
	Descending  bool
	NullsLast   bool
}

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

// Lit 创建字面量表达式
func Lit(value interface{}) Expr {
	var lit *pb.Literal

	switch v := value.(type) {
	case int:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: int64(v)},
		}
	case int64:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: v},
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
	case nil:
		lit = &pb.Literal{
			Value: &pb.Literal_NullVal{NullVal: &pb.NullValue{}},
		}
	default:
		// 默认转换为字符串
		lit = &pb.Literal{
			Value: &pb.Literal_StringVal{StringVal: ""},
		}
	}

	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Lit{
				Lit: lit,
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
func (e Expr) Cast(dataType pb.DataType, strict bool) Expr {
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
func (e Expr) StrictCast(dataType pb.DataType) Expr {
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
					Expr:        e.inner,
					PartitionBy: partitions,
					OrderBy:     orderBy,
					Descending:  opts.Descending,
					NullsLast:   opts.NullsLast,
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

// toProto 转换为 Protobuf 表达式
func (e Expr) toProto() *pb.Expr {
	return e.inner
}
