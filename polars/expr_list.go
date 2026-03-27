package polars

import pb "github.com/isesword/polars-go/proto"

// ListNameSpace exposes list expression helpers.
type ListNameSpace struct {
	expr Expr
}

type ListSortOptions struct {
	Descending bool
	NullsLast  bool
}

type ListSampleOptions struct {
	WithReplacement bool
	Shuffle         bool
	Seed            *uint64
}

// Element references the current list element inside List().Eval(...).
func Element() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Element{
				Element: &pb.Element{},
			},
		},
	}
}

// ConcatList horizontally concatenates expressions into a list expression.
func ConcatList(exprs ...Expr) Expr {
	protoExprs := make([]*pb.Expr, 0, len(exprs))
	for _, expr := range exprs {
		protoExprs = append(protoExprs, expr.toProto())
	}
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ConcatList{
				ConcatList: &pb.ConcatList{
					Exprs: protoExprs,
				},
			},
		},
	}
}

// List enters the list namespace for list-typed expressions.
func (e Expr) List() ListNameSpace {
	return ListNameSpace{expr: e}
}

// Eval performs element-wise computation within each list.
func (ns ListNameSpace) Eval(evaluation Expr) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListEval{
				ListEval: &pb.ListEval{
					Expr:       ns.expr.toProto(),
					Evaluation: evaluation.toProto(),
				},
			},
		},
	}
}

// Agg performs list aggregation semantics within each list.
func (ns ListNameSpace) Agg(evaluation Expr) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListAgg{
				ListAgg: &pb.ListAgg{
					Expr:       ns.expr.toProto(),
					Evaluation: evaluation.toProto(),
				},
			},
		},
	}
}

// Len returns the number of values in each list.
func (ns ListNameSpace) Len() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListLen{
				ListLen: &pb.ListFunction{Expr: ns.expr.toProto()},
			},
		},
	}
}

// Sum returns the sum of each sublist.
func (ns ListNameSpace) Sum() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListSum{
				ListSum: &pb.ListFunction{Expr: ns.expr.toProto()},
			},
		},
	}
}

// Mean returns the mean of each sublist.
func (ns ListNameSpace) Mean() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListMean{
				ListMean: &pb.ListFunction{Expr: ns.expr.toProto()},
			},
		},
	}
}

// Median returns the median of each sublist.
func (ns ListNameSpace) Median() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListMedian{
				ListMedian: &pb.ListFunction{Expr: ns.expr.toProto()},
			},
		},
	}
}

// Max returns the maximum of each sublist.
func (ns ListNameSpace) Max() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListMax{
				ListMax: &pb.ListFunction{Expr: ns.expr.toProto()},
			},
		},
	}
}

// Min returns the minimum of each sublist.
func (ns ListNameSpace) Min() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListMin{
				ListMin: &pb.ListFunction{Expr: ns.expr.toProto()},
			},
		},
	}
}

// Std returns the sample standard deviation of each sublist.
func (ns ListNameSpace) Std(ddof ...uint8) Expr {
	value := uint32(1)
	if len(ddof) > 0 {
		value = uint32(ddof[0])
	}
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListStd{
				ListStd: &pb.ListMoment{
					Expr: ns.expr.toProto(),
					Ddof: value,
				},
			},
		},
	}
}

// Var returns the sample variance of each sublist.
func (ns ListNameSpace) Var(ddof ...uint8) Expr {
	value := uint32(1)
	if len(ddof) > 0 {
		value = uint32(ddof[0])
	}
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListVar{
				ListVar: &pb.ListMoment{
					Expr: ns.expr.toProto(),
					Ddof: value,
				},
			},
		},
	}
}

// Contains checks whether each sublist contains the provided value.
func (ns ListNameSpace) Contains(value interface{}) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListContains{
				ListContains: &pb.ListContains{
					Expr:       ns.expr.toProto(),
					Item:       exprFromValue(value).toProto(),
					NullsEqual: false,
				},
			},
		},
	}
}

// Sort sorts each sublist using the provided options.
func (ns ListNameSpace) Sort(opts ...ListSortOptions) Expr {
	var sortOpts ListSortOptions
	if len(opts) > 0 {
		sortOpts = opts[0]
	}
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListSort{
				ListSort: &pb.ListSort{
					Expr:       ns.expr.toProto(),
					Descending: sortOpts.Descending,
					NullsLast:  sortOpts.NullsLast,
				},
			},
		},
	}
}

// Reverse reverses each sublist.
func (ns ListNameSpace) Reverse() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListReverse{
				ListReverse: &pb.ListFunction{Expr: ns.expr.toProto()},
			},
		},
	}
}

// Unique keeps unique values in each sublist.
func (ns ListNameSpace) Unique() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListUnique{
				ListUnique: &pb.ListUnique{
					Expr:          ns.expr.toProto(),
					MaintainOrder: false,
				},
			},
		},
	}
}

// UniqueStable keeps unique values in each sublist while preserving order.
func (ns ListNameSpace) UniqueStable() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListUniqueStable{
				ListUniqueStable: &pb.ListUnique{
					Expr:          ns.expr.toProto(),
					MaintainOrder: true,
				},
			},
		},
	}
}

// NUnique returns the number of unique values in each sublist.
func (ns ListNameSpace) NUnique() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListNUnique{
				ListNUnique: &pb.ListFunction{Expr: ns.expr.toProto()},
			},
		},
	}
}

// ArgMin returns the index of the minimum value in each sublist.
func (ns ListNameSpace) ArgMin() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListArgMin{
				ListArgMin: &pb.ListFunction{Expr: ns.expr.toProto()},
			},
		},
	}
}

// ArgMax returns the index of the maximum value in each sublist.
func (ns ListNameSpace) ArgMax() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListArgMax{
				ListArgMax: &pb.ListFunction{Expr: ns.expr.toProto()},
			},
		},
	}
}

// Any returns whether any value in each boolean sublist is true.
func (ns ListNameSpace) Any() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListAny{
				ListAny: &pb.ListFunction{Expr: ns.expr.toProto()},
			},
		},
	}
}

// All returns whether all values in each boolean sublist are true.
func (ns ListNameSpace) All() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListAll{
				ListAll: &pb.ListFunction{Expr: ns.expr.toProto()},
			},
		},
	}
}

// DropNulls removes null values from each sublist.
func (ns ListNameSpace) DropNulls() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListDropNulls{
				ListDropNulls: &pb.ListFunction{Expr: ns.expr.toProto()},
			},
		},
	}
}

// CountMatches counts how often the provided value occurs in each sublist.
func (ns ListNameSpace) CountMatches(element interface{}) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListCountMatches{
				ListCountMatches: &pb.ListCountMatches{
					Expr:    ns.expr.toProto(),
					Element: exprFromValue(element).toProto(),
				},
			},
		},
	}
}

// Get returns the item at the provided index in each sublist.
func (ns ListNameSpace) Get(index interface{}) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListGet{
				ListGet: &pb.ListGet{
					Expr:      ns.expr.toProto(),
					Index:     exprFromValue(index).toProto(),
					NullOnOob: true,
				},
			},
		},
	}
}

// Gather gets items in each sublist by index expression.
func (ns ListNameSpace) Gather(index interface{}, nullOnOob ...bool) Expr {
	nulls := false
	if len(nullOnOob) > 0 {
		nulls = nullOnOob[0]
	}
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListGather{
				ListGather: &pb.ListGather{
					Expr:      ns.expr.toProto(),
					Index:     exprFromValue(index).toProto(),
					NullOnOob: nulls,
				},
			},
		},
	}
}

// GatherEvery gathers every n-th item from each sublist starting at offset.
func (ns ListNameSpace) GatherEvery(n, offset interface{}) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListGatherEvery{
				ListGatherEvery: &pb.ListGatherEvery{
					Expr:   ns.expr.toProto(),
					N:      exprFromValue(n).toProto(),
					Offset: exprFromValue(offset).toProto(),
				},
			},
		},
	}
}

// Slice slices each sublist with the provided offset and length.
func (ns ListNameSpace) Slice(offset, length interface{}) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListSlice{
				ListSlice: &pb.ListSlice{
					Expr:   ns.expr.toProto(),
					Offset: exprFromValue(offset).toProto(),
					Length: exprFromValue(length).toProto(),
				},
			},
		},
	}
}

// First returns the first item from each sublist.
func (ns ListNameSpace) First() Expr {
	return ns.Get(int64(0))
}

// Last returns the last item from each sublist.
func (ns ListNameSpace) Last() Expr {
	return ns.Get(int64(-1))
}

// Head returns the first n items from each sublist.
func (ns ListNameSpace) Head(n interface{}) Expr {
	return ns.Slice(int64(0), n)
}

// Tail returns the last n items from each sublist.
func (ns ListNameSpace) Tail(n interface{}) Expr {
	count := exprFromValue(n)
	return ns.Slice(Lit(int64(0)).Sub(count.Cast(Int64, false)), count)
}

// Join joins string values in each sublist using the provided separator.
func (ns ListNameSpace) Join(separator interface{}, ignoreNulls ...bool) Expr {
	ignore := false
	if len(ignoreNulls) > 0 {
		ignore = ignoreNulls[0]
	}
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListJoin{
				ListJoin: &pb.ListJoin{
					Expr:        ns.expr.toProto(),
					Separator:   exprFromValue(separator).toProto(),
					IgnoreNulls: ignore,
				},
			},
		},
	}
}

// Shift shifts each sublist by the provided number of periods.
func (ns ListNameSpace) Shift(periods interface{}) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListShift{
				ListShift: &pb.ListShift{
					Expr:    ns.expr.toProto(),
					Periods: exprFromValue(periods).toProto(),
				},
			},
		},
	}
}

// Diff calculates the discrete difference inside each sublist.
func (ns ListNameSpace) Diff(periods int64, nullBehavior DiffNullBehavior) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListDiff{
				ListDiff: &pb.ListDiff{
					Expr:         ns.expr.toProto(),
					Periods:      periods,
					NullBehavior: nullBehavior,
				},
			},
		},
	}
}

func (ns ListNameSpace) sample(value interface{}, isFraction bool, opts ...ListSampleOptions) Expr {
	var sampleOpts ListSampleOptions
	if len(opts) > 0 {
		sampleOpts = opts[0]
	}
	seed := uint64(0)
	hasSeed := false
	if sampleOpts.Seed != nil {
		seed = *sampleOpts.Seed
		hasSeed = true
	}
	msg := &pb.ListSample{
		Expr:            ns.expr.toProto(),
		Value:           exprFromValue(value).toProto(),
		WithReplacement: sampleOpts.WithReplacement,
		Shuffle:         sampleOpts.Shuffle,
		Seed:            seed,
		HasSeed:         hasSeed,
	}
	if isFraction {
		return Expr{inner: &pb.Expr{Kind: &pb.Expr_ListSampleFraction{ListSampleFraction: msg}}}
	}
	return Expr{inner: &pb.Expr{Kind: &pb.Expr_ListSampleN{ListSampleN: msg}}}
}

// SampleN samples n values from each sublist.
func (ns ListNameSpace) SampleN(n interface{}, opts ...ListSampleOptions) Expr {
	return ns.sample(n, false, opts...)
}

// SampleFraction samples a fraction of each sublist.
func (ns ListNameSpace) SampleFraction(fraction interface{}, opts ...ListSampleOptions) Expr {
	return ns.sample(fraction, true, opts...)
}

func (ns ListNameSpace) setOperation(other interface{}, op pb.SetOperation) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListUnion{
				ListUnion: &pb.ListSetOperation{
					Expr:  ns.expr.toProto(),
					Other: exprFromValue(other).toProto(),
					Op:    op,
				},
			},
		},
	}
}

// Union returns the set union between list arrays.
func (ns ListNameSpace) Union(other interface{}) Expr {
	return ns.setOperation(other, pb.SetOperation_UNION)
}

// SetDifference returns the set difference between list arrays.
func (ns ListNameSpace) SetDifference(other interface{}) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListSetDifference{
				ListSetDifference: &pb.ListSetOperation{
					Expr:  ns.expr.toProto(),
					Other: exprFromValue(other).toProto(),
					Op:    pb.SetOperation_DIFFERENCE,
				},
			},
		},
	}
}

// SetIntersection returns the set intersection between list arrays.
func (ns ListNameSpace) SetIntersection(other interface{}) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListSetIntersection{
				ListSetIntersection: &pb.ListSetOperation{
					Expr:  ns.expr.toProto(),
					Other: exprFromValue(other).toProto(),
					Op:    pb.SetOperation_INTERSECTION,
				},
			},
		},
	}
}

// SetSymmetricDifference returns the symmetric difference between list arrays.
func (ns ListNameSpace) SetSymmetricDifference(other interface{}) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_ListSetSymmetricDifference{
				ListSetSymmetricDifference: &pb.ListSetOperation{
					Expr:  ns.expr.toProto(),
					Other: exprFromValue(other).toProto(),
					Op:    pb.SetOperation_SYMMETRIC_DIFFERENCE,
				},
			},
		},
	}
}
