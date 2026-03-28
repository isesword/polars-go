use crate::error::BridgeError;
use crate::executor::build_expr;
use crate::proto;
use polars::lazy::dsl::{concat_list, element};
use polars::prelude::*;
use polars::series::ops::NullBehavior;
use polars_ops::prelude::SetOperation;

pub fn build_list_expr(kind: &proto::expr::Kind) -> Option<Result<Expr, BridgeError>> {
    use proto::expr::Kind;

    match kind {
        Kind::Element(_) => Some(Ok(element())),
        Kind::ConcatList(exprs) => Some(build_concat_list(exprs)),
        Kind::ListEval(eval) => Some(build_list_eval(eval)),
        Kind::ListAgg(agg) => Some(build_list_agg(agg)),
        Kind::ListLen(func) => Some(build_unary(func, "ListLen", |expr| expr.list().len())),
        Kind::ListSum(func) => Some(build_unary(func, "ListSum", |expr| expr.list().sum())),
        Kind::ListMean(func) => Some(build_unary(func, "ListMean", |expr| expr.list().mean())),
        Kind::ListMedian(func) => {
            Some(build_unary(func, "ListMedian", |expr| expr.list().median()))
        }
        Kind::ListMax(func) => Some(build_unary(func, "ListMax", |expr| expr.list().max())),
        Kind::ListMin(func) => Some(build_unary(func, "ListMin", |expr| expr.list().min())),
        Kind::ListStd(moment) => Some(build_list_std(moment)),
        Kind::ListVar(moment) => Some(build_list_var(moment)),
        Kind::ListContains(contains) => Some(build_list_contains(contains)),
        Kind::ListSort(sort) => Some(build_list_sort(sort)),
        Kind::ListReverse(func) => Some(build_unary(func, "ListReverse", |expr| {
            expr.list().reverse()
        })),
        Kind::ListUnique(unique) => Some(build_list_unique(unique)),
        Kind::ListNUnique(func) => Some(build_unary(func, "ListNUnique", |expr| {
            expr.list().n_unique()
        })),
        Kind::ListArgMin(func) => Some(build_unary(func, "ListArgMin", |expr| {
            expr.list().arg_min()
        })),
        Kind::ListArgMax(func) => Some(build_unary(func, "ListArgMax", |expr| {
            expr.list().arg_max()
        })),
        Kind::ListAny(func) => Some(build_unary(func, "ListAny", |expr| expr.list().any())),
        Kind::ListAll(func) => Some(build_unary(func, "ListAll", |expr| expr.list().all())),
        Kind::ListDropNulls(func) => Some(build_unary(func, "ListDropNulls", |expr| {
            expr.list().drop_nulls()
        })),
        Kind::ListCountMatches(count) => Some(build_list_count_matches(count)),
        Kind::ListGet(get) => Some(build_list_get(get)),
        Kind::ListGather(gather) => Some(build_list_gather(gather)),
        Kind::ListGatherEvery(gather) => Some(build_list_gather_every(gather)),
        Kind::ListSlice(slice) => Some(build_list_slice(slice)),
        Kind::ListJoin(join) => Some(build_list_join(join)),
        Kind::ListShift(shift) => Some(build_list_shift(shift)),
        Kind::ListDiff(diff) => Some(build_list_diff(diff)),
        Kind::ListSampleN(sample) => Some(build_list_sample(sample, false)),
        Kind::ListSampleFraction(sample) => Some(build_list_sample(sample, true)),
        Kind::ListUnion(set_op) => Some(build_list_set_operation(set_op)),
        Kind::ListSetDifference(set_op) => Some(build_list_set_operation(set_op)),
        Kind::ListSetIntersection(set_op) => Some(build_list_set_operation(set_op)),
        Kind::ListSetSymmetricDifference(set_op) => Some(build_list_set_operation(set_op)),
        Kind::ListUniqueStable(unique) => Some(build_list_unique(unique)),
        _ => None,
    }
}

fn build_concat_list(exprs: &proto::ConcatList) -> Result<Expr, BridgeError> {
    if exprs.exprs.is_empty() {
        return Err(BridgeError::InvalidArgument(
            "ConcatList requires at least one expression".into(),
        ));
    }

    let built = exprs
        .exprs
        .iter()
        .map(build_expr)
        .collect::<Result<Vec<_>, _>>()?;

    concat_list(built).map_err(|e| BridgeError::Execution(format!("ConcatList failed: {}", e)))
}

fn build_list_eval(eval: &proto::ListEval) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&eval.expr, "ListEval")?;
    let evaluation = build_inner_expr(&eval.evaluation, "ListEval")?;
    Ok(expr.list().eval(evaluation))
}

fn build_list_agg(agg: &proto::ListAgg) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&agg.expr, "ListAgg")?;
    let evaluation = build_inner_expr(&agg.evaluation, "ListAgg")?;
    Ok(expr.list().agg(evaluation))
}

fn build_unary<F>(func: &proto::ListFunction, name: &str, op: F) -> Result<Expr, BridgeError>
where
    F: FnOnce(Expr) -> Expr,
{
    let expr = build_inner_expr(&func.expr, name)?;
    Ok(op(expr))
}

fn build_list_contains(contains: &proto::ListContains) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&contains.expr, "ListContains")?;
    let item = build_inner_expr(&contains.item, "ListContains")?;
    Ok(expr.list().contains(item, contains.nulls_equal))
}

fn build_list_sort(sort: &proto::ListSort) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&sort.expr, "ListSort")?;
    let options = SortOptions::default()
        .with_order_descending(sort.descending)
        .with_nulls_last(sort.nulls_last);
    Ok(expr.list().sort(options))
}

fn build_list_get(get: &proto::ListGet) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&get.expr, "ListGet")?;
    let index = build_inner_expr(&get.index, "ListGet")?;
    Ok(expr.list().get(index, get.null_on_oob))
}

fn build_list_gather(gather: &proto::ListGather) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&gather.expr, "ListGather")?;
    let index = build_inner_expr(&gather.index, "ListGather")?;
    Ok(expr.list().gather(index, gather.null_on_oob))
}

fn build_list_gather_every(gather: &proto::ListGatherEvery) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&gather.expr, "ListGatherEvery")?;
    let n = build_inner_expr(&gather.n, "ListGatherEvery")?;
    let offset = build_inner_expr(&gather.offset, "ListGatherEvery")?;
    Ok(expr.list().gather_every(n, offset))
}

fn build_list_slice(slice: &proto::ListSlice) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&slice.expr, "ListSlice")?;
    let offset = build_inner_expr(&slice.offset, "ListSlice")?;
    let length = build_inner_expr(&slice.length, "ListSlice")?;
    Ok(expr.list().slice(offset, length))
}

fn build_list_join(join: &proto::ListJoin) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&join.expr, "ListJoin")?;
    let separator = build_inner_expr(&join.separator, "ListJoin")?;
    Ok(expr.list().join(separator, join.ignore_nulls))
}

fn build_list_count_matches(count: &proto::ListCountMatches) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&count.expr, "ListCountMatches")?;
    let element = build_inner_expr(&count.element, "ListCountMatches")?;
    Ok(expr.list().count_matches(element))
}

fn build_list_shift(shift: &proto::ListShift) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&shift.expr, "ListShift")?;
    let periods = build_inner_expr(&shift.periods, "ListShift")?;
    Ok(expr.list().shift(periods))
}

fn build_list_sample(sample: &proto::ListSample, is_fraction: bool) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(
        &sample.expr,
        if is_fraction {
            "ListSampleFraction"
        } else {
            "ListSampleN"
        },
    )?;
    let value = build_inner_expr(
        &sample.value,
        if is_fraction {
            "ListSampleFraction"
        } else {
            "ListSampleN"
        },
    )?;
    let seed = if sample.has_seed {
        Some(sample.seed)
    } else {
        None
    };
    if is_fraction {
        Ok(expr
            .list()
            .sample_fraction(value, sample.with_replacement, sample.shuffle, seed))
    } else {
        Ok(expr
            .list()
            .sample_n(value, sample.with_replacement, sample.shuffle, seed))
    }
}

fn build_list_diff(diff: &proto::ListDiff) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&diff.expr, "ListDiff")?;
    let null_behavior = match proto::DiffNullBehavior::try_from(diff.null_behavior) {
        Ok(proto::DiffNullBehavior::DiffNullIgnore) => NullBehavior::Ignore,
        Ok(proto::DiffNullBehavior::DiffNullDrop) => NullBehavior::Drop,
        Err(_) => {
            return Err(BridgeError::Unsupported(format!(
                "Unknown ListDiff null behavior: {}",
                diff.null_behavior
            )))
        }
    };
    Ok(expr.list().diff(diff.periods, null_behavior))
}

fn build_list_std(moment: &proto::ListMoment) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&moment.expr, "ListStd")?;
    let ddof = u8::try_from(moment.ddof).map_err(|_| {
        BridgeError::InvalidArgument(format!("ListStd ddof out of range for u8: {}", moment.ddof))
    })?;
    Ok(expr.list().std(ddof))
}

fn build_list_var(moment: &proto::ListMoment) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&moment.expr, "ListVar")?;
    let ddof = u8::try_from(moment.ddof).map_err(|_| {
        BridgeError::InvalidArgument(format!("ListVar ddof out of range for u8: {}", moment.ddof))
    })?;
    Ok(expr.list().var(ddof))
}

fn build_list_unique(unique: &proto::ListUnique) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&unique.expr, "ListUnique")?;
    if unique.maintain_order {
        Ok(expr.list().unique_stable())
    } else {
        Ok(expr.list().unique())
    }
}

fn build_list_set_operation(set_op: &proto::ListSetOperation) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&set_op.expr, "ListSetOperation")?;
    let other = build_inner_expr(&set_op.other, "ListSetOperation")?;
    let op = match proto::SetOperation::try_from(set_op.op) {
        Ok(proto::SetOperation::Union) => SetOperation::Union,
        Ok(proto::SetOperation::Difference) => SetOperation::Difference,
        Ok(proto::SetOperation::Intersection) => SetOperation::Intersection,
        Ok(proto::SetOperation::SymmetricDifference) => SetOperation::SymmetricDifference,
        Err(_) => {
            return Err(BridgeError::Unsupported(format!(
                "Unknown ListSetOperation op: {}",
                set_op.op
            )))
        }
    };
    match op {
        SetOperation::Union => Ok(expr.list().union(other)),
        SetOperation::Difference => Ok(expr.list().set_difference(other)),
        SetOperation::Intersection => Ok(expr.list().set_intersection(other)),
        SetOperation::SymmetricDifference => Ok(expr.list().set_symmetric_difference(other)),
    }
}

fn build_inner_expr(expr: &Option<Box<proto::Expr>>, name: &str) -> Result<Expr, BridgeError> {
    let expr = expr
        .as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic(format!("{name} has no expr")))?;
    build_expr(expr.as_ref())
}
