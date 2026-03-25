use crate::call_go_expr_map_batches;
use crate::error::BridgeError;
use crate::expr_str;
use crate::expr_temporal;
use crate::proto;
use polars::lazy::dsl::concat as concat_lf;
use polars::lazy::dsl::{all, by_name};
use polars::prelude::IntoLazy;
use polars::prelude::NullValues;
use polars::prelude::*;
use polars::series::ops::NullBehavior;
use polars::sql::SQLContext;
use std::sync::Arc;

/// 执行 Plan 并打印结果（调用 Polars 原生的 Display）
pub fn execute_and_print(plan: &proto::Plan) -> Result<(), BridgeError> {
    let result_df = execute_plan_df(plan, &[])?;
    df_print(&result_df)
}

/// 执行 Plan，返回 DataFrame（可复用）
pub fn execute_plan_df(
    plan: &proto::Plan,
    input_dfs: &[&DataFrame],
) -> Result<DataFrame, BridgeError> {
    let root = plan
        .root
        .as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic("Plan has no root node".into()))?;

    // 从 Plan 构建 LazyFrame（根据节点类型自动决定数据源）
    let lf = build_lazy_frame(root, input_dfs)?;

    // 执行 LazyFrame
    let result_df = lf
        .collect()
        .map_err(|e| BridgeError::Execution(format!("Failed to collect LazyFrame: {}", e)))?;

    Ok(result_df)
}

pub fn build_plan_lazy_frame(
    plan: &proto::Plan,
    input_dfs: &[&DataFrame],
) -> Result<LazyFrame, BridgeError> {
    let root = plan
        .root
        .as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic("Plan has no root node".into()))?;
    build_lazy_frame(root, input_dfs)
}

/// 打印 DataFrame（使用 Polars 原生 Display）
pub fn df_print(df: &DataFrame) -> Result<(), BridgeError> {
    println!("{}\n", df);
    Ok(())
}

/// 从 Node 构建 LazyFrame（递归）
fn build_lazy_frame(
    node: &proto::Node,
    input_dfs: &[&DataFrame],
) -> Result<LazyFrame, BridgeError> {
    use proto::node::Kind;

    let kind = node
        .kind
        .as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic("Node has no kind".into()))?;

    match kind {
        Kind::CsvScan(scan) => {
            // 从 CSV 文件路径懒加载
            let mut reader = LazyCsvReader::new(PlRefPath::new(scan.path.as_str()));
            if let Some(has_header) = scan.has_header {
                reader = reader.with_has_header(has_header);
            }
            if let Some(separator) = scan.separator {
                let separator = u8::try_from(separator).map_err(|_| {
                    BridgeError::InvalidArgument(format!(
                        "CsvScan separator out of range: {}",
                        separator
                    ))
                })?;
                reader = reader.with_separator(separator);
            }
            if let Some(skip_rows) = scan.skip_rows {
                reader = reader.with_skip_rows(skip_rows as usize);
            }
            if let Some(infer_schema_length) = scan.infer_schema_length {
                reader = reader.with_infer_schema_length(Some(infer_schema_length as usize));
            }
            if let Some(null_value) = &scan.null_value {
                reader = reader.with_null_values(Some(NullValues::AllColumnsSingle(
                    null_value.clone().into(),
                )));
            }
            if let Some(try_parse_dates) = scan.try_parse_dates {
                reader = reader.with_try_parse_dates(try_parse_dates);
            }
            if let Some(ignore_errors) = scan.ignore_errors {
                reader = reader.with_ignore_errors(ignore_errors);
            }
            if let Some(quote_char) = scan.quote_char {
                let quote_char = u8::try_from(quote_char).map_err(|_| {
                    BridgeError::InvalidArgument(format!(
                        "CsvScan quote_char out of range: {}",
                        quote_char
                    ))
                })?;
                reader = reader.with_quote_char(Some(quote_char));
            }
            if let Some(comment_prefix) = &scan.comment_prefix {
                reader = reader.with_comment_prefix(Some(comment_prefix.clone().into()));
            }
            if let Some(encoding) = scan.encoding {
                let encoding = match proto::CsvEncoding::try_from(encoding) {
                    Ok(proto::CsvEncoding::Utf8) => CsvEncoding::Utf8,
                    Ok(proto::CsvEncoding::LossyUtf8) => CsvEncoding::LossyUtf8,
                    Err(_) => {
                        return Err(BridgeError::Unsupported(format!(
                            "Unknown CSV encoding: {}",
                            encoding
                        )))
                    }
                };
                reader = reader.with_encoding(encoding);
            }
            if !scan.schema.is_empty() {
                let schema = scan
                    .schema
                    .iter()
                    .map(|(name, data_type)| {
                        Ok(Field::new(
                            name.clone().into(),
                            proto_data_type_to_polars(*data_type)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, BridgeError>>()?;
                reader = reader.with_dtype_overwrite(Some(Arc::new(Schema::from_iter(schema))));
            }

            reader.finish().map_err(|e| {
                BridgeError::Execution(format!("CsvScan failed for '{}': {}", scan.path, e))
            })
        }
        Kind::ParquetScan(scan) => {
            let mut args = ScanArgsParquet::default();
            if let Some(rechunk) = scan.rechunk {
                args.rechunk = rechunk;
            }
            LazyFrame::scan_parquet(PlRefPath::new(scan.path.as_str()), args).map_err(|e| {
                BridgeError::Execution(format!("ParquetScan failed for '{}': {}", scan.path, e))
            })
        }
        Kind::MemoryScan(scan) => {
            let source_id = scan.source_id as usize;
            let df = *input_dfs.get(source_id).ok_or_else(|| {
                BridgeError::Unsupported(format!(
                    "MemoryScan source_id {} is out of range for {} inputs",
                    scan.source_id,
                    input_dfs.len()
                ))
            })?;
            let mut lf = df.clone().lazy();
            if !scan.column_names.is_empty() {
                let exprs: Vec<Expr> = scan
                    .column_names
                    .iter()
                    .map(|name| col(name.as_str()))
                    .collect();
                lf = lf.select(&exprs);
            }
            Ok(lf)
        }
        Kind::Project(proj) => {
            let input_node = proj
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Project has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;

            let exprs: Vec<Expr> = proj
                .expressions
                .iter()
                .map(|e| build_expr(e))
                .collect::<Result<_, _>>()?;

            Ok(lf.select(&exprs))
        }
        Kind::Drop(drop) => {
            let input_node = drop
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Drop has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;
            Ok(lf.drop(by_name(drop.columns.iter().cloned(), true, false)))
        }
        Kind::Rename(rename) => {
            let input_node = rename
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Rename has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;
            Ok(lf.rename(
                rename.existing.iter().map(|s| s.as_str()),
                rename.new.iter().map(|s| s.as_str()),
                rename.strict,
            ))
        }
        Kind::Filter(filter) => {
            let input_node = filter
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Filter has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;

            let pred = filter
                .predicate
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Filter has no predicate".into()))?;
            let pred_expr = build_expr(pred)?;

            Ok(lf.filter(pred_expr))
        }
        Kind::WithColumns(with_cols) => {
            let input_node = with_cols
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("WithColumns has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;

            let exprs: Vec<Expr> = with_cols
                .expressions
                .iter()
                .map(|e| build_expr(e))
                .collect::<Result<_, _>>()?;

            Ok(lf.with_columns(&exprs))
        }
        Kind::Limit(limit) => {
            let input_node = limit
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Limit has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;

            Ok(lf.limit(limit.n as u32))
        }
        Kind::Slice(slice) => {
            let input_node = slice
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Slice has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;
            Ok(lf.slice(slice.offset, slice.len as IdxSize))
        }
        Kind::GroupBy(group_by) => {
            let input_node = group_by
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("GroupBy has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;

            let keys: Vec<Expr> = group_by
                .keys
                .iter()
                .map(|e| build_expr(e))
                .collect::<Result<_, _>>()?;
            let aggs: Vec<Expr> = group_by
                .aggs
                .iter()
                .map(|e| build_expr(e))
                .collect::<Result<_, _>>()?;

            Ok(lf.group_by(keys).agg(aggs))
        }
        Kind::Join(join) => {
            let left_node = join
                .left
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Join has no left input".into()))?;
            let right_node = join
                .right
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Join has no right input".into()))?;
            let left_lf = build_lazy_frame(left_node, input_dfs)?;
            let right_lf = build_lazy_frame(right_node, input_dfs)?;

            let left_on: Vec<Expr> = join
                .left_on
                .iter()
                .map(|e| build_expr(e))
                .collect::<Result<_, _>>()?;
            let right_on: Vec<Expr> = join
                .right_on
                .iter()
                .map(|e| build_expr(e))
                .collect::<Result<_, _>>()?;

            let how = match proto::JoinType::try_from(join.how) {
                Ok(proto::JoinType::JoinInner) => JoinType::Inner,
                Ok(proto::JoinType::JoinLeft) => JoinType::Left,
                Ok(proto::JoinType::JoinRight) => JoinType::Right,
                Ok(proto::JoinType::JoinFull) => JoinType::Full,
                Ok(proto::JoinType::JoinSemi) => JoinType::Semi,
                Ok(proto::JoinType::JoinAnti) => JoinType::Anti,
                Ok(proto::JoinType::JoinCross) => JoinType::Cross,
                Err(_) => {
                    return Err(BridgeError::Unsupported(format!(
                        "Unknown join type: {}",
                        join.how
                    )))
                }
            };

            let args = JoinArgs::new(how).with_suffix(Some(join.suffix.clone().into()));
            Ok(left_lf.join(right_lf, left_on, right_on, args))
        }
        Kind::Sort(sort) => {
            let input_node = sort
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Sort has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;

            let by: Vec<Expr> = sort
                .by
                .iter()
                .map(|e| build_expr(e))
                .collect::<Result<_, _>>()?;

            let options = SortMultipleOptions::default()
                .with_order_descending_multi(sort.descending.clone())
                .with_nulls_last_multi(sort.nulls_last.clone());

            Ok(lf.sort_by_exprs(by, options))
        }
        Kind::Unique(unique) => {
            let input_node = unique
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Unique has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;

            let subset = if unique.subset.is_empty() {
                None
            } else {
                Some(cols(
                    unique.subset.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                ))
            };

            let keep = match proto::UniqueKeepStrategy::try_from(unique.keep) {
                Ok(proto::UniqueKeepStrategy::UniqueKeepFirst) => UniqueKeepStrategy::First,
                Ok(proto::UniqueKeepStrategy::UniqueKeepLast) => UniqueKeepStrategy::Last,
                Ok(proto::UniqueKeepStrategy::UniqueKeepAny) => UniqueKeepStrategy::Any,
                Ok(proto::UniqueKeepStrategy::UniqueKeepNone) => UniqueKeepStrategy::None,
                Err(_) => {
                    return Err(BridgeError::Unsupported(format!(
                        "Unknown unique keep strategy: {}",
                        unique.keep
                    )))
                }
            };

            if unique.maintain_order {
                Ok(lf.unique_stable(subset, keep))
            } else {
                Ok(lf.unique(subset, keep))
            }
        }
        Kind::Concat(concat) => {
            if concat.inputs.is_empty() {
                return Err(BridgeError::PlanSemantic("Concat has no inputs".into()));
            }
            let inputs = concat
                .inputs
                .iter()
                .map(|node| build_lazy_frame(node, input_dfs))
                .collect::<Result<Vec<_>, _>>()?;
            let args = UnionArgs {
                rechunk: concat.rechunk,
                parallel: concat.parallel,
                to_supertypes: concat.to_supertypes,
                diagonal: concat.diagonal,
                strict: false,
                from_partitioned_ds: false,
                maintain_order: concat.maintain_order,
            };
            concat_lf(inputs, args)
                .map_err(|e| BridgeError::Execution(format!("Failed to concat LazyFrames: {}", e)))
        }
        Kind::Explode(explode) => {
            let input_node = explode
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Explode has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;
            Ok(lf.explode(
                by_name(explode.columns.iter().cloned(), true, false),
                ExplodeOptions {
                    empty_as_null: false,
                    keep_nulls: true,
                },
            ))
        }
        Kind::DropNulls(drop_nulls) => {
            let input_node = drop_nulls
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("DropNulls has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;
            let subset = if drop_nulls.subset.is_empty() {
                None
            } else {
                Some(by_name(drop_nulls.subset.iter().cloned(), true, false))
            };
            Ok(lf.drop_nulls(subset))
        }
        Kind::DropNans(drop_nans) => {
            let input_node = drop_nans
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("DropNans has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;
            let subset = if drop_nans.subset.is_empty() {
                None
            } else {
                Some(by_name(drop_nans.subset.iter().cloned(), true, false))
            };
            Ok(lf.drop_nans(subset))
        }
        Kind::FillNan(fill_nan) => {
            let input_node = fill_nan
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("FillNan has no input".into()))?;
            let fill_value = fill_nan
                .fill_value
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("FillNan has no fill_value".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;
            let fill_expr = build_expr(fill_value)?;
            Ok(lf.fill_nan(fill_expr))
        }
        Kind::Reverse(reverse) => {
            let input_node = reverse
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Reverse has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;
            Ok(lf.reverse())
        }
        Kind::Sample(sample) => {
            let input_node = sample
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Sample has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;
            let expr = if sample.is_fraction {
                col(PlSmallStr::from_static("*")).sample_frac(
                    lit(sample.value),
                    sample.with_replacement,
                    sample.shuffle,
                    sample.seed,
                )
            } else {
                col(PlSmallStr::from_static("*")).sample_n(
                    lit(sample.value as u64),
                    sample.with_replacement,
                    sample.shuffle,
                    sample.seed,
                )
            };
            Ok(lf.select(vec![expr]))
        }
        Kind::Unpivot(unpivot) => {
            let input_node = unpivot
                .input
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Unpivot has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_dfs)?;
            let args = UnpivotArgsDSL {
                on: if unpivot.on.is_empty() {
                    None
                } else {
                    Some(by_name(unpivot.on.iter().cloned(), true, false))
                },
                index: by_name(unpivot.index.iter().cloned(), true, false),
                variable_name: if unpivot.variable_name.is_empty() {
                    None
                } else {
                    Some(unpivot.variable_name.clone().into())
                },
                value_name: if unpivot.value_name.is_empty() {
                    None
                } else {
                    Some(unpivot.value_name.clone().into())
                },
            };
            Ok(lf.unpivot(args))
        }
        Kind::SqlQuery(sql_query) => {
            if sql_query.query.is_empty() {
                return Err(BridgeError::PlanSemantic("SqlQuery has empty query".into()));
            }
            if sql_query.table_names.len() != sql_query.inputs.len() {
                return Err(BridgeError::PlanSemantic(format!(
                    "SqlQuery table_names length {} does not match inputs length {}",
                    sql_query.table_names.len(),
                    sql_query.inputs.len()
                )));
            }

            let mut ctx = SQLContext::new();
            for (name, input) in sql_query.table_names.iter().zip(sql_query.inputs.iter()) {
                let lf = build_lazy_frame(input, input_dfs)?;
                ctx.register(name, lf);
            }

            ctx.execute(sql_query.query.as_str())
                .map_err(|e| BridgeError::Execution(format!("SQL execution failed: {}", e)))
        }
    }
}

pub fn build_expr(expr: &proto::Expr) -> Result<Expr, BridgeError> {
    use proto::expr::Kind;

    let kind = expr
        .kind
        .as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic("Expr has no kind".into()))?;

    if let Some(result) = expr_str::build_string_expr(kind) {
        return result;
    }
    if let Some(result) = expr_temporal::build_temporal_expr(kind) {
        return result;
    }

    match kind {
        Kind::Col(col) => Ok(polars::prelude::col(&col.name)),
        Kind::Lit(lit) => {
            use proto::literal::Value;
            let val = lit
                .value
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Literal has no value".into()))?;

            match val {
                Value::IntVal(v) => Ok(polars::prelude::lit(*v)),
                Value::FloatVal(v) => Ok(polars::prelude::lit(*v)),
                Value::BoolVal(v) => Ok(polars::prelude::lit(*v)),
                Value::StringVal(v) => Ok(polars::prelude::lit(v.as_str())),
                Value::NullVal(_) => Ok(polars::prelude::lit(NULL)),
            }
        }
        Kind::Binary(bin) => {
            let left = bin
                .left
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Binary has no left".into()))?;
            let right = bin
                .right
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Binary has no right".into()))?;

            let left_expr = build_expr(left)?;
            let right_expr = build_expr(right)?;

            use proto::BinaryOperator;
            match proto::BinaryOperator::try_from(bin.op) {
                Ok(BinaryOperator::Add) => Ok(left_expr + right_expr),
                Ok(BinaryOperator::Sub) => Ok(left_expr - right_expr),
                Ok(BinaryOperator::Mul) => Ok(left_expr * right_expr),
                Ok(BinaryOperator::Div) => Ok(left_expr / right_expr),
                Ok(BinaryOperator::Eq) => Ok(left_expr.eq(right_expr)),
                Ok(BinaryOperator::Ne) => Ok(left_expr.neq(right_expr)),
                Ok(BinaryOperator::Lt) => Ok(left_expr.lt(right_expr)),
                Ok(BinaryOperator::Le) => Ok(left_expr.lt_eq(right_expr)),
                Ok(BinaryOperator::Gt) => Ok(left_expr.gt(right_expr)),
                Ok(BinaryOperator::Ge) => Ok(left_expr.gt_eq(right_expr)),
                Ok(BinaryOperator::And) => Ok(left_expr.and(right_expr)),
                Ok(BinaryOperator::Or) => Ok(left_expr.or(right_expr)),
                Ok(BinaryOperator::Mod) => Ok(left_expr % right_expr),
                Ok(BinaryOperator::Pow) => Ok(left_expr.pow(right_expr)),
                Ok(BinaryOperator::Xor) => Ok(left_expr.xor(right_expr)),
                Err(_) => Err(BridgeError::Unsupported(format!(
                    "Unknown binary operator: {}",
                    bin.op
                ))),
            }
        }
        Kind::Alias(alias) => {
            let expr = alias
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Alias has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.alias(&alias.name))
        }
        Kind::IsNull(is_null) => {
            let expr = is_null
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("IsNull has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.is_null())
        }
        Kind::IsNotNull(is_not_null) => {
            let expr = is_not_null
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("IsNotNull has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.is_not_null())
        }
        Kind::Not(not) => {
            let expr = not
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Not has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.not())
        }
        Kind::Wildcard(_) => {
            // Wildcard 表示选择所有列
            Ok(polars::prelude::col("*"))
        }
        Kind::Exclude(exclude) => {
            let expr = exclude
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Exclude has no expr".into()))?;
            match expr.kind.as_ref() {
                Some(proto::expr::Kind::Wildcard(_)) => {
                    Ok(all().exclude_cols(exclude.columns.clone()).as_expr())
                }
                _ => Err(BridgeError::Unsupported(
                    "Exclude currently supports wildcard expressions only".into(),
                )),
            }
        }
        Kind::Cast(cast) => {
            // 处理类型转换
            let expr = cast
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Cast has no expr".into()))?;
            let e = build_expr(expr)?;

            // 将 proto DataType 转换为 Polars DataType
            let target_type = proto_data_type_to_polars(cast.data_type)?;

            // 根据 strict 参数选择 cast 或 strict_cast
            if cast.strict {
                Ok(e.strict_cast(target_type))
            } else {
                Ok(e.cast(target_type))
            }
        }
        Kind::Sum(agg) => build_agg_expr(agg, |e| e.sum(), "Sum"),
        Kind::Mean(agg) => build_agg_expr(agg, |e| e.mean(), "Mean"),
        Kind::Min(agg) => build_agg_expr(agg, |e| e.min(), "Min"),
        Kind::Max(agg) => build_agg_expr(agg, |e| e.max(), "Max"),
        Kind::Count(agg) => build_agg_expr(agg, |e| e.count(), "Count"),
        Kind::First(agg) => build_agg_expr(agg, |e| e.first(), "First"),
        Kind::Last(agg) => build_agg_expr(agg, |e| e.last(), "Last"),
        Kind::Len(agg) => build_agg_expr(agg, |e| e.len(), "Len"),
        Kind::NullCount(agg) => {
            let expr = agg
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("NullCount has no expr".into()))?;
            Ok(build_expr(expr)?.null_count())
        }
        Kind::NUnique(agg) => build_agg_expr(agg, |e| e.n_unique(), "NUnique"),
        Kind::Median(agg) => build_agg_expr(agg, |e| e.median(), "Median"),
        Kind::Std(agg) => build_agg_expr(agg, |e| e.std(1), "Std"),
        Kind::Var(agg) => build_agg_expr(agg, |e| e.var(1), "Var"),
        Kind::Quantile(quantile) => {
            let expr = quantile
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Quantile has no expr".into()))?;
            if !(0.0..=1.0).contains(&quantile.quantile) {
                return Err(BridgeError::InvalidArgument(format!(
                    "Quantile must be between 0 and 1, got {}",
                    quantile.quantile
                )));
            }
            let e = build_expr(expr)?;
            let method = match proto::QuantileMethod::try_from(quantile.method) {
                Ok(proto::QuantileMethod::QuantileNearest) => QuantileMethod::Nearest,
                Ok(proto::QuantileMethod::QuantileLower) => QuantileMethod::Lower,
                Ok(proto::QuantileMethod::QuantileHigher) => QuantileMethod::Higher,
                Ok(proto::QuantileMethod::QuantileMidpoint) => QuantileMethod::Midpoint,
                Ok(proto::QuantileMethod::QuantileLinear) => QuantileMethod::Linear,
                Ok(proto::QuantileMethod::QuantileEquiprobable) => QuantileMethod::Equiprobable,
                Err(_) => {
                    return Err(BridgeError::Unsupported(format!(
                        "Unknown quantile method: {}",
                        quantile.method
                    )))
                }
            };
            Ok(e.quantile(lit(quantile.quantile), method))
        }
        Kind::WhenThen(when_then) => {
            let predicate = when_then
                .predicate
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("WhenThen has no predicate".into()))?;
            let truthy = when_then
                .truthy
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("WhenThen has no truthy expr".into()))?;
            let falsy = when_then
                .falsy
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("WhenThen has no falsy expr".into()))?;

            let predicate_expr = build_expr(predicate)?;
            let truthy_expr = build_expr(truthy)?;
            let falsy_expr = build_expr(falsy)?;

            Ok(when(predicate_expr).then(truthy_expr).otherwise(falsy_expr))
        }
        Kind::MapBatches(map_batches) => {
            if map_batches.exprs.is_empty() {
                return Err(BridgeError::PlanSemantic(
                    "MapBatches requires at least one input expr".into(),
                ));
            }
            let exprs = map_batches
                .exprs
                .iter()
                .map(build_expr)
                .collect::<Result<Vec<_>, _>>()?;
            let udg_id = map_batches.udg_id;
            let output_dtype = proto_data_type_to_polars(map_batches.output_type)?;
            let output_dtype_for_udf = output_dtype.clone();
            let output_dtype_for_field = output_dtype.clone();

            let first_expr = exprs[0].clone();
            let additional_exprs = exprs[1..].to_vec();

            Ok(first_expr.map_many(
                move |columns| {
                    let output_name = columns
                        .first()
                        .map(|column| column.name().clone())
                        .unwrap_or_else(|| PlSmallStr::from_static("map_batches"));
                    let input_columns = columns.iter_mut().map(std::mem::take).collect::<Vec<_>>();
                    let input_df = DataFrame::new_infer_height(input_columns).map_err(|err| {
                        PolarsError::ComputeError(
                            format!("failed to build MapBatches input dataframe: {}", err).into(),
                        )
                    })?;
                    let output_df = call_go_expr_map_batches(udg_id, &input_df).map_err(|err| {
                        PolarsError::ComputeError(
                            format!("failed to execute Go Expr.MapBatches: {}", err).into(),
                        )
                    })?;

                    if output_df.width() != 1 {
                        return Err(PolarsError::ComputeError(
                            format!(
                                "Go Expr.MapBatches must return exactly one column, got {}",
                                output_df.width()
                            )
                            .into(),
                        ));
                    }

                    let mut columns = output_df.into_columns();
                    let mut output = columns.pop().ok_or_else(|| {
                        PolarsError::ComputeError("Go Expr.MapBatches returned no columns".into())
                    })?;

                    let must_cast = output
                        .dtype()
                        .matches_schema_type(&output_dtype_for_udf)
                        .map_err(|_| {
                            PolarsError::SchemaMismatch(
                                format!(
                                    "expected output type '{:?}', got '{:?}'",
                                    output_dtype_for_udf,
                                    output.dtype()
                                )
                                .into(),
                            )
                        })?;
                    if must_cast {
                        output = output.cast(&output_dtype_for_udf)?;
                    }
                    output.rename(output_name);
                    Ok(output)
                },
                &additional_exprs,
                move |_schema, fields| {
                    let name = fields
                        .first()
                        .map(|field| field.name().clone())
                        .unwrap_or_else(|| PlSmallStr::from_static("map_batches"));
                    Ok(Field::new(name, output_dtype_for_field.clone()))
                },
            ))
        }
        Kind::Over(window) => {
            let expr = window
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Over has no expr".into()))?;
            if window.partition_by.is_empty() && window.order_by.is_empty() {
                return Err(BridgeError::PlanSemantic(
                    "Over requires at least one partition_by or order_by expression".into(),
                ));
            }
            let e = build_expr(expr)?;
            let partition_by = window
                .partition_by
                .iter()
                .map(build_expr)
                .collect::<Result<Vec<_>, _>>()?;
            let order_by = window
                .order_by
                .iter()
                .map(build_expr)
                .collect::<Result<Vec<_>, _>>()?;

            if order_by.is_empty() {
                Ok(e.over(partition_by))
            } else {
                let sort_options = SortOptions {
                    descending: window.descending,
                    nulls_last: window.nulls_last,
                    ..Default::default()
                };
                e.over_with_options(
                    if partition_by.is_empty() {
                        None::<Vec<Expr>>
                    } else {
                        Some(partition_by)
                    },
                    Some((order_by, sort_options)),
                    Default::default(),
                )
                .map_err(|err| {
                    BridgeError::Execution(format!(
                        "Failed to build ordered window expression: {}",
                        err
                    ))
                })
            }
        }
        Kind::Rank(rank) => {
            let expr = rank
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Rank has no expr".into()))?;
            let e = build_expr(expr)?;
            let method = match proto::RankMethod::try_from(rank.method) {
                Ok(proto::RankMethod::RankAverage) => RankMethod::Average,
                Ok(proto::RankMethod::RankMin) => RankMethod::Min,
                Ok(proto::RankMethod::RankMax) => RankMethod::Max,
                Ok(proto::RankMethod::RankDense) => RankMethod::Dense,
                Ok(proto::RankMethod::RankOrdinal) => RankMethod::Ordinal,
                Err(_) => {
                    return Err(BridgeError::Unsupported(format!(
                        "Unknown rank method: {}",
                        rank.method
                    )))
                }
            };
            let options = RankOptions {
                method,
                descending: rank.descending,
            };
            Ok(e.rank(options, rank.seed))
        }
        Kind::CumSum(cum_sum) => {
            let expr = cum_sum
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("CumSum has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.cum_sum(cum_sum.reverse))
        }
        Kind::CumCount(cum_count) => {
            let expr = cum_count
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("CumCount has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.cum_count(cum_count.reverse))
        }
        Kind::CumMin(cum_min) => {
            let expr = cum_min
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("CumMin has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.cum_min(cum_min.reverse))
        }
        Kind::CumMax(cum_max) => {
            let expr = cum_max
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("CumMax has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.cum_max(cum_max.reverse))
        }
        Kind::CumProd(cum_prod) => {
            let expr = cum_prod
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("CumProd has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.cum_prod(cum_prod.reverse))
        }
        Kind::Shift(shift) => {
            let expr = shift
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Shift has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.shift(lit(shift.periods)))
        }
        Kind::Diff(diff) => {
            let expr = diff
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Diff has no expr".into()))?;
            let e = build_expr(expr)?;
            let null_behavior = match proto::DiffNullBehavior::try_from(diff.null_behavior) {
                Ok(proto::DiffNullBehavior::DiffNullIgnore) => NullBehavior::Ignore,
                Ok(proto::DiffNullBehavior::DiffNullDrop) => NullBehavior::Drop,
                Err(_) => {
                    return Err(BridgeError::Unsupported(format!(
                        "Unknown diff null behavior: {}",
                        diff.null_behavior
                    )))
                }
            };
            Ok(e.diff(lit(diff.periods), null_behavior))
        }
        Kind::ForwardFill(fill) => {
            let expr = fill
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("ForwardFill has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.fill_null_with_strategy(FillNullStrategy::Forward(
                fill.limit.map(|v| v as IdxSize),
            )))
        }
        Kind::FillNull(fill) => {
            let expr = fill
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("FillNull has no expr".into()))?;
            let fill_value = fill
                .fill_value
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("FillNull has no fill_value".into()))?;
            let e = build_expr(expr)?;
            let fv = build_expr(fill_value)?;
            Ok(e.fill_null(fv))
        }
        Kind::BackwardFill(fill) => {
            let expr = fill
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("BackwardFill has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.fill_null_with_strategy(FillNullStrategy::Backward(
                fill.limit.map(|v| v as IdxSize),
            )))
        }
        Kind::FillNan(fill) => {
            let expr = fill
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("FillNan has no expr".into()))?;
            let fill_value = fill
                .fill_value
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("FillNan has no fill_value".into()))?;
            let e = build_expr(expr)?;
            let fv = build_expr(fill_value)?;
            Ok(e.fill_nan(fv))
        }
        Kind::IsNan(is_nan) => {
            let expr = is_nan
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("IsNan has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.is_nan())
        }
        Kind::IsFinite(is_finite) => {
            let expr = is_finite
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("IsFinite has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.is_finite())
        }
        Kind::Abs(abs) => {
            let expr = abs
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Abs has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.abs())
        }
        Kind::Round(round) => {
            let expr = round
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Round has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.round(round.decimals, RoundMode::default()))
        }
        Kind::Sqrt(sqrt) => {
            let expr = sqrt
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Sqrt has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.sqrt())
        }
        Kind::Log(log) => {
            let expr = log
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Log has no expr".into()))?;
            let base = log
                .base
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Log has no base".into()))?;
            let e = build_expr(expr)?;
            let base_expr = build_expr(base)?;
            Ok(e.log(base_expr))
        }
        Kind::Clip(clip) => {
            let expr = clip
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Clip has no expr".into()))?;
            let min = clip
                .min
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Clip has no min".into()))?;
            let max = clip
                .max
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Clip has no max".into()))?;
            let e = build_expr(expr)?;
            let min_expr = build_expr(min)?;
            let max_expr = build_expr(max)?;
            Ok(e.clip(min_expr, max_expr))
        }
        Kind::ValueCounts(value_counts) => {
            let expr = value_counts
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("ValueCounts has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.value_counts(
                value_counts.sort,
                value_counts.parallel,
                value_counts.name.as_str(),
                value_counts.normalize,
            ))
        }
        Kind::IsDuplicated(is_duplicated) => {
            let expr = is_duplicated
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("IsDuplicated has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.is_duplicated())
        }
        Kind::RollingMin(rolling) => {
            build_rolling_expr(rolling, |e, opts| e.rolling_min(opts), "RollingMin")
        }
        Kind::RollingMax(rolling) => {
            build_rolling_expr(rolling, |e, opts| e.rolling_max(opts), "RollingMax")
        }
        Kind::RollingMean(rolling) => {
            build_rolling_expr(rolling, |e, opts| e.rolling_mean(opts), "RollingMean")
        }
        Kind::RollingSum(rolling) => {
            build_rolling_expr(rolling, |e, opts| e.rolling_sum(opts), "RollingSum")
        }
        Kind::Reverse(reverse) => {
            let expr = reverse
                .expr
                .as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Reverse has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.reverse())
        }
        _ => Err(BridgeError::Unsupported(
            "Expression type is not yet supported".into(),
        )),
    }
}

fn build_agg_expr<F>(agg: &proto::AggExpr, f: F, label: &str) -> Result<Expr, BridgeError>
where
    F: FnOnce(Expr) -> Expr,
{
    let expr = agg
        .expr
        .as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic(format!("{} has no expr", label)))?;
    Ok(f(build_expr(expr)?))
}

fn build_rolling_expr<F>(rolling: &proto::Rolling, f: F, label: &str) -> Result<Expr, BridgeError>
where
    F: FnOnce(Expr, RollingOptionsFixedWindow) -> Expr,
{
    let expr = rolling
        .expr
        .as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic(format!("{} has no expr", label)))?;
    let window_size = rolling.window_size as usize;
    let min_periods = rolling.min_periods as usize;
    if window_size == 0 {
        return Err(BridgeError::InvalidArgument(format!(
            "{} window_size must be > 0",
            label
        )));
    }
    if min_periods == 0 || min_periods > window_size {
        return Err(BridgeError::InvalidArgument(format!(
            "{} min_periods must be between 1 and window_size",
            label
        )));
    }
    let opts = RollingOptionsFixedWindow {
        window_size,
        min_periods,
        center: rolling.center,
        ..Default::default()
    };
    Ok(f(build_expr(expr)?, opts))
}

fn proto_data_type_to_polars(data_type: i32) -> Result<DataType, BridgeError> {
    match proto::DataType::try_from(data_type) {
        Ok(proto::DataType::Int64) => Ok(DataType::Int64),
        Ok(proto::DataType::Int32) => Ok(DataType::Int32),
        Ok(proto::DataType::Int16) => Ok(DataType::Int16),
        Ok(proto::DataType::Int8) => Ok(DataType::Int8),
        Ok(proto::DataType::Uint64) => Ok(DataType::UInt64),
        Ok(proto::DataType::Uint32) => Ok(DataType::UInt32),
        Ok(proto::DataType::Uint16) => Ok(DataType::UInt16),
        Ok(proto::DataType::Uint8) => Ok(DataType::UInt8),
        Ok(proto::DataType::Float64) => Ok(DataType::Float64),
        Ok(proto::DataType::Float32) => Ok(DataType::Float32),
        Ok(proto::DataType::Bool) => Ok(DataType::Boolean),
        Ok(proto::DataType::Utf8) => Ok(DataType::String),
        Ok(proto::DataType::Date) => Ok(DataType::Date),
        Ok(proto::DataType::Datetime) => Ok(DataType::Datetime(TimeUnit::Microseconds, None)),
        Ok(proto::DataType::Time) => Ok(DataType::Time),
        Err(_) => Err(BridgeError::Unsupported(format!(
            "Unknown data type: {}",
            data_type
        ))),
    }
}
