use polars::prelude::*;

use crate::error::BridgeError;
use crate::executor::build_expr;
use crate::proto;

pub fn build_temporal_expr(kind: &proto::expr::Kind) -> Option<Result<Expr, BridgeError>> {
    use proto::expr::Kind;

    match kind {
        Kind::DtYear(func) => Some(build_unary(func, "DtYear", |expr| expr.dt().year())),
        Kind::DtMonth(func) => Some(build_unary(func, "DtMonth", |expr| expr.dt().month())),
        Kind::DtDay(func) => Some(build_unary(func, "DtDay", |expr| expr.dt().day())),
        Kind::DtWeekday(func) => Some(build_unary(func, "DtWeekday", |expr| expr.dt().weekday())),
        Kind::DtHour(func) => Some(build_unary(func, "DtHour", |expr| expr.dt().hour())),
        Kind::DtMinute(func) => Some(build_unary(func, "DtMinute", |expr| expr.dt().minute())),
        Kind::DtSecond(func) => Some(build_unary(func, "DtSecond", |expr| expr.dt().second())),
        Kind::DtMonthStart(func) => Some(build_unary(func, "DtMonthStart", |expr| {
            expr.dt().month_start()
        })),
        Kind::DtMonthEnd(func) => Some(build_unary(func, "DtMonthEnd", |expr| {
            expr.dt().month_end()
        })),
        Kind::StrToDate(parse) => Some(build_str_to_date(parse)),
        Kind::StrToDatetime(parse) => Some(build_str_to_datetime(parse)),
        Kind::StrToTime(parse) => Some(build_str_to_time(parse)),
        _ => None,
    }
}

fn build_unary<F>(func: &proto::TemporalUnary, name: &str, op: F) -> Result<Expr, BridgeError>
where
    F: FnOnce(Expr) -> Expr,
{
    let expr = build_inner_expr(&func.expr, name)?;
    Ok(op(expr))
}

fn build_str_to_date(parse: &proto::TemporalParse) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&parse.expr, "StrToDate")?;
    let options = StrptimeOptions {
        format: Some(parse.format.clone().into()),
        strict: true,
        exact: true,
        cache: true,
    };
    Ok(expr.str().to_date(options))
}

fn build_str_to_datetime(parse: &proto::TemporalParse) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&parse.expr, "StrToDatetime")?;
    let options = StrptimeOptions {
        format: Some(parse.format.clone().into()),
        strict: true,
        exact: true,
        cache: true,
    };
    Ok(expr.str().to_datetime(None, None, options, lit("raise")))
}

fn build_str_to_time(parse: &proto::TemporalParse) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&parse.expr, "StrToTime")?;
    let options = StrptimeOptions {
        format: Some(parse.format.clone().into()),
        strict: true,
        exact: true,
        cache: true,
    };
    Ok(expr.str().to_time(options))
}

fn build_inner_expr(expr: &Option<Box<proto::Expr>>, name: &str) -> Result<Expr, BridgeError> {
    let expr = expr
        .as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic(format!("{name} has no expr")))?;
    build_expr(expr.as_ref())
}
