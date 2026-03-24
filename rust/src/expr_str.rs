use polars::prelude::*;

use crate::error::BridgeError;
use crate::executor::build_expr;
use crate::proto;

pub fn build_string_expr(kind: &proto::expr::Kind) -> Option<Result<Expr, BridgeError>> {
    use proto::expr::Kind;

    match kind {
        Kind::StrLenBytes(func) => Some(build_unary(func, "StrLenBytes", |expr| {
            expr.str().len_bytes()
        })),
        Kind::StrLenChars(func) => Some(build_unary(func, "StrLenChars", |expr| {
            expr.str().len_chars()
        })),
        Kind::StrContains(contains) => Some(build_contains(contains)),
        Kind::StrStartsWith(starts) => Some(build_starts_with(starts)),
        Kind::StrEndsWith(ends) => Some(build_ends_with(ends)),
        Kind::StrExtract(extract) => Some(build_extract(extract)),
        Kind::StrReplace(replace) => Some(build_replace(replace)),
        Kind::StrReplaceAll(replace) => Some(build_replace_all(replace)),
        Kind::StrToLowercase(func) => Some(build_unary(func, "StrToLowercase", |expr| {
            expr.str().to_lowercase()
        })),
        Kind::StrToUppercase(func) => Some(build_unary(func, "StrToUppercase", |expr| {
            expr.str().to_uppercase()
        })),
        Kind::StrStripChars(strip) => Some(build_strip_chars(strip)),
        Kind::StrSlice(slice) => Some(build_slice(slice)),
        Kind::StrSplit(split) => Some(build_split(split)),
        Kind::StrPadStart(pad) => Some(build_pad_start(pad)),
        Kind::StrPadEnd(pad) => Some(build_pad_end(pad)),
        Kind::StrStripPrefix(affix) => Some(build_strip_prefix(affix)),
        Kind::StrStripSuffix(affix) => Some(build_strip_suffix(affix)),
        Kind::StrExtractAll(pattern) => Some(build_extract_all(pattern)),
        Kind::StrCountMatches(pattern) => Some(build_count_matches(pattern)),
        Kind::StrReplaceN(replace) => Some(build_replace_n(replace)),
        _ => None,
    }
}

fn build_unary<F>(func: &proto::StringFunction, name: &str, op: F) -> Result<Expr, BridgeError>
where
    F: FnOnce(Expr) -> Expr,
{
    let expr = build_inner_expr(&func.expr, name)?;
    Ok(op(expr))
}

fn build_contains(contains: &proto::StringContains) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&contains.expr, "StrContains")?;
    let pattern = lit(contains.pattern.as_str());

    if contains.literal {
        Ok(expr.str().contains_literal(pattern))
    } else {
        Ok(expr.str().contains(pattern, true))
    }
}

fn build_starts_with(starts: &proto::StringStartsWith) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&starts.expr, "StrStartsWith")?;
    Ok(expr.str().starts_with(lit(starts.prefix.as_str())))
}

fn build_ends_with(ends: &proto::StringEndsWith) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&ends.expr, "StrEndsWith")?;
    Ok(expr.str().ends_with(lit(ends.suffix.as_str())))
}

fn build_extract(extract: &proto::StringExtract) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&extract.expr, "StrExtract")?;
    Ok(expr
        .str()
        .extract(lit(extract.pattern.as_str()), extract.group_index as usize))
}

fn build_replace(replace: &proto::StringReplace) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&replace.expr, "StrReplace")?;
    Ok(expr.str().replace(
        lit(replace.pattern.as_str()),
        lit(replace.value.as_str()),
        replace.literal,
    ))
}

fn build_replace_all(replace: &proto::StringReplace) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&replace.expr, "StrReplaceAll")?;
    Ok(expr.str().replace_all(
        lit(replace.pattern.as_str()),
        lit(replace.value.as_str()),
        replace.literal,
    ))
}

fn build_strip_chars(strip: &proto::StringStripChars) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&strip.expr, "StrStripChars")?;
    let matches_expr = if strip.chars.is_empty() {
        lit(NULL).cast(DataType::String)
    } else {
        lit(strip.chars.as_str())
    };

    Ok(expr.str().strip_chars(matches_expr))
}

fn build_slice(slice: &proto::StringSlice) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&slice.expr, "StrSlice")?;
    let offset = lit(slice.offset);
    let length = match slice.length {
        Some(length) => lit(length),
        None => lit(NULL).cast(DataType::UInt64),
    };

    Ok(expr.str().slice(offset, length))
}

fn build_split(split: &proto::StringSplit) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&split.expr, "StrSplit")?;
    Ok(expr.str().split(lit(split.by.as_str())))
}

fn build_pad_start(pad: &proto::StringPad) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&pad.expr, "StrPadStart")?;
    let fill_char = parse_fill_char(&pad.fill_char, "StrPadStart")?;
    Ok(expr.str().pad_start(lit(pad.length), fill_char))
}

fn build_pad_end(pad: &proto::StringPad) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&pad.expr, "StrPadEnd")?;
    let fill_char = parse_fill_char(&pad.fill_char, "StrPadEnd")?;
    Ok(expr.str().pad_end(lit(pad.length), fill_char))
}

fn build_strip_prefix(affix: &proto::StringAffix) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&affix.expr, "StrStripPrefix")?;
    Ok(expr.str().strip_prefix(lit(affix.value.as_str())))
}

fn build_strip_suffix(affix: &proto::StringAffix) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&affix.expr, "StrStripSuffix")?;
    Ok(expr.str().strip_suffix(lit(affix.value.as_str())))
}

fn build_extract_all(pattern: &proto::StringPattern) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&pattern.expr, "StrExtractAll")?;
    Ok(expr.str().extract_all(lit(pattern.pattern.as_str())))
}

fn build_count_matches(pattern: &proto::StringContains) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&pattern.expr, "StrCountMatches")?;
    Ok(expr
        .str()
        .count_matches(lit(pattern.pattern.as_str()), pattern.literal))
}

fn build_replace_n(replace: &proto::StringReplaceN) -> Result<Expr, BridgeError> {
    let expr = build_inner_expr(&replace.expr, "StrReplaceN")?;
    Ok(expr.str().replace_n(
        lit(replace.pattern.as_str()),
        lit(replace.value.as_str()),
        replace.literal,
        replace.n,
    ))
}

fn build_inner_expr(expr: &Option<Box<proto::Expr>>, name: &str) -> Result<Expr, BridgeError> {
    let expr = expr
        .as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic(format!("{name} has no expr")))?;
    build_expr(expr.as_ref())
}

fn parse_fill_char(value: &str, name: &str) -> Result<char, BridgeError> {
    let mut chars = value.chars();
    let fill_char = chars
        .next()
        .ok_or_else(|| BridgeError::InvalidArgument(format!("{name} fill_char cannot be empty")))?;

    if chars.next().is_some() {
        return Err(BridgeError::InvalidArgument(format!(
            "{name} fill_char must be a single character"
        )));
    }

    Ok(fill_char)
}
