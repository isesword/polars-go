use crate::error::BridgeError;
use crate::proto;
use polars::datatypes::{DataType, Field, TimeUnit};
use polars::prelude::{AnyValue, DataFrame, NamedFrom, PlSmallStr, Series};
use polars_core::utils::any_values_to_supertype;
use std::collections::HashMap;
use std::env;
use std::time::{Duration, Instant};

const DEFAULT_INFER_SCHEMA_LENGTH: usize = 100;

#[derive(Clone)]
struct SelectedField {
    name: String,
    dtype: Option<DataType>,
}

pub fn dataframe_from_rows_payload(payload: proto::RowsPayload) -> Result<DataFrame, BridgeError> {
    let profile_enabled = rows_import_profile_enabled();
    let total_started = Instant::now();
    let selected_started = Instant::now();
    let options = payload.options.unwrap_or_default();
    let selected_fields = selected_fields(&options, &payload.row_order)?;
    let selected_elapsed = selected_started.elapsed();

    if let Some(df) = try_dataframe_from_rows_typed_fast_path(
        &payload.rows,
        &payload.row_order,
        &selected_fields,
        &options,
        selected_elapsed,
        total_started,
        profile_enabled,
    )? {
        return Ok(df);
    }

    let decode_columns_started = Instant::now();
    let column_values =
        decode_rows_to_columns(&payload.rows, &payload.row_order, &selected_fields)?;
    let decode_columns_elapsed = decode_columns_started.elapsed();

    if payload.rows.is_empty() && selected_fields.is_empty() {
        if options.schema_overrides.is_empty() {
            return Err(BridgeError::Execution(
                "no data, cannot infer schema".to_string(),
            ));
        }
        return DataFrame::new(0, vec![]).map_err(|e| {
            BridgeError::Execution(format!("failed to create empty dataframe: {}", e))
        });
    }

    let infer_len = normalize_infer_schema_length(options.infer_schema_length, payload.rows.len());
    let mut columns = Vec::with_capacity(selected_fields.len());
    let build_started = Instant::now();
    let mut resolve_dtype_elapsed = Duration::ZERO;
    let mut coerce_elapsed = Duration::ZERO;
    let mut series_elapsed = Duration::ZERO;
    for (idx, field) in selected_fields.iter().enumerate() {
        let values = &column_values[idx];
        let resolve_started = Instant::now();
        let dtype = resolved_dtype(field, &options, values, infer_len)?;
        resolve_dtype_elapsed += resolve_started.elapsed();

        let coerce_started = Instant::now();
        let coerced_values = coerce_values_for_dtype(
            values.as_slice(),
            &dtype,
            options.strict,
            field.name.as_str(),
        )?;
        coerce_elapsed += coerce_started.elapsed();

        let series_started = Instant::now();
        let series = Series::from_any_values_and_dtype(
            field.name.clone().into(),
            coerced_values.as_slice(),
            &dtype,
            true,
        )
        .map_err(|e| {
            BridgeError::Execution(format!(
                "failed to create series for column {:?}: {}",
                field.name, e
            ))
        })?;
        series_elapsed += series_started.elapsed();
        columns.push(series.into());
    }
    drop(column_values);
    let build_elapsed = build_started.elapsed();

    let dataframe_started = Instant::now();
    let df = DataFrame::new_infer_height(columns).map_err(|e| {
        BridgeError::Execution(format!("failed to create dataframe from rows: {}", e))
    })?;
    let dataframe_elapsed = dataframe_started.elapsed();

    if profile_enabled {
        eprintln!(
            "[rows-import] rows={} cols={} selected={} decode_to_columns={} build_columns={} resolve_dtype={} coerce={} series={} dataframe_new={} total={}",
            payload.rows.len(),
            df.width(),
            fmt_duration(selected_elapsed),
            fmt_duration(decode_columns_elapsed),
            fmt_duration(build_elapsed),
            fmt_duration(resolve_dtype_elapsed),
            fmt_duration(coerce_elapsed),
            fmt_duration(series_elapsed),
            fmt_duration(dataframe_elapsed),
            fmt_duration(total_started.elapsed()),
        );
    }

    Ok(df)
}

fn try_dataframe_from_rows_typed_fast_path(
    rows: &[proto::RowRecord],
    row_order: &[String],
    selected_fields: &[SelectedField],
    options: &proto::RowsImportOptions,
    selected_elapsed: Duration,
    total_started: Instant,
    profile_enabled: bool,
) -> Result<Option<DataFrame>, BridgeError> {
    if !options.strict {
        return Ok(None);
    }
    if selected_fields.is_empty() {
        return Ok(None);
    }

    let mut builders = Vec::with_capacity(selected_fields.len());
    for field in selected_fields {
        let Some(dtype) = explicit_dtype(field, options) else {
            return Ok(None);
        };
        let Some(builder) = FastSeriesBuilder::new(field.name.clone(), dtype) else {
            return Ok(None);
        };
        builders.push(builder);
    }

    let build_started = Instant::now();
    let row_index = row_order
        .iter()
        .enumerate()
        .map(|(idx, name)| (name.as_str(), idx))
        .collect::<HashMap<_, _>>();
    let selected_positions = selected_fields
        .iter()
        .map(|field| row_index.get(field.name.as_str()).copied())
        .collect::<Vec<_>>();

    for row in rows {
        if row.values.len() > row_order.len() {
            return Err(BridgeError::InvalidArgument(format!(
                "row has {} values but row order only defines {} columns",
                row.values.len(),
                row_order.len()
            )));
        }

        for (builder, position) in builders.iter_mut().zip(selected_positions.iter()) {
            match position.and_then(|idx| row.values.get(idx)) {
                Some(value) => builder.push_value(value)?,
                None => builder.push_null(),
            }
        }
    }

    let columns = builders
        .into_iter()
        .map(|builder| builder.finish().map(Into::into))
        .collect::<Result<Vec<_>, BridgeError>>()?;
    let build_elapsed = build_started.elapsed();

    let dataframe_started = Instant::now();
    let df = DataFrame::new_infer_height(columns).map_err(|e| {
        BridgeError::Execution(format!("failed to create dataframe from rows: {}", e))
    })?;
    let dataframe_elapsed = dataframe_started.elapsed();

    if profile_enabled {
        eprintln!(
            "[rows-import] rows={} cols={} selected={} fast_path_build={} dataframe_new={} total={} mode=typed",
            rows.len(),
            df.width(),
            fmt_duration(selected_elapsed),
            fmt_duration(build_elapsed),
            fmt_duration(dataframe_elapsed),
            fmt_duration(total_started.elapsed()),
        );
    }

    Ok(Some(df))
}

fn selected_fields(
    options: &proto::RowsImportOptions,
    row_order: &[String],
) -> Result<Vec<SelectedField>, BridgeError> {
    if !options.schema_fields.is_empty() {
        return options
            .schema_fields
            .iter()
            .map(|field| {
                Ok(SelectedField {
                    name: field.name.clone(),
                    dtype: if field.has_dtype {
                        Some(proto_data_type_to_polars(field.dtype)?)
                    } else {
                        None
                    },
                })
            })
            .collect();
    }

    Ok(row_order
        .iter()
        .cloned()
        .map(|name| SelectedField { name, dtype: None })
        .collect())
}

fn explicit_dtype(field: &SelectedField, options: &proto::RowsImportOptions) -> Option<DataType> {
    if let Some(dtype) = options.schema_overrides.get(field.name.as_str()) {
        return proto_data_type_to_polars(*dtype).ok();
    }
    field.dtype.clone()
}

fn normalize_infer_schema_length(value: Option<u64>, row_count: usize) -> usize {
    match value {
        Some(0) => row_count,
        Some(v) => usize::min(v as usize, row_count),
        None => usize::min(DEFAULT_INFER_SCHEMA_LENGTH, row_count),
    }
}

fn resolved_dtype(
    field: &SelectedField,
    options: &proto::RowsImportOptions,
    values: &[AnyValue<'static>],
    infer_len: usize,
) -> Result<DataType, BridgeError> {
    if let Some(dtype) = options.schema_overrides.get(field.name.as_str()) {
        return proto_data_type_to_polars(*dtype);
    }
    if let Some(dtype) = &field.dtype {
        return Ok(dtype.clone());
    }
    if values.is_empty() || infer_len == 0 {
        return Ok(DataType::Null);
    }

    any_values_to_supertype(values.iter().take(infer_len)).map_err(|e| {
        BridgeError::Execution(format!(
            "failed to infer schema for column {:?}: {}",
            field.name, e
        ))
    })
}

fn coerce_values_for_dtype(
    values: &[AnyValue<'static>],
    dtype: &DataType,
    strict: bool,
    column_name: &str,
) -> Result<Vec<AnyValue<'static>>, BridgeError> {
    values
        .iter()
        .map(|value| coerce_value_for_dtype(value, dtype, strict, column_name))
        .collect()
}

fn coerce_value_for_dtype(
    value: &AnyValue<'static>,
    dtype: &DataType,
    strict: bool,
    column_name: &str,
) -> Result<AnyValue<'static>, BridgeError> {
    if value.is_null() {
        return Ok(AnyValue::Null);
    }

    if strict {
        return value
            .try_strict_cast(dtype)
            .map(|v| v.into_static())
            .map_err(|e| {
                BridgeError::Execution(format!(
                    "failed to coerce column {:?} value {:?} to {:?}: {}",
                    column_name, value, dtype, e
                ))
            });
    }

    Ok(value.cast(dtype).into_static())
}

fn decode_rows_to_columns(
    rows: &[proto::RowRecord],
    row_order: &[String],
    selected_fields: &[SelectedField],
) -> Result<Vec<Vec<AnyValue<'static>>>, BridgeError> {
    let mut columns = selected_fields
        .iter()
        .map(|_| Vec::with_capacity(rows.len()))
        .collect::<Vec<_>>();
    if selected_fields.is_empty() {
        return Ok(columns);
    }

    let row_index = row_order
        .iter()
        .enumerate()
        .map(|(idx, name)| (name.as_str(), idx))
        .collect::<HashMap<_, _>>();
    let selected_positions = selected_fields
        .iter()
        .map(|field| row_index.get(field.name.as_str()).copied())
        .collect::<Vec<_>>();

    for row in rows {
        if row.values.len() > row_order.len() {
            return Err(BridgeError::InvalidArgument(format!(
                "row has {} values but row order only defines {} columns",
                row.values.len(),
                row_order.len()
            )));
        }

        for (column_idx, position) in selected_positions.iter().enumerate() {
            let value = match position.and_then(|idx| row.values.get(idx)) {
                Some(value) => decode_row_value(value)?,
                None => AnyValue::Null,
            };
            columns[column_idx].push(value);
        }
    }

    Ok(columns)
}

fn decode_row_value(value: &proto::RowValue) -> Result<AnyValue<'static>, BridgeError> {
    use proto::row_value::Kind;

    match value.kind.as_ref() {
        Some(Kind::NullVal(_)) | None => Ok(AnyValue::Null),
        Some(Kind::BoolVal(v)) => Ok(AnyValue::Boolean(*v)),
        Some(Kind::IntVal(v)) => Ok(AnyValue::Int64(*v)),
        Some(Kind::UintVal(v)) => Ok(AnyValue::UInt64(*v)),
        Some(Kind::FloatVal(v)) => Ok(AnyValue::Float64(*v)),
        Some(Kind::StringVal(v)) => Ok(AnyValue::StringOwned(v.clone().into())),
        Some(Kind::BytesVal(v)) => Ok(AnyValue::BinaryOwned(v.clone())),
        Some(Kind::DateVal(v)) => Ok(AnyValue::Date(*v)),
        Some(Kind::DatetimeVal(v)) => Ok(AnyValue::DatetimeOwned(*v, TimeUnit::Microseconds, None)),
        Some(Kind::TimeVal(v)) => Ok(AnyValue::Time(*v)),
        Some(Kind::ListVal(list)) => decode_list_value(list),
        Some(Kind::StructVal(strct)) => decode_struct_value(strct),
    }
}

enum FastSeriesBuilder {
    Bool {
        name: String,
        values: Vec<Option<bool>>,
    },
    Int64 {
        name: String,
        values: Vec<Option<i64>>,
    },
    Int32 {
        name: String,
        values: Vec<Option<i32>>,
    },
    Int16 {
        name: String,
        values: Vec<Option<i16>>,
    },
    Int8 {
        name: String,
        values: Vec<Option<i8>>,
    },
    UInt64 {
        name: String,
        values: Vec<Option<u64>>,
    },
    UInt32 {
        name: String,
        values: Vec<Option<u32>>,
    },
    UInt16 {
        name: String,
        values: Vec<Option<u32>>,
    },
    UInt8 {
        name: String,
        values: Vec<Option<u32>>,
    },
    Float64 {
        name: String,
        values: Vec<Option<f64>>,
    },
    Float32 {
        name: String,
        values: Vec<Option<f32>>,
    },
    String {
        name: String,
        values: Vec<Option<String>>,
    },
    Date {
        name: String,
        values: Vec<Option<i32>>,
    },
    Datetime {
        name: String,
        values: Vec<Option<i64>>,
    },
    Time {
        name: String,
        values: Vec<Option<i64>>,
    },
}

impl FastSeriesBuilder {
    fn new(name: String, dtype: DataType) -> Option<Self> {
        Some(match dtype {
            DataType::Boolean => Self::Bool {
                name,
                values: Vec::new(),
            },
            DataType::Int64 => Self::Int64 {
                name,
                values: Vec::new(),
            },
            DataType::Int32 => Self::Int32 {
                name,
                values: Vec::new(),
            },
            DataType::Int16 => Self::Int16 {
                name,
                values: Vec::new(),
            },
            DataType::Int8 => Self::Int8 {
                name,
                values: Vec::new(),
            },
            DataType::UInt64 => Self::UInt64 {
                name,
                values: Vec::new(),
            },
            DataType::UInt32 => Self::UInt32 {
                name,
                values: Vec::new(),
            },
            DataType::UInt16 => Self::UInt16 {
                name,
                values: Vec::new(),
            },
            DataType::UInt8 => Self::UInt8 {
                name,
                values: Vec::new(),
            },
            DataType::Float64 => Self::Float64 {
                name,
                values: Vec::new(),
            },
            DataType::Float32 => Self::Float32 {
                name,
                values: Vec::new(),
            },
            DataType::String => Self::String {
                name,
                values: Vec::new(),
            },
            DataType::Date => Self::Date {
                name,
                values: Vec::new(),
            },
            DataType::Datetime(TimeUnit::Microseconds, None) => Self::Datetime {
                name,
                values: Vec::new(),
            },
            DataType::Time => Self::Time {
                name,
                values: Vec::new(),
            },
            _ => return None,
        })
    }

    fn push_null(&mut self) {
        match self {
            Self::Bool { values, .. } => values.push(None),
            Self::Int64 { values, .. } => values.push(None),
            Self::Int32 { values, .. } => values.push(None),
            Self::Int16 { values, .. } => values.push(None),
            Self::Int8 { values, .. } => values.push(None),
            Self::UInt64 { values, .. } => values.push(None),
            Self::UInt32 { values, .. } => values.push(None),
            Self::UInt16 { values, .. } => values.push(None),
            Self::UInt8 { values, .. } => values.push(None),
            Self::Float64 { values, .. } => values.push(None),
            Self::Float32 { values, .. } => values.push(None),
            Self::String { values, .. } => values.push(None),
            Self::Date { values, .. } => values.push(None),
            Self::Datetime { values, .. } => values.push(None),
            Self::Time { values, .. } => values.push(None),
        }
    }

    fn push_value(&mut self, value: &proto::RowValue) -> Result<(), BridgeError> {
        match self {
            Self::Bool { name, values } => values.push(decode_bool_value(value, name.as_str())?),
            Self::Int64 { name, values } => values.push(decode_int64_value(value, name.as_str())?),
            Self::Int32 { name, values } => values.push(decode_int32_value(value, name.as_str())?),
            Self::Int16 { name, values } => values.push(decode_int16_value(value, name.as_str())?),
            Self::Int8 { name, values } => values.push(decode_int8_value(value, name.as_str())?),
            Self::UInt64 { name, values } => {
                values.push(decode_uint64_value(value, name.as_str())?)
            }
            Self::UInt32 { name, values } => {
                values.push(decode_uint32_value(value, name.as_str())?)
            }
            Self::UInt16 { name, values } => {
                values.push(decode_uint16_value(value, name.as_str())?.map(u32::from))
            }
            Self::UInt8 { name, values } => {
                values.push(decode_uint8_value(value, name.as_str())?.map(u32::from))
            }
            Self::Float64 { name, values } => {
                values.push(decode_float64_value(value, name.as_str())?)
            }
            Self::Float32 { name, values } => {
                values.push(decode_float32_value(value, name.as_str())?)
            }
            Self::String { name, values } => {
                values.push(decode_string_value(value, name.as_str())?)
            }
            Self::Date { name, values } => values.push(decode_date_value(value, name.as_str())?),
            Self::Datetime { name, values } => {
                values.push(decode_datetime_value(value, name.as_str())?)
            }
            Self::Time { name, values } => values.push(decode_time_value(value, name.as_str())?),
        }
        Ok(())
    }

    fn finish(self) -> Result<Series, BridgeError> {
        match self {
            Self::Bool { name, values } => Ok(Series::new(name.into(), values.as_slice())),
            Self::Int64 { name, values } => Ok(Series::new(name.into(), values.as_slice())),
            Self::Int32 { name, values } => Ok(Series::new(name.into(), values.as_slice())),
            Self::Int16 { name, values } => Ok(Series::new(name.into(), values.as_slice())),
            Self::Int8 { name, values } => Ok(Series::new(name.into(), values.as_slice())),
            Self::UInt64 { name, values } => Ok(Series::new(name.into(), values.as_slice())),
            Self::UInt32 { name, values } => Ok(Series::new(name.into(), values.as_slice())),
            Self::UInt16 { name, values } => Series::new(name.clone().into(), values.as_slice())
                .cast(&DataType::UInt16)
                .map_err(|e| {
                    BridgeError::Execution(format!(
                        "failed to create typed uint16 series for column {:?}: {}",
                        name, e
                    ))
                }),
            Self::UInt8 { name, values } => Series::new(name.clone().into(), values.as_slice())
                .cast(&DataType::UInt8)
                .map_err(|e| {
                    BridgeError::Execution(format!(
                        "failed to create typed uint8 series for column {:?}: {}",
                        name, e
                    ))
                }),
            Self::Float64 { name, values } => Ok(Series::new(name.into(), values.as_slice())),
            Self::Float32 { name, values } => Ok(Series::new(name.into(), values.as_slice())),
            Self::String { name, values } => Ok(Series::new(name.into(), values.as_slice())),
            Self::Date { name, values } => Series::new(name.clone().into(), values.as_slice())
                .cast(&DataType::Date)
                .map_err(|e| {
                    BridgeError::Execution(format!(
                        "failed to create typed date series for column {:?}: {}",
                        name, e
                    ))
                }),
            Self::Datetime { name, values } => Series::new(name.clone().into(), values.as_slice())
                .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
                .map_err(|e| {
                    BridgeError::Execution(format!(
                        "failed to create typed datetime series for column {:?}: {}",
                        name, e
                    ))
                }),
            Self::Time { name, values } => Series::new(name.clone().into(), values.as_slice())
                .cast(&DataType::Time)
                .map_err(|e| {
                    BridgeError::Execution(format!(
                        "failed to create typed time series for column {:?}: {}",
                        name, e
                    ))
                }),
        }
    }
}

fn decode_bool_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<bool>, BridgeError> {
    use proto::row_value::Kind;

    match value.kind.as_ref() {
        Some(Kind::NullVal(_)) | None => Ok(None),
        Some(Kind::BoolVal(v)) => Ok(Some(*v)),
        _ => Err(type_mismatch(column_name, "Bool", value)),
    }
}

fn decode_int64_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<i64>, BridgeError> {
    use proto::row_value::Kind;

    match value.kind.as_ref() {
        Some(Kind::NullVal(_)) | None => Ok(None),
        Some(Kind::IntVal(v)) => Ok(Some(*v)),
        Some(Kind::UintVal(v)) => i64::try_from(*v)
            .map(Some)
            .map_err(|_| type_mismatch(column_name, "Int64", value)),
        Some(Kind::StringVal(v)) => v
            .parse::<i64>()
            .map(Some)
            .map_err(|_| type_mismatch(column_name, "Int64", value)),
        _ => Err(type_mismatch(column_name, "Int64", value)),
    }
}

fn decode_int32_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<i32>, BridgeError> {
    decode_int64_value(value, column_name)?
        .map(i32::try_from)
        .transpose()
        .map_err(|_| type_mismatch(column_name, "Int32", value))
}

fn decode_int16_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<i16>, BridgeError> {
    decode_int64_value(value, column_name)?
        .map(i16::try_from)
        .transpose()
        .map_err(|_| type_mismatch(column_name, "Int16", value))
}

fn decode_int8_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<i8>, BridgeError> {
    decode_int64_value(value, column_name)?
        .map(i8::try_from)
        .transpose()
        .map_err(|_| type_mismatch(column_name, "Int8", value))
}

fn decode_uint64_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<u64>, BridgeError> {
    use proto::row_value::Kind;

    match value.kind.as_ref() {
        Some(Kind::NullVal(_)) | None => Ok(None),
        Some(Kind::UintVal(v)) => Ok(Some(*v)),
        Some(Kind::IntVal(v)) if *v >= 0 => Ok(Some(*v as u64)),
        Some(Kind::StringVal(v)) => v
            .parse::<u64>()
            .map(Some)
            .map_err(|_| type_mismatch(column_name, "UInt64", value)),
        _ => Err(type_mismatch(column_name, "UInt64", value)),
    }
}

fn decode_uint32_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<u32>, BridgeError> {
    decode_uint64_value(value, column_name)?
        .map(u32::try_from)
        .transpose()
        .map_err(|_| type_mismatch(column_name, "UInt32", value))
}

fn decode_uint16_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<u16>, BridgeError> {
    decode_uint64_value(value, column_name)?
        .map(u16::try_from)
        .transpose()
        .map_err(|_| type_mismatch(column_name, "UInt16", value))
}

fn decode_uint8_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<u8>, BridgeError> {
    decode_uint64_value(value, column_name)?
        .map(u8::try_from)
        .transpose()
        .map_err(|_| type_mismatch(column_name, "UInt8", value))
}

fn decode_float64_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<f64>, BridgeError> {
    use proto::row_value::Kind;

    match value.kind.as_ref() {
        Some(Kind::NullVal(_)) | None => Ok(None),
        Some(Kind::FloatVal(v)) => Ok(Some(*v)),
        Some(Kind::IntVal(v)) => Ok(Some(*v as f64)),
        Some(Kind::UintVal(v)) => Ok(Some(*v as f64)),
        Some(Kind::StringVal(v)) => v
            .parse::<f64>()
            .map(Some)
            .map_err(|_| type_mismatch(column_name, "Float64", value)),
        _ => Err(type_mismatch(column_name, "Float64", value)),
    }
}

fn decode_float32_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<f32>, BridgeError> {
    Ok(decode_float64_value(value, column_name)?.map(|v| v as f32))
}

fn decode_string_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<String>, BridgeError> {
    use proto::row_value::Kind;

    match value.kind.as_ref() {
        Some(Kind::NullVal(_)) | None => Ok(None),
        Some(Kind::StringVal(v)) => Ok(Some(v.clone())),
        Some(Kind::BoolVal(v)) => Ok(Some(v.to_string())),
        Some(Kind::IntVal(v)) => Ok(Some(v.to_string())),
        Some(Kind::UintVal(v)) => Ok(Some(v.to_string())),
        Some(Kind::FloatVal(v)) => Ok(Some(v.to_string())),
        Some(Kind::DateVal(v)) => Ok(Some(v.to_string())),
        Some(Kind::DatetimeVal(v)) => Ok(Some(v.to_string())),
        Some(Kind::TimeVal(v)) => Ok(Some(v.to_string())),
        _ => Err(type_mismatch(column_name, "Utf8", value)),
    }
}

fn decode_date_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<i32>, BridgeError> {
    use proto::row_value::Kind;

    match value.kind.as_ref() {
        Some(Kind::NullVal(_)) | None => Ok(None),
        Some(Kind::DateVal(v)) => Ok(Some(*v)),
        Some(Kind::IntVal(v)) => i32::try_from(*v)
            .map(Some)
            .map_err(|_| type_mismatch(column_name, "Date", value)),
        _ => Err(type_mismatch(column_name, "Date", value)),
    }
}

fn decode_datetime_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<i64>, BridgeError> {
    use proto::row_value::Kind;

    match value.kind.as_ref() {
        Some(Kind::NullVal(_)) | None => Ok(None),
        Some(Kind::DatetimeVal(v)) => Ok(Some(*v)),
        Some(Kind::IntVal(v)) => Ok(Some(*v)),
        Some(Kind::UintVal(v)) => i64::try_from(*v)
            .map(Some)
            .map_err(|_| type_mismatch(column_name, "Datetime", value)),
        _ => Err(type_mismatch(column_name, "Datetime", value)),
    }
}

fn decode_time_value(
    value: &proto::RowValue,
    column_name: &str,
) -> Result<Option<i64>, BridgeError> {
    use proto::row_value::Kind;

    match value.kind.as_ref() {
        Some(Kind::NullVal(_)) | None => Ok(None),
        Some(Kind::TimeVal(v)) => Ok(Some(*v)),
        Some(Kind::IntVal(v)) => Ok(Some(*v)),
        Some(Kind::UintVal(v)) => i64::try_from(*v)
            .map(Some)
            .map_err(|_| type_mismatch(column_name, "Time", value)),
        _ => Err(type_mismatch(column_name, "Time", value)),
    }
}

fn type_mismatch(column_name: &str, expected: &str, value: &proto::RowValue) -> BridgeError {
    BridgeError::Execution(format!(
        "failed to coerce column {:?} value {:?} to {}",
        column_name, value.kind, expected
    ))
}

fn decode_list_value(list: &proto::RowList) -> Result<AnyValue<'static>, BridgeError> {
    let values = list
        .values
        .iter()
        .map(decode_row_value)
        .collect::<Result<Vec<_>, BridgeError>>()?;
    let inner_dtype = if values.is_empty() {
        DataType::Null
    } else {
        any_values_to_supertype(values.iter()).map_err(|e| {
            BridgeError::Execution(format!("failed to infer nested list dtype: {}", e))
        })?
    };
    let series = Series::from_any_values_and_dtype(
        PlSmallStr::EMPTY,
        values.as_slice(),
        &inner_dtype,
        false,
    )
    .map_err(|e| BridgeError::Execution(format!("failed to build nested list value: {}", e)))?;
    Ok(AnyValue::List(series))
}

fn decode_struct_value(strct: &proto::RowStruct) -> Result<AnyValue<'static>, BridgeError> {
    let mut values = Vec::with_capacity(strct.fields.len());
    let mut fields = Vec::with_capacity(strct.fields.len());
    for field in &strct.fields {
        let value = field
            .value
            .as_ref()
            .map(decode_row_value)
            .transpose()?
            .unwrap_or(AnyValue::Null);
        let dtype = value.dtype();
        values.push(value);
        fields.push(Field::new(field.name.clone().into(), dtype));
    }
    Ok(AnyValue::StructOwned(Box::new((values, fields))))
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
        Err(_) => Err(BridgeError::InvalidArgument(format!(
            "unsupported schema data type code: {}",
            data_type
        ))),
    }
}

fn rows_import_profile_enabled() -> bool {
    matches!(
        env::var("POLARS_BRIDGE_PROFILE_ROWS_IMPORT").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

fn fmt_duration(duration: Duration) -> String {
    format!("{:.3}ms", duration.as_secs_f64() * 1000.0)
}
