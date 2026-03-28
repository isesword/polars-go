use polars::datatypes::{DataType, TimeUnit};
use polars::io::json::{JsonFormat, JsonWriter};
use polars::io::SerWriter;
use polars::lazy::prelude::NDJsonWriterOptions;
use polars::prelude::{by_name, element};
use polars::prelude::{AnyValue, DataFrame, IntoLazy, Series};
use polars::prelude::{ExternalCompression, FileWriteFormat, SinkTarget, UnifiedSinkArgs};
use polars::sql::SQLContext;
use polars_plan::prelude::SinkDestination;
use polars_utils::pl_path::PlRefPath;
use prost::Message;
use std::cell::RefCell;
use std::collections::HashMap;
use std::env;
use std::ffi::CString;
use std::io::Cursor;
use std::os::raw::{c_char, c_int};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

mod proto {
    include!("proto/polars_bridge.rs");
}

mod arrow_bridge;
mod error;
mod executor;
mod expr_list;
mod expr_str;
mod expr_temporal;
mod rows_import;

use error::{BridgeError, ErrorCode};

// ABI 版本
const ABI_VERSION: u32 = 1;

// 线程局部错误存储
thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = RefCell::new(None);
}

type GoExprMapBatchesCallback = extern "C" fn(
    u64,
    *const ArrowSchema,
    *const ArrowArray,
    *mut ArrowSchema,
    *mut ArrowArray,
) -> c_int;

static GO_EXPR_MAP_BATCHES_CALLBACK: OnceLock<GoExprMapBatchesCallback> = OnceLock::new();
static MEMORY_LIMIT_BYTES: AtomicI64 = AtomicI64::new(0);

fn enforce_memory_limit(df: &DataFrame, context: &str) -> Result<(), BridgeError> {
    let limit = MEMORY_LIMIT_BYTES.load(Ordering::Relaxed);
    if limit <= 0 {
        return Ok(());
    }

    let estimated = df.estimated_size() as i64;
    if estimated > limit {
        return Err(BridgeError::Oom(format!(
            "memory limit exceeded during {}: estimated {} bytes exceeds configured limit {} bytes",
            context, estimated, limit
        )));
    }
    Ok(())
}

fn enforce_input_memory_limit(input_dfs: &[&DataFrame], context: &str) -> Result<(), BridgeError> {
    let limit = MEMORY_LIMIT_BYTES.load(Ordering::Relaxed);
    if limit <= 0 || input_dfs.is_empty() {
        return Ok(());
    }

    let estimated: i64 = input_dfs.iter().map(|df| df.estimated_size() as i64).sum();
    if estimated > limit {
        return Err(BridgeError::Oom(format!(
            "memory limit exceeded before {}: input estimate {} bytes exceeds configured limit {} bytes",
            context, estimated, limit
        )));
    }
    Ok(())
}

fn set_last_error(msg: String) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = CString::new(msg).ok();
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = None;
    });
}

fn write_output_string(
    value: String,
    out_ptr: *mut *const c_char,
    out_len: *mut usize,
) -> Result<(), BridgeError> {
    if out_ptr.is_null() || out_len.is_null() {
        return Err(BridgeError::InvalidArgument("Null output pointers".into()));
    }

    let cstr = CString::new(value)
        .map_err(|e| BridgeError::Execution(format!("Failed to encode output: {}", e)))?;
    let len = cstr.as_bytes().len();
    let ptr = cstr.into_raw();
    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }
    Ok(())
}

fn export_dataframe_json(df: &DataFrame, format: c_int) -> Result<String, BridgeError> {
    let json_format = match format {
        0 => JsonFormat::Json,
        1 => JsonFormat::JsonLines,
        other => {
            return Err(BridgeError::InvalidArgument(format!(
                "Unsupported JSON format code {}",
                other
            )))
        }
    };

    let mut df = df.clone();
    for idx in 0..df.width() {
        let column = &df.columns()[idx];
        let needs_binary_cast = matches!(column.dtype(), DataType::Binary);
        let column_name = column.name().to_string();
        if needs_binary_cast {
            let cast_name = column_name.clone();
            let casted = column
                .as_materialized_series()
                .cast(&DataType::String)
                .map_err(|e| {
                    BridgeError::Execution(format!(
                        "Failed to cast binary column {} to string for JSON export: {}",
                        cast_name, e
                    ))
                })?;
            df.replace_column(idx, casted.into()).map_err(|e| {
                BridgeError::Execution(format!(
                    "Failed to replace normalized JSON column {}: {}",
                    column_name, e
                ))
            })?;
        }
    }

    let mut buffer = Cursor::new(Vec::<u8>::new());
    JsonWriter::new(&mut buffer)
        .with_json_format(json_format)
        .finish(&mut df)
        .map_err(|e| BridgeError::Execution(format!("Failed to write dataframe as JSON: {}", e)))?;

    String::from_utf8(buffer.into_inner())
        .map_err(|e| BridgeError::Execution(format!("JSON output was not valid UTF-8: {}", e)))
}

fn ndjson_external_compression(
    compression_code: c_int,
    compression_level: c_int,
) -> Result<ExternalCompression, BridgeError> {
    match compression_code {
        0 => Ok(ExternalCompression::Uncompressed),
        1 => {
            if compression_level < 0 {
                return Err(BridgeError::InvalidArgument(format!(
                    "bridge_plan_sink_ndjson: compression_level must be non-negative for gzip, got {}",
                    compression_level
                )));
            }
            let level = if compression_level == 0 {
                None
            } else {
                Some(compression_level as u32)
            };
            Ok(ExternalCompression::Gzip { level })
        },
        other => Err(BridgeError::InvalidArgument(format!(
            "bridge_plan_sink_ndjson: unsupported compression code {}; supported values: 0 (none), 1 (gzip)",
            other
        ))),
    }
}

// 错误处理宏
macro_rules! ffi_guard {
    ($body:expr) => {
        match catch_unwind(AssertUnwindSafe(|| $body)) {
            Ok(Ok(v)) => {
                clear_last_error();
                v
            }
            Ok(Err(e)) => {
                let (code, msg) = error::bridge_error_to_code(&e);
                set_last_error(format!("[{}] {}", code, msg));
                code as c_int
            }
            Err(panic) => {
                let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown panic".to_string()
                };
                set_last_error(format!("[ERR_UNKNOWN] Panic: {}", msg));
                ErrorCode::Unknown as c_int
            }
        }
    };
}

// 1. 版本与能力
#[no_mangle]
pub extern "C" fn bridge_abi_version() -> u32 {
    ABI_VERSION
}

#[no_mangle]
pub extern "C" fn bridge_set_memory_limit_bytes(limit_bytes: i64) -> c_int {
    ffi_guard!({
        MEMORY_LIMIT_BYTES.store(limit_bytes.max(0), Ordering::Relaxed);
        Ok(0)
    })
}

// 使用静态字符串避免内存泄露
static VERSION_STR: &str = env!("CARGO_PKG_VERSION");

#[no_mangle]
pub extern "C" fn bridge_engine_version(ptr_out: *mut *const c_char, len_out: *mut usize) -> c_int {
    ffi_guard!({
        if ptr_out.is_null() || len_out.is_null() {
            return Err(BridgeError::InvalidArgument("Null output pointers".into()));
        }

        unsafe {
            *ptr_out = VERSION_STR.as_ptr() as *const c_char;
            *len_out = VERSION_STR.len();
        }
        Ok(0)
    })
}

// 使用静态字符串避免内存泄露
static CAPABILITIES_STR: &str = r#"{
    "abi_version": 1,
    "min_plan_version_supported": 1,
    "max_plan_version_supported": 1,
    "supported_nodes": ["MemoryScan", "CsvScan", "ParquetScan", "Project", "Filter", "WithColumns", "Limit", "GroupBy", "Join", "Sort", "Unique", "Concat", "SqlQuery"],
    "supported_exprs": ["Col", "Lit", "Binary", "Alias", "IsNull", "Not", "Wildcard", "Cast", "Sum", "Mean", "Min", "Max", "Count", "First", "Last", "StrLenBytes", "StrLenChars", "StrContains", "StrStartsWith", "StrEndsWith", "StrExtract", "StrReplace", "StrReplaceAll", "StrToLowercase", "StrToUppercase", "StrStripChars", "StrSlice", "StrSplit", "StrPadStart", "StrPadEnd"],
    "supported_dtypes": ["Int64", "Int32", "Int16", "Int8", "UInt64", "UInt32", "UInt16", "UInt8", "Float64", "Float32", "Bool", "Utf8", "Date", "Datetime", "Time"],
    "execution_modes": ["collect"],
    "copy_behavior": "copy_on_boundary"
}"#;

#[no_mangle]
pub extern "C" fn bridge_capabilities(ptr_out: *mut *const c_char, len_out: *mut usize) -> c_int {
    ffi_guard!({
        if ptr_out.is_null() || len_out.is_null() {
            return Err(BridgeError::InvalidArgument("Null output pointers".into()));
        }

        unsafe {
            *ptr_out = CAPABILITIES_STR.as_ptr() as *const c_char;
            *len_out = CAPABILITIES_STR.len();
        }
        Ok(0)
    })
}

// 2. 错误通道
#[no_mangle]
pub extern "C" fn bridge_last_error(ptr_out: *mut *const c_char, len_out: *mut usize) -> c_int {
    if ptr_out.is_null() || len_out.is_null() {
        return ErrorCode::InvalidArgument as c_int;
    }

    LAST_ERROR.with(|e| {
        if let Some(ref err) = *e.borrow() {
            unsafe {
                *ptr_out = err.as_ptr();
                *len_out = err.as_bytes().len();
            }
            0
        } else {
            unsafe {
                *ptr_out = ptr::null();
                *len_out = 0;
            }
            0
        }
    })
}

#[no_mangle]
pub extern "C" fn bridge_last_error_free(ptr: *const c_char, _len: usize) {
    if !ptr.is_null() {
        unsafe {
            let _ = CString::from_raw(ptr as *mut c_char);
        }
    }
}

#[no_mangle]
pub extern "C" fn bridge_string_free(ptr: *const c_char, len: usize) {
    bridge_last_error_free(ptr, len)
}

// 3. Plan 编译
#[no_mangle]
pub extern "C" fn bridge_plan_compile(
    plan_bytes_ptr: *const u8,
    plan_bytes_len: usize,
    out_plan_handle_ptr: *mut u64,
) -> c_int {
    ffi_guard!({
        if plan_bytes_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_compile: plan_bytes_ptr is null".into(),
            ));
        }
        if out_plan_handle_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_compile: out_plan_handle_ptr is null".into(),
            ));
        }

        let plan_bytes = unsafe { slice::from_raw_parts(plan_bytes_ptr, plan_bytes_len) };
        let plan = proto::Plan::decode(plan_bytes).map_err(|e| {
            BridgeError::PlanDecode(format!("failed to decode protobuf plan: {}", e))
        })?;

        if plan.plan_version != 1 {
            return Err(BridgeError::PlanVersionUnsupported(plan.plan_version));
        }

        let handle = Box::into_raw(Box::new(plan)) as u64;
        unsafe {
            *out_plan_handle_ptr = handle;
        }

        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn bridge_plan_free(plan_handle: u64) {
    if plan_handle != 0 {
        unsafe {
            let _ = Box::from_raw(plan_handle as *mut proto::Plan);
        }
    }
}

#[no_mangle]
pub extern "C" fn bridge_plan_explain(
    plan_handle: u64,
    input_df_handles_ptr: *const u64,
    input_df_handles_len: usize,
    optimized: c_int,
    out_ptr: *mut *const c_char,
    out_len: *mut usize,
) -> c_int {
    ffi_guard!({
        if plan_handle == 0 {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_explain: plan_handle must not be zero".into(),
            ));
        }
        if out_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_explain: out_ptr is null".into(),
            ));
        }
        if out_len.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_explain: out_len is null".into(),
            ));
        }

        let plan = unsafe { &*(plan_handle as *const proto::Plan) };
        let input_df_handles = if input_df_handles_len == 0 {
            &[][..]
        } else {
            if input_df_handles_ptr.is_null() {
                return Err(BridgeError::InvalidArgument(
                    "Input dataframe handles pointer is null".into(),
                ));
            }
            unsafe { slice::from_raw_parts(input_df_handles_ptr, input_df_handles_len) }
        };
        let input_dfs: Vec<&DataFrame> = input_df_handles
            .iter()
            .map(|handle| {
                if *handle == 0 {
                    return Err(BridgeError::InvalidArgument(
                        "bridge_plan_explain: input dataframe handle must not be zero".into(),
                    ));
                }
                Ok(unsafe { &*(*handle as *const DataFrame) })
            })
            .collect::<Result<_, _>>()?;
        let lf = executor::build_plan_lazy_frame(plan, &input_dfs)?;
        let optimized = optimized != 0;
        let explanation = lf
            .explain(optimized)
            .map_err(|e| BridgeError::Execution(format!("Failed to explain plan: {}", e)))?;
        write_output_string(explanation, out_ptr, out_len)?;
        Ok(0)
    })
}

// 4b. 执行并返回 DataFrame（句柄）
#[no_mangle]
pub extern "C" fn bridge_plan_collect_df(
    plan_handle: u64,
    input_df_handles_ptr: *const u64,
    input_df_handles_len: usize,
    out_df_handle_ptr: *mut u64,
) -> c_int {
    ffi_guard!({
        if plan_handle == 0 {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_collect_df: plan_handle must not be zero".into(),
            ));
        }
        if out_df_handle_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_collect_df: out_df_handle_ptr is null".into(),
            ));
        }
        if input_df_handles_ptr.is_null() && input_df_handles_len != 0 {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_collect_df: input_df_handles_ptr is null while input_df_handles_len is non-zero".into(),
            ));
        }

        let plan = unsafe { &*(plan_handle as *const proto::Plan) };
        let input_df_handles = if input_df_handles_len == 0 {
            &[][..]
        } else {
            unsafe { slice::from_raw_parts(input_df_handles_ptr, input_df_handles_len) }
        };
        let input_dfs: Vec<&DataFrame> = input_df_handles
            .iter()
            .map(|handle| {
                if *handle == 0 {
                    return Err(BridgeError::InvalidArgument(
                        "bridge_plan_collect_df: input dataframe handle must not be zero".into(),
                    ));
                }
                Ok(unsafe { &*(*handle as *const DataFrame) })
            })
            .collect::<Result<_, _>>()?;

        enforce_input_memory_limit(&input_dfs, "collect")?;
        let df = executor::execute_plan_df(plan, &input_dfs)?;
        enforce_memory_limit(&df, "collect")?;
        let handle = Box::into_raw(Box::new(df)) as u64;
        unsafe {
            *out_df_handle_ptr = handle;
        }

        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn bridge_plan_sink_ndjson(
    plan_handle: u64,
    path_ptr: *const u8,
    path_len: usize,
    input_df_handles_ptr: *const u64,
    input_df_handles_len: usize,
    compression_code: c_int,
    compression_level: c_int,
) -> c_int {
    ffi_guard!({
        if plan_handle == 0 {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_sink_ndjson: plan_handle must not be zero".into(),
            ));
        }
        if path_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_sink_ndjson: path_ptr is null".into(),
            ));
        }
        if path_len == 0 {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_sink_ndjson: path is empty".into(),
            ));
        }
        if input_df_handles_ptr.is_null() && input_df_handles_len != 0 {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_sink_ndjson: input_df_handles_ptr is null while input_df_handles_len is non-zero".into(),
            ));
        }

        let path_bytes = unsafe { slice::from_raw_parts(path_ptr, path_len) };
        let path = std::str::from_utf8(path_bytes).map_err(|e| {
            BridgeError::InvalidArgument(format!(
                "bridge_plan_sink_ndjson: path is not valid UTF-8: {}",
                e
            ))
        })?;

        let compression = ndjson_external_compression(compression_code, compression_level)?;
        let plan = unsafe { &*(plan_handle as *const proto::Plan) };
        let input_df_handles = if input_df_handles_len == 0 {
            &[][..]
        } else {
            unsafe { slice::from_raw_parts(input_df_handles_ptr, input_df_handles_len) }
        };
        let input_dfs: Vec<&DataFrame> = input_df_handles
            .iter()
            .map(|handle| {
                if *handle == 0 {
                    return Err(BridgeError::InvalidArgument(
                        "bridge_plan_sink_ndjson: input dataframe handle must not be zero".into(),
                    ));
                }
                Ok(unsafe { &*(*handle as *const DataFrame) })
            })
            .collect::<Result<_, _>>()?;

        enforce_input_memory_limit(&input_dfs, "ndjson sink")?;

        let lf = executor::build_plan_lazy_frame(plan, &input_dfs)?;
        let sink = lf
            .sink(
                SinkDestination::File {
                    target: SinkTarget::Path(PlRefPath::new(path)),
                },
                FileWriteFormat::NDJson(NDJsonWriterOptions {
                    compression,
                    check_extension: false,
                }),
                UnifiedSinkArgs::default(),
            )
            .map_err(|e| {
                BridgeError::Execution(format!(
                    "bridge_plan_sink_ndjson: failed to build sink for path {:?}: {}",
                    path, e
                ))
            })?;

        sink.collect().map_err(|e| {
            BridgeError::Execution(format!(
                "bridge_plan_sink_ndjson: failed to write NDJSON to {:?}: {}",
                path, e
            ))
        })?;

        Ok(0)
    })
}

// 4c. DataFrame -> Arrow C Data Interface
#[no_mangle]
pub extern "C" fn bridge_df_to_arrow(
    df_handle: u64,
    output_schema: *mut ArrowSchema,
    output_array: *mut ArrowArray,
) -> c_int {
    ffi_guard!({
        if df_handle == 0 {
            return Err(BridgeError::InvalidArgument(
                "bridge_df_to_arrow: df_handle must not be zero".into(),
            ));
        }
        if output_schema.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_df_to_arrow: output_schema is null".into(),
            ));
        }
        if output_array.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_df_to_arrow: output_array is null".into(),
            ));
        }

        let df = unsafe { &*(df_handle as *const DataFrame) };
        arrow_bridge::export_dataframe_to_arrow(df, output_schema, output_array)?;
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn bridge_df_to_json(
    df_handle: u64,
    format: c_int,
    out_ptr: *mut *const c_char,
    out_len: *mut usize,
) -> c_int {
    ffi_guard!({
        if df_handle == 0 {
            return Err(BridgeError::InvalidArgument("Null dataframe handle".into()));
        }

        let df = unsafe { &*(df_handle as *const DataFrame) };
        let json = export_dataframe_json(df, format)?;
        write_output_string(json, out_ptr, out_len)?;
        Ok(0)
    })
}

// 4d. 打印 DataFrame
#[no_mangle]
pub extern "C" fn bridge_df_print(df_handle: u64) -> c_int {
    ffi_guard!({
        if df_handle == 0 {
            return Err(BridgeError::InvalidArgument("Null dataframe handle".into()));
        }

        let df = unsafe { &*(df_handle as *const DataFrame) };
        executor::df_print(df)?;
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn bridge_df_free(df_handle: u64) {
    if df_handle != 0 {
        unsafe {
            let _ = Box::from_raw(df_handle as *mut DataFrame);
        }
    }
}

// 5. 执行并直接打印（使用 Polars 原生 Display）
#[no_mangle]
pub extern "C" fn bridge_plan_execute_and_print(plan_handle: u64) -> c_int {
    ffi_guard!({
        if plan_handle == 0 {
            return Err(BridgeError::InvalidArgument("Null plan handle".into()));
        }

        let plan = unsafe { &*(plan_handle as *const proto::Plan) };

        // 执行并打印结果
        executor::execute_and_print(plan)?;

        Ok(0)
    })
}

// 5. Arrow-based execution (zero-copy)
use polars_arrow::ffi::{ArrowArray, ArrowSchema};

pub(crate) fn call_go_expr_map_batches(
    udg_id: u64,
    input_df: &DataFrame,
) -> Result<DataFrame, BridgeError> {
    let callback = GO_EXPR_MAP_BATCHES_CALLBACK.get().ok_or_else(|| {
        BridgeError::Execution("Go Expr.MapBatches callback has not been registered".into())
    })?;

    let mut input_schema = std::mem::MaybeUninit::<ArrowSchema>::uninit();
    let mut input_array = std::mem::MaybeUninit::<ArrowArray>::uninit();
    arrow_bridge::export_dataframe_to_arrow(
        input_df,
        input_schema.as_mut_ptr(),
        input_array.as_mut_ptr(),
    )?;

    let mut output_schema = std::mem::MaybeUninit::<ArrowSchema>::uninit();
    let mut output_array = std::mem::MaybeUninit::<ArrowArray>::uninit();

    let ret = callback(
        udg_id,
        input_schema.as_ptr(),
        input_array.as_ptr(),
        output_schema.as_mut_ptr(),
        output_array.as_mut_ptr(),
    );
    if ret != 0 {
        return Err(BridgeError::Execution(format!(
            "Go Expr.MapBatches callback failed for udg_id {}",
            udg_id
        )));
    }

    arrow_bridge::import_dataframe_from_arrow(output_schema.as_ptr(), output_array.as_ptr())
}

#[no_mangle]
pub extern "C" fn bridge_register_go_expr_map_batches_callback(callback: usize) -> c_int {
    ffi_guard!({
        if callback == 0 {
            return Err(BridgeError::InvalidArgument(
                "Callback pointer must not be zero".into(),
            ));
        }

        let callback: GoExprMapBatchesCallback = unsafe { std::mem::transmute(callback) };
        let _ = GO_EXPR_MAP_BATCHES_CALLBACK.get_or_init(|| callback);
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn bridge_plan_execute_arrow(
    plan_handle: u64,
    input_schema: *const ArrowSchema,
    input_array: *const ArrowArray,
    output_schema: *mut ArrowSchema,
    output_array: *mut ArrowArray,
) -> c_int {
    ffi_guard!({
        if plan_handle == 0 {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_execute_arrow: plan_handle must not be zero".into(),
            ));
        }
        if output_schema.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_execute_arrow: output_schema is null".into(),
            ));
        }
        if output_array.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_plan_execute_arrow: output_array is null".into(),
            ));
        }

        if (input_schema.is_null() && !input_array.is_null())
            || (!input_schema.is_null() && input_array.is_null())
        {
            return Err(BridgeError::InvalidArgument(
                "Input schema/array must both be null or both be set".into(),
            ));
        }

        let plan = unsafe { &*(plan_handle as *const proto::Plan) };
        let input_df = if input_schema.is_null() {
            None
        } else {
            Some(arrow_bridge::import_dataframe_from_arrow(
                input_schema,
                input_array,
            )?)
        };

        let input_dfs: Vec<&DataFrame> = input_df.as_ref().into_iter().collect();
        enforce_input_memory_limit(&input_dfs, "arrow execution")?;
        let df = executor::execute_plan_df(plan, &input_dfs)?;
        enforce_memory_limit(&df, "arrow execution")?;
        arrow_bridge::export_dataframe_to_arrow(&df, output_schema, output_array)?;
        Ok(0)
    })
}

// 6. 从列数据创建 DataFrame（支持动态类型推断）
// 数据格式：{"columns":[{"name":"col1","values":[1,2,3]}],"schema":{"col1":"INT64"}}
#[no_mangle]
pub extern "C" fn bridge_df_from_columns(
    json_ptr: *const c_char,
    json_len: usize,
    out_df_handle: *mut u64,
) -> c_int {
    ffi_guard!({
        if json_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_df_from_columns: json_ptr is null".into(),
            ));
        }
        if out_df_handle.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_df_from_columns: out_df_handle is null".into(),
            ));
        }

        // 读取 JSON 数据
        let json_slice = unsafe { slice::from_raw_parts(json_ptr as *const u8, json_len) };
        let json_str = std::str::from_utf8(json_slice).map_err(|e| {
            BridgeError::InvalidArgument(format!(
                "bridge_df_from_columns: payload is not valid UTF-8: {}",
                e
            ))
        })?;

        // 解析 JSON
        let payload: ColumnPayload = serde_json::from_str(json_str).map_err(|e| {
            BridgeError::InvalidArgument(format!(
                "bridge_df_from_columns: payload is not valid JSON column data: {}",
                e
            ))
        })?;

        let (columns, schema) = payload.into_parts();

        // 构建 Series 列表
        let mut series_vec = Vec::new();

        for col in columns {
            let name = col["name"].as_str().ok_or_else(|| {
                BridgeError::InvalidArgument(
                    "bridge_df_from_columns: each column object must contain a string 'name' field"
                        .into(),
                )
            })?;

            let values = col["values"].as_array().ok_or_else(|| {
                BridgeError::InvalidArgument(format!(
                    "bridge_df_from_columns: column {:?} must contain a 'values' array",
                    name
                ))
            })?;

            let target_type = if let Some(dtype_name) = schema.get(name) {
                Some(parse_data_type(dtype_name)?)
            } else {
                None
            };

            let any_values: Vec<AnyValue> = values
                .iter()
                .map(|v| {
                    if let Some(ref dtype) = target_type {
                        json_value_to_any_value_with_dtype(v, dtype)
                    } else {
                        Ok(json_value_to_any_value(v))
                    }
                })
                .collect::<Result<_, _>>()?;

            let mut series =
                Series::from_any_values(name.into(), &any_values, true).map_err(|e| {
                    BridgeError::Execution(format!(
                        "failed to create series for column {:?}: {}",
                        name, e
                    ))
                })?;

            if let Some(target_type) = target_type {
                series = series.cast(&target_type).map_err(|e| {
                    BridgeError::Execution(format!(
                        "failed to cast column {:?} to {:?}: {}",
                        name, target_type, e
                    ))
                })?;
            }
            series_vec.push(series);
        }

        // 创建 DataFrame
        let columns: Vec<_> = series_vec.into_iter().map(|s| s.into()).collect();
        let df = DataFrame::new_infer_height(columns).map_err(|e| {
            BridgeError::Execution(format!("failed to create dataframe from columns: {}", e))
        })?;
        enforce_memory_limit(&df, "dataframe import from columns")?;

        // 存储 DataFrame 并返回句柄
        let handle = Box::into_raw(Box::new(df)) as u64;
        unsafe { *out_df_handle = handle };

        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn bridge_df_from_rows(
    payload_ptr: *const u8,
    payload_len: usize,
    out_df_handle: *mut u64,
) -> c_int {
    ffi_guard!({
        if payload_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_df_from_rows: payload_ptr is null".into(),
            ));
        }
        if out_df_handle.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_df_from_rows: out_df_handle is null".into(),
            ));
        }

        let payload_slice = unsafe { slice::from_raw_parts(payload_ptr, payload_len) };
        let profile_enabled = rows_import_profile_enabled();
        let decode_started = Instant::now();
        let payload = proto::RowsPayload::decode(payload_slice).map_err(|e| {
            BridgeError::InvalidArgument(format!(
                "bridge_df_from_rows: payload is not valid protobuf row data: {}",
                e
            ))
        })?;
        let decode_elapsed = decode_started.elapsed();

        let import_started = Instant::now();
        let df = rows_import::dataframe_from_rows_payload(payload)?;
        let import_elapsed = import_started.elapsed();
        enforce_memory_limit(&df, "dataframe import from rows")?;

        if profile_enabled {
            eprintln!(
                "[bridge_df_from_rows] protobuf_decode={:.3}ms rust_rows_import={:.3}ms rows={} cols={}",
                decode_elapsed.as_secs_f64() * 1000.0,
                import_elapsed.as_secs_f64() * 1000.0,
                df.height(),
                df.width(),
            );
        }

        let handle = Box::into_raw(Box::new(df)) as u64;
        unsafe { *out_df_handle = handle };
        Ok(0)
    })
}

fn rows_import_profile_enabled() -> bool {
    matches!(
        env::var("POLARS_BRIDGE_PROFILE_ROWS_IMPORT").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

#[no_mangle]
pub extern "C" fn bridge_df_from_arrow(
    input_schema: *const ArrowSchema,
    input_array: *const ArrowArray,
    out_df_handle: *mut u64,
) -> c_int {
    ffi_guard!({
        if input_schema.is_null() || input_array.is_null() || out_df_handle.is_null() {
            return Err(BridgeError::InvalidArgument("Null pointers".into()));
        }

        let df = arrow_bridge::import_dataframe_from_arrow(input_schema, input_array)?;
        enforce_memory_limit(&df, "dataframe import from arrow")?;
        let handle = Box::into_raw(Box::new(df)) as u64;
        unsafe {
            *out_df_handle = handle;
        }
        Ok(0)
    })
}

#[derive(serde::Deserialize)]
struct PivotRequest {
    on: Vec<String>,
    #[serde(default)]
    index: Vec<String>,
    #[serde(default)]
    values: Vec<String>,
    #[serde(default)]
    aggregate: String,
    #[serde(default)]
    sort_columns: bool,
    #[serde(default)]
    maintain_order: bool,
    #[serde(default)]
    separator: String,
}

fn build_pivot_agg_expr(name: &str) -> Result<Option<polars::prelude::Expr>, BridgeError> {
    let expr = match name {
        "" => return Ok(None),
        "first" => element().first(),
        "last" => element().last(),
        "sum" => element().sum(),
        "min" => element().min(),
        "max" => element().max(),
        "mean" => element().mean(),
        "median" => element().median(),
        "count" | "len" => element().count(),
        other => {
            return Err(BridgeError::Unsupported(format!(
                "unsupported pivot aggregate {:?}; supported values: first, last, sum, min, max, mean, median, count, len",
                other
            )))
        }
    };
    Ok(Some(expr))
}

#[no_mangle]
pub extern "C" fn bridge_df_pivot(
    df_handle: u64,
    options_ptr: *const c_char,
    options_len: usize,
    out_df_handle: *mut u64,
) -> c_int {
    ffi_guard!({
        if df_handle == 0 {
            return Err(BridgeError::InvalidArgument(
                "bridge_df_pivot: df_handle must not be zero".into(),
            ));
        }
        if options_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_df_pivot: options_ptr is null".into(),
            ));
        }
        if out_df_handle.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_df_pivot: out_df_handle is null".into(),
            ));
        }

        let df = unsafe { &*(df_handle as *const DataFrame) };
        let options_slice = unsafe { slice::from_raw_parts(options_ptr as *const u8, options_len) };
        let opts: PivotRequest = serde_json::from_slice(options_slice).map_err(|e| {
            BridgeError::InvalidArgument(format!(
                "bridge_df_pivot: options payload is not valid JSON: {}",
                e
            ))
        })?;
        let _ = opts.sort_columns;

        if opts.on.is_empty() {
            return Err(BridgeError::InvalidArgument(
                "pivot options require at least one 'on' column".into(),
            ));
        }
        let index = if opts.index.is_empty() {
            None
        } else {
            Some(opts.index.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        };
        let values = if opts.values.is_empty() {
            None
        } else {
            Some(opts.values.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        };
        let agg_expr = build_pivot_agg_expr(opts.aggregate.as_str())?;
        let separator = if opts.separator.is_empty() {
            None
        } else {
            Some(opts.separator.as_str())
        };

        let on_columns = df
            .select(opts.on.iter().map(|s| s.as_str()))
            .and_then(|selected| {
                let subset = opts.on.clone();
                selected.unique_stable(
                    Some(subset.as_slice()),
                    polars::prelude::UniqueKeepStrategy::Any,
                    None,
                )
            })
            .map_err(|e| {
                BridgeError::Execution(format!("failed to derive pivot columns: {}", e))
            })?;

        let result = df
            .clone()
            .lazy()
            .pivot(
                by_name(opts.on.iter().cloned(), true, false),
                std::sync::Arc::new(on_columns),
                match &index {
                    Some(items) => by_name(items.iter().copied(), true, false),
                    None => by_name(std::iter::empty::<&str>(), true, false),
                },
                match &values {
                    Some(items) => by_name(items.iter().copied(), true, false),
                    None => by_name(std::iter::empty::<&str>(), true, false),
                },
                agg_expr.unwrap_or_else(|| element().first()),
                opts.maintain_order,
                separator.unwrap_or("").into(),
            )
            .collect()
            .map_err(|e| BridgeError::Execution(format!("pivot execution failed: {}", e)))?;

        let handle = Box::into_raw(Box::new(result)) as u64;
        unsafe { *out_df_handle = handle };
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn bridge_sql_collect_df(
    query_ptr: *const c_char,
    query_len: usize,
    table_names_json_ptr: *const c_char,
    table_names_json_len: usize,
    input_df_handles_ptr: *const u64,
    input_df_handles_len: usize,
    out_df_handle: *mut u64,
) -> c_int {
    ffi_guard!({
        if query_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_sql_collect_df: query_ptr is null".into(),
            ));
        }
        if table_names_json_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_sql_collect_df: table_names_json_ptr is null".into(),
            ));
        }
        if out_df_handle.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_sql_collect_df: out_df_handle is null".into(),
            ));
        }
        if input_df_handles_ptr.is_null() && input_df_handles_len != 0 {
            return Err(BridgeError::InvalidArgument(
                "bridge_sql_collect_df: input_df_handles_ptr is null while input_df_handles_len is non-zero".into(),
            ));
        }

        let query_slice = unsafe { slice::from_raw_parts(query_ptr as *const u8, query_len) };
        let query = std::str::from_utf8(query_slice).map_err(|e| {
            BridgeError::InvalidArgument(format!(
                "bridge_sql_collect_df: query is not valid UTF-8: {}",
                e
            ))
        })?;
        let names_slice = unsafe {
            slice::from_raw_parts(table_names_json_ptr as *const u8, table_names_json_len)
        };
        let table_names: Vec<String> = serde_json::from_slice(names_slice).map_err(|e| {
            BridgeError::InvalidArgument(format!(
                "bridge_sql_collect_df: table_names_json is invalid JSON: {}",
                e
            ))
        })?;

        if table_names.len() != input_df_handles_len {
            return Err(BridgeError::InvalidArgument(format!(
                "Table names length {} does not match dataframe handles length {}",
                table_names.len(),
                input_df_handles_len
            )));
        }

        let input_df_handles = if input_df_handles_len == 0 {
            &[][..]
        } else {
            unsafe { slice::from_raw_parts(input_df_handles_ptr, input_df_handles_len) }
        };

        let mut ctx = SQLContext::new();
        for (name, handle) in table_names.iter().zip(input_df_handles.iter()) {
            if *handle == 0 {
                return Err(BridgeError::InvalidArgument(format!(
                    "Input dataframe handle for table '{}' must not be zero",
                    name
                )));
            }
            let df = unsafe { &*(*handle as *const DataFrame) };
            ctx.register(name, df.clone().lazy());
        }

        let input_dfs: Vec<&DataFrame> = input_df_handles
            .iter()
            .map(|handle| unsafe { &*(*handle as *const DataFrame) })
            .collect();
        enforce_input_memory_limit(&input_dfs, "sql collect")?;

        let lf = ctx.execute(query).map_err(|e| {
            BridgeError::Execution(format!(
                "SQL execution failed for query {:?} with tables {:?}: {}",
                query, table_names, e
            ))
        })?;
        let df = lf.collect().map_err(|e| {
            BridgeError::Execution(format!(
                "SQL collect failed for query {:?} with tables {:?}: {}",
                query, table_names, e
            ))
        })?;
        enforce_memory_limit(&df, "sql collect")?;

        let handle = Box::into_raw(Box::new(df)) as u64;
        unsafe {
            *out_df_handle = handle;
        }
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn bridge_sql_collect_df_from_plans(
    query_ptr: *const c_char,
    query_len: usize,
    table_names_json_ptr: *const c_char,
    table_names_json_len: usize,
    plan_handles_ptr: *const u64,
    plan_handles_len: usize,
    input_df_handles_ptr: *const u64,
    input_df_handles_len: usize,
    input_df_offsets_ptr: *const usize,
    input_df_offsets_len: usize,
    out_df_handle: *mut u64,
) -> c_int {
    ffi_guard!({
        if query_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_sql_collect_df_from_plans: query_ptr is null".into(),
            ));
        }
        if table_names_json_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_sql_collect_df_from_plans: table_names_json_ptr is null".into(),
            ));
        }
        if plan_handles_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_sql_collect_df_from_plans: plan_handles_ptr is null".into(),
            ));
        }
        if input_df_offsets_ptr.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_sql_collect_df_from_plans: input_df_offsets_ptr is null".into(),
            ));
        }
        if out_df_handle.is_null() {
            return Err(BridgeError::InvalidArgument(
                "bridge_sql_collect_df_from_plans: out_df_handle is null".into(),
            ));
        }
        if input_df_handles_ptr.is_null() && input_df_handles_len != 0 {
            return Err(BridgeError::InvalidArgument(
                "bridge_sql_collect_df_from_plans: input_df_handles_ptr is null while input_df_handles_len is non-zero".into(),
            ));
        }

        let query_slice = unsafe { slice::from_raw_parts(query_ptr as *const u8, query_len) };
        let query = std::str::from_utf8(query_slice).map_err(|e| {
            BridgeError::InvalidArgument(format!(
                "bridge_sql_collect_df_from_plans: query is not valid UTF-8: {}",
                e
            ))
        })?;
        let names_slice = unsafe {
            slice::from_raw_parts(table_names_json_ptr as *const u8, table_names_json_len)
        };
        let table_names: Vec<String> = serde_json::from_slice(names_slice).map_err(|e| {
            BridgeError::InvalidArgument(format!(
                "bridge_sql_collect_df_from_plans: table_names_json is invalid JSON: {}",
                e
            ))
        })?;

        let plan_handles = unsafe { slice::from_raw_parts(plan_handles_ptr, plan_handles_len) };
        let input_df_handles = if input_df_handles_len == 0 {
            &[][..]
        } else {
            unsafe { slice::from_raw_parts(input_df_handles_ptr, input_df_handles_len) }
        };
        let input_df_offsets =
            unsafe { slice::from_raw_parts(input_df_offsets_ptr, input_df_offsets_len) };

        if table_names.len() != plan_handles.len() {
            return Err(BridgeError::InvalidArgument(format!(
                "Table names length {} does not match plan handles length {}",
                table_names.len(),
                plan_handles.len()
            )));
        }
        if input_df_offsets.len() != plan_handles.len() + 1 {
            return Err(BridgeError::InvalidArgument(format!(
                "Input dataframe offsets length {} does not match plan handles length {} + 1",
                input_df_offsets.len(),
                plan_handles.len()
            )));
        }
        if *input_df_offsets.first().unwrap_or(&usize::MAX) != 0 {
            return Err(BridgeError::InvalidArgument(
                "Input dataframe offsets must start with 0".into(),
            ));
        }
        if *input_df_offsets.last().unwrap_or(&usize::MAX) != input_df_handles.len() {
            return Err(BridgeError::InvalidArgument(format!(
                "Input dataframe offsets end {} does not match handles length {}",
                input_df_offsets.last().copied().unwrap_or_default(),
                input_df_handles.len()
            )));
        }

        let mut ctx = SQLContext::new();
        for (index, name) in table_names.iter().enumerate() {
            let plan_handle = plan_handles[index];
            if plan_handle == 0 {
                return Err(BridgeError::InvalidArgument(format!(
                    "Plan handle for table '{}' must not be zero",
                    name
                )));
            }
            let plan = unsafe { &*(plan_handle as *const proto::Plan) };

            let start = input_df_offsets[index];
            let end = input_df_offsets[index + 1];
            if start > end || end > input_df_handles.len() {
                return Err(BridgeError::InvalidArgument(format!(
                    "Invalid input dataframe offsets for table '{}': {}..{}",
                    name, start, end
                )));
            }
            let input_dfs: Vec<&DataFrame> = input_df_handles[start..end]
                .iter()
                .map(|handle| {
                    if *handle == 0 {
                        return Err(BridgeError::InvalidArgument(
                            "Input dataframe handle must not be zero".into(),
                        ));
                    }
                    Ok(unsafe { &*(*handle as *const DataFrame) })
                })
                .collect::<Result<_, _>>()?;

            enforce_input_memory_limit(&input_dfs, "sql collect")?;

            let lf = executor::build_plan_lazy_frame(plan, &input_dfs)?;
            ctx.register(name, lf);
        }

        let lf = ctx.execute(query).map_err(|e| {
            BridgeError::Execution(format!(
                "SQL execution failed for query {:?} with tables {:?}: {}",
                query, table_names, e
            ))
        })?;
        let df = lf.collect().map_err(|e| {
            BridgeError::Execution(format!(
                "SQL collect failed for query {:?} with tables {:?}: {}",
                query, table_names, e
            ))
        })?;
        enforce_memory_limit(&df, "sql collect")?;

        let handle = Box::into_raw(Box::new(df)) as u64;
        unsafe {
            *out_df_handle = handle;
        }
        Ok(0)
    })
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ColumnPayload {
    LegacyColumns(Vec<serde_json::Value>),
    WithSchema {
        columns: Vec<serde_json::Value>,
        #[serde(default)]
        schema: HashMap<String, String>,
    },
}

impl ColumnPayload {
    fn into_parts(self) -> (Vec<serde_json::Value>, HashMap<String, String>) {
        match self {
            ColumnPayload::LegacyColumns(columns) => (columns, HashMap::new()),
            ColumnPayload::WithSchema { columns, schema } => (columns, schema),
        }
    }
}

fn parse_data_type(dtype_name: &str) -> Result<DataType, BridgeError> {
    match dtype_name {
        "INT64" => Ok(DataType::Int64),
        "INT32" => Ok(DataType::Int32),
        "INT16" => Ok(DataType::Int16),
        "INT8" => Ok(DataType::Int8),
        "UINT64" => Ok(DataType::UInt64),
        "UINT32" => Ok(DataType::UInt32),
        "UINT16" => Ok(DataType::UInt16),
        "UINT8" => Ok(DataType::UInt8),
        "FLOAT64" => Ok(DataType::Float64),
        "FLOAT32" => Ok(DataType::Float32),
        "BOOL" => Ok(DataType::Boolean),
        "UTF8" => Ok(DataType::String),
        "DATE" => Ok(DataType::Date),
        "DATETIME" => Ok(DataType::Datetime(TimeUnit::Microseconds, None)),
        "TIME" => Ok(DataType::Time),
        _ => Err(BridgeError::InvalidArgument(format!(
            "Unsupported schema data type: {}",
            dtype_name
        ))),
    }
}

// 辅助函数：将 JSON 值转换为 AnyValue
fn json_value_to_any_value(v: &serde_json::Value) -> AnyValue<'static> {
    match v {
        serde_json::Value::Null => AnyValue::Null,
        serde_json::Value::Bool(b) => AnyValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                AnyValue::Int64(i)
            } else if let Some(f) = n.as_f64() {
                AnyValue::Float64(f)
            } else {
                AnyValue::Null
            }
        }
        serde_json::Value::String(s) => {
            // 需要静态生命周期，所以克隆字符串
            AnyValue::StringOwned(s.clone().into())
        }
        _ => AnyValue::Null,
    }
}

fn json_value_to_any_value_with_dtype(
    v: &serde_json::Value,
    dtype: &DataType,
) -> Result<AnyValue<'static>, BridgeError> {
    if matches!(v, serde_json::Value::Null) {
        return Ok(AnyValue::Null);
    }

    match dtype {
        DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
            let value = json_to_i64(v)?;
            Ok(AnyValue::Int64(value))
        }
        DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => {
            let value = json_to_u64(v)?;
            Ok(AnyValue::UInt64(value))
        }
        DataType::Float64 | DataType::Float32 => {
            let value = json_to_f64(v)?;
            Ok(AnyValue::Float64(value))
        }
        DataType::Boolean => match v {
            serde_json::Value::Bool(b) => Ok(AnyValue::Boolean(*b)),
            _ => Err(BridgeError::InvalidArgument(format!(
                "Expected bool JSON value, got {}",
                v
            ))),
        },
        DataType::String | DataType::Date | DataType::Datetime(_, _) | DataType::Time => match v {
            serde_json::Value::String(s) => Ok(AnyValue::StringOwned(s.clone().into())),
            _ => Ok(AnyValue::StringOwned(v.to_string().into())),
        },
        _ => Ok(json_value_to_any_value(v)),
    }
}

fn json_to_i64(v: &serde_json::Value) -> Result<i64, BridgeError> {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i)
            } else if let Some(u) = n.as_u64() {
                i64::try_from(u).map_err(|_| {
                    BridgeError::InvalidArgument(format!("Value out of range for i64: {}", u))
                })
            } else if let Some(f) = n.as_f64() {
                Ok(f as i64)
            } else {
                Err(BridgeError::InvalidArgument(format!(
                    "Cannot parse numeric value {} as i64",
                    n
                )))
            }
        }
        serde_json::Value::String(s) => s.parse::<i64>().map_err(|e| {
            BridgeError::InvalidArgument(format!("Cannot parse '{}' as i64: {}", s, e))
        }),
        _ => Err(BridgeError::InvalidArgument(format!(
            "Expected numeric/string JSON value for i64, got {}",
            v
        ))),
    }
}

fn json_to_u64(v: &serde_json::Value) -> Result<u64, BridgeError> {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                Ok(u)
            } else if let Some(i) = n.as_i64() {
                u64::try_from(i).map_err(|_| {
                    BridgeError::InvalidArgument(format!("Value out of range for u64: {}", i))
                })
            } else if let Some(f) = n.as_f64() {
                Ok(f as u64)
            } else {
                Err(BridgeError::InvalidArgument(format!(
                    "Cannot parse numeric value {} as u64",
                    n
                )))
            }
        }
        serde_json::Value::String(s) => s.parse::<u64>().map_err(|e| {
            BridgeError::InvalidArgument(format!("Cannot parse '{}' as u64: {}", s, e))
        }),
        _ => Err(BridgeError::InvalidArgument(format!(
            "Expected numeric/string JSON value for u64, got {}",
            v
        ))),
    }
}

fn json_to_f64(v: &serde_json::Value) -> Result<f64, BridgeError> {
    match v {
        serde_json::Value::Number(n) => n.as_f64().ok_or_else(|| {
            BridgeError::InvalidArgument(format!("Cannot parse numeric value {} as f64", n))
        }),
        serde_json::Value::String(s) => s.parse::<f64>().map_err(|e| {
            BridgeError::InvalidArgument(format!("Cannot parse '{}' as f64: {}", s, e))
        }),
        _ => Err(BridgeError::InvalidArgument(format!(
            "Expected numeric/string JSON value for f64, got {}",
            v
        ))),
    }
}
