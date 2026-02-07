use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;
use prost::Message;
use polars::prelude::{DataFrame, Series, AnyValue, IntoLazy, NamedFrom};
use polars::series::SeriesIter;

mod proto {
    include!("proto/polars_bridge.rs");
}

mod executor;
mod error;
mod arrow_bridge;
mod expr_str;

use error::{BridgeError, ErrorCode};

// ABI 版本
const ABI_VERSION: u32 = 1;

// 线程局部错误存储
thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = RefCell::new(None);
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
    "supported_nodes": ["MemoryScan", "Project", "Filter", "WithColumns", "Limit"],
    "supported_exprs": ["Col", "Lit", "Binary", "Alias", "IsNull", "Not", "Wildcard", "Cast", "StrLenBytes", "StrLenChars", "StrContains", "StrStartsWith", "StrEndsWith", "StrExtract", "StrReplace", "StrReplaceAll", "StrToLowercase", "StrToUppercase", "StrStripChars", "StrSlice", "StrSplit", "StrPadStart", "StrPadEnd"],
    "supported_dtypes": ["Int64", "Float64", "Bool", "Utf8"],
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

// 3. Plan 编译
#[no_mangle]
pub extern "C" fn bridge_plan_compile(
    plan_bytes_ptr: *const u8,
    plan_bytes_len: usize,
    out_plan_handle_ptr: *mut u64,
) -> c_int {
    ffi_guard!({
        if plan_bytes_ptr.is_null() || out_plan_handle_ptr.is_null() {
            return Err(BridgeError::InvalidArgument("Null pointers".into()));
        }
        
        let plan_bytes = unsafe { slice::from_raw_parts(plan_bytes_ptr, plan_bytes_len) };
        let plan = proto::Plan::decode(plan_bytes)
            .map_err(|e| BridgeError::PlanDecode(e.to_string()))?;
        
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

// 4. 执行（返回 Arrow IPC 二进制数据）
#[no_mangle]
pub extern "C" fn bridge_plan_execute_simple(
    plan_handle: u64,
    _input_json_ptr: *const c_char,
    _input_json_len: usize,
    output_ptr: *mut *mut u8,
    output_len: *mut usize,
) -> c_int {
    ffi_guard!({
        if plan_handle == 0 || output_ptr.is_null() || output_len.is_null() {
            return Err(BridgeError::InvalidArgument("Null pointers".into()));
        }
        
        let plan = unsafe { &*(plan_handle as *const proto::Plan) };
        
        // 注意：input_json 参数被忽略，因为数据源已经在 Plan 里（CsvScan 等）
        let result_bytes = executor::execute_plan(plan)?;
        
        // 分配内存并拷贝结果
        let len = result_bytes.len();
        let ptr = result_bytes.as_ptr() as *mut u8;
        
        unsafe {
            *output_len = len;
            *output_ptr = ptr;
        }
        
        // 防止 Rust 释放这块内存
        std::mem::forget(result_bytes);
        
        Ok(0)
    })
}

// 4b. 执行并返回 DataFrame（句柄）
#[no_mangle]
pub extern "C" fn bridge_plan_collect_df(
    plan_handle: u64,
    input_df_handle: u64,
    out_df_handle_ptr: *mut u64,
) -> c_int {
    ffi_guard!({
        if plan_handle == 0 || out_df_handle_ptr.is_null() {
            return Err(BridgeError::InvalidArgument("Null pointers".into()));
        }

        let plan = unsafe { &*(plan_handle as *const proto::Plan) };
        let input_df = if input_df_handle != 0 {
            Some(unsafe { &*(input_df_handle as *const DataFrame) })
        } else {
            None
        };

        let df = executor::execute_plan_df(plan, input_df)?;
        let handle = Box::into_raw(Box::new(df)) as u64;
        unsafe {
            *out_df_handle_ptr = handle;
        }

        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn bridge_output_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            // 重新构造 Vec 并让它自动释放
            let _ = Vec::from_raw_parts(ptr, len, len);
        }
    }
}

// 4c. DataFrame -> Arrow IPC
#[no_mangle]
pub extern "C" fn bridge_df_to_ipc(
    df_handle: u64,
    output_ptr: *mut *mut u8,
    output_len: *mut usize,
) -> c_int {
    ffi_guard!({
        if df_handle == 0 || output_ptr.is_null() || output_len.is_null() {
            return Err(BridgeError::InvalidArgument("Null pointers".into()));
        }

        let df = unsafe { &*(df_handle as *const DataFrame) };
        let result_bytes = executor::df_to_ipc(df)?;

        let len = result_bytes.len();
        let ptr = result_bytes.as_ptr() as *mut u8;

        unsafe {
            *output_len = len;
            *output_ptr = ptr;
        }

        std::mem::forget(result_bytes);
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
pub extern "C" fn bridge_plan_execute_and_print(
    plan_handle: u64,
) -> c_int {
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

#[no_mangle]
pub extern "C" fn bridge_plan_execute_arrow(
    plan_handle: u64,
    input_schema: *const ArrowSchema,
    input_array: *const ArrowArray,
    output_schema: *mut ArrowSchema,
    output_array: *mut ArrowArray,
) -> c_int {
    ffi_guard!({
        if plan_handle == 0 || output_schema.is_null() || output_array.is_null() {
            return Err(BridgeError::InvalidArgument("Null pointers".into()));
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

        let df = executor::execute_plan_df(plan, input_df.as_ref())?;
        arrow_bridge::export_dataframe_to_arrow(&df, output_schema, output_array)?;
        Ok(0)
    })
}

// 6. 从列数据创建 DataFrame（支持动态类型推断）
// 数据格式：JSON array of columns
// [{"name": "col1", "values": [1, 2, 3]}, {"name": "col2", "values": ["a", "b", "c"]}]
#[no_mangle]
pub extern "C" fn bridge_df_from_columns(
    json_ptr: *const c_char,
    json_len: usize,
    out_df_handle: *mut u64,
) -> c_int {
    ffi_guard!({
        if json_ptr.is_null() || out_df_handle.is_null() {
            return Err(BridgeError::InvalidArgument("Null pointers".into()));
        }

        // 读取 JSON 数据
        let json_slice = unsafe { slice::from_raw_parts(json_ptr as *const u8, json_len) };
        let json_str = std::str::from_utf8(json_slice)
            .map_err(|e| BridgeError::InvalidArgument(format!("Invalid UTF-8: {}", e)))?;

        // 解析 JSON
        let columns: Vec<serde_json::Value> = serde_json::from_str(json_str)
            .map_err(|e| BridgeError::InvalidArgument(format!("Invalid JSON: {}", e)))?;

        // 构建 Series 列表
        let mut series_vec = Vec::new();
        
        for col in columns {
            let name = col["name"].as_str()
                .ok_or_else(|| BridgeError::InvalidArgument("Column missing 'name' field".into()))?;
            
            let values = col["values"].as_array()
                .ok_or_else(|| BridgeError::InvalidArgument("Column missing 'values' array".into()))?;

            // 将 JSON 值转换为 AnyValue
            let any_values: Vec<AnyValue> = values.iter()
                .map(|v| json_value_to_any_value(v))
                .collect();

            // Polars 自动推断类型！
            let series = Series::from_any_values(name.into(), &any_values, true)
                .map_err(|e| BridgeError::Execution(format!("Failed to create series: {}", e)))?;
            series_vec.push(series);
        }

        // 创建 DataFrame
        let columns: Vec<_> = series_vec.into_iter().map(|s| s.into()).collect();
        let df = DataFrame::new(columns)
            .map_err(|e| BridgeError::Execution(format!("Failed to create DataFrame: {}", e)))?;

        // 存储 DataFrame 并返回句柄
        let handle = Box::into_raw(Box::new(df)) as u64;
        unsafe { *out_df_handle = handle };

        Ok(0)
    })
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
