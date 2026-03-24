use std::sync::Arc;

use crate::error::BridgeError;
use polars::prelude::*;
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, ArrowSchema, Field};
use polars_arrow::ffi::{
    export_array_to_c, export_field_to_c, import_array_from_c, import_field_from_c, ArrowArray,
    ArrowSchema as FFIArrowSchema,
};
use polars_arrow::record_batch::RecordBatch;

/// 将 Polars DataFrame 导出为 Arrow C Data Interface
pub fn export_dataframe_to_arrow(
    df: &DataFrame,
    out_schema: *mut FFIArrowSchema,
    out_array: *mut ArrowArray,
) -> Result<(), BridgeError> {
    if out_schema.is_null() || out_array.is_null() {
        return Err(BridgeError::InvalidArgument(
            "Null output schema/array pointers".into(),
        ));
    }

    let record_batch = df.clone().rechunk_to_record_batch(CompatLevel::newest());
    let height = record_batch.height();
    let (schema, arrays) = record_batch.into_schema_and_arrays();
    let fields: Vec<Field> = schema.iter_values().cloned().collect();
    let dtype = ArrowDataType::Struct(fields.clone());

    let struct_array = StructArray::try_new(dtype, height, arrays, None)
        .map_err(|e| BridgeError::ArrowExport(e.to_string()))?;
    let array = export_array_to_c(Box::new(struct_array));
    let schema = export_field_to_c(&Field::new("".into(), ArrowDataType::Struct(fields), false));

    unsafe {
        std::ptr::write(out_array, array);
        std::ptr::write(out_schema, schema);
    }

    Ok(())
}

/// 从 Arrow C Data Interface 导入 Polars DataFrame
pub fn import_dataframe_from_arrow(
    in_schema: *const FFIArrowSchema,
    in_array: *const ArrowArray,
) -> Result<DataFrame, BridgeError> {
    if in_schema.is_null() || in_array.is_null() {
        return Err(BridgeError::InvalidArgument(
            "Null input schema/array pointers".into(),
        ));
    }

    let schema = unsafe { std::ptr::read(in_schema) };
    let field = unsafe { import_field_from_c(&schema) }
        .map_err(|e| BridgeError::ArrowImport(e.to_string()))?;

    let dtype = field.dtype.clone();
    let array = unsafe { std::ptr::read(in_array) };
    let array = unsafe { import_array_from_c(array, dtype.clone()) }
        .map_err(|e| BridgeError::ArrowImport(e.to_string()))?;

    let fields = match dtype {
        ArrowDataType::Struct(fields) => fields,
        _ => {
            return Err(BridgeError::ArrowImport(
                "Arrow record batch must be a Struct type".into(),
            ))
        }
    };

    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| BridgeError::ArrowImport("Arrow array is not a StructArray".into()))?;

    if struct_array.validity().is_some() {
        return Err(BridgeError::ArrowImport(
            "StructArray contains top-level nulls".into(),
        ));
    }

    let schema: ArrowSchema = fields.into_iter().collect();
    let arrays = struct_array.values().iter().cloned().collect::<Vec<_>>();
    let record_batch = RecordBatch::try_new(struct_array.len(), Arc::new(schema), arrays)
        .map_err(|e| BridgeError::ArrowImport(e.to_string()))?;

    Ok(DataFrame::from(record_batch))
}
