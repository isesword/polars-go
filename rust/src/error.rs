#[repr(i32)]
pub enum ErrorCode {
    Unknown = 1,
    InvalidArgument = 2,
    PlanVersionUnsupported = 4,
    PlanDecode = 5,
    PlanSemantic = 6,
    ArrowImport = 7,
    ArrowExport = 8,
    Execution = 9,
    Unsupported = 10,
    Oom = 11,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ErrorCode::Unknown => write!(f, "ERR_UNKNOWN"),
            ErrorCode::InvalidArgument => write!(f, "ERR_INVALID_ARGUMENT"),
            ErrorCode::PlanVersionUnsupported => write!(f, "ERR_PLAN_VERSION_UNSUPPORTED"),
            ErrorCode::PlanDecode => write!(f, "ERR_PLAN_DECODE"),
            ErrorCode::PlanSemantic => write!(f, "ERR_PLAN_SEMANTIC"),
            ErrorCode::ArrowImport => write!(f, "ERR_ARROW_IMPORT"),
            ErrorCode::ArrowExport => write!(f, "ERR_ARROW_EXPORT"),
            ErrorCode::Execution => write!(f, "ERR_EXECUTION"),
            ErrorCode::Unsupported => write!(f, "ERR_UNSUPPORTED"),
            ErrorCode::Oom => write!(f, "ERR_OOM"),
        }
    }
}

#[derive(Debug)]
pub enum BridgeError {
    InvalidArgument(String),
    PlanVersionUnsupported(u32),
    PlanDecode(String),
    PlanSemantic(String),
    ArrowImport(String),
    ArrowExport(String),
    Oom(String),
    Execution(String),
    Unsupported(String),
}

impl std::fmt::Display for BridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BridgeError::InvalidArgument(s) => write!(f, "Invalid argument: {}", s),
            BridgeError::PlanVersionUnsupported(v) => write!(f, "Plan version {} unsupported", v),
            BridgeError::PlanDecode(s) => write!(f, "Plan decode error: {}", s),
            BridgeError::PlanSemantic(s) => write!(f, "Plan semantic error: {}", s),
            BridgeError::ArrowImport(s) => write!(f, "Arrow import error: {}", s),
            BridgeError::ArrowExport(s) => write!(f, "Arrow export error: {}", s),
            BridgeError::Oom(s) => write!(f, "Out of memory: {}", s),
            BridgeError::Execution(s) => write!(f, "Execution error: {}", s),
            BridgeError::Unsupported(s) => write!(f, "Unsupported: {}", s),
        }
    }
}

impl std::error::Error for BridgeError {}

pub fn bridge_error_to_code(err: &BridgeError) -> (ErrorCode, String) {
    match err {
        BridgeError::InvalidArgument(s) => (ErrorCode::InvalidArgument, s.clone()),
        BridgeError::PlanVersionUnsupported(_) => {
            (ErrorCode::PlanVersionUnsupported, err.to_string())
        }
        BridgeError::PlanDecode(s) => (ErrorCode::PlanDecode, s.clone()),
        BridgeError::PlanSemantic(s) => (ErrorCode::PlanSemantic, s.clone()),
        BridgeError::ArrowImport(s) => (ErrorCode::ArrowImport, s.clone()),
        BridgeError::ArrowExport(s) => (ErrorCode::ArrowExport, s.clone()),
        BridgeError::Oom(s) => (ErrorCode::Oom, s.clone()),
        BridgeError::Execution(s) => (ErrorCode::Execution, s.clone()),
        BridgeError::Unsupported(s) => (ErrorCode::Unsupported, s.clone()),
    }
}
