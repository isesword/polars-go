//go:build !windows
// +build !windows

package bridge

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

var (
	defaultBridgeOnce sync.Once
	defaultBridge     *Bridge
	defaultBridgeErr  error
	bridgeCacheMu     sync.Mutex
	bridgeCache       = map[string]*Bridge{}
)

// Bridge Rust FFI 接口
type Bridge struct {
	lib                              uintptr
	abiVersion                       func() uint32
	engineVersion                    func(*uintptr, *uintptr) int32
	capabilities                     func(*uintptr, *uintptr) int32
	lastError                        func(*uintptr, *uintptr) int32
	lastErrorFree                    func(uintptr, uintptr)
	stringFree                       func(uintptr, uintptr)
	planCompile                      func(*byte, uintptr, *uint64) int32
	planFree                         func(uint64)
	planExplain                      func(uint64, *uint64, uintptr, int32, *uintptr, *uintptr) int32
	planExecutePrint                 func(uint64) int32
	planExecuteArrow                 func(uint64, *ArrowSchema, *ArrowArray, *ArrowSchema, *ArrowArray) int32
	planCollectDF                    func(uint64, *uint64, uintptr, *uint64) int32
	dfToArrow                        func(uint64, *ArrowSchema, *ArrowArray) int32
	dfToJSON                         func(uint64, int32, *uintptr, *uintptr) int32
	dfPrint                          func(uint64) int32
	dfFree                           func(uint64)
	dfFromColumns                    func(*byte, uintptr, *uint64) int32
	dfFromArrow                      func(*ArrowSchema, *ArrowArray, *uint64) int32
	dfPivot                          func(uint64, *byte, uintptr, *uint64) int32
	sqlCollectDF                     func(*byte, uintptr, *byte, uintptr, *uint64, uintptr, *uint64) int32
	sqlCollectDFFromPlans            func(*byte, uintptr, *byte, uintptr, *uint64, uintptr, *uint64, uintptr, *uintptr, uintptr, *uint64) int32
	setMemoryLimitBytes              func(int64) int32
	registerGoExprMapBatchesCallback func(uintptr) int32
}

// LoadBridge 加载动态库
func LoadBridge(libPath string) (*Bridge, error) {
	if libPath == "" {
		defaultBridgeOnce.Do(func() {
			defaultBridge, defaultBridgeErr = loadBridge("")
		})
		return defaultBridge, defaultBridgeErr
	}

	return loadBridge(libPath)
}

func loadBridge(libPath string) (*Bridge, error) {
	resolvedPath, err := resolveLibPath(libPath)
	if err != nil {
		return nil, err
	}

	bridgeCacheMu.Lock()
	if cached := bridgeCache[resolvedPath]; cached != nil {
		bridgeCacheMu.Unlock()
		return cached, nil
	}
	bridgeCacheMu.Unlock()

	lib, err := purego.Dlopen(resolvedPath, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return nil, fmt.Errorf("failed to load library %s: %w", resolvedPath, err)
	}

	b := &Bridge{lib: lib}

	// 加载所有函数
	purego.RegisterLibFunc(&b.abiVersion, lib, "bridge_abi_version")
	purego.RegisterLibFunc(&b.engineVersion, lib, "bridge_engine_version")
	purego.RegisterLibFunc(&b.capabilities, lib, "bridge_capabilities")
	purego.RegisterLibFunc(&b.lastError, lib, "bridge_last_error")
	purego.RegisterLibFunc(&b.lastErrorFree, lib, "bridge_last_error_free")
	purego.RegisterLibFunc(&b.stringFree, lib, "bridge_string_free")
	purego.RegisterLibFunc(&b.planCompile, lib, "bridge_plan_compile")
	purego.RegisterLibFunc(&b.planFree, lib, "bridge_plan_free")
	purego.RegisterLibFunc(&b.planExplain, lib, "bridge_plan_explain")
	purego.RegisterLibFunc(&b.planExecutePrint, lib, "bridge_plan_execute_and_print")
	purego.RegisterLibFunc(&b.planExecuteArrow, lib, "bridge_plan_execute_arrow")
	purego.RegisterLibFunc(&b.planCollectDF, lib, "bridge_plan_collect_df")
	purego.RegisterLibFunc(&b.dfToArrow, lib, "bridge_df_to_arrow")
	purego.RegisterLibFunc(&b.dfToJSON, lib, "bridge_df_to_json")
	purego.RegisterLibFunc(&b.dfPrint, lib, "bridge_df_print")
	purego.RegisterLibFunc(&b.dfFree, lib, "bridge_df_free")
	purego.RegisterLibFunc(&b.dfFromColumns, lib, "bridge_df_from_columns")
	purego.RegisterLibFunc(&b.dfFromArrow, lib, "bridge_df_from_arrow")
	purego.RegisterLibFunc(&b.dfPivot, lib, "bridge_df_pivot")
	purego.RegisterLibFunc(&b.sqlCollectDF, lib, "bridge_sql_collect_df")
	purego.RegisterLibFunc(&b.sqlCollectDFFromPlans, lib, "bridge_sql_collect_df_from_plans")
	purego.RegisterLibFunc(&b.setMemoryLimitBytes, lib, "bridge_set_memory_limit_bytes")
	purego.RegisterLibFunc(&b.registerGoExprMapBatchesCallback, lib, "bridge_register_go_expr_map_batches_callback")

	// 验证 ABI 版本
	abiVer := b.AbiVersion()
	if abiVer != 1 {
		return nil, fmt.Errorf("ABI version mismatch: expected 1, got %d", abiVer)
	}

	bridgeCacheMu.Lock()
	if cached := bridgeCache[resolvedPath]; cached != nil {
		bridgeCacheMu.Unlock()
		return cached, nil
	}
	bridgeCache[resolvedPath] = b
	bridgeCacheMu.Unlock()

	return b, nil
}

// DefaultBridge returns the cached default bridge instance.
//
// It is equivalent to LoadBridge("") and loads the default dynamic library at
// most once per process.
func DefaultBridge() (*Bridge, error) {
	return LoadBridge("")
}

func resolveLibPath(libPath string) (string, error) {
	if libPath == "" {
		resolvedPath, err := resolveDefaultLibPath()
		if err != nil {
			return "", err
		}
		libPath = resolvedPath
	}

	if _, err := os.Stat(libPath); os.IsNotExist(err) {
		return "", fmt.Errorf("library not found: %s", libPath)
	}

	absPath, err := filepath.Abs(libPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve library path %s: %w", libPath, err)
	}
	if realPath, err := filepath.EvalSymlinks(absPath); err == nil {
		return realPath, nil
	}
	return absPath, nil
}

func resolveDefaultLibPath() (string, error) {
	if libPath := os.Getenv("POLARS_BRIDGE_LIB"); libPath != "" {
		return libPath, nil
	}

	libName := getLibName()
	candidates := make([]string, 0, 8)

	if exePath, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(exePath), libName))
	}

	if cwd, err := os.Getwd(); err == nil {
		dir := cwd
		for {
			candidates = append(candidates, filepath.Join(dir, libName))
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	if len(candidates) == 0 {
		return "", fmt.Errorf("library not found: %s", libName)
	}
	return "", fmt.Errorf("library not found: %s", candidates[0])
}

func getLibName() string {
	switch runtime.GOOS {
	case "windows":
		return "polars_bridge.dll"
	case "darwin":
		return "libpolars_bridge.dylib"
	default:
		return "libpolars_bridge.so"
	}
}

// AbiVersion 获取 ABI 版本
func (b *Bridge) AbiVersion() uint32 {
	return b.abiVersion()
}

// EngineVersion 获取引擎版本
func (b *Bridge) EngineVersion() (string, error) {
	var ptr uintptr
	var length uintptr
	ret := b.engineVersion(&ptr, &length)
	if ret != 0 {
		return "", b.getLastError()
	}
	return ptrToString(ptr, int(length)), nil
}

// Capabilities 获取能力信息
func (b *Bridge) Capabilities() (string, error) {
	var ptr uintptr
	var length uintptr
	ret := b.capabilities(&ptr, &length)
	if ret != 0 {
		return "", b.getLastError()
	}
	return ptrToString(ptr, int(length)), nil
}

// CompilePlan 编译计划
func (b *Bridge) CompilePlan(planBytes []byte) (uint64, error) {
	var handle uint64
	ret := b.planCompile(&planBytes[0], uintptr(len(planBytes)), &handle)
	runtime.KeepAlive(planBytes)

	if ret != 0 {
		return 0, b.getLastError()
	}
	return handle, nil
}

// FreePlan 释放计划
func (b *Bridge) FreePlan(handle uint64) {
	b.planFree(handle)
}

// ExplainPlan returns the plan explanation string.
func (b *Bridge) ExplainPlan(handle uint64, inputDFHandles []uint64, optimized bool) (string, error) {
	var ptr uintptr
	var length uintptr
	var inputPtr *uint64
	if len(inputDFHandles) > 0 {
		inputPtr = &inputDFHandles[0]
	}
	var optimizedFlag int32
	if optimized {
		optimizedFlag = 1
	}
	ret := b.planExplain(handle, inputPtr, uintptr(len(inputDFHandles)), optimizedFlag, &ptr, &length)
	runtime.KeepAlive(inputDFHandles)
	if ret != 0 {
		return "", b.getLastError()
	}
	defer b.stringFree(ptr, length)
	return ptrToString(ptr, int(length)), nil
}

// DataFrameToJSON exports a Rust-side dataframe handle as JSON text.
func (b *Bridge) DataFrameToJSON(handle uint64, format int32) ([]byte, error) {
	var ptr uintptr
	var length uintptr
	ret := b.dfToJSON(handle, format, &ptr, &length)
	if ret != 0 {
		return nil, b.getLastError()
	}
	defer b.stringFree(ptr, length)
	return ptrToBytes(ptr, int(length)), nil
}

// ExecuteArrow 执行计划并通过 Arrow C Data Interface 返回结果（零拷贝）。
//
// 重要的所有权规则：
// - 输入的 schema/array 所有权会转移给 Rust（无论成功还是失败）
// - 调用方不应在调用后访问或释放输入的 schema/array
// - 如果成功，调用方必须在使用完输出后调用 ReleaseArrowSchema/ReleaseArrowArray
// - 如果失败，输出的 schema/array 无需释放（未初始化）
func (b *Bridge) ExecuteArrow(
	handle uint64,
	inputSchema *ArrowSchema,
	inputArray *ArrowArray,
) (*ArrowSchema, *ArrowArray, error) {
	if !cgoEnabled {
		return nil, nil, fmt.Errorf("ExecuteArrow requires cgo (set CGO_ENABLED=1)")
	}

	outSchema := &ArrowSchema{}
	outArray := &ArrowArray{}

	ret := b.planExecuteArrow(handle, inputSchema, inputArray, outSchema, outArray)
	if ret != 0 {
		// 注意：输入的资源已被 Rust 接管（无论成功失败）
		// 输出资源未初始化，无需释放
		return nil, nil, b.getLastError()
	}

	return outSchema, outArray, nil
}

// ExecuteAndPrint 执行并打印结果（使用 Polars 原生 Display）
func (b *Bridge) ExecuteAndPrint(handle uint64) error {
	ret := b.planExecutePrint(handle)

	if ret != 0 {
		return b.getLastError()
	}

	return nil
}

// CollectPlanDF executes a plan and returns a DataFrame handle.
func (b *Bridge) CollectPlanDF(planHandle uint64, inputDFHandles []uint64) (uint64, error) {
	var dfHandle uint64
	var inputPtr *uint64
	if len(inputDFHandles) > 0 {
		inputPtr = &inputDFHandles[0]
	}
	ret := b.planCollectDF(planHandle, inputPtr, uintptr(len(inputDFHandles)), &dfHandle)
	runtime.KeepAlive(inputDFHandles)

	if ret != 0 {
		return 0, b.getLastError()
	}
	return dfHandle, nil
}

// CreateDataFrameFromColumns creates a DataFrame from JSON-encoded column data.
//
// This is the low-level compatibility JSON import path used by the
// higher-level
// NewDataFrameFromMap and NewDataFrameFromRows APIs in the polars package.
func (b *Bridge) CreateDataFrameFromColumns(jsonData []byte) (uint64, error) {
	if len(jsonData) == 0 {
		return 0, fmt.Errorf("jsonData is empty")
	}

	var dfHandle uint64
	ret := b.dfFromColumns(&jsonData[0], uintptr(len(jsonData)), &dfHandle)
	runtime.KeepAlive(jsonData) // 确保在 FFI 调用期间 jsonData 不被 GC

	if ret != 0 {
		return 0, b.getLastError()
	}

	return dfHandle, nil
}

// CreateDataFrameFromArrow creates a DataFrame from Arrow C Data Interface
// input.
//
// This is the low-level Arrow import path used by the higher-level
// NewDataFrameFromArrowRecordBatch API in the polars package.
//
// Ownership rules:
// - inputSchema and inputArray ownership transfers to Rust on call
// - callers must not access or release them after this function returns
func (b *Bridge) CreateDataFrameFromArrow(
	inputSchema *ArrowSchema,
	inputArray *ArrowArray,
) (uint64, error) {
	if !cgoEnabled {
		return 0, fmt.Errorf("CreateDataFrameFromArrow requires cgo (set CGO_ENABLED=1)")
	}
	if inputSchema == nil || inputArray == nil {
		return 0, fmt.Errorf("input schema/array must not be nil")
	}

	var dfHandle uint64
	ret := b.dfFromArrow(inputSchema, inputArray, &dfHandle)
	if ret != 0 {
		return 0, b.getLastError()
	}
	return dfHandle, nil
}

// PivotDataFrame performs an eager pivot operation on the given dataframe handle.
func (b *Bridge) PivotDataFrame(handle uint64, optsJSON []byte) (uint64, error) {
	if handle == 0 {
		return 0, fmt.Errorf("dataframe handle must not be zero")
	}
	if len(optsJSON) == 0 {
		return 0, fmt.Errorf("pivot options are empty")
	}

	var dfHandle uint64
	ret := b.dfPivot(handle, &optsJSON[0], uintptr(len(optsJSON)), &dfHandle)
	runtime.KeepAlive(optsJSON)
	if ret != 0 {
		return 0, b.getLastError()
	}
	return dfHandle, nil
}

// RegisterGoExprMapBatchesCallback registers the Go callback used by
// expression-level MapBatches UDG execution.
func (b *Bridge) RegisterGoExprMapBatchesCallback(callback uintptr) error {
	if callback == 0 {
		return fmt.Errorf("callback pointer must not be zero")
	}
	ret := b.registerGoExprMapBatchesCallback(callback)
	if ret != 0 {
		return b.getLastError()
	}
	return nil
}

// SetMemoryLimitBytes configures the Rust-side execution memory limit.
//
// A non-positive value disables the limit.
func (b *Bridge) SetMemoryLimitBytes(limitBytes int64) error {
	ret := b.setMemoryLimitBytes(limitBytes)
	if ret != 0 {
		return b.getLastError()
	}
	return nil
}

// SQLCollectDF executes a SQL query against registered dataframe handles and
// returns the resulting dataframe handle.
func (b *Bridge) SQLCollectDF(query []byte, tableNamesJSON []byte, inputDFHandles []uint64) (uint64, error) {
	if len(query) == 0 {
		return 0, fmt.Errorf("query is empty")
	}
	if len(tableNamesJSON) == 0 {
		return 0, fmt.Errorf("tableNamesJSON is empty")
	}

	var dfHandle uint64
	var inputPtr *uint64
	if len(inputDFHandles) > 0 {
		inputPtr = &inputDFHandles[0]
	}
	ret := b.sqlCollectDF(&query[0], uintptr(len(query)), &tableNamesJSON[0], uintptr(len(tableNamesJSON)), inputPtr, uintptr(len(inputDFHandles)), &dfHandle)
	runtime.KeepAlive(query)
	runtime.KeepAlive(tableNamesJSON)
	runtime.KeepAlive(inputDFHandles)
	if ret != 0 {
		return 0, b.getLastError()
	}
	return dfHandle, nil
}

// SQLCollectDFFromPlans executes a SQL query against registered plan handles
// and returns the resulting dataframe handle.
func (b *Bridge) SQLCollectDFFromPlans(
	query []byte,
	tableNamesJSON []byte,
	planHandles []uint64,
	inputDFHandles []uint64,
	inputDFOffsets []uintptr,
) (uint64, error) {
	if len(query) == 0 {
		return 0, fmt.Errorf("query is empty")
	}
	if len(tableNamesJSON) == 0 {
		return 0, fmt.Errorf("tableNamesJSON is empty")
	}
	if len(planHandles) == 0 {
		return 0, fmt.Errorf("planHandles is empty")
	}
	if len(inputDFOffsets) != len(planHandles)+1 {
		return 0, fmt.Errorf("inputDFOffsets length must equal len(planHandles)+1")
	}

	var (
		dfHandle    uint64
		planPtr     = &planHandles[0]
		inputPtr    *uint64
		inputOffPtr = &inputDFOffsets[0]
	)
	if len(inputDFHandles) > 0 {
		inputPtr = &inputDFHandles[0]
	}
	ret := b.sqlCollectDFFromPlans(
		&query[0], uintptr(len(query)),
		&tableNamesJSON[0], uintptr(len(tableNamesJSON)),
		planPtr, uintptr(len(planHandles)),
		inputPtr, uintptr(len(inputDFHandles)),
		inputOffPtr, uintptr(len(inputDFOffsets)),
		&dfHandle,
	)
	runtime.KeepAlive(query)
	runtime.KeepAlive(tableNamesJSON)
	runtime.KeepAlive(planHandles)
	runtime.KeepAlive(inputDFHandles)
	runtime.KeepAlive(inputDFOffsets)
	if ret != 0 {
		return 0, b.getLastError()
	}
	return dfHandle, nil
}

// ExportDataFrameToArrow exports a DataFrame through the Arrow C Data
// Interface.
//
// If successful, the caller owns the returned schema/array and must release
// them with ReleaseArrowSchema/ReleaseArrowArray.
func (b *Bridge) ExportDataFrameToArrow(handle uint64) (*ArrowSchema, *ArrowArray, error) {
	if !cgoEnabled {
		return nil, nil, fmt.Errorf("ExportDataFrameToArrow requires cgo (set CGO_ENABLED=1)")
	}

	outSchema := &ArrowSchema{}
	outArray := &ArrowArray{}
	ret := b.dfToArrow(handle, outSchema, outArray)
	if ret != 0 {
		return nil, nil, b.getLastError()
	}
	return outSchema, outArray, nil
}

// DataFramePrint 打印 DataFrame（使用 Polars 原生 Display）
func (b *Bridge) DataFramePrint(handle uint64) error {
	ret := b.dfPrint(handle)
	if ret != 0 {
		return b.getLastError()
	}
	return nil
}

// FreeDataFrame 释放 DataFrame 句柄
func (b *Bridge) FreeDataFrame(handle uint64) {
	b.dfFree(handle)
}

func (b *Bridge) getLastError() error {
	var ptr uintptr
	var length uintptr
	b.lastError(&ptr, &length)

	if ptr == 0 {
		return fmt.Errorf("unknown error")
	}

	errMsg := ptrToString(ptr, int(length))
	return fmt.Errorf("%s", errMsg)
}

func ptrToString(ptr uintptr, length int) string {
	if ptr == 0 || length == 0 {
		return ""
	}
	bytes := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), length)
	return string(bytes)
}

func ptrToBytes(ptr uintptr, length int) []byte {
	if ptr == 0 || length == 0 {
		return nil
	}
	src := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), length)
	out := make([]byte, length)
	copy(out, src)
	return out
}
