//go:build windows
// +build windows

package bridge

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"unsafe"
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
	lib                              *syscall.DLL
	abiVersion                       *syscall.Proc
	engineVersion                    *syscall.Proc
	capabilities                     *syscall.Proc
	lastError                        *syscall.Proc
	lastErrorFree                    *syscall.Proc
	planCompile                      *syscall.Proc
	planFree                         *syscall.Proc
	planExplain                      *syscall.Proc
	planExecutePrint                 *syscall.Proc
	planExecuteArrow                 *syscall.Proc
	planCollectDF                    *syscall.Proc
	dfToArrow                        *syscall.Proc
	dfPrint                          *syscall.Proc
	dfFree                           *syscall.Proc
	dfFromColumns                    *syscall.Proc
	dfFromArrow                      *syscall.Proc
	dfPivot                          *syscall.Proc
	sqlCollectDF                     *syscall.Proc
	sqlCollectDFFromPlans            *syscall.Proc
	setMemoryLimitBytes              *syscall.Proc
	registerGoExprMapBatchesCallback *syscall.Proc
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

	lib, err := syscall.LoadDLL(resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load library %s: %w", resolvedPath, err)
	}

	b := &Bridge{lib: lib}

	// 加载所有函数
	if b.abiVersion, err = lib.FindProc("bridge_abi_version"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_abi_version: %w", err)
	}
	if b.engineVersion, err = lib.FindProc("bridge_engine_version"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_engine_version: %w", err)
	}
	if b.capabilities, err = lib.FindProc("bridge_capabilities"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_capabilities: %w", err)
	}
	if b.lastError, err = lib.FindProc("bridge_last_error"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_last_error: %w", err)
	}
	if b.lastErrorFree, err = lib.FindProc("bridge_last_error_free"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_last_error_free: %w", err)
	}
	if b.planCompile, err = lib.FindProc("bridge_plan_compile"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_plan_compile: %w", err)
	}
	if b.planFree, err = lib.FindProc("bridge_plan_free"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_plan_free: %w", err)
	}
	if b.planExplain, err = lib.FindProc("bridge_plan_explain"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_plan_explain: %w", err)
	}
	if b.planExecutePrint, err = lib.FindProc("bridge_plan_execute_and_print"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_plan_execute_and_print: %w", err)
	}
	if b.planExecuteArrow, err = lib.FindProc("bridge_plan_execute_arrow"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_plan_execute_arrow: %w", err)
	}
	if b.planCollectDF, err = lib.FindProc("bridge_plan_collect_df"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_plan_collect_df: %w", err)
	}
	if b.dfToArrow, err = lib.FindProc("bridge_df_to_arrow"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_df_to_arrow: %w", err)
	}
	if b.dfPrint, err = lib.FindProc("bridge_df_print"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_df_print: %w", err)
	}
	if b.dfFree, err = lib.FindProc("bridge_df_free"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_df_free: %w", err)
	}
	if b.dfFromColumns, err = lib.FindProc("bridge_df_from_columns"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_df_from_columns: %w", err)
	}
	if b.dfFromArrow, err = lib.FindProc("bridge_df_from_arrow"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_df_from_arrow: %w", err)
	}
	if b.dfPivot, err = lib.FindProc("bridge_df_pivot"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_df_pivot: %w", err)
	}
	if b.sqlCollectDF, err = lib.FindProc("bridge_sql_collect_df"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_sql_collect_df: %w", err)
	}
	if b.sqlCollectDFFromPlans, err = lib.FindProc("bridge_sql_collect_df_from_plans"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_sql_collect_df_from_plans: %w", err)
	}
	if b.setMemoryLimitBytes, err = lib.FindProc("bridge_set_memory_limit_bytes"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_set_memory_limit_bytes: %w", err)
	}
	if b.registerGoExprMapBatchesCallback, err = lib.FindProc("bridge_register_go_expr_map_batches_callback"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_register_go_expr_map_batches_callback: %w", err)
	}
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

func boolToUintptr(v bool) uintptr {
	if v {
		return 1
	}
	return 0
}

// AbiVersion 获取 ABI 版本
func (b *Bridge) AbiVersion() uint32 {
	ret, _, _ := b.abiVersion.Call()
	return uint32(ret)
}

// EngineVersion 获取引擎版本
func (b *Bridge) EngineVersion() (string, error) {
	var ptr uintptr
	var length uintptr
	ret, _, _ := b.engineVersion.Call(uintptr(unsafe.Pointer(&ptr)), uintptr(unsafe.Pointer(&length)))
	if ret != 0 {
		return "", b.getLastError()
	}
	return ptrToString(ptr, int(length)), nil
}

// Capabilities 获取能力信息
func (b *Bridge) Capabilities() (string, error) {
	var ptr uintptr
	var length uintptr
	ret, _, _ := b.capabilities.Call(uintptr(unsafe.Pointer(&ptr)), uintptr(unsafe.Pointer(&length)))
	if ret != 0 {
		return "", b.getLastError()
	}
	return ptrToString(ptr, int(length)), nil
}

// CompilePlan 编译计划
func (b *Bridge) CompilePlan(planBytes []byte) (uint64, error) {
	var handle uint64
	ret, _, _ := b.planCompile.Call(
		uintptr(unsafe.Pointer(&planBytes[0])),
		uintptr(len(planBytes)),
		uintptr(unsafe.Pointer(&handle)),
	)
	runtime.KeepAlive(planBytes)

	if ret != 0 {
		return 0, b.getLastError()
	}
	return handle, nil
}

// FreePlan 释放计划
func (b *Bridge) FreePlan(handle uint64) {
	b.planFree.Call(uintptr(handle))
}

// ExplainPlan returns the plan explanation string.
func (b *Bridge) ExplainPlan(handle uint64, inputDFHandles []uint64, optimized bool) (string, error) {
	var ptr uintptr
	var length uintptr
	var inputPtr *uint64
	if len(inputDFHandles) > 0 {
		inputPtr = &inputDFHandles[0]
	}
	ret, _, _ := b.planExplain.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(inputPtr)),
		uintptr(len(inputDFHandles)),
		boolToUintptr(optimized),
		uintptr(unsafe.Pointer(&ptr)),
		uintptr(unsafe.Pointer(&length)),
	)
	runtime.KeepAlive(inputDFHandles)
	if ret != 0 {
		return "", b.getLastError()
	}
	defer b.lastErrorFree.Call(ptr, length)
	return ptrToString(ptr, int(length)), nil
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

	ret, _, _ := b.planExecuteArrow.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(inputSchema)),
		uintptr(unsafe.Pointer(inputArray)),
		uintptr(unsafe.Pointer(outSchema)),
		uintptr(unsafe.Pointer(outArray)),
	)

	if ret != 0 {
		// 注意：输入的资源已被 Rust 接管（无论成功失败）
		// 输出资源未初始化，无需释放
		return nil, nil, b.getLastError()
	}

	return outSchema, outArray, nil
}

// ExecuteAndPrint 执行并打印结果（使用 Polars 原生 Display）
func (b *Bridge) ExecuteAndPrint(handle uint64) error {
	ret, _, _ := b.planExecutePrint.Call(uintptr(handle))

	if ret != 0 {
		return b.getLastError()
	}

	return nil
}

// CollectPlanDF 执行计划并返回 DataFrame 句柄
func (b *Bridge) CollectPlanDF(planHandle uint64, inputDFHandles []uint64) (uint64, error) {
	var dfHandle uint64
	var inputPtr *uint64
	if len(inputDFHandles) > 0 {
		inputPtr = &inputDFHandles[0]
	}
	ret, _, _ := b.planCollectDF.Call(
		uintptr(planHandle),
		uintptr(unsafe.Pointer(inputPtr)),
		uintptr(len(inputDFHandles)),
		uintptr(unsafe.Pointer(&dfHandle)),
	)

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

	ret, _, _ := b.dfToArrow.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(outSchema)),
		uintptr(unsafe.Pointer(outArray)),
	)
	if int32(ret) != 0 {
		return nil, nil, b.getLastError()
	}
	return outSchema, outArray, nil
}

// DataFramePrint 打印 DataFrame（使用 Polars 原生 Display）
func (b *Bridge) DataFramePrint(handle uint64) error {
	ret, _, _ := b.dfPrint.Call(uintptr(handle))
	if ret != 0 {
		return b.getLastError()
	}
	return nil
}

// FreeDataFrame 释放 DataFrame 句柄
func (b *Bridge) FreeDataFrame(handle uint64) {
	b.dfFree.Call(uintptr(handle))
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
	ret, _, _ := b.dfFromColumns.Call(
		uintptr(unsafe.Pointer(&jsonData[0])),
		uintptr(len(jsonData)),
		uintptr(unsafe.Pointer(&dfHandle)),
	)
	runtime.KeepAlive(jsonData)

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
	ret, _, _ := b.dfFromArrow.Call(
		uintptr(unsafe.Pointer(inputSchema)),
		uintptr(unsafe.Pointer(inputArray)),
		uintptr(unsafe.Pointer(&dfHandle)),
	)
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
	ret, _, _ := b.dfPivot.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(&optsJSON[0])),
		uintptr(len(optsJSON)),
		uintptr(unsafe.Pointer(&dfHandle)),
	)
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
	ret, _, _ := b.registerGoExprMapBatchesCallback.Call(callback)
	if ret != 0 {
		return b.getLastError()
	}
	return nil
}

// SetMemoryLimitBytes configures the Rust-side execution memory limit.
//
// A non-positive value disables the limit.
func (b *Bridge) SetMemoryLimitBytes(limitBytes int64) error {
	ret, _, _ := b.setMemoryLimitBytes.Call(uintptr(limitBytes))
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
	ret, _, _ := b.sqlCollectDF.Call(
		uintptr(unsafe.Pointer(&query[0])),
		uintptr(len(query)),
		uintptr(unsafe.Pointer(&tableNamesJSON[0])),
		uintptr(len(tableNamesJSON)),
		uintptr(unsafe.Pointer(inputPtr)),
		uintptr(len(inputDFHandles)),
		uintptr(unsafe.Pointer(&dfHandle)),
	)
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
		dfHandle uint64
		inputPtr *uint64
	)
	if len(inputDFHandles) > 0 {
		inputPtr = &inputDFHandles[0]
	}
	ret, _, _ := b.sqlCollectDFFromPlans.Call(
		uintptr(unsafe.Pointer(&query[0])),
		uintptr(len(query)),
		uintptr(unsafe.Pointer(&tableNamesJSON[0])),
		uintptr(len(tableNamesJSON)),
		uintptr(unsafe.Pointer(&planHandles[0])),
		uintptr(len(planHandles)),
		uintptr(unsafe.Pointer(inputPtr)),
		uintptr(len(inputDFHandles)),
		uintptr(unsafe.Pointer(&inputDFOffsets[0])),
		uintptr(len(inputDFOffsets)),
		uintptr(unsafe.Pointer(&dfHandle)),
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

func (b *Bridge) getLastError() error {
	var ptr uintptr
	var length uintptr
	b.lastError.Call(uintptr(unsafe.Pointer(&ptr)), uintptr(unsafe.Pointer(&length)))

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
	bytes := make([]byte, length)
	for i := 0; i < length; i++ {
		bytes[i] = *(*byte)(unsafe.Pointer(ptr + uintptr(i)))
	}
	return string(bytes)
}
