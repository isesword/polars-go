//go:build windows
// +build windows

package bridge

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"unsafe"
)

// Bridge Rust FFI 接口
type Bridge struct {
	lib               *syscall.DLL
	abiVersion        *syscall.Proc
	engineVersion     *syscall.Proc
	capabilities      *syscall.Proc
	lastError         *syscall.Proc
	lastErrorFree     *syscall.Proc
	planCompile       *syscall.Proc
	planFree          *syscall.Proc
	planExecuteSimple *syscall.Proc
	planExecutePrint  *syscall.Proc
	planExecuteArrow  *syscall.Proc
	planCollectDF     *syscall.Proc
	dfToIPC           *syscall.Proc
	dfPrint           *syscall.Proc
	dfFree            *syscall.Proc
	dfFromColumns     *syscall.Proc
	outputFree        *syscall.Proc
}

// LoadBridge 加载动态库
func LoadBridge(libPath string) (*Bridge, error) {
	if libPath == "" {
		// 优先级：环境变量 > 可执行文件目录
		libPath = os.Getenv("POLARS_BRIDGE_LIB")
		if libPath == "" {
			exePath, err := os.Executable()
			if err != nil {
				return nil, fmt.Errorf("failed to get executable path: %w", err)
			}
			exeDir := filepath.Dir(exePath)
			libPath = filepath.Join(exeDir, getLibName())
		}
	}

	if _, err := os.Stat(libPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("library not found: %s", libPath)
	}

	lib, err := syscall.LoadDLL(libPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load library %s: %w", libPath, err)
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
	if b.planExecuteSimple, err = lib.FindProc("bridge_plan_execute_simple"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_plan_execute_simple: %w", err)
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
	if b.dfToIPC, err = lib.FindProc("bridge_df_to_ipc"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_df_to_ipc: %w", err)
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
	if b.outputFree, err = lib.FindProc("bridge_output_free"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_output_free: %w", err)
	}

	// 验证 ABI 版本
	abiVer := b.AbiVersion()
	if abiVer != 1 {
		return nil, fmt.Errorf("ABI version mismatch: expected 1, got %d", abiVer)
	}

	return b, nil
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

// ExecuteSimple 简单执行（返回 Arrow IPC 二进制数据）
func (b *Bridge) ExecuteSimple(handle uint64, inputJSON string) ([]byte, error) {
	inputBytes := []byte(inputJSON)
	var outputPtr uintptr
	var outputLen uintptr

	// 处理空输入的情况（文件扫描不需要输入数据）
	var inputPtr uintptr
	if len(inputBytes) > 0 {
		inputPtr = uintptr(unsafe.Pointer(&inputBytes[0]))
	}

	ret, _, _ := b.planExecuteSimple.Call(
		uintptr(handle),
		inputPtr,
		uintptr(len(inputBytes)),
		uintptr(unsafe.Pointer(&outputPtr)),
		uintptr(unsafe.Pointer(&outputLen)),
	)
	runtime.KeepAlive(inputBytes)

	if ret != 0 {
		return nil, b.getLastError()
	}

	// 把二进制数据拷贝到 Go 的 slice
	output := make([]byte, outputLen)
	for i := uintptr(0); i < outputLen; i++ {
		output[i] = *(*byte)(unsafe.Pointer(outputPtr + i))
	}
	b.outputFree.Call(outputPtr, outputLen)

	return output, nil
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
func (b *Bridge) CollectPlanDF(planHandle uint64, inputDFHandle uint64) (uint64, error) {
	var dfHandle uint64
	ret, _, _ := b.planCollectDF.Call(
		uintptr(planHandle),
		uintptr(inputDFHandle),
		uintptr(unsafe.Pointer(&dfHandle)),
	)

	if ret != 0 {
		return 0, b.getLastError()
	}
	return dfHandle, nil
}

// DataFrameToIPC 将 DataFrame 导出为 Arrow IPC 二进制数据
func (b *Bridge) DataFrameToIPC(handle uint64) ([]byte, error) {
	var outputPtr uintptr
	var outputLen uintptr
	ret, _, _ := b.dfToIPC.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(&outputPtr)),
		uintptr(unsafe.Pointer(&outputLen)),
	)
	if ret != 0 {
		return nil, b.getLastError()
	}

	output := make([]byte, outputLen)
	for i := uintptr(0); i < outputLen; i++ {
		output[i] = *(*byte)(unsafe.Pointer(outputPtr + i))
	}
	b.outputFree.Call(outputPtr, outputLen)

	return output, nil
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

// CreateDataFrameFromColumns 从列数据创建 DataFrame（JSON 格式）
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
