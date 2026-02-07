//go:build !windows
// +build !windows

package bridge

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
)

// Bridge Rust FFI 接口
type Bridge struct {
	lib               uintptr
	abiVersion        func() uint32
	engineVersion     func(*uintptr, *uintptr) int32
	capabilities      func(*uintptr, *uintptr) int32
	lastError         func(*uintptr, *uintptr) int32
	lastErrorFree     func(uintptr, uintptr)
	planCompile       func(*byte, uintptr, *uint64) int32
	planFree          func(uint64)
	planExecuteSimple func(uint64, *byte, uintptr, *uintptr, *uintptr) int32
	planExecutePrint  func(uint64) int32
	planExecuteArrow  func(uint64, *ArrowSchema, *ArrowArray, *ArrowSchema, *ArrowArray) int32
	planCollectDF     func(uint64, uint64, *uint64) int32
	dfToIPC           func(uint64, *uintptr, *uintptr) int32
	dfPrint           func(uint64) int32
	dfFree            func(uint64)
	dfFromColumns     func(*byte, uintptr, *uint64) int32
	outputFree        func(uintptr, uintptr)
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

	lib, err := purego.Dlopen(libPath, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return nil, fmt.Errorf("failed to load library %s: %w", libPath, err)
	}

	b := &Bridge{lib: lib}

	// 加载所有函数
	purego.RegisterLibFunc(&b.abiVersion, lib, "bridge_abi_version")
	purego.RegisterLibFunc(&b.engineVersion, lib, "bridge_engine_version")
	purego.RegisterLibFunc(&b.capabilities, lib, "bridge_capabilities")
	purego.RegisterLibFunc(&b.lastError, lib, "bridge_last_error")
	purego.RegisterLibFunc(&b.lastErrorFree, lib, "bridge_last_error_free")
	purego.RegisterLibFunc(&b.planCompile, lib, "bridge_plan_compile")
	purego.RegisterLibFunc(&b.planFree, lib, "bridge_plan_free")
	purego.RegisterLibFunc(&b.planExecuteSimple, lib, "bridge_plan_execute_simple")
	purego.RegisterLibFunc(&b.planExecutePrint, lib, "bridge_plan_execute_and_print")
	purego.RegisterLibFunc(&b.planExecuteArrow, lib, "bridge_plan_execute_arrow")
	purego.RegisterLibFunc(&b.planCollectDF, lib, "bridge_plan_collect_df")
	purego.RegisterLibFunc(&b.dfToIPC, lib, "bridge_df_to_ipc")
	purego.RegisterLibFunc(&b.dfPrint, lib, "bridge_df_print")
	purego.RegisterLibFunc(&b.dfFree, lib, "bridge_df_free")
	purego.RegisterLibFunc(&b.dfFromColumns, lib, "bridge_df_from_columns")
	purego.RegisterLibFunc(&b.outputFree, lib, "bridge_output_free")

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

// ExecuteSimple 简单执行（返回 Arrow IPC 二进制数据）
func (b *Bridge) ExecuteSimple(handle uint64, inputJSON string) ([]byte, error) {
	inputBytes := []byte(inputJSON)
	var outputPtr uintptr
	var outputLen uintptr

	// 处理空输入的情况（文件扫描不需要输入数据）
	var inputPtr *byte
	if len(inputBytes) > 0 {
		inputPtr = &inputBytes[0]
	}

	ret := b.planExecuteSimple(
		handle,
		inputPtr,
		uintptr(len(inputBytes)),
		&outputPtr,
		&outputLen,
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
	b.outputFree(outputPtr, outputLen)

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

// CollectPlanDF 执行计划并返回 DataFrame 句柄
func (b *Bridge) CollectPlanDF(planHandle uint64, inputDFHandle uint64) (uint64, error) {
	var dfHandle uint64
	ret := b.planCollectDF(planHandle, inputDFHandle, &dfHandle)

	if ret != 0 {
		return 0, b.getLastError()
	}
	return dfHandle, nil
}

// CreateDataFrameFromColumns 从 JSON 格式的列数据创建 DataFrame
// jsonData 格式: [{"name": "col1", "values": [1, 2, 3]}, {"name": "col2", "values": ["a", "b", "c"]}]
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

// DataFrameToIPC 将 DataFrame 导出为 Arrow IPC 二进制数据
func (b *Bridge) DataFrameToIPC(handle uint64) ([]byte, error) {
	var outputPtr uintptr
	var outputLen uintptr

	ret := b.dfToIPC(handle, &outputPtr, &outputLen)
	if ret != 0 {
		return nil, b.getLastError()
	}

	output := make([]byte, outputLen)
	for i := uintptr(0); i < outputLen; i++ {
		output[i] = *(*byte)(unsafe.Pointer(outputPtr + i))
	}
	b.outputFree(outputPtr, outputLen)

	return output, nil
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
	return unsafe.String((*byte)(unsafe.Pointer(ptr)), length)
}
