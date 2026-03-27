# Changelog

## Contents

- [2026-03-25](#2026-03-25)

## 2026-03-25

这一版的重点是把 `polars-go` 往“更像 Go 库、也更适合真实业务接入”的方向推进了一步。

### Go Struct Import / Export

- 新增原生 Go struct 导入导出能力。
- 现在可以直接使用 `polars.NewDataFrame([]MyStruct{...})` 或 `polars.NewDataFrameFromStructs(...)` 导入数据。
- 现在可以使用 `polars.ToStructs[T](df)` 和 `polars.ToStructPointers[T](df)` 将结果导出为 typed Go 对象。
- 当前支持 `polars` tag、忽略字段、指针空值、嵌套 struct、`[]string`、`[]byte`、常见数值类型和 `time.Time`。

### Type System And Bridge Coverage

- 补齐了更多 Arrow binary / fixed-size-binary / nested struct / list 的导入导出支持。
- 显式 `WithArrowSchema(...)` 的场景更完整。
- 复杂嵌套数据更容易稳定走列式路径。

### Errors And Lifecycle

- 新增 `ErrInvalidInput`、`ErrNilDataFrame`、`ErrClosedDataFrame`、`ErrNilLazyFrame`、`ErrNilSQLContext` 和 `ValidationError`。
- 输入校验失败、对象关闭后误用、空对象调用现在都能更稳定地识别。
- `DataFrame` 和 `EagerFrame` 增加了 `Closed()` 状态判断。
- `DataFrame` / `EagerFrame` / `LazyFrame` 现在都支持 `LogicalPlan()` 和 `OptimizedPlan()`，调试体验更好。

### Performance

- `ToStructs` 现在优先走 Arrow 直转。
- 常见标量列、嵌套 struct 和常见 list 列已命中快路径。
- 当前基线下，`ToStructs` 已经同时优于 `ToMaps` 的延迟、内存和分配次数。
- `ToStructPointers` 比 `ToStructs` 稍贵一些，但仍明显优于 `ToMaps`。

### Docs And Examples

- README 现在包含 `[]struct <-> DataFrame` 的完整说明、性能建议和最新 benchmark。
- 新增对照示例 [examples/struct_roundtrip/main.go](../examples/struct_roundtrip/main.go)。

### Verification

- `go test ./polars`
