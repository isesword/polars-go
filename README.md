# Polars Go Bridge

一个高性能的 Go 语言 Polars 数据处理库，通过 FFI 桥接 Rust Polars，提供类似 Polars 的 Fluent API。

## ✨ 特性

- 🚀 **零拷贝数据传输**：使用 Arrow C Data Interface，避免中间序列化开销
- 📁 **懒加载文件扫描**：直接从 CSV/Parquet 文件读取，Go 不参与数据加载
- ⛓️ **Fluent API**：链式调用，类似 Polars 的使用体验
- 🔧 **Lazy Evaluation**：构建查询计划，延迟执行，优化性能
- 🌐 **跨平台**：支持 macOS、Linux、Windows
- 🔄 **表达式展开**：支持 `Cols()` 多列选择、`All()` 选择所有列
- 🔃 **类型转换**：支持严格/非严格模式的类型转换，支持所有数值类型

## 🏗️ 架构

```
Go (Fluent API)
    ↓ Protobuf (Plan)
Rust (Polars Bridge)
    ↓ 调用 Polars API
Polars (执行引擎)
    ↓ Arrow C Data Interface
Go (获取结果)
```

**核心设计**：
- Go 侧构建**查询计划（Plan）**，使用 Protobuf 序列化
- Rust 侧将 Plan 翻译成 Polars 的 LazyFrame 调用
- Go/Rust 之间的列式数据交换使用 **Arrow C Data Interface**

## 🔀 数据交换

项目现在区分两类入口：

- 高层对象入口：`NewDataFrame(...)`
  这是默认推荐入口，遵循 Go 风格命名，由 Rust 负责 schema 推断和建表；显式 schema 仍然可选。
- 补充构造器：`NewDataFrameFromMaps(...)` / `NewDataFrameFromColumns(...)`
  当你希望显式表达“rows 导入”或“columns 导入”时，可以使用这两个更具体的入口。
- 显式 Arrow 入口：`NewDataFrameFromArrow(...)` / `brg.ExecuteArrow(...)`
  这类入口用于已经有 Arrow 数据或明确想走 Arrow C Data Interface 的场景。
  如果你的输入是 Go rows / columns，但需要显式声明嵌套 `List / Struct` schema，也可以用 `WithArrowSchema(...)`。

默认推荐的入口：

- `polars.NewDataFrame(...)`
- `polars.NewDataFrameFromArrow(...)`
- `brg.ExecuteArrow(...)`

对接 GoFrame / MySQL 时，推荐直接用：

- `polars.NewDataFrame(rows, polars.WithSchema(schema))`
- `polars.NewDataFrame(structRows)`

这样高层会走对象导入流程，并由 Rust 负责推断；如果你已经有 Arrow，再显式使用 `NewDataFrameFromArrow(...)`。
如果你更偏好显式命名，也可以用 `NewDataFrameFromMaps(...)` 或 `NewDataFrameFromColumns(...)`。
如果你需要为 Go 的 row-oriented / column-oriented 数据显式声明嵌套 schema，可以使用 `polars.NewDataFrame(data, polars.WithArrowSchema(schema))`。

### Go / Python Polars 命名对照

为了保持 Go 风格，这个库没有直接照搬 Python Polars 的命名；但语义是一一对应的：

| Go | Python Polars |
|---|---|
| `NewDataFrame(...)` | `pl.DataFrame(...)` |
| `NewDataFrameFromMaps(...)` | `pl.from_dicts(...)` |
| `NewDataFrameFromColumns(...)` | `pl.from_dict(...)` / `pl.DataFrame({...})` |
| `NewDataFrameFromArrow(...)` | `pl.from_arrow(...)` |
| `ToArrow()` | `to_arrow()` |
| `Collect()` | `collect()` |
| `ToMaps()` | `to_dicts()` |
| `polars.ToStructs[T](df)` | `to_dicts()` 后绑定到 typed objects |

如果你是从 Python Polars 迁移过来，可以优先按这张表找对应入口。

### Go Struct 导入 / 导出

除了 `[]map[string]any` / `map[string]interface{}`，现在也支持直接使用 Go struct slice：

- 导入：
  - `polars.NewDataFrame([]MyStruct{...})`
  - `polars.NewDataFrameFromStructs([]MyStruct{...})`
- 导出：
  - `polars.ToStructs[MyStruct](df)`
  - `polars.ToStructPointers[MyStruct](df)`

示例：

```go
type Profile struct {
    City string `polars:"city"`
    Zip  int64  `polars:"zip"`
}

type User struct {
    ID       int64    `polars:"id"`
    Name     string   `polars:"name"`
    Age      *int     `polars:"age"`
    Tags     []string `polars:"tags"`
    Payload  []byte   `polars:"payload"`
    Profile  Profile  `polars:"profile"`
    Nickname *string  `polars:"nickname"`
    IgnoreMe string   `polars:"-"`
}

users := []User{
    {ID: 1, Name: "Alice", Tags: []string{"go", "polars"}},
    {ID: 2, Name: "Bob"},
}

df, err := polars.NewDataFrame(users)
if err != nil {
    log.Fatal(err)
}
defer df.Close()

typed, err := polars.ToStructs[User](df)
if err != nil {
    log.Fatal(err)
}
fmt.Println(typed[0].Name)
```

tag 规则：

- `polars:"name"`：指定列名
- `polars:"-"`：忽略字段
- 未加 tag 时默认使用 Go 字段名

当前行为：

- 支持 `struct` / `*struct` slice 导入
- 指针字段会映射为 nullable
- 支持嵌套 struct、`[]string`、`[]byte`、常见数值类型、`time.Time`
- `ToStructs` / `ToStructPointers` 当前优先走 Arrow 直转；常见标量列、嵌套 struct 和常见 list 列会命中快路径

当前限制：

- `ToStructs[T]` / `ToStructPointers[T]` 的 `T` 必须是 struct，不能是 `*struct`
- 重复的 `polars` 列名会返回 `ErrInvalidInput`
- 如果列值无法转换到目标字段类型，也会返回 `ErrInvalidInput`

### Arrow C Data Interface

项目已经移除了旧的序列化结果通道，Go 和 Rust 之间统一通过 `Arrow C Data Interface` 交换列式数据。

- 它不是文件格式，而是一套内存交换协议。
- 双方通过 `schema` 和 `array` 指针描述同一份 Arrow 列式内存。
- 在本项目里，这条路径用于：
  - `polars.NewDataFrameFromArrow(...)`
  - `brg.ExecuteArrow(...)`
  - `Collect()` / `ToMaps()` / `DataFrame.ToMaps()` 的底层结果导出

可以这样理解：

- `Arrow C Data Interface` 更像“把 Arrow 内存结构直接借给另一侧使用”
- `ToMaps()` 最后仍会组装成 Go 的 `[]map[string]interface{}`，但中间不再经过额外的二进制序列化/反序列化
- `ToArrow()` 则直接返回 Arrow `RecordBatch`，更接近 Python Polars 的 `to_arrow()`

### JSON 导出

当前 JSON 导出已经对齐到更接近 Polars 的命名：

- eager:
  - `(*DataFrame).WriteJSON(w io.Writer)`
  - `(*DataFrame).WriteNDJSON(w io.Writer, opts ...polars.WriteNDJSONOptions)`
- lazy:
  - `(*LazyFrame).SinkJSON(w io.Writer)`
  - `(*LazyFrame).SinkNDJSON(w io.Writer, opts ...polars.SinkNDJSONOptions)`

实现说明：

- JSON 序列化由 Rust / Polars `JsonWriter` 完成，不再由 Go 侧逐行编码
- eager 路径更接近 Python Polars 的 `write_json()` / `write_ndjson()`
- lazy 路径使用 `sink_*` 命名，对齐 Python Polars 的 `sink_ndjson()` 心智
- `NDJSON` 当前提供对 `io.Writer` 真正有意义的附加选项：
  - `Compression: none | gzip`
  - `CompressionLevel`

示例：

```go
df, _ := polars.NewDataFrame([]map[string]any{
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
})
defer df.Close()

var jsonBuf bytes.Buffer
_ = df.WriteJSON(&jsonBuf)

var ndjsonBuf bytes.Buffer
_ = df.WriteNDJSON(&ndjsonBuf, polars.WriteNDJSONOptions{
    Compression: polars.NDJSONCompressionGzip,
})

lf := df.Filter(polars.Col("id").Gt(polars.Lit(1)))

var sinkBuf bytes.Buffer
_ = lf.SinkJSON(&sinkBuf)

var sinkNDJSONBuf bytes.Buffer
_ = lf.SinkNDJSON(&sinkNDJSONBuf, polars.SinkNDJSONOptions{
    Compression: polars.NDJSONCompressionNone,
})
```

注意：

- 当前顶层 binary 列会先在 Rust 侧归一化后再写 JSON，避免 Polars 0.52 对 binary JSON writer 的未实现分支
- `SinkJSON(...)` / `SinkNDJSON(...)` 当前会先 collect 结果，再写出 JSON；接口先对齐，后续如果需要可以再下沉成真正的 plan-to-sink
- Python Polars 里 `write_ndjson` / `sink_ndjson` 还包含更偏文件路径语义的参数；当前 Go 版本面向 `io.Writer`，所以只保留与 writer 直接相关的压缩选项

### Excel 读取

当前提供一个先走 Go 侧实现的 Excel 读取入口：

- `polars.ReadExcel(path string, opts ...polars.ExcelReadOptions)`
- `polars.ReadExcelSheets(path string, opts ...polars.ExcelReadOptions)`

第一版支持范围：

- `.xlsx`
- 单 sheet 读取
- 多 sheet 读取并返回 `map[string]*DataFrame`
- `SheetName` / `SheetID`
- `SheetNames` / `SheetIDs`
- 有表头 / 无表头
- 空表报错或放行

示例：

```go
df, err := polars.ReadExcel("users.xlsx", polars.ExcelReadOptions{
    SheetName: "Sheet1",
})
if err != nil {
    panic(err)
}
defer df.Close()

rows, _ := df.ToMaps()
fmt.Println(rows)
```

无表头时会自动生成列名：

```go
hasHeader := false

df, err := polars.ReadExcel("users.xlsx", polars.ExcelReadOptions{
    SheetID:   2,
    HasHeader: &hasHeader,
})
```

读取多个 sheet：

```go
frames, err := polars.ReadExcelSheets("users.xlsx")
if err != nil {
    panic(err)
}
defer func() {
    for _, df := range frames {
        df.Close()
    }
}()
```

说明：

- 当前实现基于 `excelize`，然后复用现有 `NewDataFrame(...)` 导入链
- 类型推断按列进行，目标是比逐格猜类型更稳定
- 像 `"001"` 这种带前导零的文本会保留成字符串
- 当前还不支持 `.xls` / `.xlsb`

### UDG（User-defined Go functions）

当前已提供三类 Go 侧 UDG 入口：

- `DataFrame.MapRows(...)`
  对齐 Python Polars 的 `DataFrame.map_rows`
- `DataFrame.MapBatches(...)`
  对齐 Python Polars 的 `map_batches` 命名，但当前仍是 eager Go-side batch UDG
- `Expr.MapBatches(...)`
  对齐 Python Polars 的 `Expr.map_batches` 命名，第一版走 Arrow batch 回调，适合单列表达式的高性能 Go-side UDG
- `polars.MapBatches([]Expr{...}, ...)`
  对齐 Python Polars 多表达式 `map_batches` 心智，适合多列表达式输入的高性能 Go-side UDG

建议：

- 能用原生表达式时，优先用原生表达式
- 需要逐行 Go 逻辑时，使用 `MapRows(...)`
- 需要对整张表做 eager Arrow batch 处理时，使用 `DataFrame.MapBatches(...)`
- 需要在 `Select(...)` / `WithColumns(...)` 里做单列表达式级 Arrow batch 处理时，使用 `Expr.MapBatches(...)`
- 需要在 `Select(...)` / `WithColumns(...)` 里做多列表达式级 Arrow batch 处理时，使用 `polars.MapBatches(...)`

### SQL

当前已提供与 Polars SQLContext 对齐的 Go 入口：

- `polars.NewSQLContext()`
- `(*SQLContext).Register(name, table)`
- `(*SQLContext).RegisterMany(tables)`
- `(*SQLContext).Unregister(name)`
- `(*SQLContext).Tables()`
- `(*SQLContext).ExecuteLazy(query)`
- `(*SQLContext).Execute(query)`
- `polars.SQLLazy(query, tables)`
- `polars.SQL(query, tables)`

第一版支持注册：

- `*DataFrame`
- `*EagerFrame`
- `*LazyFrame`

说明：

- SQL 执行由 Rust Polars SQL 引擎完成
- `Register(...)` / `RegisterMany(...)` 返回 `*SQLContext`，可以链式调用
- `DataFrame` / `EagerFrame` / `LazyFrame` 都会以计划形式注册到 SQLContext
- `LazyFrame` 当前已经支持 lazy 直通注册，不再需要先 `Collect()`
- `ExecuteLazy(...)` 对应 Python Polars 的 `eager=False`，返回的是真正 SQL 查询计划对应的 `LazyFrame`
- `ExecuteLazy(...)` 不会提前物化，可以继续链式 `Filter(...)` / `Select(...)` / `Collect()`
- `Execute(...)` 对应 Python Polars 的 `eager=True`

示例：

```go
ctx := polars.NewSQLContext().
    Register("population", df).
    RegisterMany(map[string]any{
        "cities": cities,
    })

tables, err := ctx.Execute("SHOW TABLES")
if err != nil {
    return err
}
defer tables.Close()

result, err := ctx.Execute(`
    SELECT country, city
    FROM population
    ORDER BY city
`)
if err != nil {
    return err
}
defer result.Close()
```

移除表后也会立即反映到上下文和 SQL 查询中：

```go
ctx := polars.NewSQLContext().
    Register("left_tbl", left).
    Register("right_tbl", right).
    Unregister("right_tbl")

result, err := ctx.Execute("SHOW TABLES")
if err != nil {
    return err
}
defer result.Close()
```

### Execution Options

当前已提供第一版执行配置入口：

- `polars.SetExecutionOptions(polars.ExecutionOptions{MemoryLimitBytes: ...})`

说明：

- 这是一个**可选**执行保护配置，默认关闭；不设置时行为与之前一致
- `MemoryLimitBytes <= 0` 表示关闭限制
- 这是本项目提供的执行保护能力，不是 Python Polars 主库现成同名 API 的直接镜像
- 当前第一版由 Go 提供配置入口，Rust 在 collect / SQL / Arrow 导入导出结果上做超限检查
- 超限时会返回 `ERR_OOM`
- 这是一版最小可用的 memory limit，不是完整的 spill-to-disk 方案

## 📦 安装

### 作为 Go Package 使用

#### 1. 安装 Go 包

```bash
go get github.com/isesword/polars-go-bridge
```

#### 2. 下载预编译的动态库

从 [GitHub Releases](https://github.com/YOUR_USERNAME/polars-go-bridge/releases) 下载对应平台的动态库：

- **macOS (Intel)**: `libpolars_bridge.dylib` (x86_64-apple-darwin)
- **macOS (Apple Silicon)**: `libpolars_bridge.dylib` (aarch64-apple-darwin)
- **Linux**: `libpolars_bridge.so` (x86_64-unknown-linux-gnu)
- **Windows**: `polars_bridge.dll` (x86_64-pc-windows-msvc)

将动态库放置在以下位置之一：
- 项目根目录
- 系统库路径（Linux: `/usr/local/lib`, macOS: `/usr/local/lib`, Windows: `C:\Windows\System32`）
- 通过环境变量 `POLARS_BRIDGE_LIB` 指定路径

默认加载顺序：

1. `POLARS_BRIDGE_LIB`
2. 当前可执行文件所在目录
3. 当前工作目录以及向上的父目录（直到找到项目根目录中的动态库）

这意味着在仓库根目录下运行 `go test ./...`、`go run main.go` 或 `go run examples/...` 时，如果根目录里已经有 `libpolars_bridge.*` / `polars_bridge.dll`，通常不需要额外设置环境变量。

#### 3. 使用示例

```go
package main

import (
    "log"
    "github.com/isesword/polars-go-bridge/polars"
)

func main() {
    // 使用默认 DataFrame 构造器创建托管 DataFrame
    df, err := polars.NewDataFrame(map[string]interface{}{
        "name":   []string{"Alice", "Bob", "Charlie"},
        "age":    []int64{25, 30, 35},
        "salary": []float64{50000, 60000, 70000},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer df.Close()

    // 链式操作
    _, _ = df.
        Filter(polars.Col("age").Gt(polars.Lit(28))).
        Select(polars.Col("name"), polars.Col("salary")).
        Collect()
    
    // 或从 CSV 文件扫描
    lf := polars.ScanCSV("data.csv")
    lf.Print()
}
```

#### 4. Arrow 优先示例

```go
package main

import (
    "log"

    "github.com/isesword/polars-go-bridge/polars"
    pb "github.com/isesword/polars-go-bridge/proto"
)

func main() {
    rows := []map[string]any{
        {"id": uint64(1), "name": "Alice", "age": nil},
        {"id": uint64(2), "name": "Bob", "age": 20},
    }

    schema := map[string]pb.DataType{
        "id":   pb.DataType_UINT64,
        "name": pb.DataType_UTF8,
        "age":  pb.DataType_INT32,
    }

    record, err := polars.NewArrowRecordBatchFromRowsWithSchema(rows, schema)
    if err != nil {
        log.Fatal(err)
    }

    df, err := polars.NewDataFrameFromArrow(record)
    if err != nil {
        log.Fatal(err)
    }
    defer df.Close()

    if err := df.Print(); err != nil {
        log.Fatal(err)
    }
}
```

### 前置要求（仅构建时需要）

- Go 1.21+
- Rust 1.70+
- Protobuf compiler

### 构建

```bash
# 1. 使用项目脚本编译 Rust 动态库
# 脚本会在编译完成后自动把动态库复制到项目根目录
./scripts/build.sh

# Windows
scripts\\build.bat

# 2. 生成 Protobuf 代码（如果修改了 proto 文件）
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
export PATH=$PATH:$GOPATH/bin
cd /Users/esword/GolandProjects/src/polars-go-bridge && export PATH=$PATH:$GOPATH/bin && protoc --go_out=. --go_opt=paths=source_relative proto/polars_bridge.proto
```

### Benchmark

```bash
go test ./polars -run '^$' -bench Benchmark -benchmem
```

`ToMaps()` 的专项 benchmark / `pprof` 分析流程见：

- [docs/tomaps-profiling.md](/Users/esword/GolandProjects/src/polars-go-bridge/docs/tomaps-profiling.md)

按规模跑某一组：

```bash
go test ./polars -run '^$' -bench 'BenchmarkImportRows/Rows2000|BenchmarkQueryCollect/Rows4000|BenchmarkToMaps/Rows4000|BenchmarkExprVsMapBatches/Rows4000' -benchtime=3x -benchmem
```

当前基线（Apple M4 Pro, `go test ./polars -run '^$' -bench 'BenchmarkImportRows/Rows2000|BenchmarkQueryCollect/Rows4000|BenchmarkToMaps/Rows4000|BenchmarkExprVsMapBatches/Rows4000' -benchtime=3x -benchmem`）：

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BenchmarkImportRows/Rows2000/JSONWithSchema` | 9,068,083 | 777,936 | 7,935 |
| `BenchmarkImportRows/Rows2000/ArrowSchema` | 4,936,000 | 69,480 | 4,368 |
| `BenchmarkQueryCollect/Rows4000/InMemoryArrowBacked` | 17,183,750 | 1,405,000 | 20,013 |
| `BenchmarkQueryCollect/Rows4000/CSVScan` | 20,517,500 | 1,075,976 | 18,028 |
| `BenchmarkToMaps/Rows4000` | 676,434 | 1,689,452 | 31,846 |
| `BenchmarkToStructs/Rows4000` | 382,076 | 781,858 | 12,102 |
| `BenchmarkToStructPointers/Rows4000` | 453,279 | 1,070,625 | 16,103 |
| `BenchmarkExprVsMapBatches/Rows4000/NativeSingle` | 580,681 | 1,381,933 | 8,079 |
| `BenchmarkExprVsMapBatches/Rows4000/MapBatchesSingle` | 543,986 | 1,467,624 | 8,171 |
| `BenchmarkExprVsMapBatches/Rows4000/NativeMulti` | 648,181 | 1,413,965 | 12,079 |
| `BenchmarkExprVsMapBatches/Rows4000/MapBatchesMulti` | 769,208 | 1,500,549 | 12,182 |

当前 smoke 基线说明：

- `ArrowSchema` 导入明显快于 `JSONWithSchema`，而且分配更少
- 4k 行样本下，内存 Arrow-backed 查询比 CSV 扫描更快
- `ToMaps()` 目前仍然有比较明显的分配成本，后续值得继续优化
- `ToStructs()` 现在优先走 Arrow 直转并带有常见标量列的直赋值快路径；在当前基线下，已经同时优于 `ToMaps()` 的延迟、内存和分配次数
- `ToStructPointers()` 会比 `ToStructs()` 多一层对象分配，但在当前基线下仍明显优于 `ToMaps()`
- `Expr.MapBatches(...)` 单输入在当前实现下已经和原生表达式非常接近
- 多输入 `polars.MapBatches(...)` 会有额外的 Arrow batch 回调成本，但仍保持在同一量级

`ToMaps()` / `ToStructs()` / `ToStructPointers()` 对照（Apple M4 Pro, `go test ./polars -run '^$' -bench 'Benchmark(ToMaps|ToStructs|ToStructPointers)' -benchmem`）：

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BenchmarkToMaps/Rows1000` | 174,895 | 426,788 | 7,843 |
| `BenchmarkToStructs/Rows1000` | 100,829 | 201,819 | 3,102 |
| `BenchmarkToStructPointers/Rows1000` | 113,734 | 274,012 | 4,103 |
| `BenchmarkToMaps/Rows4000` | 747,331 | 1,689,469 | 31,847 |
| `BenchmarkToStructs/Rows4000` | 382,076 | 781,858 | 12,102 |
| `BenchmarkToStructPointers/Rows4000` | 453,279 | 1,070,625 | 16,103 |
| `BenchmarkToMaps/Rows16000` | 2,991,847 | 6,734,175 | 127,849 |
| `BenchmarkToStructs/Rows16000` | 1,501,154 | 3,085,616 | 48,102 |
| `BenchmarkToStructPointers/Rows16000` | 2,075,547 | 4,240,708 | 64,103 |

Struct 导出选型建议：

- 需要 typed 结果并且希望吞吐/内存都更好时，优先用 `polars.ToStructs[T](df)`
- 需要保留指针语义或想直接得到 `[]*T` 时，用 `polars.ToStructPointers[T](df)`
- 需要最灵活、无 schema 假设的 Go 原生结果时，用 `ToMaps()`
- 如果输入本身是复杂嵌套 rows，想让导入和导出都更稳定命中列式路径，优先配合 `WithArrowSchema(...)`

## 🚀 快速开始

### 基本用法

```go
package main

import (
    "fmt"
    "log"
    "github.com/isesword/polars-go-bridge/polars"
)

func main() {
    // 方式 1: 直接打印结果（使用 Polars 原生格式，不需要 Free）
    polars.ScanCSV("data.csv").
        Filter(polars.Col("age").Gt(polars.Lit(25))).
        Select(polars.Col("name"), polars.Col("age")).
        Limit(10).
        Print()

    // 方式 2: 使用表达式展开选择多列
    polars.ScanCSV("data.csv").
        Select(polars.Cols("name", "age", "salary")...).
        Print()

    // 方式 3: 选择所有列
    polars.ScanCSV("data.csv").
        Select(polars.All()).
        Limit(10).
        Print()

    // 方式 4: 获取 Go 原生数据结构
    rowsDF, err := polars.ScanCSV("data.csv").
        Filter(polars.Col("age").Gt(polars.Lit(25))).
        Collect()
    if err != nil {
        log.Fatal(err)
    }
    defer rowsDF.Free()
    rows, err := rowsDF.ToMaps()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(rows)
    
    // 方式 5: 获取 EagerFrame 对象（低层接口，推荐仅在需要时使用）
    eagerDF, err := polars.ScanCSV("data.csv").Collect()
    if err != nil {
        log.Fatal(err)
    }
    defer eagerDF.Free()
    
    eagerDF.Print()
}
```

### Struct 用法

```go
package main

import (
    "fmt"
    "log"

    "github.com/isesword/polars-go-bridge/polars"
)

type Employee struct {
    ID   int64  `polars:"id"`
    Name string `polars:"name"`
    Team string `polars:"team"`
}

func main() {
    df, err := polars.NewDataFrame([]Employee{
        {ID: 1, Name: "Alice", Team: "Engineering"},
        {ID: 2, Name: "Bob", Team: "Marketing"},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer df.Close()

    out, err := polars.ToStructs[Employee](df)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println(out[1].Name) // Bob
}
```

**输出示例**：
```
shape: (3, 2)
┌─────────┬─────┐
│ name    ┆ age │
│ ---     ┆ --- │
│ str     ┆ i64 │
╞═════════╪═════╡
│ Bob     ┆ 30  │
│ Charlie ┆ 35  │
│ Diana   ┆ 28  │
└─────────┴─────┘
```

### 支持的操作

#### 数据源

```go
// 从 CSV 文件扫描（懒加载）
lf := polars.ScanCSV("path/to/file.csv")

// 从 Parquet 文件扫描（懒加载）
parquetLF := polars.ScanParquet("path/to/file.parquet")

// 带读取参数的 CSV / Parquet 扫描
hasHeader := true
separator := byte(';')
skipRows := uint64(1)
nullValue := "NA"
tryParseDates := true
quoteChar := byte('\'')
commentPrefix := "#"

csvWithOptions := polars.ScanCSVWithOptions("path/to/file.csv", polars.CSVScanOptions{
    HasHeader:     &hasHeader,
    Separator:     &separator,
    SkipRows:      &skipRows,
    NullValue:     &nullValue,
    TryParseDates: &tryParseDates,
    QuoteChar:     &quoteChar,
    CommentPrefix: &commentPrefix,
    Schema: map[string]pb.DataType{
        "name":      pb.DataType_UTF8,
        "age":       pb.DataType_INT64,
        "joined_at": pb.DataType_DATE,
    },
})

rechunk := true
parquetWithOptions := polars.ScanParquetWithOptions("path/to/file.parquet", polars.ParquetScanOptions{
    Rechunk: &rechunk,
})

// 从内存数据（高层默认入口）
df, _ := polars.NewDataFrame(rows, polars.WithSchema(schema))
defer df.Close()

// 从内存数据（更显式的 rows/columns 入口）
dfFromMaps, _ := polars.NewDataFrameFromMaps(rows, polars.WithSchema(schema))
defer dfFromMaps.Close()

// 从内存数据（Arrow 入口）
record, _ := polars.NewArrowRecordBatchFromRowsWithSchema(rows, schema)
dfFromArrow, _ := polars.NewDataFrameFromArrow(record)
defer dfFromArrow.Close()

_ = parquetLF
_ = csvWithOptions
_ = parquetWithOptions
_ = dfFromMaps
_ = dfFromArrow
```

#### 转换操作

```go
// 过滤、选择、添加列、限制行数
query := lf.
    Filter(polars.Col("age").Gt(polars.Lit(18))).
    Select(polars.Col("name"), polars.Col("age"), polars.Col("salary"), polars.Col("department")).
    WithColumns(
        polars.Col("age").Add(polars.Lit(1)).Alias("next_year_age"),
    ).
    Limit(100)

// GroupBy + Aggregation
summary := lf.GroupBy("department").Agg(
    polars.Col("salary").Sum().Alias("total_salary"),
    polars.Col("name").Count().Alias("employee_count"),
    polars.Col("salary").Median().Alias("median_salary"),
    polars.Col("name").NUnique().Alias("unique_names"),
)

// Sort / Unique
sorted := query.Sort(polars.SortOptions{
    By:         []polars.Expr{polars.Col("age")},
    Descending: []bool{true},
})

uniqueDepartments := lf.Select(polars.Col("department")).Unique(polars.UniqueOptions{
    Subset: []string{"department"},
    Keep:   "first",
})

// Forward fill
ffilled := lf.Select(
    polars.Col("id"),
    polars.Col("value").FFill().Alias("value_ffill"),
)

// Basic dataframe utilities
trimmed := df.Drop("bonus").Rename(map[string]string{
    "name": "person_name",
})
head := df.Head(5)
tail := df.Tail(5)
filled := df.FillNull(int64(0))
filledNan := df.FillNan(float64(0))
backfilled := df.BFill()
nonNull := df.DropNulls("age", "salary")
nonNan := df.DropNans("score")
reversed := df.Reverse()
sampled := df.SampleN(10, polars.SampleOptions{})
exploded := df.Explode("tags")
unpivoted := df.Unpivot(polars.UnpivotOptions{
    On:           []string{"math", "english"},
    Index:        []string{"name"},
    VariableName: "subject",
    ValueName:    "score",
})

// When / Then / Otherwise
labeled := lf.Select(
    polars.Col("name"),
    polars.When(polars.Col("age").Gt(polars.Lit(30))).
        Then(polars.Lit("senior")).
        Otherwise(polars.Lit("junior")).
        Alias("level"),
)

// UDG: DataFrame.MapRows
mappedDF, _ := df.MapRows(func(row map[string]any) (map[string]any, error) {
    level := "junior"
    if age, ok := row["age"].(int64); ok && age > 30 {
        level = "senior"
    }
    return map[string]any{
        "name":  row["name"],
        "level": level,
    }, nil
}, polars.MapRowsOptions{})

// UDG: DataFrame.MapBatches
batchMappedDF, _ := df.MapBatches(func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
    // 第一版示例先透传 batch；如果需要变换，可以返回新的 Arrow RecordBatch
    return batch, nil
}, polars.MapBatchesOptions{})

// UDG: Expr.MapBatches
exprMapped, _ := df.Select(
    polars.Col("age").MapBatches(func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
        return batch, nil
    }, polars.ExprMapBatchesOptions{
        ReturnType: polars.Int64,
    }).Alias("age_udg"),
).Collect()

// UDG: polars.MapBatches with multiple input expressions
exprMappedMany, _ := df.Select(
    polars.MapBatches([]polars.Expr{
        polars.Col("age"),
        polars.Col("bonus"),
    }, func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
        return batch, nil
    }, polars.ExprMapBatchesOptions{
        ReturnType: polars.Int64,
    }).Alias("age_bonus"),
).Collect()

// Pivot (eager-only, aligned with Polars eager pivot semantics)
pivoted, _ := df.Pivot(polars.PivotOptions{
    On:            []string{"subject"},
    Index:         []string{"name"},
    Values:        []string{"score"},
    Aggregate:     "first",
    MaintainOrder: true,
})
defer pivoted.Close()

// 避免示例里未使用变量
_ = summary
_ = sorted
_ = uniqueDepartments
_ = ffilled
_ = trimmed
_ = head
_ = tail
_ = filled
_ = filledNan
_ = backfilled
_ = nonNull
_ = nonNan
_ = reversed
_ = sampled
_ = exploded
_ = unpivoted
_ = labeled
_ = mappedDF
_ = batchMappedDF
_ = exprMapped
_ = exprMappedMany

// Window functions
windowed := lf.Select(
    polars.Col("name"),
    polars.Col("department"),
    polars.Col("salary"),
    polars.Col("salary").Sum().Over(polars.Col("department")).Alias("department_total_salary"),
    polars.Col("salary").Rank(polars.RankOptions{
        Method:     polars.RankDense,
        Descending: true,
    }).Over(polars.Col("department")).Alias("department_salary_rank"),
    polars.Col("salary").CumSum(false).Alias("running_salary"),
    polars.Col("salary").CumCount(false).Alias("running_count"),
    polars.Col("salary").CumMin(false).Alias("running_min_salary"),
    polars.Col("salary").CumMax(false).Alias("running_max_salary"),
    polars.Col("age").CumProd(false).Alias("running_age_product"),
)

orderedWindow := lf.Select(
    polars.Col("name"),
    polars.Col("department"),
    polars.Col("age"),
    polars.Col("salary").CumSum(false).OverWithOptions(polars.OverOptions{
        PartitionBy: []polars.Expr{polars.Col("department")},
        OrderBy:     []polars.Expr{polars.Col("age")},
    }).Alias("department_running_salary"),
)

_ = windowed
_ = orderedWindow

numericHelpers := lf.Select(
    polars.Col("value").IsNotNull().Alias("value_not_null"),
    polars.Col("score").IsNan().Alias("score_is_nan"),
    polars.Col("score").IsFinite().Alias("score_is_finite"),
    polars.Col("value").Abs().Alias("abs_value"),
    polars.Col("value").Round(0).Alias("round_value"),
    polars.Col("value").Clip(-2.0, 5.0).Alias("clip_value"),
    polars.Col("value").Sqrt().Alias("sqrt_value"),
    polars.Col("value").Log(10.0).Alias("log10_value"),
    polars.Col("value").NullCount().Alias("value_null_count"),
    polars.Col("department").ValueCounts(polars.ValueCountsOptions{
        Sort: true,
        Name: "n",
    }).Alias("department_counts"),
)

_ = numericHelpers

analytic := lf.Select(
    polars.Col("name"),
    polars.Col("age"),
    polars.Col("age").Shift(1).Alias("prev_age"),
    polars.Col("age").Diff(1, polars.DiffNullIgnore).Alias("age_diff"),
)

_ = analytic

temporal := lf.Select(
    polars.Col("date_str").StrToDate("%Y-%m-%d").Alias("date_value"),
    polars.Col("date_str").StrToDate("%Y-%m-%d").DtYear().Alias("year"),
    polars.Col("date_str").StrToDate("%Y-%m-%d").DtMonth().Alias("month"),
    polars.Col("date_str").StrToDate("%Y-%m-%d").DtDay().Alias("day"),
    polars.Col("date_str").StrToDate("%Y-%m-%d").DtWeekday().Alias("weekday"),
    polars.Col("date_str").StrToDate("%Y-%m-%d").DtMonthStart().Alias("month_start"),
    polars.Col("date_str").StrToDate("%Y-%m-%d").DtMonthEnd().Alias("month_end"),
    polars.Col("ts_str").StrToDatetime("%Y-%m-%d %H:%M:%S").Alias("ts_value"),
    polars.Col("ts_str").StrToDatetime("%Y-%m-%d %H:%M:%S").DtHour().Alias("hour"),
    polars.Col("ts_str").StrToDatetime("%Y-%m-%d %H:%M:%S").DtMinute().Alias("minute"),
    polars.Col("ts_str").StrToDatetime("%Y-%m-%d %H:%M:%S").DtSecond().Alias("second"),
    polars.Col("time_str").StrToTime("%H:%M:%S").Alias("time_value"),
)

_ = temporal

// 方式 1: 打印结果（使用 Polars 原生的漂亮表格格式）
query.Print()

// 方式 2: 收集为 EagerFrame 并转换成 Go 数据结构（[]map[string]interface{}）
resultDF, _ := query.Collect()
defer resultDF.Free()
rows, _ := resultDF.ToMaps()
fmt.Println(rows)

// 方式 3: 收集为 EagerFrame 并进一步处理
eagerDF, _ := query.Collect()
defer eagerDF.Free()

// EagerFrame 仍支持链式操作
result, _ := eagerDF.Filter(polars.Col("age").Gt(polars.Lit(30))).
    Select(polars.Col("name")).
    Collect()
defer result.Free()
```

#### Join 示例

```go
left := polars.ScanCSV("users.csv")
right := polars.ScanCSV("profiles.csv")

// 同名 key join
joined := left.Join(right, polars.JoinOptions{
    On:  []string{"id"},
    How: polars.JoinInner,
    Suffix: "_right", // 可选，默认也是 _right
})

// 只保留左表中“在右表存在匹配 key”的行
semiJoined := left.Join(right, polars.JoinOptions{
    On:  []string{"id"},
    How: polars.JoinSemi,
})

// 只保留左表中“在右表不存在匹配 key”的行
antiJoined := left.Join(right, polars.JoinOptions{
    On:  []string{"id"},
    How: polars.JoinAnti,
})

// 笛卡尔积 join，不需要 join key
crossJoined := left.Join(right, polars.JoinOptions{
    How: polars.JoinCross,
})

// 异名 key join
joined = left.Join(right, polars.JoinOptions{
    LeftOn:  []polars.Expr{polars.Col("user_id")},
    RightOn: []polars.Expr{polars.Col("id")},
    How:     polars.JoinLeft,
})

// 两个内存 DataFrame 也支持直接 join
leftDF, _ := polars.NewDataFrame([]map[string]any{
    {"id": 1, "name": "Alice"},
})
defer leftDF.Close()

rightDF, _ := polars.NewDataFrame([]map[string]any{
    {"id": 1, "age": 25},
})
defer rightDF.Close()

rows, _ := leftDF.Join(rightDF, polars.JoinOptions{
    On:  []string{"id"},
    How: polars.JoinInner,
}).Collect()
defer rows.Free()

_ = joined
_ = semiJoined
_ = antiJoined
_ = crossJoined
```

`Semi` 和 `Anti` 都只返回左表的列：

- `Semi`：保留左表中“能在右表找到匹配 key”的行
- `Anti`：保留左表中“在右表找不到匹配 key”的行

例如左表有 `id = [1, 2, 3]`，右表有 `id = [1, 3]`：

- `JoinSemi` 返回左表中的 `1, 3`
- `JoinAnti` 返回左表中的 `2`

#### Concat 示例

```go
first := polars.ScanCSV("part1.csv")
second := polars.ScanCSV("part2.csv")

combined := polars.Concat([]*polars.LazyFrame{first, second}, polars.ConcatOptions{
    Parallel:      true,
    MaintainOrder: true,
})

// 也可以直接从一个 LazyFrame 出发继续拼接
combined = first.Concat(second)

_ = combined
```

#### 表达式

```go
// 列引用
polars.Col("column_name")

// 多列引用（表达式展开）
polars.Cols("col1", "col2", "col3")  // 返回 []Expr

// 选择所有列
polars.All()  // 相当于 pl.all()

// 字面量
polars.Lit(42)          // 整数
polars.Lit(3.14)        // 浮点数
polars.Lit("hello")     // 字符串
polars.Lit(true)        // 布尔值

// 算术操作
polars.Col("x").Add(polars.Lit(1))      // 加法 x + 1
polars.Col("x").Sub(polars.Lit(2))      // 减法 x - 2
polars.Col("x").Mul(polars.Lit(3))      // 乘法 x * 3
polars.Col("x").Div(polars.Lit(4))      // 除法 x / 4
polars.Col("x").Mod(polars.Lit(3))      // 取模 x % 3
polars.Col("x").Pow(polars.Lit(2))      // 幂运算 x ** 2

// 比较操作
polars.Col("age").Gt(polars.Lit(18))    // 大于 >
polars.Col("age").Ge(polars.Lit(18))    // 大于等于 >=
polars.Col("age").Lt(polars.Lit(65))    // 小于 <
polars.Col("age").Le(polars.Lit(65))    // 小于等于 <=
polars.Col("age").Eq(polars.Lit(30))    // 等于 ==
polars.Col("age").Ne(polars.Lit(30))    // 不等于 !=

// 逻辑操作
polars.Col("a").And(polars.Col("b"))    // 逻辑与
polars.Col("a").Or(polars.Col("b"))     // 逻辑或
polars.Col("a").Not()                    // 逻辑取反
polars.Col("a").Xor(polars.Col("b"))    // 异或

// 类型转换
polars.Col("age").Cast(polars.Int32, true)       // 严格模式转换
polars.Col("age").Cast(polars.Float64, false)    // 非严格模式（失败转 null）
polars.Col("age").StrictCast(polars.Int16)       // 严格模式快捷方法

// 支持的数据类型
polars.Int64, polars.Int32, polars.Int16, polars.Int8
polars.UInt64, polars.UInt32, polars.UInt16, polars.UInt8
polars.Float64, polars.Float32
polars.Boolean
polars.String
polars.Date, polars.Datetime, polars.Time

// 别名
polars.Col("salary").Mul(polars.Lit(1.1)).Alias("new_salary")

// 空值检查
polars.Col("phone").IsNull()
```

## 📚 完整示例

查看 [examples/scan_csv_example.go](examples/scan_csv_example.go) 获取完整的使用示例。

**示例 1: 基本扫描**
```go
polars.ScanCSV("testdata/sample.csv").Print()
// 输出: 7 行 4 列的完整表格
```

**示例 2: 过滤操作**
```go
polars.ScanCSV("testdata/sample.csv").
    Filter(polars.Col("age").Gt(polars.Lit(28))).
    Print()
// 输出: 4 行（年龄 > 28）
```

**示例 3: 选择列**
```go
// 单列选择
polars.ScanCSV("testdata/sample.csv").
    Select(polars.Col("name"), polars.Col("age")).
    Print()
// 输出: 7 行 2 列（只有 name 和 age）

// 多列选择（表达式展开）
polars.ScanCSV("testdata/sample.csv").
    Select(polars.Cols("name", "age", "salary")...).
    Print()

// 选择所有列
polars.ScanCSV("testdata/sample.csv").
    Select(polars.All()).
    Print()
```

**示例 4: 组合操作**
```go
polars.ScanCSV("testdata/sample.csv").
    Filter(polars.Col("age").Gt(polars.Lit(25))).
    Select(polars.Col("name"), polars.Col("salary")).
    Limit(3).
    Print()
// 输出: 3 行 2 列
```

**示例 5: 复杂过滤**
```go
polars.ScanCSV("testdata/sample.csv").
    Filter(
        polars.Col("department").Eq(polars.Lit("Engineering")).
            And(polars.Col("salary").Gt(polars.Lit(60000))),
    ).
    Print()
// 输出: 2 行（Engineering 部门且工资 > 60000）
```

**示例 6: 类型转换**
```go
polars.ScanCSV("testdata/sample.csv").
    Select(
        polars.Col("age"),
        polars.Col("age").Cast(polars.Int32, true).Alias("age_int32"),
        polars.Col("age").Cast(polars.Float32, true).Alias("age_float"),
        polars.Col("age").Gt(polars.Lit(30)).Cast(polars.Int8, true).Alias("is_old"),
    ).
    Limit(3).
    Print()
// 输出: 3 行，展示了数值类型转换和布尔转整数
```

### 运行示例

```bash
cd /path/to/polars-go-bridge
POLARS_BRIDGE_LIB=./libpolars_bridge.dylib go run examples/scan_csv_example.go
```

**输出示例**：
```
=== Polars Go Bridge - CSV Scan Example ===

📖 示例 1: 基本 CSV 扫描
shape: (7, 4)
┌─────────┬─────┬────────┬─────────────┐
│ name    ┆ age ┆ salary ┆ department  │
│ ---     ┆ --- ┆ ---    ┆ ---         │
│ str     ┆ i64 ┆ i64    ┆ str         │
╞═════════╪═════╪════════╪═════════════╡
│ Alice   ┆ 25  ┆ 50000  ┆ Engineering │
│ Bob     ┆ 30  ┆ 60000  ┆ Marketing   │
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering │
...

✅ 所有示例执行成功！
```

## 🧪 测试

```bash
# 设置动态库路径
export POLARS_BRIDGE_LIB=/path/to/libpolars_bridge.dylib  # macOS
export POLARS_BRIDGE_LIB=/path/to/libpolars_bridge.so     # Linux
export POLARS_BRIDGE_LIB=/path/to/polars_bridge.dll       # Windows

# 运行测试
go test -v ./polars

# 运行特定测试
go test -v ./polars -run TestScanCSV
```

如果你已经在仓库根目录执行过构建，并且根目录下存在动态库文件，那么通常也可以直接运行：

```bash
go test ./...
```

✅ **已验证功能**：
- CSV 扫描（懒加载）
- Filter + Select + WithColumns + Limit 链式操作
- EagerFrame 链式操作支持
- Arrow C Data Interface 数据传输（支持所有数值类型、布尔类型、字符串类型）
- Arrow C Data Interface 导入/导出
- `[]map[string]any` -> DataFrame（自动优先 Arrow，必要时回退）
- `[]map[string]any` -> Arrow RecordBatch -> DataFrame（显式 Arrow 导入）
- `Collect().ToMaps()` 推荐导出方式
- Polars 原生格式打印
- 表达式展开：`Cols()` 多列选择、`All()` 选择所有列
- 类型转换：`Cast()` 和 `StrictCast()` 支持所有数值类型转换

## ⚠️ 资源管理最佳实践

由于本库通过 FFI 桥接 Rust，需要注意正确的资源管理以避免内存泄露。

### 规则 1: Print() 不需要 Free

```go
✅ 正确：Print() 自动管理资源
polars.ScanCSV("data.csv").
    Filter(polars.Col("age").Gt(polars.Lit(25))).
    Print()
// 无需调用 Free
```

### 规则 2: Collect() 后再 ToMaps() 时要释放临时 EagerFrame

```go
✅ 推荐：Collect() 后 defer Free，再调用 ToMaps()
df, err := polars.ScanCSV("data.csv").
    Filter(polars.Col("age").Gt(polars.Lit(25))).
    Collect()
if err != nil {
    return err
}
defer df.Free()

rows, err := df.ToMaps()
```

### 规则 3: Collect() 必须 Free

```go
✅ 正确：使用 defer 确保释放
df, err := polars.ScanCSV("data.csv").Collect()
if err != nil {
    log.Fatal(err)
}
defer df.Free()  // 必须调用！

df.Print()

❌ 错误：忘记调用 Free 会导致 Rust 端内存泄露
df, _ := polars.ScanCSV("data.csv").Collect()
df.Print()  // 泄露！
```

### 规则 4: 高层构造器推荐 Close

```go
✅ 推荐：高层入口使用 Close
df, err := polars.NewDataFrame(rows, polars.WithSchema(schema))
if err != nil {
    log.Fatal(err)
}
defer df.Close()

df.Print()
```

### 规则 4b: NewDataFrameFromArrow() 接管输入 Record，但 DataFrame 仍建议 Close

```go
✅ 正确
record, err := polars.NewArrowRecordBatchFromRowsWithSchema(rows, schema)
if err != nil {
    log.Fatal(err)
}

df, err := polars.NewDataFrameFromArrow(record)
if err != nil {
    log.Fatal(err)
}
defer df.Close()
```

### 规则 4c: 低层兼容入口仍然需要显式释放

```go
✅ 兼容模式：低层入口仍然可用
df, err := polars.NewEagerFrameFromRowsWithSchema(brg, rows, schema)
if err != nil {
    log.Fatal(err)
}
defer df.Free()
```

### 规则 5: EagerFrame 链式操作返回 LazyFrame

```go
✅ 正确：链式操作后使用 Collect() + ToMaps() 或 Print
df, _ := polars.ScanCSV("data.csv").Collect()
defer df.Free()

// EagerFrame 的 Filter/Select 返回 LazyFrame
result, _ := df.Filter(polars.Col("age").Gt(polars.Lit(30))).
    Collect()
defer result.Free()

❌ 注意：再次 Collect() 会创建新的 EagerFrame
df2, _ := df.Filter(polars.Col("age").Gt(polars.Lit(30))).
    Collect()
defer df2.Free()  // 需要再次 Free！
```

### 规则 6: Arrow C Data Interface 资源

```go
✅ 正确：使用 defer 释放 Arrow 资源
outSchema, outArray, err := brg.ExecuteArrow(handle, nil, nil)
if err != nil {
    log.Fatal(err)
}
defer bridge.ReleaseArrowSchema(outSchema)
defer bridge.ReleaseArrowArray(outArray)

// 使用 Arrow 数据...
```

### 内存泄露检测

在开发时可以使用以下代码检测是否有内存泄露：

```go
func TestMemoryLeak(t *testing.T) {
    brg, _ := bridge.LoadBridge("")
    
    var m1, m2 runtime.MemStats
    runtime.GC()
    runtime.ReadMemStats(&m1)
    
    // 创建和释放大量 EagerFrame
    for i := 0; i < 10000; i++ {
        df, _ := polars.ScanCSV("testdata/sample.csv").Collect()
        df.Free()
    }
    
    runtime.GC()
    runtime.ReadMemStats(&m2)
    
    // 检查内存是否有明显增长
    if m2.Alloc > m1.Alloc*2 {
        t.Errorf("Possible memory leak: %v -> %v", m1.Alloc, m2.Alloc)
    }
}
```

### 总结

| 方法 | 是否需要 Free | 说明 |
|------|--------------|------|
| `Print()` | ❌ 否 | 内部管理资源 |
| `Collect()` + `ToMaps()` | ✅ 是 | 对 `Collect()` 返回的 `EagerFrame` 调用 `Free()` |
| `Collect()` | ✅ 是 | 返回 `EagerFrame`，必须手动 Free |
| `NewDataFrame()` | ❌ 否 | 返回托管 `DataFrame`，推荐 `Close()` 及时释放 |
| `NewDataFrameFromArrow()` | ❌ 否 | 接管输入 Arrow RecordBatch，返回托管 `DataFrame` |
| `NewEagerFrameFromMap()` | ✅ 是 | 低层兼容/高级入口，返回 `EagerFrame` |
| `ExecuteArrow()` | ✅ 是 | 输出需要 ReleaseArrow* |

## 📂 项目结构

```
polars-go-bridge/
├── bridge/                    # Go FFI 桥接层
│   ├── arrow_cdata.go        # Arrow C Data Interface 定义
│   ├── loader_unix.go        # Unix/macOS 动态库加载
│   ├── loader_win.go         # Windows 动态库加载
│   └── types.go              # 错误码等类型定义
├── polars/                   # Go Fluent API
│   ├── dataframe.go          # LazyFrame 和链式操作
│   ├── dataframe_handle.go   # EagerFrame 和链式操作
│   ├── expr.go               # 表达式构建器
│   ├── utils.go              # Arrow RecordBatch 解析工具
│   └── scan_test.go          # 测试用例
├── proto/                    # Protobuf 协议定义
│   ├── polars_bridge.proto  # Plan 定义
│   └── polars_bridge.pb.go  # 生成的 Go 代码
├── rust/                     # Rust 桥接层
│   ├── src/
│   │   ├── lib.rs           # FFI 导出函数
│   │   ├── executor.rs      # Plan 执行器
│   │   ├── error.rs         # 错误处理
│   │   └── arrow_bridge.rs  # Arrow 导入/导出桥接
│   ├── Cargo.toml
│   └── build.rs
├── testdata/                 # 测试数据
│   ├── sample.csv
│   ├── small.csv
│   └── large_sample.csv
├── examples/                 # 示例代码
│   └── scan_csv_example.go
└── scripts/                  # 构建脚本
    ├── build.sh
    └── run.sh
```

## 🔧 开发指南

### 添加新的操作节点

1. **在 `proto/polars_bridge.proto` 中定义新节点**：
   ```protobuf
   message GroupBy {
     Node input = 1;
     repeated string by = 2;
   }
   
   message Node {
     oneof kind {
       // ...
       GroupBy group_by = 17;
     }
   }
   ```

2. **重新生成 Protobuf**：
   ```bash
   protoc --go_out=. --go_opt=paths=source_relative proto/polars_bridge.proto
   ```

3. **在 Rust `executor.rs` 中实现**：
   ```rust
   Kind::GroupBy(gb) => {
       let input_node = gb.input.as_ref()?;
       let lf = build_lazy_frame(input_node)?;
       Ok(lf.group_by(&gb.by))
   }
   ```

4. **在 Go `polars/dataframe.go` 中添加 API**：
   ```go
   func (lf *LazyFrame) GroupBy(by ...string) *LazyFrame {
       newNode := &pb.Node{
           Id: lf.nextNodeID(),
           Kind: &pb.Node_GroupBy{
               GroupBy: &pb.GroupBy{
                   Input: lf.root,
                   By:    by,
               },
           },
       }
       return &LazyFrame{root: newNode, nodeID: lf.nodeID}
   }
   ```

## ❓ 常见问题 (Q&A)

### Q1: LazyFrame、DataFrame 和 EagerFrame 有什么区别？

可以先这样记：
- **LazyFrame** = 查询计划，还没执行
- **EagerFrame** = 已执行完成的底层结果，需要手动 `Free()`
- **DataFrame** = 对 `EagerFrame` 的高层托管包装，推荐 `Close()`

#### 三者关系图

```text
Go 输入数据 / 文件
    ↓
LazyFrame
    只描述“要做什么”，还没执行
    入口：ScanCSV() / ScanParquet() / df.Filter() / df.Select()

    ↓ Collect()
EagerFrame
    已执行完成，持有 Rust 侧真实 DataFrame 句柄
    需要手动 Free()

    ↓ 包装
DataFrame
    高层托管对象，内部封装 EagerFrame
    推荐 Close()，更适合普通业务代码
```

#### 最常见的入口

```go
lf := polars.ScanCSV("data.csv")     // *LazyFrame
eagerDF, _ := lf.Collect()           // *EagerFrame
df, _ := polars.NewDataFrame(rows)   // *DataFrame
```

#### 怎么选

- 普通业务代码、对接数据库结果、希望 API 更接近 Python Polars 时：优先用 `DataFrame`
- 从文件开始链式查询、希望延迟执行和减少中间结果时：优先用 `LazyFrame`
- 需要精细控制释放时机，或者明确在处理底层已物化结果时：使用 `EagerFrame`

#### LazyFrame（懒加载模式）

```go
// ❌ 这些操作都不会立即执行，只是构建执行计划
lf := polars.ScanCSV("data.csv")           // 计划①: 要读 CSV
lf2 := lf.Filter(polars.Col("age").Gt(polars.Lit(25)))   // 计划②: 要过滤
lf3 := lf2.Select(polars.Col("name"))                    // 计划③: 要选择列

// ✅ 直到这里才真正执行所有操作！
df, _ := lf3.Collect()  // 现在才读文件、过滤、选择
```

**LazyFrame 就像一张"待办清单"**：只记录要做什么，但实际上什么都没做，直到调用 `Collect()` 或 `Print()` 才执行。

#### DataFrame / EagerFrame（立即执行模式）

```go
// 高层托管对象：更接近 pl.DataFrame(...)
df, _ := polars.NewDataFrame(rows, polars.WithSchema(schema))
defer df.Close()

// 低层已物化对象：通常只在需要精细控制时使用
eagerDF, _ := polars.ScanCSV("data.csv").Collect()
defer eagerDF.Free()

// 两者都可以直接访问数据
rows1, _ := df.ToMaps()
rows, _ := eagerDF.ToMaps()  // 获取所有行
eagerDF.Print()              // 打印数据
_ = rows1
```

### Q2: 为什么 `df.Filter()` 返回 LazyFrame 而不是 `*EagerFrame`？

这是为了**性能优化**和**内存效率**：

#### 场景对比

**方案 A：返回 LazyFrame（当前实现）✅**
```go
df, _ := polars.ScanCSV("data.csv").Collect()  // 100MB 内存
defer df.Free()

// 构建执行计划（几乎不占内存）
lazyResult := df.Filter(polars.Col("age").Gt(polars.Lit(28))).
    Select(polars.Col("name"), polars.Col("age")).
    Limit(10)

// 一次性执行优化后的计划
result, _ := lazyResult.Collect()  // 只分配需要的内存
defer result.Free()

// 总内存：100MB（原始 df）+ 少量结果内存
```

**方案 B：如果返回 EagerFrame（假设）❌**
```go
df, _ := polars.ScanCSV("data.csv").Collect()  // 100MB 内存
defer df.Free()

// 每一步都立即执行，创建中间结果
df1 := df.Filter(polars.Col("age").Gt(polars.Lit(28)))  // 立即执行：80MB
df2 := df1.Select(polars.Col("name"), polars.Col("age"))  // 立即执行：60MB
df3 := df2.Limit(10)  // 立即执行：1KB

// 总内存：100MB + 80MB + 60MB + 1KB = 240MB+
// 而且需要 3 次数据复制！
```

#### 优势总结

1. **查询优化**：LazyFrame 可以优化执行计划，避免不必要的计算
2. **内存效率**：避免创建中间 EagerFrame，只在最后分配一次内存
3. **统一接口**：与 LazyFrame 的链式调用保持一致

### Q3: 什么时候使用 LazyFrame，什么时候使用 DataFrame / EagerFrame？

#### 使用 LazyFrame（推荐）

✅ **适用场景**：
- 直接从文件读取并处理
- 一次性查询，不需要重复使用数据
- 追求最佳性能和内存效率

```go
// ✅ 全程懒加载，最优性能
df, _ := polars.ScanCSV("data.csv").
    Filter(polars.Col("age").Gt(polars.Lit(25))).
    Select(polars.Col("name")).
    Collect()  // 一次性执行所有操作
defer df.Free()
rows, _ := df.ToMaps()
```

#### 使用 DataFrame / EagerFrame

✅ **适用场景**：
- 需要多次使用同一份数据
- 需要查看中间结果
- 数据需要在内存中保留

```go
// 先物化到 EagerFrame
df, _ := polars.ScanCSV("data.csv").Collect()
defer df.Free()

// 多次使用同一份数据
tmp1, _ := df.Filter(polars.Col("age").Gt(polars.Lit(25))).Collect()
defer tmp1.Free()
result1, _ := tmp1.ToMaps()

tmp2, _ := df.Filter(polars.Col("age").Lt(polars.Lit(30))).Collect()
defer tmp2.Free()
result2, _ := tmp2.ToMaps()

tmp3, _ := df.Select(polars.Col("name")).Collect()
defer tmp3.Free()
result3, _ := tmp3.ToMaps()
```

### Q4: Collect() 和 ToMaps() 有什么区别？

#### `Collect()` - 返回 EagerFrame

```go
df, err := lf.Collect()
if err != nil {
    return err
}
defer df.Free()  // ⚠️ 需要手动释放内存

// 可以继续操作
df.Print()
next, _ := df.Filter(...).Collect()
defer next.Free()
result, _ := next.ToMaps()
```

**使用场景**：需要对结果进行进一步操作，或者需要低层资源控制。

#### `Collect().ToMaps()` - 返回 Go 数据结构

```go
df, err := lf.Collect()
if err != nil {
    return err
}
defer df.Free()

rows, err := df.ToMaps()
if err != nil {
    return err
}

// rows 是 []map[string]interface{}
for _, row := range rows {
    fmt.Println(row["name"], row["age"])
}
```

**使用场景**：只需要获取最终数据，不需要进一步操作。

**实现关系**：推荐写法是 `Collect()` → `df.ToMaps()`。

### Q5: 如何理解执行流程？

#### 完整执行流程示例

```go
// 步骤 1: 创建懒加载计划（什么都没执行）
lf := polars.ScanCSV("data.csv").           // 计划①: 读 CSV
    Filter(polars.Col("age").Gt(polars.Lit(25))).        // 计划②: 过滤
    Select(polars.Col("name"), polars.Col("age"))        // 计划③: 选择列

// 步骤 2: 执行计划，获得 EagerFrame（现在才真正执行）
df, _ := lf.Collect()  // 🚀 执行！数据进入内存
defer df.Free()

// 此时：df 是一个内存中的已物化结果（比如 1000 行）

// 步骤 3: 在 EagerFrame 上继续操作
// ⚠️ df.Filter() 又返回了 LazyFrame！
lf2 := df.Filter(polars.Col("age").Gt(polars.Lit(30)))   // 又变回懒加载计划

// 步骤 4: 再次执行
result, _ := lf2.Collect()  // 🚀 再次执行！
defer result.Free()
```

#### 可视化流程

```
LazyFrame（懒加载）
    ↓ 构建执行计划
    ↓ 不执行任何操作
    ↓ 只是记录要做什么
    ↓
Collect() ← 触发执行
    ↓
    ↓ 读取数据
    ↓ 应用所有操作
    ↓ 优化执行计划
    ↓
EagerFrame（内存中的结果）
    ↓
df.Filter() ← 又返回 LazyFrame
    ↓
LazyFrame（新的执行计划）
    ↓
Collect() ← 再次执行
    ↓
EagerFrame（新的结果）
```

## 🚧 TODO

### 已完成 ✅
- [x] 实现 Arrow RecordBatch 解析（Go 侧，基于 Apache Arrow Go）
- [x] 支持多种 Arrow 类型（Int32/64, Float32/64, Boolean, String, LargeString, StringView, Date, Datetime, Time, List, LargeList, Struct）
- [x] EagerFrame 链式操作支持
- [x] `Collect().ToMaps()` 导出方式
- [x] CSV 文件懒加载扫描
- [x] Parquet 文件懒加载扫描
- [x] CSV / Parquet 扫描参数：HasHeader / Separator / SkipRows / InferSchemaLength / NullValue / TryParseDates / QuoteChar / CommentPrefix / Schema / Encoding / IgnoreErrors / Rechunk
- [x] Filter / Select / WithColumns / Limit 操作
- [x] Drop / Rename / Slice / Head / Tail 操作
- [x] FillNull / FillNan / FFill / BFill / DropNulls / DropNans / Reverse / Sample / Explode / Unpivot(Melt) 操作
- [x] 完整的表达式系统：
  - 算术操作：Add, Sub, Mul, Div, **Mod, Pow**
  - 比较操作：Eq, Ne, Gt, Ge, Lt, Le
  - 逻辑操作：And, Or, **Not, Xor**
  - 类型转换：Cast, StrictCast
  - 其他：Alias, IsNull, IsNotNull, IsNan, IsFinite, Cols, All, Exclude, FillNull, FillNan, FFill, BFill, Reverse
  - 数值辅助：Abs, Round, Clip, Sqrt, Log, NullCount, ValueCounts
- [x] 字符串表达式：
  - StrLenBytes, StrLenChars
  - StrContains, StrStartsWith, StrEndsWith
  - StrExtract, StrReplace, StrReplaceAll
  - StrToLowercase, StrToUppercase
  - StrStripChars, StrSlice, StrSplit
  - StrPadStart, StrPadEnd
- [x] Arrow C Data Interface 导入/导出
- [x] Arrow 优先的内存数据导入（显式 Arrow / `WithArrowSchema(...)` / 自动回退路径）
- [x] Arrow 嵌套类型导出到 Go 值（List / LargeList / Struct -> `[]any` / `map[string]any`）
- [x] Arrow 导入导出边界测试与 schema mismatch 错误信息补强
- [x] 托管 `DataFrame` 与低层 `EagerFrame` 双层 API
- [x] GroupBy / Aggregation 操作
- [x] 更完整的聚合函数：Len / NUnique / Median / Std / Var
- [x] Quantile 聚合
- [x] 窗口函数：Over / Rank / CumSum / CumCount / CumMin / CumMax / CumProd
- [x] 分析表达式：Shift / Diff
- [x] 时间表达式：DtYear / DtMonth / DtDay / DtWeekday / DtHour / DtMinute / DtSecond / DtMonthStart / DtMonthEnd / StrToDate / StrToDatetime / StrToTime
- [x] Join 操作
- [x] Semi / Anti Join 操作
- [x] Cross Join 操作
- [x] Sort / Unique 操作
- [x] Pivot 操作（eager）
- [x] Forward fill (`FFill`)
- [x] Concat / Union 风格的数据联合操作
- [x] 完善的测试用例

### 计划中 📋
- [ ] 支持更多表达式（日期函数、更多窗口函数等）
- [x] 基础 benchmark 套件（JSON vs Arrow 导入 / 内存输入 vs CSV 扫描 / ToMaps）
- [ ] 继续完善错误处理和错误信息
- [ ] 支持流式处理大文件

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

MIT License

## 🙏 致谢

- [Polars](https://github.com/pola-rs/polars) - 高性能 DataFrame 库
- [Apache Arrow](https://arrow.apache.org/) - 列式内存格式
- [prost](https://github.com/tokio-rs/prost) - Rust Protobuf 实现
- [purgo](https://github.com/guonaihong/purgo) - Go 调用 Rust 的工具链支持
