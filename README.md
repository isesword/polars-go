# Polars Go Bridge

一个高性能的 Go 语言 Polars 数据处理库，通过 FFI 桥接 Rust Polars，提供类似 Polars 的 Fluent API。

## ✨ 特性

- 🚀 **零拷贝数据传输**：使用 Arrow IPC 格式，避免 JSON 序列化开销
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
    ↓ Arrow IPC
Go (获取结果)
```

**核心设计**：
- Go 侧构建**查询计划（Plan）**，使用 Protobuf 序列化
- Rust 侧将 Plan 翻译成 Polars 的 LazyFrame 调用
- 数据传输使用 **Arrow IPC** 二进制格式（不使用 JSON）

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

#### 3. 使用示例

```go
package main

import (
    "log"
    "github.com/isesword/polars-go-bridge/bridge"
    "github.com/isesword/polars-go-bridge/polars"
)

func main() {
    // 自动加载动态库（从当前目录、系统路径或 POLARS_BRIDGE_LIB 环境变量）
    brg, err := bridge.LoadBridge("")
    if err != nil {
        log.Fatalf("Failed to load bridge: %v", err)
    }

    // 使用 Map 创建 DataFrame（类似 py-polars）
    df, err := polars.NewDataFrameFromMap(brg, map[string]interface{}{
        "name":   []string{"Alice", "Bob", "Charlie"},
        "age":    []int64{25, 30, 35},
        "salary": []float64{50000, 60000, 70000},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer df.Free()  // ⚠️ 重要：必须调用 Free

    // 链式操作
    result, _ := df.
        Filter(polars.Col("age").Gt(polars.Lit(28))).
        Select(polars.Col("name"), polars.Col("salary")).
        CollectRows(brg)
    
    // 或从 CSV 文件扫描
    lf := polars.ScanCSV("data.csv")
    lf.Print(brg)
}
```

### 前置要求（仅构建时需要）

- Go 1.21+
- Rust 1.70+
- Protobuf compiler

### 构建

```bash
# 1. 编译 Rust 动态库
cd rust
cargo build --release

# 2. 复制动态库到项目根目录
# macOS
cp target/release/libpolars_bridge.dylib ..

# Linux
cp target/release/libpolars_bridge.so ..

# Windows
cp target/release/polars_bridge.dll ..

# 3. 生成 Protobuf 代码（如果修改了 proto 文件）
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
export PATH=$PATH:$GOPATH/bin
cd /Users/esword/GolandProjects/src/polars-go-bridge && export PATH=$PATH:$GOPATH/bin && protoc --go_out=. --go_opt=paths=source_relative proto/polars_bridge.proto
```

## 🚀 快速开始

### 基本用法

```go
package main

import (
    "fmt"
    "log"
    "github.com/isesword/polars-go-bridge/bridge"
    "github.com/isesword/polars-go-bridge/polars"
)

func main() {
    // 加载 Polars Bridge
    brg, err := bridge.LoadBridge("")
    if err != nil {
        log.Fatal(err)
    }

    // 方式 1: 直接打印结果（使用 Polars 原生格式，不需要 Free）
    polars.ScanCSV("data.csv").
        Filter(polars.Col("age").Gt(polars.Lit(25))).
        Select(polars.Col("name"), polars.Col("age")).
        Limit(10).
        Print(brg)

    // 方式 2: 使用表达式展开选择多列
    polars.ScanCSV("data.csv").
        Select(polars.Cols("name", "age", "salary")...).
        Print(brg)

    // 方式 3: 选择所有列
    polars.ScanCSV("data.csv").
        Select(polars.All()).
        Limit(10).
        Print(brg)

    // 方式 4: 获取 Go 原生数据结构（内部会自动 Free）
    rows, err := polars.ScanCSV("data.csv").
        Filter(polars.Col("age").Gt(polars.Lit(25))).
        CollectRows(brg)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(rows)
    
    // 方式 5: 获取 DataFrame 对象（必须显式 Free）
    df, err := polars.ScanCSV("data.csv").Collect(brg)
    if err != nil {
        log.Fatal(err)
    }
    defer df.Free()  // ⚠️ 重要：必须调用 Free 释放 Rust 端资源
    
    df.Print()
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

// 从 Parquet 文件扫描（TODO）
lf := polars.ScanParquet("path/to/file.parquet")

// 从内存数据（TODO：将来支持 Arrow FFI）
// lf := polars.NewLazyFrame(data)
```

#### 转换操作

```go
// 过滤行
lf.Filter(polars.Col("age").Gt(polars.Lit(18)))

// 选择列
lf.Select(polars.Col("name"), polars.Col("age"))

// 添加/修改列
lf.WithColumns(
    polars.Col("age").Add(polars.Lit(1)).Alias("next_year_age"),
)

// 限制行数
lf.Limit(100)

// 方式 1: 打印结果（使用 Polars 原生的漂亮表格格式）
lf.Print(brg)

// 方式 2: 获取 Go 数据结构（[]map[string]interface{}）
rows, _ := lf.CollectRows(brg)
fmt.Println(rows)

// 方式 3: 收集为 DataFrame 并进一步处理
df, _ := lf.Collect(brg)
defer df.Free()

// DataFrame 支持链式操作！
result, _ := df.Filter(polars.Col("age").Gt(polars.Lit(30))).
    Select(polars.Col("name")).
    CollectRows(brg)
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
polars.ScanCSV("testdata/sample.csv").Print(brg)
// 输出: 7 行 4 列的完整表格
```

**示例 2: 过滤操作**
```go
polars.ScanCSV("testdata/sample.csv").
    Filter(polars.Col("age").Gt(polars.Lit(28))).
    Print(brg)
// 输出: 4 行（年龄 > 28）
```

**示例 3: 选择列**
```go
// 单列选择
polars.ScanCSV("testdata/sample.csv").
    Select(polars.Col("name"), polars.Col("age")).
    Print(brg)
// 输出: 7 行 2 列（只有 name 和 age）

// 多列选择（表达式展开）
polars.ScanCSV("testdata/sample.csv").
    Select(polars.Cols("name", "age", "salary")...).
    Print(brg)

// 选择所有列
polars.ScanCSV("testdata/sample.csv").
    Select(polars.All()).
    Print(brg)
```

**示例 4: 组合操作**
```go
polars.ScanCSV("testdata/sample.csv").
    Filter(polars.Col("age").Gt(polars.Lit(25))).
    Select(polars.Col("name"), polars.Col("salary")).
    Limit(3).
    Print(brg)
// 输出: 3 行 2 列
```

**示例 5: 复杂过滤**
```go
polars.ScanCSV("testdata/sample.csv").
    Filter(
        polars.Col("department").Eq(polars.Lit("Engineering")).
            And(polars.Col("salary").Gt(polars.Lit(60000))),
    ).
    Print(brg)
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
    Print(brg)
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

✅ **已验证功能**：
- CSV 扫描（懒加载）
- Filter + Select + WithColumns + Limit 链式操作
- DataFrame 链式操作支持
- Arrow IPC 数据传输（支持所有数值类型、布尔类型、字符串类型）
- CollectRows() 快捷方法
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
    Print(brg)
// 无需调用 Free
```

### 规则 2: CollectRows() 不需要 Free

```go
✅ 正确：CollectRows() 内部自动 Free
rows, err := polars.ScanCSV("data.csv").
    Filter(polars.Col("age").Gt(polars.Lit(25))).
    CollectRows(brg)
// 数据已复制到 Go，内部已自动释放 DataFrame
```

### 规则 3: Collect() 必须 Free

```go
✅ 正确：使用 defer 确保释放
df, err := polars.ScanCSV("data.csv").Collect(brg)
if err != nil {
    log.Fatal(err)
}
defer df.Free()  // 必须调用！

df.Print()

❌ 错误：忘记调用 Free 会导致 Rust 端内存泄露
df, _ := polars.ScanCSV("data.csv").Collect(brg)
df.Print()  // 泄露！
```

### 规则 4: NewDataFrameFromMap() 必须 Free

```go
✅ 正确
df, err := polars.NewDataFrameFromMap(brg, map[string]interface{}{
    "name": []string{"Alice", "Bob"},
    "age":  []int64{25, 30},
})
if err != nil {
    log.Fatal(err)
}
defer df.Free()  // 必须调用！

df.Print()
```

### 规则 5: DataFrame 链式操作返回 LazyFrame

```go
✅ 正确：链式操作后使用 CollectRows 或 Print
df, _ := polars.ScanCSV("data.csv").Collect(brg)
defer df.Free()

// DataFrame 的 Filter/Select 返回 LazyFrame
// 使用 CollectRows 会自动管理资源
result, _ := df.Filter(polars.Col("age").Gt(polars.Lit(30))).
    CollectRows(brg)

❌ 错误：如果再次 Collect()，会创建新的 DataFrame
df2, _ := df.Filter(polars.Col("age").Gt(polars.Lit(30))).
    Collect(brg)
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
    
    // 创建和释放大量 DataFrame
    for i := 0; i < 10000; i++ {
        df, _ := polars.ScanCSV("testdata/sample.csv").Collect(brg)
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
| `CollectRows()` | ❌ 否 | 内部自动 Free |
| `Collect()` | ✅ 是 | 返回 DataFrame，必须手动 Free |
| `NewDataFrameFromMap()` | ✅ 是 | 创建 DataFrame，必须手动 Free |
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
│   ├── dataframe_handle.go   # DataFrame 和链式操作
│   ├── expr.go               # 表达式构建器
│   ├── utils.go              # Arrow IPC 解析工具
│   └── scan_test.go          # 测试用例
├── proto/                    # Protobuf 协议定义
│   ├── polars_bridge.proto  # Plan 定义
│   └── polars_bridge.pb.go  # 生成的 Go 代码
├── rust/                     # Rust 桥接层
│   ├── src/
│   │   ├── lib.rs           # FFI 导出函数
│   │   ├── executor.rs      # Plan 执行器
│   │   ├── error.rs         # 错误处理
│   │   └── arrow_bridge.rs  # Arrow IPC 导出
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

### Q1: LazyFrame 和 DataFrame 有什么区别？

**简单理解**：
- **LazyFrame** = 执行计划（懒加载，延迟执行）
- **DataFrame** = 实际数据（已物化，在内存中）

#### LazyFrame（懒加载模式）

```go
// ❌ 这些操作都不会立即执行，只是构建执行计划
lf := polars.ScanCSV("data.csv")           // 计划①: 要读 CSV
lf2 := lf.Filter(Col("age").Gt(Lit(25)))   // 计划②: 要过滤
lf3 := lf2.Select(Col("name"))             // 计划③: 要选择列

// ✅ 直到这里才真正执行所有操作！
df, _ := lf3.Collect(brg)  // 现在才读文件、过滤、选择
```

**LazyFrame 就像一张"待办清单"**：只记录要做什么，但实际上什么都没做，直到调用 `Collect()` 或 `Print()` 才执行。

#### DataFrame（立即执行模式）

```go
// ✅ 数据已经在内存中
df, _ := polars.ScanCSV("data.csv").Collect(brg)
defer df.Free()

// DataFrame 可以直接访问数据
rows, _ := df.Rows()  // 获取所有行
df.Print()            // 打印数据
```

### Q2: 为什么 `df.Filter()` 返回 LazyFrame 而不是 DataFrame？

这是为了**性能优化**和**内存效率**：

#### 场景对比

**方案 A：返回 LazyFrame（当前实现）✅**
```go
df := ScanCSV("data.csv").Collect(brg)  // 100MB 内存

// 构建执行计划（几乎不占内存）
lazyResult := df.Filter(Col("age").Gt(Lit(28))).
    Select(Col("name"), Col("age")).
    Limit(10)

// 一次性执行优化后的计划
result := lazyResult.Collect(brg)  // 只分配需要的内存

// 总内存：100MB（原始 df）+ 少量结果内存
```

**方案 B：如果返回 DataFrame（假设）❌**
```go
df := ScanCSV("data.csv").Collect(brg)  // 100MB 内存

// 每一步都立即执行，创建中间结果
df1 := df.Filter(Col("age").Gt(Lit(28)))  // 立即执行：80MB
df2 := df1.Select(Col("name"), Col("age"))  // 立即执行：60MB
df3 := df2.Limit(10)  // 立即执行：1KB

// 总内存：100MB + 80MB + 60MB + 1KB = 240MB+
// 而且需要 3 次数据复制！
```

#### 优势总结

1. **查询优化**：LazyFrame 可以优化执行计划，避免不必要的计算
2. **内存效率**：避免创建中间 DataFrame，只在最后分配一次内存
3. **统一接口**：与 LazyFrame 的链式调用保持一致

### Q3: 什么时候使用 LazyFrame，什么时候使用 DataFrame？

#### 使用 LazyFrame（推荐）

✅ **适用场景**：
- 直接从文件读取并处理
- 一次性查询，不需要重复使用数据
- 追求最佳性能和内存效率

```go
// ✅ 全程懒加载，最优性能
rows, _ := polars.ScanCSV("data.csv").
    Filter(Col("age").Gt(Lit(25))).
    Select(Col("name")).
    CollectRows(brg)  // 一次性执行所有操作
```

#### 使用 DataFrame

✅ **适用场景**：
- 需要多次使用同一份数据
- 需要查看中间结果
- 数据需要在内存中保留

```go
// 先物化到 DataFrame
df, _ := polars.ScanCSV("data.csv").Collect(brg)
defer df.Free()

// 多次使用同一份数据
result1, _ := df.Filter(Col("age").Gt(Lit(25))).CollectRows(brg)
result2, _ := df.Filter(Col("age").Lt(Lit(30))).CollectRows(brg)
result3, _ := df.Select(Col("name")).CollectRows(brg)
```

### Q4: Collect() 和 CollectRows() 有什么区别？

#### `Collect()` - 返回 DataFrame

```go
df, err := lf.Collect(brg)
if err != nil {
    return err
}
defer df.Free()  // ⚠️ 需要手动释放内存

// 可以继续操作
df.Print()
result, _ := df.Filter(...).CollectRows(brg)
```

**使用场景**：需要对结果进行进一步操作。

#### `CollectRows()` - 返回 Go 数据结构

```go
rows, err := lf.CollectRows(brg)
if err != nil {
    return err
}
// ✅ 自动释放内存，无需手动管理

// rows 是 []map[string]interface{}
for _, row := range rows {
    fmt.Println(row["name"], row["age"])
}
```

**使用场景**：只需要获取最终数据，不需要进一步操作。

**实现关系**：`CollectRows` 内部调用 `Collect()` → `df.Rows()` → 自动释放。

### Q5: 如何理解执行流程？

#### 完整执行流程示例

```go
// 步骤 1: 创建懒加载计划（什么都没执行）
lf := polars.ScanCSV("data.csv").           // 计划①: 读 CSV
    Filter(Col("age").Gt(Lit(25))).         // 计划②: 过滤
    Select(Col("name"), Col("age"))          // 计划③: 选择列

// 步骤 2: 执行计划，获得 DataFrame（现在才真正执行）
df, _ := lf.Collect(brg)  // 🚀 执行！数据进入内存
defer df.Free()

// 此时：df 是一个内存中的数据表（比如 1000 行）

// 步骤 3: 在 DataFrame 上继续操作
// ⚠️ df.Filter() 又返回了 LazyFrame！
lf2 := df.Filter(Col("age").Gt(Lit(30)))   // 又变回懒加载计划

// 步骤 4: 再次执行
result, _ := lf2.Collect(brg)  // 🚀 再次执行！
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
DataFrame（内存中的数据）
    ↓
df.Filter() ← 又返回 LazyFrame
    ↓
LazyFrame（新的执行计划）
    ↓
Collect() ← 再次执行
    ↓
DataFrame（新的结果）
```

## 🚧 TODO

### 已完成 ✅
- [x] 实现 Arrow IPC 解析（Go 侧，基于 Apache Arrow Go）
- [x] 支持多种 Arrow 类型（Int32/64, Float32/64, Boolean, String, LargeString, StringView）
- [x] DataFrame 链式操作支持
- [x] CollectRows() 便捷方法
- [x] CSV 文件懒加载扫描
- [x] Filter / Select / WithColumns / Limit 操作
- [x] 完整的表达式系统：
  - 算术操作：Add, Sub, Mul, Div, **Mod, Pow**
  - 比较操作：Eq, Ne, Gt, Ge, Lt, Le
  - 逻辑操作：And, Or, **Not, Xor**
  - 其他：Alias, IsNull
- [x] 完善的测试用例

### 计划中 📋
- [ ] 支持 Parquet 文件扫描
- [ ] 支持更多 Arrow 类型（Date, Datetime, List, Struct 等）
- [ ] 支持更多表达式（字符串函数、日期函数、聚合函数等）
- [ ] 支持 GroupBy / Aggregation
- [ ] 支持 Join 操作
- [ ] 支持 Sort / Unique 操作
- [ ] Arrow FFI 零拷贝（用于内存数据源）
- [ ] 性能基准测试
- [ ] 完善错误处理和错误信息
- [ ] 支持流式处理大文件

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

MIT License

## 🙏 致谢

- [Polars](https://github.com/pola-rs/polars) - 高性能 DataFrame 库
- [Apache Arrow](https://arrow.apache.org/) - 列式内存格式
- [prost](https://github.com/tokio-rs/prost) - Rust Protobuf 实现
