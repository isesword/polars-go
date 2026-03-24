# ToMaps Profiling Guide

这份文档整理了 `ToMaps()` 的 benchmark 和 `pprof` 分析流程，方便后续重复测试和对比优化结果。

## 适用场景

当你修改了这些路径时，建议跑一遍本文档里的流程：

- `polars/utils.go`
- `polars/dataframe_handle.go`
- `polars/arrow_record_export_cgo.go`
- Arrow 导出 / RecordBatch 解析相关代码

## 前置条件

先确保项目已经构建出最新动态库：

```bash
./scripts/build.sh
```

如果改了 `proto/polars_bridge.proto`，先重新生成 Go 代码：

```bash
protoc --go_out=. --go_opt=paths=source_relative proto/polars_bridge.proto
```

## 1. 先跑功能回归

这一步先确认 `ToMaps()` 相关能力没被改坏：

```bash
go test ./polars -run 'TestArrowNestedRoundTrip|TestNewDataFrameWithArrowSchema|TestNewDataFrameFromColumnsWithArrowSchema' -v
go test ./...
```

如果这里不过，先不要看 benchmark。

## 2. 跑基准测试

最常用的是直接盯 `BenchmarkToMaps/Rows4000`：

```bash
go test ./polars -run '^$' -bench BenchmarkToMaps/Rows4000 -benchmem
```

如果想看整套基准：

```bash
go test ./polars -run '^$' -bench Benchmark -benchmem
```

如果想跑一组 smoke benchmark：

```bash
go test ./polars -run '^$' -bench 'BenchmarkImportRows/Rows2000|BenchmarkQueryCollect/Rows4000|BenchmarkToMaps/Rows4000' -benchtime=1x -benchmem
```

## 3. 生成 pprof 文件

生成 CPU 和内存 profile：

```bash
go test ./polars -run '^$' -bench BenchmarkToMaps/Rows4000 -benchmem \
  -cpuprofile /tmp/polars_tomaps_cpu.out \
  -memprofile /tmp/polars_tomaps_mem.out
```

为了让 `pprof` 能做源码定位，再单独编出测试二进制：

```bash
go test -c ./polars -o /tmp/polars.test
```

## 4. 看热点

先看总览：

```bash
go tool pprof -top -alloc_space /tmp/polars.test /tmp/polars_tomaps_mem.out
go tool pprof -top /tmp/polars.test /tmp/polars_tomaps_cpu.out
```

再看具体函数：

```bash
go tool pprof -list=parseArrowRecordBatch -alloc_space /tmp/polars.test /tmp/polars_tomaps_mem.out
go tool pprof -list=newArrowColumnDecoder.func18 -alloc_space /tmp/polars.test /tmp/polars_tomaps_mem.out
```

如果想开浏览器界面：

```bash
go tool pprof -http=:8080 /tmp/polars.test /tmp/polars_tomaps_cpu.out
```

## 5. 怎么解读结果

当前 `ToMaps()` 的典型热点通常集中在：

- `parseArrowRecordBatch`
- `newArrowColumnDecoder` 里的字符串 decoder
- `strings.Clone`
- `runtime.mapassign_faststr`

重点看三类指标：

- `ns/op`
  代表整体速度，越低越好。
- `B/op`
  代表每次 benchmark 分配的总字节数，越低越好。
- `allocs/op`
  代表每次 benchmark 的分配次数，越低越好。

常见判断方式：

- `ns/op` 下降，`B/op` 和 `allocs/op` 基本不变
  说明执行路径更顺了，但还没打掉结构性分配。
- `B/op` 和 `allocs/op` 明显下降
  说明真正减少了对象分配，这类优化通常更值。
- `pprof -list` 显示 `row[field] = value` 很重
  说明 `map[string]any` 构造和写入仍然是主要成本。
- `strings.Clone` 占比明显
  说明字符串路径是重点，但需要先确认内存生命周期再动。

## 6. 当前结论

截至这份文档更新时，已经验证过这些点：

- “按行填充 map” 比“按列回填 map”更快，值得保留
- 把 decoder 改成“直接写 row”这一步没有赚，已经回退
- `ToMaps()` 的主要成本仍然在：
  - `map[string]any` 写入
  - 字符串 clone

## 7. 一个建议的优化顺序

后面继续优化 `ToMaps()` 时，建议按这个顺序推进：

1. 先跑本文档里的 benchmark 和 pprof，拿一份基线
2. 每次只改一小步
3. 改完立即重跑同一条 benchmark
4. 如果收益不稳定，就回退
5. 只有在确认字符串生命周期安全后，才动 `strings.Clone`

## 8. 关于字符串生命周期

`ToMaps()` 内部会先导出 `arrow.RecordBatch`，然后在返回前释放它：

- `String.Value()` / `LargeString.Value()` 返回的是底层 Arrow buffer 的切片
- `StringView.Value()` 甚至直接基于底层 buffer 做 `unsafe` string

所以：

- 不能为了省分配，直接去掉 `strings.Clone`
- 否则 `ToMaps()` 返回后的字符串可能引用已经释放的 Arrow 内存

这块优化要非常保守。
