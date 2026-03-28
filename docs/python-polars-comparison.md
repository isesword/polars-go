# Go Bridge vs Python Polars

这份文档记录一套“同机器、近似同口径”的对比流程，用来观察本仓库和 Python Polars 在常见数据处理路径上的端到端差异。

## 对比目标

这里关注的是更贴近业务接入的整体路径，而不是只盯底层算子：

- 内存 DataFrame 上的 `filter + select + materialize`
- `ToMaps()` / `to_dicts()`
- CSV / Parquet 扫描后再物化
- `GroupBy().Agg(...)`
- `Join(...)`
- NDJSON 文件 sink

说明：

- Go 侧结果包含 Go <-> Rust 桥接以及结果物化成本。
- Python 侧结果包含 Python 对象物化成本。
- 两边底层都使用 Rust Polars，但宿主语言对象导出路径不同。
- 当前这版脚本会尽量做更严格的控制变量：
  - 两边都优先按列构造内存 DataFrame
  - 每个 case 只初始化自己真正需要的对象
  - 峰值内存对照使用“每个 case 单独进程”的方式采集

## 脚本

仓库提供两份可复跑脚本：

- `scripts/compare_python_polars.py`
  负责生成共享 CSV / Parquet fixture，并运行 Python Polars 对照。
- `scripts/compare_go_polars.go`
  负责运行本仓库 Go bridge 的同口径场景。
- `scripts/compare_with_python_polars.sh`
  一次性串起 fixture 准备、Python 对照和 Go bridge 对照。
- `scripts/compare_memory_with_python_polars.sh`
  以“每个 case 单独进程”的方式跑峰值内存对照，并输出 `maxrss_mib`。

补充：

- `scripts/compare_go_polars.go` 以及两个 shell 脚本现在支持 `--import-mode` / `--go-import-mode`
  可以在 Go 侧切换 `json` 和 `arrow` 两种内存导入方式。

## 运行方式

先准备共享 fixture：

```bash
/path/to/python-with-polars scripts/compare_python_polars.py --prepare-only
```

然后分别跑 Python 和 Go：

```bash
/path/to/python-with-polars scripts/compare_python_polars.py
go run ./scripts/compare_go_polars.go
```

如果想一条命令跑完整套：

```bash
scripts/compare_with_python_polars.sh --python /path/to/python-with-polars
```

如果想换 fixture 目录：

```bash
/path/to/python-with-polars scripts/compare_python_polars.py --fixture-dir /tmp/polars-go-compare
go run ./scripts/compare_go_polars.go --fixture-dir /tmp/polars-go-compare
```

默认参数：

- fixture 目录：`/tmp/polars-go-compare`
- 行数：`4000,16000`

大样本示例：

```bash
scripts/compare_with_python_polars.sh \
  --python /path/to/python-with-polars \
  --fixture-dir /tmp/polars-go-compare-large \
  --sizes 100000,1000000
```

如果想专门验证 Go 内存导入路径：

```bash
scripts/compare_with_python_polars.sh \
  --python /path/to/python-with-polars \
  --fixture-dir /tmp/polars-go-compare-large \
  --sizes 100000 \
  --go-import-mode arrow
```

如果想看峰值内存：

```bash
scripts/compare_memory_with_python_polars.sh \
  --python /path/to/python-with-polars \
  --fixture-dir /tmp/polars-go-compare-large \
  --sizes 100000
```

## 当前样例结果

测试环境：

- 日期：`2026-03-27`
- 机器：Apple M4 Pro
- Python Polars：`1.36.1`
- 口径：按列输入、按 case 惰性构造

### 4k 行

| 场景 | Go bridge | Python Polars | 备注 |
|---|---:|---:|---|
| QueryCollect-like | `2182 us/op` | `1086 us/op` | Python 更快 |
| ToMaps / ToDicts | `2213 us/op` | `1430 us/op` | Python 更快 |
| ScanCSV | `902 us/op` | `1139 us/op` | Go 更快 |
| ScanParquet | `773 us/op` | `1001 us/op` | Go 更快 |
| GroupByAgg | `1818 us/op` | `635 us/op` | Python 更快 |
| JoinInner | `2617 us/op` | `1616 us/op` | Python 更快 |
| SinkNDJSONFile | `1865 us/op` | `945 us/op` | Python 更快 |

### 100k 行

| 场景 | Go bridge | Python Polars | 备注 |
|---|---:|---:|---|
| QueryCollect-like | `51471 us/op` | `27237 us/op` | Python 更快 |
| ToMaps / ToDicts | `56790 us/op` | `38582 us/op` | Python 更快 |
| ScanCSV | `13633 us/op` | `21466 us/op` | Go 更快 |
| ScanParquet | `9892 us/op` | `24849 us/op` | Go 更快 |
| GroupByAgg | `43888 us/op` | `7963 us/op` | Python 更快 |
| JoinInner | `61516 us/op` | `33289 us/op` | Python 更快 |
| SinkNDJSONFile | `46152 us/op` | `9720 us/op` | Python 更快 |

### 1M 行

| 场景 | Go bridge | Python Polars | 备注 |
|---|---:|---:|---|
| QueryCollect-like | `567946 us/op` | `316973 us/op` | Python 更快 |
| ToMaps / ToDicts | `592082 us/op` | `407287 us/op` | Python 更快 |
| ScanCSV | `124380 us/op` | `243888 us/op` | Go 更快 |
| ScanParquet | `89925 us/op` | `234180 us/op` | Go 更快 |
| GroupByAgg | `457104 us/op` | `80299 us/op` | Python 更快 |
| JoinInner | `626925 us/op` | `332167 us/op` | Python 更快 |
| SinkNDJSONFile | `469598 us/op` | `101503 us/op` | Python 更快 |

## 峰值内存（maxrss）

时间更快不等于进程峰值驻留内存更低，所以这里单独补一份 `maxrss` 结果。

说明：

- 这里的 `maxrss_mib` 是“每个 case 独立进程”的峰值驻留内存。
- 这更接近“真实命令跑起来最高占了多少内存”，而不是 Go benchmark 里的 `B/op`。
- `maxrss` 会包含运行时、自身进程常驻开销、桥接库加载以及测试数据本身，所以它和 `B/op` 不是一个概念。

### 4k 行

| 场景 | Python Polars | Go bridge | 备注 |
|---|---:|---:|---|
| QueryCollect-like | `80.2 MiB` | `39.9 MiB` | Go 更低 |
| ToMaps / ToDicts | `79.3 MiB` | `30.2 MiB` | Go 更低 |
| ScanCSV | `86.8 MiB` | `38.0 MiB` | Go 更低 |
| ScanParquet | `85.6 MiB` | `47.5 MiB` | Go 更低 |
| GroupByAgg | `78.5 MiB` | `37.8 MiB` | Go 更低 |
| JoinInner | `87.4 MiB` | `46.8 MiB` | Go 更低 |
| SinkNDJSONFile | `80.1 MiB` | `48.4 MiB` | Go 更低 |

### 100k 行

| 场景 | Python Polars | Go bridge | 备注 |
|---|---:|---:|---|
| QueryCollect-like | `196.9 MiB` | `328.0 MiB` | Python 更低 |
| ToMaps / ToDicts | `237.2 MiB` | `359.3 MiB` | Python 更低 |
| ScanCSV | `203.8 MiB` | `93.1 MiB` | Go 更低 |
| ScanParquet | `205.8 MiB` | `93.3 MiB` | Go 更低 |
| GroupByAgg | `128.4 MiB` | `323.6 MiB` | Python 更低 |
| JoinInner | `233.0 MiB` | `396.5 MiB` | Python 更低 |
| SinkNDJSONFile | `148.2 MiB` | `326.6 MiB` | Python 更低 |

### 1M 行

| 场景 | Python Polars | Go bridge | 备注 |
|---|---:|---:|---|
| QueryCollect-like | `1259.6 MiB` | `2193.8 MiB` | Python 更低 |
| ToMaps / ToDicts | `1746.4 MiB` | `2605.1 MiB` | Python 更低 |
| ScanCSV | `1258.6 MiB` | `577.3 MiB` | Go 更低 |
| ScanParquet | `1261.2 MiB` | `542.8 MiB` | Go 更低 |
| GroupByAgg | `603.7 MiB` | `2617.2 MiB` | Python 更低 |
| JoinInner | `1621.5 MiB` | `1885.2 MiB` | Python 更低 |
| SinkNDJSONFile | `715.5 MiB` | `2892.2 MiB` | Python 更低 |

## 解读建议

- 更严格控制变量后，结论比旧版更分化：
  - `ScanCSV` / `ScanParquet` 这类文件扫描路径，Go bridge 依然明显占优。
  - 内存 DataFrame 上直接做 `ToMaps`、`GroupBy`、`Join`、`SinkNDJSONFile`，当前样例里 Python Polars 更快。
- 峰值内存也不再是单边结论：
  - 小样本 `4k` 时，Go bridge 普遍更低。
  - 到 `100k / 1M` 时，`scan` 路径反而是 Go 更低，但内存 DataFrame / groupby / join / sink 这些路径通常是 Python 更低。
- 这说明“Go bridge vs Python Polars”不能只用一句“谁更快/更省内存”概括，更准确的说法是：
  - 文件扫描型 workload：Go bridge 很强。
  - 内存 DataFrame 上的宿主对象导出与复杂物化路径：当前样例里 Python Polars 更稳。

## Arrow 导入实验

为了验证 Go 侧内存 DataFrame 路径是不是主要被兼容 JSON 导入拖慢，额外做了一轮 `100k` 的 Go-only 对照。

测试方式：

- 同样的 case
- 同样的 Go 脚本
- 只切换 `--import-mode json` 和 `--import-mode arrow`

结果：

| 场景 | Go JSON import | Go Arrow import | 变化 |
|---|---:|---:|---|
| QueryCollect-like | `52930 us/op`, `334.3 MiB` | `10321 us/op`, `121.1 MiB` | Arrow 明显更好 |
| GroupByAgg | `45661 us/op`, `327.9 MiB` | `3531 us/op`, `65.2 MiB` | Arrow 明显更好 |
| JoinInner | `62099 us/op`, `394.8 MiB` | `13531 us/op`, `158.9 MiB` | Arrow 明显更好 |
| SinkNDJSONFile | `45776 us/op`, `330.7 MiB` | `3803 us/op`, `81.3 MiB` | Arrow 明显更好 |

这说明前面那批“内存 DataFrame 上 Python 更快”的结果，很大概率不是 Polars 算子本身不行，而是 Go 侧当前默认的内存导入路径太重。

## 注意事项

- 这不是官方 upstream 对官方 upstream 的严格基准，只是一套便于日常回归的近似同口径对照。
- Python 脚本会主动清理 `sys.path` 里的仓库根目录，避免被本仓库的 `polars/` 目录遮住真正的 Python `polars` 包。
- 如果你调整了数据形状、列类型、行数，结果可能明显变化；建议把自己的真实业务 schema 也补进脚本里复测。
