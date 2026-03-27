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

## 脚本

仓库提供两份可复跑脚本：

- `scripts/compare_python_polars.py`
  负责生成共享 CSV / Parquet fixture，并运行 Python Polars 对照。
- `scripts/compare_go_polars.go`
  负责运行本仓库 Go bridge 的同口径场景。
- `scripts/compare_with_python_polars.sh`
  一次性串起 fixture 准备、Python 对照和 Go bridge 对照。

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

## 当前样例结果

测试环境：

- 日期：`2026-03-27`
- 机器：Apple M4 Pro
- Python Polars：`1.36.1`

### 4k 行

| 场景 | Go bridge | Python Polars | 备注 |
|---|---:|---:|---|
| QueryCollect-like | `561 us/op` | `971 us/op` | Go 更快 |
| ToMaps / ToDicts | `733 us/op` | `1163 us/op` | Go 更快 |
| ScanCSV | `1062 us/op` | `1152 us/op` | Go 更快 |
| ScanParquet | `807 us/op` | `1079 us/op` | Go 更快 |
| GroupByAgg | `296 us/op` | `224 us/op` | Python 更快 |
| JoinInner | `900 us/op` | `1365 us/op` | Go 更快 |
| SinkNDJSONFile | `355 us/op` | `388 us/op` | Go 更快 |

### 16k 行

| 场景 | Go bridge | Python Polars | 备注 |
|---|---:|---:|---|
| QueryCollect-like | `1805 us/op` | `2798 us/op` | Go 更快 |
| ToMaps / ToDicts | `2593 us/op` | `4637 us/op` | Go 更快 |
| ScanCSV | `2983 us/op` | `4056 us/op` | Go 更快 |
| ScanParquet | `2041 us/op` | `3757 us/op` | Go 更快 |
| GroupByAgg | `396 us/op` | `282 us/op` | Python 略快 |
| JoinInner | `2238 us/op` | `4608 us/op` | Go 更快 |
| SinkNDJSONFile | `617 us/op` | `1983 us/op` | Go 更快 |

### 100k 行

| 场景 | Go bridge | Python Polars | 备注 |
|---|---:|---:|---|
| QueryCollect-like | `11041 us/op` | `21289 us/op` | Go 更快 |
| ToMaps / ToDicts | `16130 us/op` | `32125 us/op` | Go 更快 |
| ScanCSV | `13980 us/op` | `23759 us/op` | Go 更快 |
| ScanParquet | `11369 us/op` | `23936 us/op` | Go 更快 |
| GroupByAgg | `958 us/op` | `685 us/op` | Python 更快 |
| JoinInner | `12867 us/op` | `25606 us/op` | Go 更快 |
| SinkNDJSONFile | `3083 us/op` | `1457 us/op` | Python 更快 |

### 1M 行

| 场景 | Go bridge | Python Polars | 备注 |
|---|---:|---:|---|
| QueryCollect-like | `92538 us/op` | `297040 us/op` | Go 更快 |
| ToMaps / ToDicts | `186582 us/op` | `335882 us/op` | Go 更快 |
| ScanCSV | `138816 us/op` | `293380 us/op` | Go 更快 |
| ScanParquet | `99062 us/op` | `299717 us/op` | Go 更快 |
| GroupByAgg | `4316 us/op` | `3254 us/op` | Python 略快 |
| JoinInner | `109040 us/op` | `260900 us/op` | Go 更快 |
| SinkNDJSONFile | `21716 us/op` | `28093 us/op` | Go 更快 |

## 解读建议

- 如果业务主要是 Go 服务内做查询，然后快速导出为 Go 结果对象，本仓库当前表现很有竞争力。
- `ToStructs()` 通常会比 `ToMaps()` 更省分配、更快；如果业务允许 typed 结果，优先用 `ToStructs()`。
- `GroupBy` 这类偏纯聚合路径不一定总是 Go bridge 占优，至少在当前样例里 Python Polars 略快。
- `Parquet` 扫描通常比 `CSV` 更快，这一点两边趋势一致。
- 到 `100k / 1M` 这类更大样本时，Go bridge 在多数“查询后物化到宿主对象”的路径上仍保持优势。

## 注意事项

- 这不是官方 upstream 对官方 upstream 的严格基准，只是一套便于日常回归的近似同口径对照。
- Python 脚本会主动清理 `sys.path` 里的仓库根目录，避免被本仓库的 `polars/` 目录遮住真正的 Python `polars` 包。
- 如果你调整了数据形状、列类型、行数，结果可能明显变化；建议把自己的真实业务 schema 也补进脚本里复测。
