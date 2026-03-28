package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pl "github.com/isesword/polars-go/polars"
)

func benchmarkColumns(n int) map[string]interface{} {
	ids := make([]int64, n)
	names := make([]string, n)
	ages := make([]int64, n)
	salaries := make([]int64, n)
	departmentsCol := make([]string, n)
	departments := []string{"Engineering", "Marketing", "Sales", "Finance"}
	for i := 0; i < n; i++ {
		ids[i] = int64(i + 1)
		names[i] = fmt.Sprintf("user_%05d", i+1)
		ages[i] = int64(20 + (i % 30))
		salaries[i] = int64(50000 + (i%17)*1250)
		departmentsCol[i] = departments[i%len(departments)]
	}
	return map[string]interface{}{
		"id":         ids,
		"name":       names,
		"age":        ages,
		"salary":     salaries,
		"department": departmentsCol,
	}
}

type benchmarkStructRow struct {
	ID         int64  `polars:"id"`
	Name       string `polars:"name"`
	Age        int64  `polars:"age"`
	Salary     int64  `polars:"salary"`
	Department string `polars:"department"`
}

type benchCase struct {
	label string
	run   func() (int, error)
}

type importMode string

const (
	importModeJSON  importMode = "json"
	importModeArrow importMode = "arrow"
)

var arrowAllocator = memory.NewGoAllocator()

func joinColumns(n int) (map[string]interface{}, map[string]interface{}) {
	leftIDs := make([]int64, n)
	leftDepartments := make([]string, n)
	leftSalaries := make([]int64, n)
	rightIDs := make([]int64, n)
	rightLevels := make([]string, n)
	rightBonuses := make([]int64, n)
	for i := 0; i < n; i++ {
		leftIDs[i] = int64(i + 1)
		leftDepartments[i] = fmt.Sprintf("dept_%02d", i%16)
		leftSalaries[i] = int64(50000 + (i%17)*1250)
		rightIDs[i] = int64(i + 1)
		rightLevels[i] = fmt.Sprintf("L%d", i%6)
		rightBonuses[i] = int64(1000 + (i%9)*100)
	}
	return map[string]interface{}{
			"id":         leftIDs,
			"department": leftDepartments,
			"salary":     leftSalaries,
		}, map[string]interface{}{
			"id":    rightIDs,
			"level": rightLevels,
			"bonus": rightBonuses,
		}
}

func benchmarkArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64},
		{Name: "salary", Type: arrow.PrimitiveTypes.Int64},
		{Name: "department", Type: arrow.BinaryTypes.String},
	}, nil)
}

func joinLeftArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "department", Type: arrow.BinaryTypes.String},
		{Name: "salary", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
}

func joinRightArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "level", Type: arrow.BinaryTypes.String},
		{Name: "bonus", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
}

func memoryMiB() float64 {
	// Use a portable approximation so the benchmark helper compiles on Windows
	// as well as Unix-like systems. MemStats.Sys reflects bytes obtained from
	// the OS by the Go runtime, which is stable enough for side-by-side runs.
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return float64(mem.Sys) / (1024 * 1024)
}

func runCase(label string, loops int, fn func() (int, error)) float64 {
	if n, err := fn(); err != nil {
		panic(err)
	} else if n == 0 {
		panic("empty result")
	}

	start := time.Now()
	for i := 0; i < loops; i++ {
		n, err := fn()
		if err != nil {
			panic(err)
		}
		if n == 0 {
			panic("empty result")
		}
	}
	elapsed := time.Since(start)
	usPerOp := float64(elapsed.Microseconds()) / float64(loops)
	fmt.Printf("%s: %.0f us/op\n", label, usPerOp)
	return usPerOp
}

func collectToMapsCount(lf *pl.LazyFrame) (int, error) {
	df, err := lf.Collect()
	if err != nil {
		return 0, err
	}
	defer df.Free()
	rows, err := df.ToMaps()
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

func toMapsCount(df *pl.DataFrame) (int, error) {
	rows, err := df.ToMaps()
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

func eagerToMapsCount(df *pl.EagerFrame) (int, error) {
	rows, err := df.ToMaps()
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

func importOnlyCount(create func() (*pl.DataFrame, error)) (int, error) {
	df, err := create()
	if err != nil {
		return 0, err
	}
	defer df.Close()
	return 1, nil
}

func importJoinOnlyCount(create func() (*pl.DataFrame, *pl.DataFrame, error)) (int, error) {
	left, right, err := create()
	if err != nil {
		return 0, err
	}
	defer left.Close()
	defer right.Close()
	return 1, nil
}

func collectOnlyCount(lf *pl.LazyFrame) (int, error) {
	df, err := lf.Collect()
	if err != nil {
		return 0, err
	}
	defer df.Free()
	return 1, nil
}

func toStructsCount(df *pl.DataFrame) (int, error) {
	rows, err := pl.ToStructs[benchmarkStructRow](df)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

func sinkNDJSONCount(lf *pl.LazyFrame, path string) (int, error) {
	if err := lf.SinkNDJSONFile(path); err != nil {
		return 0, err
	}
	stat, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return int(stat.Size()), nil
}

func loopsForSize(n int) int {
	if n <= 4000 {
		return 20
	}
	if n <= 16000 {
		return 8
	}
	if n <= 100000 {
		return 5
	}
	if n <= 1000000 {
		return 3
	}
	return 8
}

func parseSizes(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid size %q: %w", part, err)
		}
		if n <= 0 {
			return nil, fmt.Errorf("size must be positive: %d", n)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no sizes provided")
	}
	return out, nil
}

func parseImportMode(raw string) (importMode, error) {
	switch importMode(raw) {
	case importModeJSON:
		return importModeJSON, nil
	case importModeArrow:
		return importModeArrow, nil
	default:
		return "", fmt.Errorf("unsupported import mode %q", raw)
	}
}

func newDataFrameFromColumnsWithMode(data map[string]interface{}, schema *arrow.Schema, mode importMode) (*pl.DataFrame, error) {
	switch mode {
	case importModeJSON:
		return pl.NewDataFrameFromColumns(data)
	case importModeArrow:
		record, err := newArrowRecordBatchFromColumns(data, schema)
		if err != nil {
			return nil, err
		}
		return pl.NewDataFrameFromArrow(record)
	default:
		return nil, fmt.Errorf("unsupported import mode %q", mode)
	}
}

func newArrowRecordBatchFromColumns(data map[string]interface{}, schema *arrow.Schema) (arrow.RecordBatch, error) {
	builder := array.NewRecordBuilder(arrowAllocator, schema)
	defer builder.Release()

	for idx, field := range schema.Fields() {
		values, ok := data[field.Name]
		if !ok {
			return nil, fmt.Errorf("missing column %q", field.Name)
		}
		if err := appendColumnValues(builder.Field(idx), values, field.Name); err != nil {
			return nil, err
		}
	}
	return builder.NewRecordBatch(), nil
}

func appendColumnValues(builder array.Builder, values interface{}, name string) error {
	switch b := builder.(type) {
	case *array.Int64Builder:
		col, ok := values.([]int64)
		if !ok {
			return fmt.Errorf("column %s: expected []int64, got %T", name, values)
		}
		b.AppendValues(col, nil)
	case *array.StringBuilder:
		col, ok := values.([]string)
		if !ok {
			return fmt.Errorf("column %s: expected []string, got %T", name, values)
		}
		b.AppendValues(col, nil)
	default:
		return fmt.Errorf("column %s: unsupported builder %T", name, builder)
	}
	return nil
}

func buildCases(n int, fixtureDir string, mode importMode) (map[string]benchCase, func(), error) {
	csvPath := filepath.Join(fixtureDir, fmt.Sprintf("polars_compare_%d.csv", n))
	parquetPath := filepath.Join(fixtureDir, fmt.Sprintf("polars_compare_%d.parquet", n))
	sinkPath := filepath.Join(fixtureDir, fmt.Sprintf("bench-%d.jsonl", n))
	baseColumns := benchmarkColumns(n)
	leftColumns, rightColumns := joinColumns(n)
	baseSchema := benchmarkArrowSchema()
	leftSchema := joinLeftArrowSchema()
	rightSchema := joinRightArrowSchema()

	newBaseFrame := func() (*pl.DataFrame, error) {
		return newDataFrameFromColumnsWithMode(baseColumns, baseSchema, mode)
	}
	newJoinFrames := func() (*pl.DataFrame, *pl.DataFrame, error) {
		left, err := newDataFrameFromColumnsWithMode(leftColumns, leftSchema, mode)
		if err != nil {
			return nil, nil, err
		}
		right, err := newDataFrameFromColumnsWithMode(rightColumns, rightSchema, mode)
		if err != nil {
			left.Close()
			return nil, nil, err
		}
		return left, right, nil
	}

	baseFrame, err := newBaseFrame()
	if err != nil {
		return nil, func() {}, err
	}
	baseQuery := baseFrame.Filter(pl.Col("age").Gt(pl.Lit(30))).Select(pl.Col("name"), pl.Col("salary"), pl.Col("department"))
	groupByQuery := baseFrame.GroupBy("department").Agg(
		pl.Col("salary").Sum().Alias("salary_sum"),
		pl.Col("id").Count().Alias("employee_count"),
		pl.Col("age").Mean().Alias("avg_age"),
	)

	leftFrame, rightFrame, err := newJoinFrames()
	if err != nil {
		baseFrame.Close()
		return nil, func() {}, err
	}
	joinQuery := leftFrame.Join(rightFrame, pl.JoinOptions{On: []string{"id"}, How: pl.JoinInner}).
		Select(pl.Col("id"), pl.Col("salary"), pl.Col("bonus"))

	var (
		groupByResult     *pl.EagerFrame
		groupByResultErr  error
		groupByResultOnce sync.Once
		joinResult        *pl.EagerFrame
		joinResultErr     error
		joinResultOnce    sync.Once
	)

	getGroupByResult := func() (*pl.EagerFrame, error) {
		groupByResultOnce.Do(func() {
			groupByResult, groupByResultErr = groupByQuery.Collect()
		})
		return groupByResult, groupByResultErr
	}

	getJoinResult := func() (*pl.EagerFrame, error) {
		joinResultOnce.Do(func() {
			joinResult, joinResultErr = joinQuery.Collect()
		})
		return joinResult, joinResultErr
	}

	cleanup := func() {
		if groupByResult != nil {
			groupByResult.Free()
		}
		if joinResult != nil {
			joinResult.Free()
		}
		baseFrame.Close()
		leftFrame.Close()
		rightFrame.Close()
	}

	return map[string]benchCase{
		"import_only": {
			label: "  ImportOnly",
			run: func() (int, error) {
				return importOnlyCount(newBaseFrame)
			},
		},
		"collect_only": {
			label: "  CollectOnly(filter+select)",
			run: func() (int, error) {
				return collectOnlyCount(baseQuery)
			},
		},
		"query_collect_like": {
			label: "  QueryCollectLike(df.filter.select.collect.to_maps)",
			run: func() (int, error) {
				df, err := newBaseFrame()
				if err != nil {
					return 0, err
				}
				defer df.Close()
				return collectToMapsCount(df.Filter(pl.Col("age").Gt(pl.Lit(30))).Select(pl.Col("name"), pl.Col("salary"), pl.Col("department")))
			},
		},
		"to_maps": {
			label: "  ToMaps",
			run: func() (int, error) {
				return toMapsCount(baseFrame)
			},
		},
		"to_structs": {
			label: "  ToStructs",
			run: func() (int, error) {
				return toStructsCount(baseFrame)
			},
		},
		"scan_csv": {
			label: "  ScanCSV(filter+select+collect+ToMaps)",
			run: func() (int, error) {
				return collectToMapsCount(
					pl.ScanCSV(csvPath).
						Filter(pl.Col("age").Gt(pl.Lit(30))).
						Select(pl.Col("name"), pl.Col("salary"), pl.Col("department")),
				)
			},
		},
		"scan_parquet": {
			label: "  ScanParquet(filter+select+collect+ToMaps)",
			run: func() (int, error) {
				return collectToMapsCount(
					pl.ScanParquet(parquetPath).
						Filter(pl.Col("age").Gt(pl.Lit(30))).
						Select(pl.Col("name"), pl.Col("salary"), pl.Col("department")),
				)
			},
		},
		"group_by_import_only": {
			label: "  GroupByImportOnly",
			run: func() (int, error) {
				return importOnlyCount(newBaseFrame)
			},
		},
		"group_by_collect_only": {
			label: "  GroupByCollectOnly",
			run: func() (int, error) {
				return collectOnlyCount(groupByQuery)
			},
		},
		"group_by_export_only": {
			label: "  GroupByExportOnly",
			run: func() (int, error) {
				result, err := getGroupByResult()
				if err != nil {
					return 0, err
				}
				return eagerToMapsCount(result)
			},
		},
		"group_by_agg": {
			label: "  GroupByAgg(to_maps)",
			run: func() (int, error) {
				return collectToMapsCount(groupByQuery)
			},
		},
		"join_inner": {
			label: "  JoinInner(select.to_maps)",
			run: func() (int, error) {
				return collectToMapsCount(joinQuery)
			},
		},
		"join_import_only": {
			label: "  JoinImportOnly",
			run: func() (int, error) {
				return importJoinOnlyCount(newJoinFrames)
			},
		},
		"join_collect_only": {
			label: "  JoinCollectOnly",
			run: func() (int, error) {
				return collectOnlyCount(joinQuery)
			},
		},
		"join_export_only": {
			label: "  JoinExportOnly",
			run: func() (int, error) {
				result, err := getJoinResult()
				if err != nil {
					return 0, err
				}
				return eagerToMapsCount(result)
			},
		},
		"sink_ndjson_file": {
			label: "  SinkNDJSONFile",
			run: func() (int, error) {
				return sinkNDJSONCount(
					baseFrame.Filter(pl.Col("age").Gt(pl.Lit(30))).Select(pl.Col("id"), pl.Col("name"), pl.Col("department")),
					sinkPath,
				)
			},
		},
	}, cleanup, nil
}

func main() {
	fixtureDir := flag.String("fixture-dir", "/tmp/polars-go-compare", "directory that contains shared CSV/Parquet fixtures")
	sizesFlag := flag.String("sizes", "4000,16000", "comma-separated row counts to benchmark")
	importModeFlag := flag.String("import-mode", "json", "Go in-memory dataframe import mode: json or arrow")
	caseName := flag.String("case", "", "run only one case in this process and print a machine-readable RESULT line")
	singleSize := flag.Int("size", 0, "single row count used together with --case")
	flag.Parse()

	sizes, err := parseSizes(*sizesFlag)
	if err != nil {
		panic(err)
	}
	mode, err := parseImportMode(*importModeFlag)
	if err != nil {
		panic(err)
	}
	if *caseName != "" {
		if *singleSize <= 0 {
			panic("--size is required when --case is set")
		}
		loops := loopsForSize(*singleSize)
		cases, cleanup, err := buildCases(*singleSize, *fixtureDir, mode)
		if err != nil {
			panic(err)
		}
		defer cleanup()
		current, ok := cases[*caseName]
		if !ok {
			panic(fmt.Sprintf("unknown case: %s", *caseName))
		}
		usPerOp := runCase(current.label, loops, current.run)
		fmt.Printf("RESULT case=%s size=%d us_per_op=%.0f memory_mib=%.1f\n", *caseName, *singleSize, usPerOp, memoryMiB())
		return
	}

	fmt.Printf("fixture_dir: %s\n", *fixtureDir)
	fmt.Printf("import_mode: %s\n\n", mode)
	for _, n := range sizes {
		loops := loopsForSize(n)
		cases, cleanup, err := buildCases(n, *fixtureDir, mode)
		if err != nil {
			panic(err)
		}

		fmt.Printf("Rows%d\n", n)
		order := []string{"import_only", "collect_only", "query_collect_like", "to_maps", "to_structs", "scan_csv", "scan_parquet", "group_by_import_only", "group_by_collect_only", "group_by_export_only", "group_by_agg", "join_import_only", "join_collect_only", "join_export_only", "join_inner", "sink_ndjson_file"}
		for _, name := range order {
			current := cases[name]
			runCase(current.label, loops, current.run)
		}
		fmt.Println()
		cleanup()
	}
}
