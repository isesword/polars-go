package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pl "github.com/isesword/polars-go/polars"
)

func benchmarkRows(n int) []map[string]any {
	rows := make([]map[string]any, n)
	departments := []string{"Engineering", "Marketing", "Sales", "Finance"}
	for i := 0; i < n; i++ {
		rows[i] = map[string]any{
			"id":         int64(i + 1),
			"name":       fmt.Sprintf("user_%05d", i+1),
			"age":        int64(20 + (i % 30)),
			"salary":     int64(50000 + (i%17)*1250),
			"department": departments[i%len(departments)],
		}
	}
	return rows
}

type benchmarkStructRow struct {
	ID         int64  `polars:"id"`
	Name       string `polars:"name"`
	Age        int64  `polars:"age"`
	Salary     int64  `polars:"salary"`
	Department string `polars:"department"`
}

func joinFrames(n int) (*pl.DataFrame, *pl.DataFrame, error) {
	leftRows := make([]map[string]any, n)
	rightRows := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		leftRows[i] = map[string]any{
			"id":         int64(i + 1),
			"department": fmt.Sprintf("dept_%02d", i%16),
			"salary":     int64(50000 + (i%17)*1250),
		}
		rightRows[i] = map[string]any{
			"id":    int64(i + 1),
			"level": fmt.Sprintf("L%d", i%6),
			"bonus": int64(1000 + (i%9)*100),
		}
	}
	left, err := pl.NewDataFrame(leftRows)
	if err != nil {
		return nil, nil, err
	}
	right, err := pl.NewDataFrame(rightRows)
	if err != nil {
		left.Close()
		return nil, nil, err
	}
	return left, right, nil
}

func runCase(label string, loops int, fn func() (int, error)) {
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
	fmt.Printf("%s: %.0f us/op\n", label, float64(elapsed.Microseconds())/float64(loops))
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

func main() {
	fixtureDir := flag.String("fixture-dir", "/tmp/polars-go-compare", "directory that contains shared CSV/Parquet fixtures")
	sizesFlag := flag.String("sizes", "4000,16000", "comma-separated row counts to benchmark")
	flag.Parse()

	sizes, err := parseSizes(*sizesFlag)
	if err != nil {
		panic(err)
	}

	fmt.Printf("fixture_dir: %s\n\n", *fixtureDir)
	for _, n := range sizes {
		loops := loopsForSize(n)
		rows := benchmarkRows(n)
		df, err := pl.NewDataFrame(rows)
		if err != nil {
			panic(err)
		}

		left, right, err := joinFrames(n)
		if err != nil {
			df.Close()
			panic(err)
		}

		csvPath := filepath.Join(*fixtureDir, fmt.Sprintf("polars_compare_%d.csv", n))
		parquetPath := filepath.Join(*fixtureDir, fmt.Sprintf("polars_compare_%d.parquet", n))
		sinkPath := filepath.Join(*fixtureDir, fmt.Sprintf("bench-%d.jsonl", n))

		fmt.Printf("Rows%d\n", n)
		runCase("  QueryCollectLike(df.filter.select.collect.to_maps)", loops, func() (int, error) {
			return collectToMapsCount(df.Filter(pl.Col("age").Gt(pl.Lit(30))).Select(pl.Col("name"), pl.Col("salary"), pl.Col("department")))
		})
		runCase("  ToMaps", loops, func() (int, error) {
			return toMapsCount(df)
		})
		runCase("  ToStructs", loops, func() (int, error) {
			return toStructsCount(df)
		})
		runCase("  ScanCSV(filter+select+collect+ToMaps)", loops, func() (int, error) {
			return collectToMapsCount(
				pl.ScanCSV(csvPath).
					Filter(pl.Col("age").Gt(pl.Lit(30))).
					Select(pl.Col("name"), pl.Col("salary"), pl.Col("department")),
			)
		})
		runCase("  ScanParquet(filter+select+collect+ToMaps)", loops, func() (int, error) {
			return collectToMapsCount(
				pl.ScanParquet(parquetPath).
					Filter(pl.Col("age").Gt(pl.Lit(30))).
					Select(pl.Col("name"), pl.Col("salary"), pl.Col("department")),
			)
		})
		runCase("  GroupByAgg(to_maps)", loops, func() (int, error) {
			return collectToMapsCount(
				df.GroupBy("department").Agg(
					pl.Col("salary").Sum().Alias("salary_sum"),
					pl.Col("id").Count().Alias("employee_count"),
					pl.Col("age").Mean().Alias("avg_age"),
				),
			)
		})
		runCase("  JoinInner(select.to_maps)", loops, func() (int, error) {
			return collectToMapsCount(
				left.Join(right, pl.JoinOptions{On: []string{"id"}, How: pl.JoinInner}).
					Select(pl.Col("id"), pl.Col("salary"), pl.Col("bonus")),
			)
		})
		runCase("  SinkNDJSONFile", loops, func() (int, error) {
			return sinkNDJSONCount(
				df.Filter(pl.Col("age").Gt(pl.Lit(30))).Select(pl.Col("id"), pl.Col("name"), pl.Col("department")),
				sinkPath,
			)
		})
		fmt.Println()

		left.Close()
		right.Close()
		df.Close()
	}
}
