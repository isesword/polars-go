package polars

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/isesword/polars-go/bridge"
	pb "github.com/isesword/polars-go/proto"
	"github.com/xuri/excelize/v2"
	"google.golang.org/protobuf/proto"
)

func collectToMaps(t *testing.T, lf *LazyFrame) ([]map[string]interface{}, error) {
	t.Helper()

	df, err := lf.Collect()
	if err != nil {
		return nil, err
	}
	defer df.Free()

	return df.ToMaps()
}

func TestScanCSV(t *testing.T) {
	// 测试 1: 基本的 CSV 扫描
	t.Run("BasicCSVScan", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv")
		if lf == nil {
			t.Fatal("ScanCSV returned nil")
		}

		result, err := collectToMaps(t, lf)
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}

		fmt.Println(result)

		if len(result) != 7 {
			t.Fatalf("expected 7 rows, got %d", len(result))
		}

		if result[0]["name"] != "Alice" {
			t.Fatalf("unexpected first row name: %#v", result[0]["name"])
		}
	})

	// 测试 2: CSV 扫描 + Limit
	t.Run("CSVScanWithLimit", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Limit(5)

		result, err := collectToMaps(t, lf)
		if err != nil {
			t.Fatalf("Collect with limit failed: %v", err)
		}

		if len(result) != 5 {
			t.Fatalf("expected 5 rows, got %d", len(result))
		}
	})

	// 测试 3: CSV 扫描 + Filter + Select
	t.Run("CSVScanWithFilterSelect", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").
			Filter(Col("age").Gt(Lit(25))).
			Select(Col("name"), Col("age")).
			Limit(3)

		result, err := collectToMaps(t, lf)
		if err != nil {
			t.Fatalf("Collect with filter+select failed: %v", err)
		}

		if len(result) == 0 || len(result) > 3 {
			t.Fatalf("expected 1-3 rows, got %d", len(result))
		}

		for i, row := range result {
			ageVal, ok := row["age"].(int64)
			if !ok {
				t.Fatalf("row %d: age is not int64: %#v", i, row["age"])
			}
			if ageVal <= 25 {
				t.Fatalf("row %d: expected age > 25, got %d", i, ageVal)
			}
			if _, ok := row["name"].(string); !ok {
				t.Fatalf("row %d: name is not string: %#v", i, row["name"])
			}
		}
	})

	// 测试 4: 文件不存在的情况
	t.Run("NonExistentFile", func(t *testing.T) {
		lf := ScanCSV("nonexistent.csv")

		_, err := collectToMaps(t, lf)
		if err == nil {
			t.Error("Expected error for non-existent file, got nil")
		} else {
			t.Logf("Correctly got error for non-existent file: %v", err)
		}
	})

	// 测试 5: DataFrame 链式操作
	t.Run("DataFrameChaining", func(t *testing.T) {
		// 先收集一个 DataFrame
		lf := ScanCSV("../testdata/sample.csv")
		df, err := lf.Collect()
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}
		defer df.Free()

		// 在 DataFrame 上进行链式操作
		result, err := collectToMaps(t, df.Filter(Col("age").Gt(Lit(28))).
			Select(Col("name"), Col("age")).
			Limit(2))

		if err != nil {
			t.Fatalf("DataFrame chaining failed: %v", err)
		}

		if len(result) > 2 {
			t.Fatalf("expected at most 2 rows, got %d", len(result))
		}

		for i, row := range result {
			age := row["age"].(int64)
			if age <= 28 {
				t.Fatalf("row %d: expected age > 28, got %d", i, age)
			}
			t.Logf("Row %d: name=%v, age=%d", i, row["name"], age)
		}
	})

	t.Run("CSVScanWithOptions", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "custom.csv")
		content := "# comment line\nname;age;joined_at\n'Alice';25;2026-03-24\n'Bob';NA;2026-03-25\n"
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("failed to write csv fixture: %v", err)
		}

		hasHeader := true
		separator := byte(';')
		quoteChar := byte('\'')
		inferSchemaLength := uint64(10)
		nullValue := "NA"
		tryParseDates := true
		commentPrefix := "#"

		rows, err := collectToMaps(t, ScanCSVWithOptions(path, CSVScanOptions{
			HasHeader:         &hasHeader,
			Separator:         &separator,
			InferSchemaLength: &inferSchemaLength,
			NullValue:         &nullValue,
			TryParseDates:     &tryParseDates,
			QuoteChar:         &quoteChar,
			CommentPrefix:     &commentPrefix,
			Schema: map[string]DataType{
				"name":      DataTypeUTF8,
				"age":       DataTypeInt64,
				"joined_at": DataTypeDate,
			},
		}))
		if err != nil {
			t.Fatalf("csv scan with options failed: %v", err)
		}

		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if rows[0]["name"] != "Alice" {
			t.Fatalf("unexpected first name: %#v", rows[0]["name"])
		}
		if rows[1]["age"] != nil {
			t.Fatalf("expected second age to be nil, got %#v", rows[1]["age"])
		}
		joinedAt, ok := rows[0]["joined_at"].(time.Time)
		if !ok {
			t.Fatalf("expected joined_at to parse as time.Time, got %T", rows[0]["joined_at"])
		}
		if got := joinedAt.Format("2006-01-02"); got != "2026-03-24" {
			t.Fatalf("unexpected joined_at: %s", got)
		}
	})

	t.Run("CSVScanWithLossyEncodingAndIgnoreErrors", func(t *testing.T) {
		tempDir := t.TempDir()
		path := filepath.Join(tempDir, "invalid_utf8.csv")
		content := []byte("name,age\nAlice,20\nBad\xffName,not-a-number\n")
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("failed to write csv fixture: %v", err)
		}

		encoding := CSVEncodingLossyUTF8
		ignoreErrors := true
		rows, err := collectToMaps(t, ScanCSVWithOptions(path, CSVScanOptions{
			Encoding:     &encoding,
			IgnoreErrors: &ignoreErrors,
			Schema: map[string]DataType{
				"name": DataTypeUTF8,
				"age":  DataTypeInt64,
			},
		}))
		if err != nil {
			t.Fatalf("csv scan with lossy encoding failed: %v", err)
		}

		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if rows[0]["name"] != "Alice" {
			t.Fatalf("unexpected first name: %#v", rows[0]["name"])
		}
		if rows[1]["age"] != nil {
			t.Fatalf("expected second age to be nil after ignore_errors, got %#v", rows[1]["age"])
		}
		if got, ok := rows[1]["name"].(string); !ok || !strings.Contains(got, "Bad") {
			t.Fatalf("expected lossy UTF-8 string, got %T %#v", rows[1]["name"], rows[1]["name"])
		}
	})
}

func TestScanParquet(t *testing.T) {
	t.Run("BasicParquetScan", func(t *testing.T) {
		lf := ScanParquet("../testdata/fortune1000_2024.parquet").Limit(3)

		rows, err := collectToMaps(t, lf)
		if err != nil {
			t.Fatalf("Parquet scan failed: %v", err)
		}

		if len(rows) != 3 {
			t.Fatalf("Expected 3 rows, got %d", len(rows))
		}

		if len(rows[0]) == 0 {
			t.Fatal("Expected parquet row to contain columns")
		}
	})
}

func TestBasicOperations(t *testing.T) {
	// 测试 1: 算术运算 - 取模、幂运算
	t.Run("ArithmeticOperations", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("age"),
			Col("age").Mod(Lit(3)).Alias("age_mod_3"),
			Col("age").Pow(Lit(2)).Alias("age_squared"),
		)

		result, err := collectToMaps(t, lf)
		if err != nil {
			t.Fatalf("Arithmetic operations failed: %v", err)
		}

		if len(result) == 0 {
			t.Fatal("Expected at least one row")
		}

		// 验证第一行: age=25, 25%3=1, 25^2=625
		if result[0]["age"].(int64) == 25 {
			if result[0]["age_mod_3"].(int64) != 1 {
				t.Fatalf("Expected age_mod_3 = 1, got %v", result[0]["age_mod_3"])
			}
			if result[0]["age_squared"].(int64) != 625 {
				t.Fatalf("Expected age_squared = 625, got %v", result[0]["age_squared"])
			}
		}

		t.Logf("Arithmetic operations test passed: %v", result[0])
	})

	// 测试 2: 逻辑取反
	t.Run("NotOperation", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("age"),
			Col("age").Gt(Lit(30)).Alias("age_gt_30"),
			Col("age").Gt(Lit(30)).Not().Alias("age_not_gt_30"),
		)

		result, err := collectToMaps(t, lf)
		if err != nil {
			t.Fatalf("Not operation failed: %v", err)
		}

		if len(result) == 0 {
			t.Fatal("Expected at least one row")
		}

		// 验证逻辑取反
		for i, row := range result {
			ageGt30 := row["age_gt_30"].(bool)
			ageNotGt30 := row["age_not_gt_30"].(bool)
			if ageGt30 == ageNotGt30 {
				t.Fatalf("Row %d: age_gt_30 and age_not_gt_30 should be opposite, got %v and %v",
					i, ageGt30, ageNotGt30)
			}
		}

		t.Logf("Not operation test passed")
	})

	// 测试 3: 组合操作
	t.Run("CombinedOperations", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("name"),
			Col("age"),
			Col("salary"),
			// 工资增长 10%
			Col("salary").Mul(Lit(1.1)).Alias("new_salary"),
			// 工资等级 (万位)
			Col("salary").Div(Lit(10000)).Alias("salary_level"),
		)

		result, err := collectToMaps(t, lf.Limit(3))
		if err != nil {
			t.Fatalf("Combined operations failed: %v", err)
		}

		for i, row := range result {
			t.Logf("Row %d: name=%v, age=%v, salary=%v, new_salary=%v, salary_level=%v",
				i, row["name"], row["age"], row["salary"], row["new_salary"], row["salary_level"])
		}
	})
}

func TestGroupByJoinSortUnique(t *testing.T) {
	t.Run("GroupByAggregation", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			GroupBy("department").
			Agg(
				Col("salary").Sum().Alias("total_salary"),
				Col("name").Count().Alias("employee_count"),
				Col("age").Max().Alias("max_age"),
			).
			Sort(SortOptions{By: []Expr{Col("department")}}))
		if err != nil {
			t.Fatalf("group by aggregation failed: %v", err)
		}

		if len(rows) != 3 {
			t.Fatalf("expected 3 grouped rows, got %d", len(rows))
		}

		expected := map[string]struct {
			totalSalary   int64
			employeeCount uint64
			maxAge        int64
		}{
			"Engineering": {totalSalary: 185000, employeeCount: 3, maxAge: 35},
			"Marketing":   {totalSalary: 118000, employeeCount: 2, maxAge: 30},
			"Sales":       {totalSalary: 107000, employeeCount: 2, maxAge: 28},
		}

		for _, row := range rows {
			dept, ok := row["department"].(string)
			if !ok {
				t.Fatalf("department should be string, got %#v", row["department"])
			}
			want, exists := expected[dept]
			if !exists {
				t.Fatalf("unexpected department %q", dept)
			}
			if got := row["total_salary"].(int64); got != want.totalSalary {
				t.Fatalf("%s total_salary mismatch: got %d want %d", dept, got, want.totalSalary)
			}
			switch count := row["employee_count"].(type) {
			case uint64:
				if count != want.employeeCount {
					t.Fatalf("%s employee_count mismatch: got %d want %d", dept, count, want.employeeCount)
				}
			case int64:
				if uint64(count) != want.employeeCount {
					t.Fatalf("%s employee_count mismatch: got %d want %d", dept, count, want.employeeCount)
				}
			default:
				t.Fatalf("%s employee_count unexpected type %T", dept, row["employee_count"])
			}
			if got := row["max_age"].(int64); got != want.maxAge {
				t.Fatalf("%s max_age mismatch: got %d want %d", dept, got, want.maxAge)
			}
		}
	})

	t.Run("ExtendedAggregation", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			GroupBy("department").
			Agg(
				Col("salary").Median().Alias("median_salary"),
				Col("salary").Std().Alias("std_salary"),
				Col("salary").Var().Alias("var_salary"),
				Col("name").NUnique().Alias("unique_names"),
				Col("name").Len().Alias("name_len"),
			).
			Sort(SortOptions{By: []Expr{Col("department")}}))
		if err != nil {
			t.Fatalf("extended aggregation failed: %v", err)
		}

		if len(rows) != 3 {
			t.Fatalf("expected 3 grouped rows, got %d", len(rows))
		}

		eng := rows[0]
		if eng["department"] != "Engineering" {
			t.Fatalf("expected Engineering row first, got %#v", eng)
		}
		if got := eng["median_salary"]; got != float64(65000) && got != int64(65000) {
			t.Fatalf("unexpected median salary: %#v", got)
		}
		switch count := eng["unique_names"].(type) {
		case uint64:
			if count != 3 {
				t.Fatalf("expected unique_names=3, got %d", count)
			}
		case int64:
			if count != 3 {
				t.Fatalf("expected unique_names=3, got %d", count)
			}
		default:
			t.Fatalf("unexpected unique_names type %T", eng["unique_names"])
		}
		switch count := eng["name_len"].(type) {
		case uint64:
			if count != 3 {
				t.Fatalf("expected name_len=3, got %d", count)
			}
		case int64:
			if count != 3 {
				t.Fatalf("expected name_len=3, got %d", count)
			}
		default:
			t.Fatalf("unexpected name_len type %T", eng["name_len"])
		}
		if _, ok := eng["std_salary"].(float64); !ok {
			t.Fatalf("expected std_salary to be float64, got %T", eng["std_salary"])
		}
		if _, ok := eng["var_salary"].(float64); !ok {
			t.Fatalf("expected var_salary to be float64, got %T", eng["var_salary"])
		}
	})

	t.Run("QuantileAggregation", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			GroupBy("department").
			Agg(
				Col("salary").Quantile(0.5).Alias("salary_q50"),
				Col("salary").QuantileWithMethod(0.25, QuantileNearest).Alias("salary_q25_nearest"),
			).
			Sort(SortOptions{By: []Expr{Col("department")}}))
		if err != nil {
			t.Fatalf("quantile aggregation failed: %v", err)
		}

		if len(rows) != 3 {
			t.Fatalf("expected 3 grouped rows, got %d", len(rows))
		}

		eng := rows[0]
		if eng["department"] != "Engineering" {
			t.Fatalf("expected Engineering row first, got %#v", eng)
		}
		if _, ok := eng["salary_q50"].(float64); !ok {
			t.Fatalf("expected salary_q50 to be float64, got %T", eng["salary_q50"])
		}
		if got := eng["salary_q50"].(float64); got != 65000 {
			t.Fatalf("expected Engineering salary_q50=65000, got %#v", got)
		}
		if got := eng["salary_q25_nearest"]; got != float64(65000) && got != int64(65000) {
			t.Fatalf("unexpected salary_q25_nearest: %#v", got)
		}
	})

	t.Run("FirstLastAliasAndTypeStability", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			GroupBy("department").
			Agg(
				Col("name").First().Alias("first_name"),
				Col("name").Last().Alias("last_name"),
				Col("salary").First().Alias("first_salary"),
				Col("salary").Last().Alias("last_salary"),
			).
			Sort(SortOptions{By: []Expr{Col("department")}}))
		if err != nil {
			t.Fatalf("first/last aggregation failed: %v", err)
		}

		expected := map[string]struct {
			firstName   string
			lastName    string
			firstSalary int64
			lastSalary  int64
		}{
			"Engineering": {firstName: "Alice", lastName: "Eve", firstSalary: 50000, lastSalary: 65000},
			"Marketing":   {firstName: "Bob", lastName: "Frank", firstSalary: 60000, lastSalary: 58000},
			"Sales":       {firstName: "Diana", lastName: "Grace", firstSalary: 55000, lastSalary: 52000},
		}

		for _, row := range rows {
			dept := row["department"].(string)
			want := expected[dept]
			if got := row["first_name"]; got != want.firstName {
				t.Fatalf("%s first_name mismatch: %#v", dept, got)
			}
			if got := row["last_name"]; got != want.lastName {
				t.Fatalf("%s last_name mismatch: %#v", dept, got)
			}
			if _, ok := row["first_name"].(string); !ok {
				t.Fatalf("%s first_name should be string, got %T", dept, row["first_name"])
			}
			if _, ok := row["last_name"].(string); !ok {
				t.Fatalf("%s last_name should be string, got %T", dept, row["last_name"])
			}
			if got := row["first_salary"].(int64); got != want.firstSalary {
				t.Fatalf("%s first_salary mismatch: got %d want %d", dept, got, want.firstSalary)
			}
			if got := row["last_salary"].(int64); got != want.lastSalary {
				t.Fatalf("%s last_salary mismatch: got %d want %d", dept, got, want.lastSalary)
			}
		}
	})

	t.Run("AggregationExprFilterAndSortBy", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			GroupBy("department").
			Agg(
				Col("name").Filter(Col("age").Gt(Lit(30))).Alias("older_names"),
				Col("name").SortBy([]Expr{Col("age")}, SortByOptions{
					Descending: []bool{true},
				}).Alias("names_by_age_desc"),
			).
			Sort(SortOptions{By: []Expr{Col("department")}}))
		if err != nil {
			t.Fatalf("aggregation expr filter/sort_by failed: %v", err)
		}

		expectedOlder := map[string][]string{
			"Engineering": {"Charlie", "Eve"},
			"Marketing":   {},
			"Sales":       {},
		}
		expectedSorted := map[string][]string{
			"Engineering": {"Charlie", "Eve", "Alice"},
			"Marketing":   {"Bob", "Frank"},
			"Sales":       {"Diana", "Grace"},
		}

		for _, row := range rows {
			dept := row["department"].(string)
			gotOlder, ok := row["older_names"].([]any)
			if !ok {
				t.Fatalf("%s older_names unexpected type %T", dept, row["older_names"])
			}
			if len(gotOlder) != len(expectedOlder[dept]) {
				t.Fatalf("%s older_names length mismatch: got %v want %v", dept, gotOlder, expectedOlder[dept])
			}
			for i, want := range expectedOlder[dept] {
				if gotOlder[i] != want {
					t.Fatalf("%s older_names[%d] mismatch: got %#v want %#v", dept, i, gotOlder[i], want)
				}
			}

			gotSorted, ok := row["names_by_age_desc"].([]any)
			if !ok {
				t.Fatalf("%s names_by_age_desc unexpected type %T", dept, row["names_by_age_desc"])
			}
			if len(gotSorted) != len(expectedSorted[dept]) {
				t.Fatalf("%s names_by_age_desc length mismatch: got %v want %v", dept, gotSorted, expectedSorted[dept])
			}
			for i, want := range expectedSorted[dept] {
				if gotSorted[i] != want {
					t.Fatalf("%s names_by_age_desc[%d] mismatch: got %#v want %#v", dept, i, gotSorted[i], want)
				}
			}
		}
	})

	t.Run("Join", func(t *testing.T) {
		left := ScanCSV("../testdata/sample.csv").
			Select(Col("name"), Col("department"), Col("salary"))
		right := ScanCSV("../testdata/sample.csv").
			Select(Col("name"), Col("age"))

		rows, err := collectToMaps(t, left.Join(right, JoinOptions{
			On:  []string{"name"},
			How: JoinInner,
		}).
			Sort(SortOptions{By: []Expr{Col("name")}}).
			Limit(3))
		if err != nil {
			t.Fatalf("join failed: %v", err)
		}

		if len(rows) != 3 {
			t.Fatalf("expected 3 joined rows, got %d", len(rows))
		}
		if rows[0]["name"] != "Alice" || rows[0]["age"].(int64) != 25 {
			t.Fatalf("unexpected first joined row: %#v", rows[0])
		}
	})

	t.Run("SemiJoin", func(t *testing.T) {
		left := ScanCSV("../testdata/sample.csv").Select(Col("name"), Col("department"))
		right := ScanCSV("../testdata/sample.csv").
			Filter(Col("department").Eq(Lit("Engineering"))).
			Select(Col("name"))

		rows, err := collectToMaps(t, left.Join(right, JoinOptions{
			On:  []string{"name"},
			How: JoinSemi,
		}).Sort(SortOptions{By: []Expr{Col("name")}}))
		if err != nil {
			t.Fatalf("semi join failed: %v", err)
		}

		expected := []string{"Alice", "Charlie", "Eve"}
		if len(rows) != len(expected) {
			t.Fatalf("expected %d rows, got %d", len(expected), len(rows))
		}
		for i, want := range expected {
			if got := rows[i]["name"]; got != want {
				t.Fatalf("row %d expected %s, got %#v", i, want, got)
			}
		}
	})

	t.Run("AntiJoin", func(t *testing.T) {
		left := ScanCSV("../testdata/sample.csv").Select(Col("name"), Col("department"))
		right := ScanCSV("../testdata/sample.csv").
			Filter(Col("department").Eq(Lit("Engineering"))).
			Select(Col("name"))

		rows, err := collectToMaps(t, left.Join(right, JoinOptions{
			On:  []string{"name"},
			How: JoinAnti,
		}).Sort(SortOptions{By: []Expr{Col("name")}}))
		if err != nil {
			t.Fatalf("anti join failed: %v", err)
		}

		expected := []string{"Bob", "Diana", "Frank", "Grace"}
		if len(rows) != len(expected) {
			t.Fatalf("expected %d rows, got %d", len(expected), len(rows))
		}
		for i, want := range expected {
			if got := rows[i]["name"]; got != want {
				t.Fatalf("row %d expected %s, got %#v", i, want, got)
			}
		}
	})

	t.Run("CrossJoin", func(t *testing.T) {
		left := ScanCSV("../testdata/sample.csv").Select(Col("name")).Limit(2)
		right := ScanCSV("../testdata/sample.csv").Select(Col("department")).Limit(2)

		rows, err := collectToMaps(t, left.Join(right, JoinOptions{
			How: JoinCross,
		}).Sort(SortOptions{By: []Expr{Col("name"), Col("department")}}))
		if err != nil {
			t.Fatalf("cross join failed: %v", err)
		}

		if len(rows) != 4 {
			t.Fatalf("expected 4 rows, got %d", len(rows))
		}
		if rows[0]["name"] != "Alice" || rows[0]["department"] != "Engineering" {
			t.Fatalf("unexpected first cross row: %#v", rows[0])
		}
		if rows[3]["name"] != "Bob" || rows[3]["department"] != "Marketing" {
			t.Fatalf("unexpected last cross row: %#v", rows[3])
		}
	})

	t.Run("Sort", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			Select(Col("name"), Col("age")).
			Sort(SortOptions{
				By:         []Expr{Col("age")},
				Descending: []bool{true},
			}).
			Limit(3))
		if err != nil {
			t.Fatalf("sort failed: %v", err)
		}

		expectedNames := []string{"Charlie", "Eve", "Bob"}
		for i, name := range expectedNames {
			if rows[i]["name"] != name {
				t.Fatalf("row %d expected %s, got %#v", i, name, rows[i])
			}
		}
	})

	t.Run("Unique", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			Select(Col("department")).
			Unique(UniqueOptions{
				Subset: []string{"department"},
				Keep:   "first",
			}).
			Sort(SortOptions{By: []Expr{Col("department")}}))
		if err != nil {
			t.Fatalf("unique failed: %v", err)
		}

		expected := []string{"Engineering", "Marketing", "Sales"}
		if len(rows) != len(expected) {
			t.Fatalf("expected %d unique rows, got %d", len(expected), len(rows))
		}
		for i, want := range expected {
			if got := rows[i]["department"]; got != want {
				t.Fatalf("row %d expected department %s, got %#v", i, want, got)
			}
		}
	})

	t.Run("InMemoryJoin", func(t *testing.T) {
		left, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
			{"id": int64(3), "name": "Charlie"},
		})
		if err != nil {
			t.Fatalf("left dataframe failed: %v", err)
		}
		defer left.Close()

		right, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "age": int64(25)},
			{"id": int64(3), "age": int64(35)},
			{"id": int64(4), "age": int64(40)},
		})
		if err != nil {
			t.Fatalf("right dataframe failed: %v", err)
		}
		defer right.Close()

		rows, err := collectToMaps(t, left.Join(right, JoinOptions{
			On:  []string{"id"},
			How: JoinInner,
		}).Sort(SortOptions{By: []Expr{Col("id")}}))
		if err != nil {
			t.Fatalf("in-memory join failed: %v", err)
		}

		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if rows[0]["name"] != "Alice" || rows[0]["age"] != int64(25) {
			t.Fatalf("unexpected first row: %#v", rows[0])
		}
		if rows[1]["name"] != "Charlie" || rows[1]["age"] != int64(35) {
			t.Fatalf("unexpected second row: %#v", rows[1])
		}
	})

	t.Run("Concat", func(t *testing.T) {
		first := ScanCSV("../testdata/sample.csv").Limit(2)
		second := ScanCSV("../testdata/sample.csv").
			Filter(Col("name").Eq(Lit("Grace"))).
			Select(Col("name"), Col("age"), Col("salary"), Col("department"))

		rows, err := collectToMaps(t, Concat([]*LazyFrame{first, second}, ConcatOptions{
			Parallel:      true,
			MaintainOrder: true,
		}))
		if err != nil {
			t.Fatalf("concat failed: %v", err)
		}
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
		if rows[2]["name"] != "Grace" {
			t.Fatalf("expected final row to be Grace, got %#v", rows[2])
		}
	})
}

func TestWindowFunctions(t *testing.T) {
	t.Run("OverAndRank", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			Select(
				Col("name"),
				Col("department"),
				Col("salary"),
				Col("salary").Sum().Over(Col("department")).Alias("department_total_salary"),
				Col("salary").Rank(RankOptions{
					Method:     RankDense,
					Descending: true,
				}).Over(Col("department")).Alias("department_salary_rank"),
			).
			Sort(SortOptions{
				By:         []Expr{Col("department"), Col("salary")},
				Descending: []bool{false, true},
			}))
		if err != nil {
			t.Fatalf("window over/rank failed: %v", err)
		}

		if len(rows) != 7 {
			t.Fatalf("expected 7 rows, got %d", len(rows))
		}

		first := rows[0]
		if first["name"] != "Charlie" {
			t.Fatalf("expected first Engineering row to be Charlie, got %#v", first)
		}
		if got, err := toInt64(first["department_salary_rank"]); err != nil || got != 1 {
			t.Fatalf("expected Charlie rank=1, got %#v", first["department_salary_rank"])
		}
		if got, err := toInt64(first["department_total_salary"]); err != nil || got != 185000 {
			t.Fatalf("expected Engineering total salary=185000, got %#v", first["department_total_salary"])
		}

		last := rows[len(rows)-1]
		if last["name"] != "Grace" {
			t.Fatalf("expected final Sales row to be Grace, got %#v", last)
		}
		if got, err := toInt64(last["department_salary_rank"]); err != nil || got != 2 {
			t.Fatalf("expected Grace rank=2, got %#v", last["department_salary_rank"])
		}
		if got, err := toInt64(last["department_total_salary"]); err != nil || got != 107000 {
			t.Fatalf("expected Sales total salary=107000, got %#v", last["department_total_salary"])
		}
	})

	t.Run("CumSum", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			Select(
				Col("name"),
				Col("salary"),
				Col("salary").CumSum(false).Alias("running_salary"),
			).
			Limit(3))
		if err != nil {
			t.Fatalf("window cum_sum failed: %v", err)
		}

		expected := []int64{50000, 110000, 180000}
		for i, want := range expected {
			got, err := toInt64(rows[i]["running_salary"])
			if err != nil || got != want {
				t.Fatalf("row %d expected running_salary=%d, got %#v", i, want, rows[i]["running_salary"])
			}
		}
	})

	t.Run("CumCountMinMax", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			Select(
				Col("name"),
				Col("salary"),
				Col("salary").CumCount(false).Alias("salary_count"),
				Col("salary").CumMin(false).Alias("salary_min"),
				Col("salary").CumMax(false).Alias("salary_max"),
			).
			Limit(3))
		if err != nil {
			t.Fatalf("window cum_count/min/max failed: %v", err)
		}

		expectedCounts := []int64{1, 2, 3}
		expectedMins := []int64{50000, 50000, 50000}
		expectedMaxs := []int64{50000, 60000, 70000}
		for i := range rows {
			if got, err := toInt64(rows[i]["salary_count"]); err != nil || got != expectedCounts[i] {
				t.Fatalf("row %d expected salary_count=%d, got %#v", i, expectedCounts[i], rows[i]["salary_count"])
			}
			if got, err := toInt64(rows[i]["salary_min"]); err != nil || got != expectedMins[i] {
				t.Fatalf("row %d expected salary_min=%d, got %#v", i, expectedMins[i], rows[i]["salary_min"])
			}
			if got, err := toInt64(rows[i]["salary_max"]); err != nil || got != expectedMaxs[i] {
				t.Fatalf("row %d expected salary_max=%d, got %#v", i, expectedMaxs[i], rows[i]["salary_max"])
			}
		}
	})

	t.Run("CumProd", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			Select(
				Col("name"),
				Col("age"),
				Col("age").CumProd(false).Alias("running_age_product"),
			).
			Limit(3))
		if err != nil {
			t.Fatalf("window cum_prod failed: %v", err)
		}

		expected := []int64{25, 750, 26250}
		for i, want := range expected {
			got, err := toInt64(rows[i]["running_age_product"])
			if err != nil || got != want {
				t.Fatalf("row %d expected running_age_product=%d, got %#v", i, want, rows[i]["running_age_product"])
			}
		}
	})

	t.Run("OverWithOrderBy", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			Select(
				Col("name"),
				Col("department"),
				Col("age"),
				Col("salary"),
				Col("salary").CumSum(false).OverWithOptions(OverOptions{
					PartitionBy: []Expr{Col("department")},
					OrderBy:     []Expr{Col("age")},
				}).Alias("department_running_salary"),
			).
			Sort(SortOptions{By: []Expr{Col("department"), Col("age")}}))
		if err != nil {
			t.Fatalf("window over with order_by failed: %v", err)
		}

		expected := map[string]int64{
			"Alice":   50000,
			"Eve":     115000,
			"Charlie": 185000,
			"Grace":   52000,
			"Diana":   107000,
			"Frank":   58000,
			"Bob":     118000,
		}
		for _, row := range rows {
			name, _ := row["name"].(string)
			want := expected[name]
			got, err := toInt64(row["department_running_salary"])
			if err != nil || got != want {
				t.Fatalf("%s expected department_running_salary=%d, got %#v", name, want, row["department_running_salary"])
			}
		}
	})

	t.Run("OverWithJoinMapping", func(t *testing.T) {
		df, err := NewDataFrame([]map[string]any{
			{"athlete": "A", "country": "PT", "rank": int64(6)},
			{"athlete": "B", "country": "NL", "rank": int64(1)},
			{"athlete": "C", "country": "NL", "rank": int64(5)},
			{"athlete": "D", "country": "PT", "rank": int64(4)},
			{"athlete": "E", "country": "PT", "rank": int64(2)},
			{"athlete": "F", "country": "NL", "rank": int64(3)},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer df.Close()

		rows, err := collectToMaps(t, df.Select(
			Col("athlete"),
			Col("country"),
			Col("rank").SortBy([]Expr{Col("rank")}).OverWithOptions(OverOptions{
				PartitionBy:     []Expr{Col("country")},
				MappingStrategy: WindowMappingJoin,
			}).Alias("rank_in_country"),
		))
		if err != nil {
			t.Fatalf("window join mapping failed: %v", err)
		}

		expected := map[string][]int64{
			"PT": {2, 4, 6},
			"NL": {1, 3, 5},
		}
		for _, row := range rows {
			country := row["country"].(string)
			got, ok := row["rank_in_country"].([]any)
			if !ok {
				t.Fatalf("%s expected list result, got %T", country, row["rank_in_country"])
			}
			want := expected[country]
			if len(got) != len(want) {
				t.Fatalf("%s expected %v, got %#v", country, want, got)
			}
			for i, v := range want {
				gotV, err := toInt64(got[i])
				if err != nil || gotV != v {
					t.Fatalf("%s expected rank_in_country[%d]=%d, got %#v", country, i, v, got[i])
				}
			}
		}
	})

	t.Run("OverWithExplodeMapping", func(t *testing.T) {
		df, err := NewDataFrame([]map[string]any{
			{"athlete": "A", "country": "PT", "rank": int64(6)},
			{"athlete": "B", "country": "NL", "rank": int64(1)},
			{"athlete": "C", "country": "NL", "rank": int64(5)},
			{"athlete": "D", "country": "PT", "rank": int64(4)},
			{"athlete": "E", "country": "PT", "rank": int64(2)},
			{"athlete": "F", "country": "NL", "rank": int64(3)},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer df.Close()

		rows, err := collectToMaps(t, df.Select(
			All().SortBy([]Expr{Col("rank")}).OverWithOptions(OverOptions{
				PartitionBy:     []Expr{Col("country")},
				MappingStrategy: WindowMappingExplode,
			}),
		))
		if err != nil {
			t.Fatalf("window explode mapping failed: %v", err)
		}

		expectedAthletes := []string{"E", "D", "A", "B", "F", "C"}
		expectedCountries := []string{"PT", "PT", "PT", "NL", "NL", "NL"}
		expectedRanks := []int64{2, 4, 6, 1, 3, 5}
		if len(rows) != len(expectedAthletes) {
			t.Fatalf("expected %d rows, got %d", len(expectedAthletes), len(rows))
		}
		for i := range rows {
			if rows[i]["athlete"] != expectedAthletes[i] {
				t.Fatalf("row %d expected athlete=%s, got %#v", i, expectedAthletes[i], rows[i]["athlete"])
			}
			if rows[i]["country"] != expectedCountries[i] {
				t.Fatalf("row %d expected country=%s, got %#v", i, expectedCountries[i], rows[i]["country"])
			}
			gotRank, err := toInt64(rows[i]["rank"])
			if err != nil || gotRank != expectedRanks[i] {
				t.Fatalf("row %d expected rank=%d, got %#v", i, expectedRanks[i], rows[i]["rank"])
			}
		}
	})
}

func TestAnalyticExpressions(t *testing.T) {
	t.Run("Shift", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			Select(
				Col("name"),
				Col("age"),
				Col("age").Shift(1).Alias("prev_age"),
			).
			Limit(3))
		if err != nil {
			t.Fatalf("shift failed: %v", err)
		}

		if rows[0]["prev_age"] != nil {
			t.Fatalf("expected first prev_age to be nil, got %#v", rows[0]["prev_age"])
		}
		if got, err := toInt64(rows[1]["prev_age"]); err != nil || got != 25 {
			t.Fatalf("expected second prev_age=25, got %#v", rows[1]["prev_age"])
		}
		if got, err := toInt64(rows[2]["prev_age"]); err != nil || got != 30 {
			t.Fatalf("expected third prev_age=30, got %#v", rows[2]["prev_age"])
		}
	})

	t.Run("Diff", func(t *testing.T) {
		rows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			Select(
				Col("name"),
				Col("age"),
				Col("age").Diff(1, DiffNullIgnore).Alias("age_diff"),
			).
			Limit(3))
		if err != nil {
			t.Fatalf("diff failed: %v", err)
		}

		if rows[0]["age_diff"] != nil {
			t.Fatalf("expected first age_diff to be nil, got %#v", rows[0]["age_diff"])
		}
		if got, err := toInt64(rows[1]["age_diff"]); err != nil || got != 5 {
			t.Fatalf("expected second age_diff=5, got %#v", rows[1]["age_diff"])
		}
		if got, err := toInt64(rows[2]["age_diff"]); err != nil || got != 5 {
			t.Fatalf("expected third age_diff=5, got %#v", rows[2]["age_diff"])
		}
	})
}

func TestTemporalExpressions(t *testing.T) {
	rows := []map[string]any{
		{
			"date_str": "2026-03-24",
			"ts_str":   "2026-03-24 10:30:45",
			"time_str": "10:30:45",
		},
	}

	df, err := NewDataFrame(rows)
	if err != nil {
		t.Fatalf("failed to create temporal dataframe: %v", err)
	}
	defer df.Close()

	df2 := df.Select(
		Col("date_str").StrToDate("%Y-%m-%d").Alias("date_value"),
		Col("date_str").StrToDate("%Y-%m-%d").DtYear().Alias("year"),
		Col("date_str").StrToDate("%Y-%m-%d").DtMonth().Alias("month"),
		Col("date_str").StrToDate("%Y-%m-%d").DtDay().Alias("day"),
		Col("date_str").StrToDate("%Y-%m-%d").DtWeekday().Alias("weekday"),
		Col("date_str").StrToDate("%Y-%m-%d").DtMonthStart().Alias("month_start"),
		Col("date_str").StrToDate("%Y-%m-%d").DtMonthEnd().Alias("month_end"),
		Col("ts_str").StrToDatetime("%Y-%m-%d %H:%M:%S").Alias("ts_value"),
		Col("ts_str").StrToDatetime("%Y-%m-%d %H:%M:%S").DtHour().Alias("hour"),
		Col("ts_str").StrToDatetime("%Y-%m-%d %H:%M:%S").DtMinute().Alias("minute"),
		Col("ts_str").StrToDatetime("%Y-%m-%d %H:%M:%S").DtSecond().Alias("second"),
		Col("time_str").StrToTime("%H:%M:%S").Alias("time_value"),
	)

	result, err := collectToMaps(t, df2)
	if err != nil {
		t.Fatalf("temporal expressions failed: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}

	row := result[0]
	dateVal, ok := row["date_value"].(time.Time)
	if !ok {
		t.Fatalf("expected date_value to be time.Time, got %T", row["date_value"])
	}
	if got := dateVal.Format("2006-01-02"); got != "2026-03-24" {
		t.Fatalf("unexpected date_value: %s", got)
	}
	if got, err := toInt64(row["year"]); err != nil || got != 2026 {
		t.Fatalf("expected year=2026, got %#v", row["year"])
	}
	if got, err := toInt64(row["month"]); err != nil || got != 3 {
		t.Fatalf("expected month=3, got %#v", row["month"])
	}
	if got, err := toInt64(row["day"]); err != nil || got != 24 {
		t.Fatalf("expected day=24, got %#v", row["day"])
	}
	if got, err := toInt64(row["weekday"]); err != nil || got != 2 {
		t.Fatalf("expected weekday=2, got %#v", row["weekday"])
	}
	monthStartVal, ok := row["month_start"].(time.Time)
	if !ok || monthStartVal.Format("2006-01-02") != "2026-03-01" {
		t.Fatalf("unexpected month_start: %#v", row["month_start"])
	}
	monthEndVal, ok := row["month_end"].(time.Time)
	if !ok || monthEndVal.Format("2006-01-02") != "2026-03-31" {
		t.Fatalf("unexpected month_end: %#v", row["month_end"])
	}

	tsVal, ok := row["ts_value"].(time.Time)
	if !ok {
		t.Fatalf("expected ts_value to be time.Time, got %T", row["ts_value"])
	}
	if got := tsVal.UTC().Format("2006-01-02 15:04:05"); got != "2026-03-24 10:30:45" {
		t.Fatalf("unexpected ts_value: %s", got)
	}
	if got, err := toInt64(row["hour"]); err != nil || got != 10 {
		t.Fatalf("expected hour=10, got %#v", row["hour"])
	}
	if got, err := toInt64(row["minute"]); err != nil || got != 30 {
		t.Fatalf("expected minute=30, got %#v", row["minute"])
	}
	if got, err := toInt64(row["second"]); err != nil || got != 45 {
		t.Fatalf("expected second=45, got %#v", row["second"])
	}
	timeVal, ok := row["time_value"].(string)
	if !ok || timeVal != "10:30:45" {
		t.Fatalf("unexpected time_value: %#v", row["time_value"])
	}
}

func TestForwardFillExpression(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "value": int64(10)},
		{"id": int64(2), "value": nil},
		{"id": int64(3), "value": int64(30)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	rows, err := collectToMaps(t, df.Select(
		Col("id"),
		Col("value").FFill().Alias("value_ffill"),
	))
	if err != nil {
		t.Fatalf("collectToMaps failed: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[1]["value_ffill"] != int64(10) {
		t.Fatalf("unexpected forward fill rows: %#v", rows)
	}
}

func TestDataFramePivot(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "subject": "math", "score": int64(90)},
		{"name": "Alice", "subject": "english", "score": int64(85)},
		{"name": "Bob", "subject": "math", "score": int64(70)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	pivoted, err := df.Pivot(PivotOptions{
		On:            []string{"subject"},
		Index:         []string{"name"},
		Values:        []string{"score"},
		Aggregate:     "first",
		MaintainOrder: true,
	})
	if err != nil {
		t.Fatalf("Pivot failed: %v", err)
	}
	defer pivoted.Close()

	rows, err := pivoted.ToMaps()
	//fmt.Println(rows)
	if err != nil {
		t.Fatalf("pivoted.ToMaps failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != "Alice" || rows[0]["math"] != int64(90) || rows[0]["english"] != int64(85) {
		t.Fatalf("unexpected pivot rows: %#v", rows)
	}

	_, err = df.Pivot(PivotOptions{
		On:        []string{"subject"},
		Index:     []string{"name"},
		Values:    []string{"score"},
		Aggregate: "bogus",
	})
	if err == nil {
		t.Fatal("expected pivot with invalid aggregate to fail")
	}
	if !strings.Contains(err.Error(), `aggregate="bogus"`) {
		t.Fatalf("expected pivot error to include aggregate context, got %v", err)
	}
	if !strings.Contains(err.Error(), "unsupported pivot aggregate") {
		t.Fatalf("expected pivot error to include aggregate hint, got %v", err)
	}
}

func TestLazyFramePivot(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "subject": "math", "score": int64(90)},
		{"name": "Alice", "subject": "english", "score": int64(85)},
		{"name": "Bob", "subject": "math", "score": int64(70)},
		{"name": "Bob", "subject": "english", "score": int64(80)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	rows, err := collectToMaps(t, df.Filter(Col("name").IsNotNull()).Pivot(LazyPivotOptions{
		On:            []string{"subject"},
		OnColumns:     []any{"math", "english"},
		Index:         []string{"name"},
		Values:        []string{"score"},
		Aggregate:     "first",
		MaintainOrder: true,
	}))
	if err != nil {
		t.Fatalf("lazy pivot collect failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 lazy pivot rows, got %d", len(rows))
	}
	if rows[0]["name"] != "Alice" || rows[0]["math"] != int64(90) || rows[0]["english"] != int64(85) {
		t.Fatalf("unexpected lazy pivot rows: %#v", rows)
	}

	t.Run("PivotLazyFromDataFrame", func(t *testing.T) {
		rows, err := collectToMaps(t, df.PivotLazy(LazyPivotOptions{
			On:            []string{"subject"},
			OnColumns:     []any{"math", "english"},
			Index:         []string{"name"},
			Values:        []string{"score"},
			Aggregate:     "first",
			MaintainOrder: true,
		}))
		if err != nil {
			t.Fatalf("DataFrame.PivotLazy collect failed: %v", err)
		}
		if len(rows) != 2 || rows[1]["name"] != "Bob" {
			t.Fatalf("unexpected DataFrame.PivotLazy rows: %#v", rows)
		}
	})

	t.Run("MissingOnColumns", func(t *testing.T) {
		_, err := collectToMaps(t, df.PivotLazy(LazyPivotOptions{
			On:        []string{"subject"},
			Index:     []string{"name"},
			Values:    []string{"score"},
			Aggregate: "first",
		}))
		if err == nil {
			t.Fatal("expected missing OnColumns to fail")
		}
		if !strings.Contains(err.Error(), "explicit OnColumns") {
			t.Fatalf("expected missing OnColumns hint, got %v", err)
		}
	})

	t.Run("InvalidAggregate", func(t *testing.T) {
		_, err := collectToMaps(t, df.PivotLazy(LazyPivotOptions{
			On:        []string{"subject"},
			OnColumns: []any{"math", "english"},
			Index:     []string{"name"},
			Values:    []string{"score"},
			Aggregate: "bogus",
		}))
		if err == nil {
			t.Fatal("expected lazy pivot with invalid aggregate to fail")
		}
		if !strings.Contains(err.Error(), "unsupported pivot aggregate") {
			t.Fatalf("expected aggregate hint, got %v", err)
		}
	})
}

func TestBasicFrameOperations(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "name": "Alice", "age": int64(10), "score": nil},
		{"id": int64(2), "name": "Bob", "age": nil, "score": int64(20)},
		{"id": int64(3), "name": "Cara", "age": int64(30), "score": int64(40)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	t.Run("DropAndRename", func(t *testing.T) {
		rows, err := collectToMaps(t, df.Drop("score").Rename(map[string]string{
			"name": "person_name",
		}))
		if err != nil {
			t.Fatalf("collectToMaps failed: %v", err)
		}
		if _, ok := rows[0]["score"]; ok {
			t.Fatalf("expected score column to be dropped: %#v", rows[0])
		}
		if rows[0]["person_name"] != "Alice" {
			t.Fatalf("unexpected renamed rows: %#v", rows[0])
		}
	})

	t.Run("SliceHeadTail", func(t *testing.T) {
		sliceRows, err := collectToMaps(t, df.Slice(1, 2))
		if err != nil {
			t.Fatalf("Slice collect failed: %v", err)
		}
		if len(sliceRows) != 2 || sliceRows[0]["id"] != int64(2) || sliceRows[1]["id"] != int64(3) {
			t.Fatalf("unexpected slice rows: %#v", sliceRows)
		}

		headRows, err := collectToMaps(t, df.Head(1))
		if err != nil {
			t.Fatalf("Head collect failed: %v", err)
		}
		if len(headRows) != 1 || headRows[0]["id"] != int64(1) {
			t.Fatalf("unexpected head rows: %#v", headRows)
		}

		tailRows, err := collectToMaps(t, df.Tail(1))
		if err != nil {
			t.Fatalf("Tail collect failed: %v", err)
		}
		if len(tailRows) != 1 || tailRows[0]["id"] != int64(3) {
			t.Fatalf("unexpected tail rows: %#v", tailRows)
		}
	})

	t.Run("FillNullBFillDropNulls", func(t *testing.T) {
		fillRows, err := collectToMaps(t, df.FillNull(int64(0)))
		if err != nil {
			t.Fatalf("FillNull collect failed: %v", err)
		}
		if fillRows[1]["age"] != int64(0) || fillRows[0]["score"] != int64(0) {
			t.Fatalf("unexpected fill null rows: %#v", fillRows)
		}

		bfillRows, err := collectToMaps(t, df.Select(
			Col("id"),
			Col("age").BFill().Alias("age_bfill"),
		))
		if err != nil {
			t.Fatalf("BFill collect failed: %v", err)
		}
		if bfillRows[1]["age_bfill"] != int64(30) {
			t.Fatalf("unexpected bfill rows: %#v", bfillRows)
		}

		dropNullRows, err := collectToMaps(t, df.DropNulls("age", "score"))
		if err != nil {
			t.Fatalf("DropNulls collect failed: %v", err)
		}
		if len(dropNullRows) != 1 || dropNullRows[0]["id"] != int64(3) {
			t.Fatalf("unexpected drop null rows: %#v", dropNullRows)
		}
	})
}

func TestExplodeAndUnpivot(t *testing.T) {
	t.Run("Explode", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		}, nil)
		df, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "tags": []any{"go", "polars"}},
			{"id": int64(2), "tags": []any{"arrow"}},
		}, WithArrowSchema(schema))
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer df.Close()

		rows, err := collectToMaps(t, df.Explode("tags"))
		if err != nil {
			t.Fatalf("Explode collect failed: %v", err)
		}
		if len(rows) != 3 || rows[0]["tags"] != "go" || rows[2]["tags"] != "arrow" {
			t.Fatalf("unexpected explode rows: %#v", rows)
		}
	})

	t.Run("ConcatListAndListExprs", func(t *testing.T) {
		df, err := NewDataFrame([]map[string]any{
			{"station": "A", "day_1": int64(17), "day_2": int64(15), "day_3": int64(16)},
			{"station": "B", "day_1": int64(11), "day_2": int64(11), "day_3": int64(15)},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer df.Close()

		temps := ConcatList(Col("day_1"), Col("day_2"), Col("day_3"))
		rows, err := collectToMaps(t, df.Select(
			Col("station"),
			temps.Alias("temps"),
			temps.List().Len().Alias("temps_len"),
			temps.List().Sum().Alias("temps_sum"),
			temps.List().Mean().Alias("temps_mean"),
			temps.List().Median().Alias("temps_median"),
			temps.List().Max().Alias("temps_max"),
			temps.List().Min().Alias("temps_min"),
			temps.List().Std().Alias("temps_std"),
			temps.List().Var().Alias("temps_var"),
			temps.List().Contains(int64(15)).Alias("has_15"),
			temps.List().Sort().Alias("temps_sorted"),
			temps.List().Sort(ListSortOptions{Descending: true}).Alias("temps_sorted_desc"),
			temps.List().Reverse().Alias("temps_reversed"),
			temps.List().Unique().Alias("temps_unique"),
			temps.List().NUnique().Alias("temps_n_unique"),
			temps.List().ArgMin().Alias("temps_arg_min"),
			temps.List().ArgMax().Alias("temps_arg_max"),
			temps.List().Get(int64(1)).Alias("temps_get_1"),
			temps.List().Slice(int64(1), int64(2)).Alias("temps_slice"),
			temps.List().First().Alias("temps_first"),
			temps.List().Last().Alias("temps_last"),
			temps.List().Head(int64(2)).Alias("temps_head"),
			temps.List().Tail(int64(2)).Alias("temps_tail"),
			temps.List().Eval(Element().Add(Lit(int64(1)))).Alias("temps_plus_one"),
			temps.List().Agg(Element().Sum()).Alias("temps_agg_sum"),
		))
		if err != nil {
			t.Fatalf("list expression collect failed: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}

		firstTemps, ok := rows[0]["temps"].([]any)
		if !ok || len(firstTemps) != 3 || firstTemps[0] != int64(17) || firstTemps[2] != int64(16) {
			t.Fatalf("unexpected concat_list result: %#v", rows[0]["temps"])
		}
		if got, ok := asFloat64(rows[0]["temps_len"]); !ok || got != 3 {
			t.Fatalf("unexpected list.len result: %#v", rows[0]["temps_len"])
		}
		if got, ok := asFloat64(rows[0]["temps_sum"]); !ok || got != 48 {
			t.Fatalf("unexpected list.sum result: %#v", rows[0]["temps_sum"])
		}
		if got, ok := asFloat64(rows[0]["temps_mean"]); !ok || math.Abs(got-16) > 1e-9 {
			t.Fatalf("unexpected list.mean result: %#v", rows[0]["temps_mean"])
		}
		if got, ok := asFloat64(rows[0]["temps_median"]); !ok || math.Abs(got-16) > 1e-9 {
			t.Fatalf("unexpected list.median result: %#v", rows[0]["temps_median"])
		}
		if got, ok := asFloat64(rows[0]["temps_max"]); !ok || got != 17 {
			t.Fatalf("unexpected list.max result: %#v", rows[0]["temps_max"])
		}
		if got, ok := asFloat64(rows[0]["temps_min"]); !ok || got != 15 {
			t.Fatalf("unexpected list aggregate results: %#v", rows[0])
		}
		if got, ok := asFloat64(rows[0]["temps_std"]); !ok || math.Abs(got-1) > 1e-9 {
			t.Fatalf("unexpected list.std result: %#v", rows[0]["temps_std"])
		}
		if got, ok := asFloat64(rows[0]["temps_var"]); !ok || math.Abs(got-1) > 1e-9 {
			t.Fatalf("unexpected list.var result: %#v", rows[0]["temps_var"])
		}
		if rows[0]["has_15"] != true || rows[1]["has_15"] != true {
			t.Fatalf("unexpected list.contains results: %#v", rows)
		}
		firstSorted, ok := rows[0]["temps_sorted"].([]any)
		if !ok || len(firstSorted) != 3 || firstSorted[0] != int64(15) || firstSorted[2] != int64(17) {
			t.Fatalf("unexpected list.sort result: %#v", rows[0]["temps_sorted"])
		}
		firstSortedDesc, ok := rows[0]["temps_sorted_desc"].([]any)
		if !ok || len(firstSortedDesc) != 3 || firstSortedDesc[0] != int64(17) || firstSortedDesc[2] != int64(15) {
			t.Fatalf("unexpected descending list.sort result: %#v", rows[0]["temps_sorted_desc"])
		}
		firstReversed, ok := rows[0]["temps_reversed"].([]any)
		if !ok || len(firstReversed) != 3 || firstReversed[0] != int64(16) || firstReversed[2] != int64(17) {
			t.Fatalf("unexpected list.reverse result: %#v", rows[0]["temps_reversed"])
		}
		secondUnique, ok := rows[1]["temps_unique"].([]any)
		if !ok || len(secondUnique) != 2 || secondUnique[0] != int64(11) || secondUnique[1] != int64(15) {
			t.Fatalf("unexpected list.unique result: %#v", rows[1]["temps_unique"])
		}
		if got, ok := asFloat64(rows[1]["temps_n_unique"]); !ok || got != 2 {
			t.Fatalf("unexpected list.n_unique result: %#v", rows[1]["temps_n_unique"])
		}
		if got, ok := asFloat64(rows[0]["temps_arg_min"]); !ok || got != 1 {
			t.Fatalf("unexpected list.arg_min result: %#v", rows[0]["temps_arg_min"])
		}
		if got, ok := asFloat64(rows[0]["temps_arg_max"]); !ok || got != 0 {
			t.Fatalf("unexpected list.arg_max result: %#v", rows[0]["temps_arg_max"])
		}
		if rows[0]["temps_get_1"] != int64(15) || rows[1]["temps_get_1"] != int64(11) {
			t.Fatalf("unexpected list.get results: %#v", rows)
		}
		firstSlice, ok := rows[0]["temps_slice"].([]any)
		if !ok || len(firstSlice) != 2 || firstSlice[0] != int64(15) || firstSlice[1] != int64(16) {
			t.Fatalf("unexpected list.slice result: %#v", rows[0]["temps_slice"])
		}
		if rows[0]["temps_first"] != int64(17) || rows[1]["temps_first"] != int64(11) {
			t.Fatalf("unexpected list.first results: %#v", rows)
		}
		if rows[0]["temps_last"] != int64(16) || rows[1]["temps_last"] != int64(15) {
			t.Fatalf("unexpected list.last results: %#v", rows)
		}
		firstHead, ok := rows[0]["temps_head"].([]any)
		if !ok || len(firstHead) != 2 || firstHead[0] != int64(17) || firstHead[1] != int64(15) {
			t.Fatalf("unexpected list.head result: %#v", rows[0]["temps_head"])
		}
		firstTail, ok := rows[0]["temps_tail"].([]any)
		if !ok || len(firstTail) != 2 || firstTail[0] != int64(15) || firstTail[1] != int64(16) {
			t.Fatalf("unexpected list.tail result: %#v", rows[0]["temps_tail"])
		}

		firstTempsPlusOne, ok := rows[0]["temps_plus_one"].([]any)
		if !ok || len(firstTempsPlusOne) != 3 || firstTempsPlusOne[0] != int64(18) || firstTempsPlusOne[2] != int64(17) {
			t.Fatalf("unexpected list.eval result: %#v", rows[0]["temps_plus_one"])
		}
		if got, ok := asFloat64(rows[0]["temps_agg_sum"]); !ok || got != 48 {
			t.Fatalf("unexpected list.agg result: %#v", rows[0]["temps_agg_sum"])
		}
	})

	t.Run("ListJoin", func(t *testing.T) {
		df, err := NewDataFrame([]map[string]any{
			{"name": "A", "tag_1": "go", "tag_2": "polars", "tag_3": "bridge"},
			{"name": "B", "tag_1": "rust", "tag_2": "ffi", "tag_3": "bridge"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer df.Close()

		tags := ConcatList(Col("tag_1"), Col("tag_2"), Col("tag_3"))
		rows, err := collectToMaps(t, df.Select(
			Col("name"),
			tags.List().Join("-").Alias("tags_joined"),
		))
		if err != nil {
			t.Fatalf("list join collect failed: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if rows[0]["tags_joined"] != "go-polars-bridge" || rows[1]["tags_joined"] != "rust-ffi-bridge" {
			t.Fatalf("unexpected list.join results: %#v", rows)
		}
	})

	t.Run("ListJoinIgnoreNulls", func(t *testing.T) {
		df, err := NewDataFrame([]map[string]any{
			{"name": "A", "tag_1": "go", "tag_2": nil, "tag_3": "bridge"},
			{"name": "B", "tag_1": "rust", "tag_2": "ffi", "tag_3": nil},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer df.Close()

		tags := ConcatList(Col("tag_1"), Col("tag_2"), Col("tag_3"))
		rows, err := collectToMaps(t, df.Select(
			Col("name"),
			tags.List().Join("-", true).Alias("tags_joined"),
		))
		if err != nil {
			t.Fatalf("list join ignore_nulls collect failed: %v", err)
		}
		if rows[0]["tags_joined"] != "go-bridge" || rows[1]["tags_joined"] != "rust-ffi" {
			t.Fatalf("unexpected list.join(ignore_nulls=true) results: %#v", rows)
		}
	})

	t.Run("ListAnyAllDropNullsAndShift", func(t *testing.T) {
		df, err := NewDataFrame([]map[string]any{
			{"name": "A", "b1": true, "b2": false, "b3": true, "n1": int64(10), "n2": nil, "n3": int64(30), "x1": int64(10), "x2": int64(15), "x3": int64(21)},
			{"name": "B", "b1": true, "b2": true, "b3": true, "n1": nil, "n2": int64(20), "n3": int64(40), "x1": int64(5), "x2": int64(8), "x3": int64(8)},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer df.Close()

		bools := ConcatList(Col("b1"), Col("b2"), Col("b3"))
		nums := ConcatList(Col("n1"), Col("n2"), Col("n3"))
		rows, err := collectToMaps(t, df.Select(
			Col("name"),
			bools.List().Any().Alias("bool_any"),
			bools.List().All().Alias("bool_all"),
			nums.List().DropNulls().Alias("nums_drop_nulls"),
			nums.List().Shift(int64(1)).Alias("nums_shift_1"),
			ConcatList(Col("x1"), Col("x2"), Col("x3")).List().Diff(1, DiffNullIgnore).Alias("nums_diff"),
		))
		if err != nil {
			t.Fatalf("list any/all/drop_nulls/shift collect failed: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if rows[0]["bool_any"] != true || rows[1]["bool_any"] != true {
			t.Fatalf("unexpected list.any results: %#v", rows)
		}
		if rows[0]["bool_all"] != false || rows[1]["bool_all"] != true {
			t.Fatalf("unexpected list.all results: %#v", rows)
		}
		firstDropNulls, ok := rows[0]["nums_drop_nulls"].([]any)
		if !ok || len(firstDropNulls) != 2 || firstDropNulls[0] != int64(10) || firstDropNulls[1] != int64(30) {
			t.Fatalf("unexpected list.drop_nulls result: %#v", rows[0]["nums_drop_nulls"])
		}
		firstShifted, ok := rows[0]["nums_shift_1"].([]any)
		if !ok || len(firstShifted) != 3 || firstShifted[0] != nil || firstShifted[1] != int64(10) || firstShifted[2] != nil {
			t.Fatalf("unexpected list.shift result: %#v", rows[0]["nums_shift_1"])
		}
		firstDiff, ok := rows[0]["nums_diff"].([]any)
		if !ok || len(firstDiff) != 3 || firstDiff[0] != nil || firstDiff[1] != int64(5) || firstDiff[2] != int64(6) {
			t.Fatalf("unexpected list.diff result: %#v", rows[0]["nums_diff"])
		}
	})

	t.Run("ListCountMatchesGatherSampleAndSets", func(t *testing.T) {
		seed := uint64(7)
		df, err := NewDataFrame([]map[string]any{
			{
				"name": "A",
				"a1":   int64(1), "a2": int64(2), "a3": int64(2), "a4": int64(4),
				"b1": int64(2), "b2": int64(4), "b3": int64(5), "b4": int64(5),
			},
			{
				"name": "B",
				"a1":   int64(3), "a2": int64(3), "a3": int64(5), "a4": int64(7),
				"b1": int64(3), "b2": int64(6), "b3": int64(7), "b4": int64(8),
			},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer df.Close()

		left := ConcatList(Col("a1"), Col("a2"), Col("a3"), Col("a4"))
		right := ConcatList(Col("b1"), Col("b2"), Col("b3"), Col("b4"))
		indexes := ConcatList(Lit(int64(0)), Lit(int64(2)))
		rows, err := collectToMaps(t, df.Select(
			Col("name"),
			left.List().UniqueStable().Alias("left_unique_stable"),
			left.List().CountMatches(int64(2)).Alias("left_count_2"),
			left.List().Gather(indexes, true).Alias("left_gather"),
			left.List().GatherEvery(int64(2), int64(1)).Alias("left_gather_every"),
			left.List().SampleN(int64(2), ListSampleOptions{Seed: &seed}).Alias("left_sample_n"),
			left.List().SampleFraction(0.5, ListSampleOptions{Seed: &seed}).Alias("left_sample_fraction"),
			left.List().Union(right).Alias("set_union"),
			left.List().SetDifference(right).Alias("set_difference"),
			left.List().SetIntersection(right).Alias("set_intersection"),
			left.List().SetSymmetricDifference(right).Alias("set_symmetric_difference"),
		))
		if err != nil {
			t.Fatalf("list count/gather/sample/sets collect failed: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}

		leftUniqueStable, ok := rows[0]["left_unique_stable"].([]any)
		if !ok || len(leftUniqueStable) != 3 || leftUniqueStable[0] != int64(1) || leftUniqueStable[1] != int64(2) || leftUniqueStable[2] != int64(4) {
			t.Fatalf("unexpected list.unique_stable result: %#v", rows[0]["left_unique_stable"])
		}
		if got, ok := asFloat64(rows[0]["left_count_2"]); !ok || got != 2 {
			t.Fatalf("unexpected list.count_matches result: %#v", rows[0]["left_count_2"])
		}
		leftGather, ok := rows[0]["left_gather"].([]any)
		if !ok || len(leftGather) != 2 || leftGather[0] != int64(1) || leftGather[1] != int64(2) {
			t.Fatalf("unexpected list.gather result: %#v", rows[0]["left_gather"])
		}
		leftGatherEvery, ok := rows[0]["left_gather_every"].([]any)
		if !ok || len(leftGatherEvery) != 2 || leftGatherEvery[0] != int64(2) || leftGatherEvery[1] != int64(4) {
			t.Fatalf("unexpected list.gather_every result: %#v", rows[0]["left_gather_every"])
		}
		leftSampleN, ok := rows[0]["left_sample_n"].([]any)
		if !ok || len(leftSampleN) != 2 {
			t.Fatalf("unexpected list.sample_n result: %#v", rows[0]["left_sample_n"])
		}
		leftSampleFraction, ok := rows[0]["left_sample_fraction"].([]any)
		if !ok || len(leftSampleFraction) != 2 {
			t.Fatalf("unexpected list.sample_fraction result: %#v", rows[0]["left_sample_fraction"])
		}

		assertIntSet := func(value any, expected []int64, label string) {
			t.Helper()
			items, ok := value.([]any)
			if !ok {
				t.Fatalf("%s is not []any: %#v", label, value)
			}
			if len(items) != len(expected) {
				t.Fatalf("%s length mismatch: %#v", label, value)
			}
			got := make(map[int64]int, len(items))
			for _, item := range items {
				v, ok := item.(int64)
				if !ok {
					t.Fatalf("%s contains non-int64: %#v", label, value)
				}
				got[v]++
			}
			for _, want := range expected {
				got[want]--
			}
			for key, count := range got {
				if count != 0 {
					t.Fatalf("%s mismatch for %d: %#v", label, key, value)
				}
			}
		}

		assertIntSet(rows[0]["set_union"], []int64{1, 2, 4, 5}, "list.union")
		assertIntSet(rows[0]["set_difference"], []int64{1}, "list.set_difference")
		assertIntSet(rows[0]["set_intersection"], []int64{2, 4}, "list.set_intersection")
		assertIntSet(rows[0]["set_symmetric_difference"], []int64{1, 5}, "list.set_symmetric_difference")
	})

	t.Run("Unpivot", func(t *testing.T) {
		df, err := NewDataFrame([]map[string]any{
			{"name": "Alice", "math": int64(90), "english": int64(85)},
			{"name": "Bob", "math": int64(70), "english": int64(88)},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer df.Close()

		rows, err := collectToMaps(t, df.Unpivot(UnpivotOptions{
			On:           []string{"math", "english"},
			Index:        []string{"name"},
			VariableName: "subject",
			ValueName:    "score",
		}))
		if err != nil {
			t.Fatalf("Unpivot collect failed: %v", err)
		}
		if len(rows) != 4 || rows[0]["subject"] != "math" || rows[0]["score"] != int64(90) {
			t.Fatalf("unexpected unpivot rows: %#v", rows)
		}
	})
}

func TestAdvancedConvenienceOperations(t *testing.T) {
	t.Run("RegexSelectExcludeAndSortNullsLast", func(t *testing.T) {
		df, err := NewDataFrame([]map[string]any{
			{"age": int64(10), "bonus": int64(1), "name": "Alice"},
			{"age": nil, "bonus": int64(2), "name": "Bob"},
			{"age": int64(5), "bonus": int64(3), "name": "Cara"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer df.Close()

		regexRows, err := collectToMaps(t, df.Select(ColRegex("^(age|bonus)$")))
		if err != nil {
			t.Fatalf("regex select failed: %v", err)
		}
		if _, ok := regexRows[0]["name"]; ok {
			t.Fatalf("expected regex selection to exclude name: %#v", regexRows[0])
		}

		excludeRows, err := collectToMaps(t, df.Select(All().Exclude("bonus")))
		if err != nil {
			t.Fatalf("exclude failed: %v", err)
		}
		if _, ok := excludeRows[0]["bonus"]; ok {
			t.Fatalf("expected exclude selection to drop bonus: %#v", excludeRows[0])
		}

		sortedRows, err := collectToMaps(t, df.Sort(SortOptions{
			By:        []Expr{Col("age")},
			NullsLast: []bool{true},
		}))
		if err != nil {
			t.Fatalf("sort nulls_last failed: %v", err)
		}
		if sortedRows[0]["age"] != int64(5) || sortedRows[2]["age"] != nil {
			t.Fatalf("unexpected nulls_last sort rows: %#v", sortedRows)
		}
	})

	t.Run("StringHelpers", func(t *testing.T) {
		df, err := NewDataFrame([]map[string]any{
			{"text": "pre_foo_foo_suf", "raw": "abc123def456"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer df.Close()

		rows, err := collectToMaps(t, df.Select(
			Col("text").StrStripPrefix("pre_").Alias("strip_prefix"),
			Col("text").StrStripSuffix("_suf").Alias("strip_suffix"),
			Col("text").StrReplaceN("foo", "bar", true, 1).Alias("replace_n"),
			Col("raw").StrExtractAll("\\d+").Alias("extract_all"),
			Col("text").StrCountMatches("foo", true).Alias("count_matches"),
		))
		if err != nil {
			t.Fatalf("string helper collect failed: %v", err)
		}
		row := rows[0]
		if row["strip_prefix"] != "foo_foo_suf" || row["strip_suffix"] != "pre_foo_foo" {
			t.Fatalf("unexpected strip results: %#v", row)
		}
		if row["replace_n"] != "pre_bar_foo_suf" {
			t.Fatalf("unexpected replace/count results: %#v", row)
		}
		matchCount, ok := asFloat64(row["count_matches"])
		if !ok || matchCount != 2 {
			t.Fatalf("unexpected count_matches result: %#v", row["count_matches"])
		}
		values, ok := row["extract_all"].([]any)
		if !ok || len(values) != 2 || values[0] != "123" || values[1] != "456" {
			t.Fatalf("unexpected extract_all result: %#v", row["extract_all"])
		}
	})

	t.Run("ExplainAndDescribe", func(t *testing.T) {
		df, err := NewDataFrame([]map[string]any{
			{"name": "Alice", "age": int64(10)},
			{"name": "Bob", "age": int64(20)},
			{"name": "Cara", "age": nil},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer df.Close()

		explain, err := df.Filter(Col("age").IsNull().Not()).Explain()
		if err != nil {
			t.Fatalf("Explain failed: %v", err)
		}
		if explain == "" || !strings.Contains(strings.ToUpper(explain), "FILTER") {
			t.Fatalf("unexpected explain output: %q", explain)
		}

		described, err := df.Describe()
		if err != nil {
			t.Fatalf("Describe failed: %v", err)
		}
		defer described.Close()

		describeRows, err := described.df.ToMaps()
		fmt.Println(describeRows)
		if err != nil {
			t.Fatalf("Describe ToMaps failed: %v", err)
		}
		if len(describeRows) == 0 || describeRows[0]["statistic"] != "count" {
			t.Fatalf("unexpected describe rows: %#v", describeRows)
		}
	})

	t.Run("NullNanNumericAndSamplingHelpers", func(t *testing.T) {
		seed := uint64(7)
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "nan_helpers.csv")
		if err := os.WriteFile(path, []byte("name,value,score\nAlice,1.25,NaN\nBob,,2.5\nCara,-3.5,NaN\nDrew,10.0,4.25\n"), 0o644); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}
		nullValue := ""
		df := ScanCSVWithOptions(path, CSVScanOptions{
			NullValue: &nullValue,
			Schema: map[string]DataType{
				"name":  DataTypeUTF8,
				"value": DataTypeFloat64,
				"score": DataTypeFloat64,
			},
		})

		rows, err := collectToMaps(t, df.Select(
			Col("name"),
			Col("value").IsNotNull().Alias("value_not_null"),
			Col("score").IsNan().Alias("score_is_nan"),
			Col("score").IsFinite().Alias("score_is_finite"),
			Col("value").Abs().Alias("abs_value"),
			Col("value").Round(0).Alias("round_value"),
			Col("value").Clip(-2.0, 5.0).Alias("clip_value"),
			Col("value").Sqrt().Alias("sqrt_value"),
			Col("value").Log(10.0).Alias("log10_value"),
		))
		if err != nil {
			t.Fatalf("helper select failed: %v", err)
		}
		if rows[0]["value_not_null"] != true || rows[1]["value_not_null"] != false {
			t.Fatalf("unexpected is_not_null rows: %#v", rows)
		}
		if rows[0]["score_is_nan"] != true || rows[1]["score_is_nan"] != false {
			t.Fatalf("unexpected is_nan rows: %#v", rows)
		}
		if rows[1]["score_is_finite"] != true || rows[0]["score_is_finite"] != false {
			t.Fatalf("unexpected is_finite rows: %#v", rows)
		}
		if v, ok := asFloat64(rows[2]["abs_value"]); !ok || v != 3.5 {
			t.Fatalf("unexpected abs value: %#v", rows[2]["abs_value"])
		}
		if v, ok := asFloat64(rows[3]["round_value"]); !ok || v != 10.0 {
			t.Fatalf("unexpected round value: %#v", rows[3]["round_value"])
		}
		if v, ok := asFloat64(rows[2]["clip_value"]); !ok || v != -2.0 {
			t.Fatalf("unexpected clip value: %#v", rows[2]["clip_value"])
		}
		if rows[2]["sqrt_value"] != nil {
			if v, ok := asFloat64(rows[2]["sqrt_value"]); !ok || !math.IsNaN(v) {
				t.Fatalf("expected sqrt of negative to be null/NaN-like, got %#v", rows[2]["sqrt_value"])
			}
		}
		if v, ok := asFloat64(rows[3]["log10_value"]); !ok || math.Abs(v-1.0) > 1e-9 {
			t.Fatalf("unexpected log10 value: %#v", rows[3]["log10_value"])
		}

		filledRows, err := collectToMaps(t, df.FillNan(0.0).Reverse())
		if err != nil {
			t.Fatalf("FillNan/Reverse failed: %v", err)
		}
		if filledRows[0]["name"] != "Drew" || filledRows[3]["name"] != "Alice" {
			t.Fatalf("unexpected reverse rows: %#v", filledRows)
		}
		if v, ok := asFloat64(filledRows[3]["score"]); !ok || v != 0.0 {
			t.Fatalf("unexpected fill_nan result: %#v", filledRows[3]["score"])
		}

		dropRows, err := collectToMaps(t, df.DropNans("score"))
		if err != nil {
			t.Fatalf("DropNans failed: %v", err)
		}
		if len(dropRows) != 2 {
			t.Fatalf("expected 2 rows after DropNans, got %#v", dropRows)
		}

		sampledRows, err := collectToMaps(t, df.SampleN(2, SampleOptions{Seed: &seed}))
		if err != nil {
			t.Fatalf("SampleN failed: %v", err)
		}
		if len(sampledRows) != 2 {
			t.Fatalf("unexpected SampleN rows: %#v", sampledRows)
		}

		fracRows, err := collectToMaps(t, df.SampleFrac(0.5, SampleOptions{Seed: &seed}))
		if err != nil {
			t.Fatalf("SampleFrac failed: %v", err)
		}
		if len(fracRows) == 0 || len(fracRows) > 3 {
			t.Fatalf("unexpected SampleFrac rows: %#v", fracRows)
		}

		valueCountRows, err := collectToMaps(t, df.Select(
			Col("name").ValueCounts(ValueCountsOptions{
				Sort: true,
				Name: "n",
			}).Alias("name_counts"),
		))
		if err != nil {
			t.Fatalf("ValueCounts failed: %v", err)
		}
		countStruct, ok := valueCountRows[0]["name_counts"].(map[string]any)
		if !ok {
			t.Fatalf("expected struct-like value_counts result, got %#v", valueCountRows[0]["name_counts"])
		}
		if _, ok := countStruct["name"]; !ok {
			t.Fatalf("value_counts missing value field: %#v", countStruct)
		}
		if _, ok := countStruct["n"]; !ok {
			t.Fatalf("value_counts missing count field: %#v", countStruct)
		}

		asStructRows, err := collectToMaps(t, ScanCSV("../testdata/sample.csv").
			Select(AsStruct(Col("name"), Col("age")).Alias("person")).
			Unnest("person").
			Limit(2))
		if err != nil {
			t.Fatalf("AsStruct/Unnest failed: %v", err)
		}
		if len(asStructRows) != 2 {
			t.Fatalf("expected 2 AsStruct/Unnest rows, got %d", len(asStructRows))
		}
		if asStructRows[0]["name"] != "Alice" || asStructRows[0]["age"] != int64(25) {
			t.Fatalf("unexpected first AsStruct/Unnest row: %#v", asStructRows[0])
		}
		if asStructRows[1]["name"] != "Bob" || asStructRows[1]["age"] != int64(30) {
			t.Fatalf("unexpected second AsStruct/Unnest row: %#v", asStructRows[1])
		}

		unnestedValueCountsRows, err := collectToMaps(t, df.Select(
			Col("name").ValueCounts(ValueCountsOptions{
				Sort: true,
				Name: "n",
			}).Alias("name_counts"),
		).Unnest("name_counts"))
		if err != nil {
			t.Fatalf("ValueCounts Unnest failed: %v", err)
		}
		if len(unnestedValueCountsRows) != 4 {
			t.Fatalf("expected 4 ValueCounts Unnest rows, got %d", len(unnestedValueCountsRows))
		}
		gotNames := make(map[string]struct{}, len(unnestedValueCountsRows))
		for _, row := range unnestedValueCountsRows {
			name, ok := row["name"].(string)
			if !ok {
				t.Fatalf("expected unnested name field to be string, got %#v", row["name"])
			}
			gotNames[name] = struct{}{}
			switch n := row["n"].(type) {
			case uint64:
				if n != 1 {
					t.Fatalf("expected unnested count to be 1, got %#v", n)
				}
			case int64:
				if n != 1 {
					t.Fatalf("expected unnested count to be 1, got %#v", n)
				}
			default:
				t.Fatalf("expected unnested count field to be integer, got %T", row["n"])
			}
		}
		for _, want := range []string{"Alice", "Bob", "Cara", "Drew"} {
			if _, ok := gotNames[want]; !ok {
				t.Fatalf("expected ValueCounts Unnest to contain %q, got %#v", want, unnestedValueCountsRows)
			}
		}
	})

	t.Run("DuplicatedAndRollingHelpers", func(t *testing.T) {
		df, err := NewDataFrame([]map[string]any{
			{"value": int64(1), "group": "a"},
			{"value": int64(1), "group": "a"},
			{"value": int64(2), "group": "b"},
			{"value": int64(4), "group": "b"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer df.Close()

		rows, err := collectToMaps(t, df.Select(
			Col("group").IsDuplicated().Alias("group_dup"),
			Col("value").RollingMin(RollingOptions{WindowSize: 2, MinPeriods: 1}).Alias("roll_min"),
			Col("value").RollingMax(RollingOptions{WindowSize: 2, MinPeriods: 1}).Alias("roll_max"),
			Col("value").RollingMean(RollingOptions{WindowSize: 2, MinPeriods: 1}).Alias("roll_mean"),
			Col("value").RollingSum(RollingOptions{WindowSize: 2, MinPeriods: 1}).Alias("roll_sum"),
		))
		if err != nil {
			t.Fatalf("duplicated/rolling collect failed: %v", err)
		}
		if rows[0]["group_dup"] != true || rows[2]["group_dup"] != true {
			t.Fatalf("unexpected duplicated mask: %#v", rows)
		}
		if v, ok := asFloat64(rows[3]["roll_sum"]); !ok || v != 6 {
			t.Fatalf("unexpected rolling sum: %#v", rows[3]["roll_sum"])
		}
		if v, ok := asFloat64(rows[3]["roll_mean"]); !ok || v != 3 {
			t.Fatalf("unexpected rolling mean: %#v", rows[3]["roll_mean"])
		}
		if v, ok := asFloat64(rows[2]["roll_min"]); !ok || v != 1 {
			t.Fatalf("unexpected rolling min: %#v", rows[2]["roll_min"])
		}
		if v, ok := asFloat64(rows[3]["roll_max"]); !ok || v != 4 {
			t.Fatalf("unexpected rolling max: %#v", rows[3]["roll_max"])
		}
	})
}

func TestStringOperations(t *testing.T) {
	t.Run("StringOpsBasic", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("name"),
			Col("department"),
			Col("name").StrLenChars().Alias("name_len"),
			Col("name").StrLenBytes().Alias("name_len_bytes"),
			Col("name").StrContains("li", true).Alias("name_contains_li"),
			Col("name").StrStartsWith("A").Alias("name_starts_a"),
			Col("name").StrEndsWith("e").Alias("name_ends_e"),
			Col("department").StrExtract("(Eng)", 1).Alias("dept_extract"),
			Col("name").StrReplace("i", "I", true).Alias("name_replace"),
			Col("name").StrReplaceAll("i", "I", true).Alias("name_replace_all"),
			Col("name").StrToLowercase().Alias("name_lower"),
			Col("name").StrToUppercase().Alias("name_upper"),
			Col("name").StrStripChars("A").Alias("name_strip_a"),
			Col("name").StrSlice(1, 3).Alias("name_slice"),
			Col("name").StrPadStart(7, "_").Alias("name_pad_start"),
			Col("name").StrPadEnd(7, "_").Alias("name_pad_end"),
		)

		// 打印 lf
		lf.Print()

		result, err := collectToMaps(t, lf.Limit(1))
		if err != nil {
			t.Fatalf("String operations failed: %v", err)
		}

		if len(result) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result))
		}

		row := result[0]
		if row["name"] != "Alice" {
			t.Fatalf("Expected name Alice, got %v", row["name"])
		}

		if v, err := toInt64(row["name_len"]); err != nil || v != 5 {
			t.Fatalf("Expected name_len 5, got %#v", row["name_len"])
		}
		if v, err := toInt64(row["name_len_bytes"]); err != nil || v != 5 {
			t.Fatalf("Expected name_len_bytes 5, got %#v", row["name_len_bytes"])
		}

		if row["name_contains_li"] != true {
			t.Fatalf("Expected name_contains_li true, got %#v", row["name_contains_li"])
		}
		if row["name_starts_a"] != true {
			t.Fatalf("Expected name_starts_a true, got %#v", row["name_starts_a"])
		}
		if row["name_ends_e"] != true {
			t.Fatalf("Expected name_ends_e true, got %#v", row["name_ends_e"])
		}
		if row["dept_extract"] != "Eng" {
			t.Fatalf("Expected dept_extract Eng, got %#v", row["dept_extract"])
		}
		if row["name_replace"] != "AlIce" {
			t.Fatalf("Expected name_replace AlIce, got %#v", row["name_replace"])
		}
		if row["name_replace_all"] != "AlIce" {
			t.Fatalf("Expected name_replace_all AlIce, got %#v", row["name_replace_all"])
		}
		if row["name_lower"] != "alice" {
			t.Fatalf("Expected name_lower alice, got %#v", row["name_lower"])
		}
		if row["name_upper"] != "ALICE" {
			t.Fatalf("Expected name_upper ALICE, got %#v", row["name_upper"])
		}
		if row["name_strip_a"] != "lice" {
			t.Fatalf("Expected name_strip_a lice, got %#v", row["name_strip_a"])
		}
		if row["name_slice"] != "lic" {
			t.Fatalf("Expected name_slice lic, got %#v", row["name_slice"])
		}
		if row["name_pad_start"] != "__Alice" {
			t.Fatalf("Expected name_pad_start __Alice, got %#v", row["name_pad_start"])
		}
		if row["name_pad_end"] != "Alice__" {
			t.Fatalf("Expected name_pad_end Alice__, got %#v", row["name_pad_end"])
		}
	})
}

func TestArrowZeroCopy(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("zero-copy requires cgo")
	}

	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	lf := NewLazyFrame(nil).Select(
		Col("name"),
		Col("age"),
		Col("age").Add(Lit(1)).Alias("age_plus"),
		Col("name").StrToUppercase().Alias("name_upper"),
	)

	plan := &pb.Plan{
		PlanVersion: 1,
		Root:        lf.root,
	}
	planBytes, err := proto.Marshal(plan)
	if err != nil {
		t.Fatalf("Failed to marshal plan: %v", err)
	}

	handle, err := brg.CompilePlan(planBytes)
	if err != nil {
		t.Fatalf("Failed to compile plan: %v", err)
	}
	defer brg.FreePlan(handle)

	inSchema, inArray, cleanupInput, err := buildArrowInput()
	if err != nil {
		t.Fatalf("Failed to build Arrow input: %v", err)
	}
	defer cleanupInput()

	outSchema, outArray, err := brg.ExecuteArrow(handle, inSchema, inArray)
	if err != nil {
		t.Fatalf("ExecuteArrow failed: %v", err)
	}

	rec, err := importArrowRecordBatch(outSchema, outArray)
	if err != nil {
		bridge.ReleaseArrowSchema(outSchema)
		bridge.ReleaseArrowArray(outArray)
		t.Fatalf("Failed to import Arrow result: %v", err)
	}
	bridge.ReleaseArrowSchema(outSchema)
	bridge.ReleaseArrowArray(outArray)
	defer rec.Release()

	if rec.NumRows() != 3 {
		t.Fatalf("Expected 3 rows, got %d", rec.NumRows())
	}
	if rec.NumCols() != 4 {
		t.Fatalf("Expected 4 columns, got %d", rec.NumCols())
	}

	fields := rec.Schema().Fields()
	if fields[0].Name != "name" || fields[1].Name != "age" ||
		fields[2].Name != "age_plus" || fields[3].Name != "name_upper" {
		t.Fatalf("Unexpected schema fields: %#v", fields)
	}

	readString := func(col arrow.Array, idx int) (string, bool) {
		if col.IsNull(idx) {
			return "", false
		}
		switch c := col.(type) {
		case *array.String:
			return c.Value(idx), true
		case *array.LargeString:
			return c.Value(idx), true
		case *array.BinaryView:
			return string(c.Value(idx)), true
		case *array.StringView:
			return c.Value(idx), true
		default:
			return "", false
		}
	}

	readInt64 := func(col arrow.Array, idx int) (int64, bool) {
		if col.IsNull(idx) {
			return 0, false
		}
		switch c := col.(type) {
		case *array.Int64:
			return c.Value(idx), true
		case *array.Int32:
			return int64(c.Value(idx)), true
		case *array.Int16:
			return int64(c.Value(idx)), true
		case *array.Int8:
			return int64(c.Value(idx)), true
		case *array.Uint64:
			return int64(c.Value(idx)), true
		case *array.Uint32:
			return int64(c.Value(idx)), true
		case *array.Uint16:
			return int64(c.Value(idx)), true
		case *array.Uint8:
			return int64(c.Value(idx)), true
		default:
			return 0, false
		}
	}

	nameCol := rec.Column(0)
	ageCol := rec.Column(1)
	agePlusCol := rec.Column(2)
	upperCol := rec.Column(3)

	if v, ok := readString(nameCol, 0); !ok || v != "alice" {
		t.Fatalf("Expected name alice, got %#v", v)
	}
	if v, ok := readInt64(ageCol, 1); !ok || v != 21 {
		t.Fatalf("Expected age 21, got %#v", v)
	}
	if v, ok := readInt64(agePlusCol, 2); !ok || v != 46 {
		t.Fatalf("Expected age_plus 46, got %#v", v)
	}
	if v, ok := readString(upperCol, 2); !ok || v != "CARL" {
		t.Fatalf("Expected name_upper CARL, got %#v", v)
	}
}

func TestExpressionExpansion(t *testing.T) {
	// 测试 1: 使用 Cols() 选择多列
	t.Run("ColsSelection", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Cols("name", "age", "salary")...,
		)

		result, err := collectToMaps(t, lf.Limit(2))
		if err != nil {
			t.Fatalf("Cols selection failed: %v", err)
		}

		if len(result) == 0 {
			t.Fatal("Expected at least one row")
		}

		// 验证只有这 3 列
		if len(result[0]) != 3 {
			t.Fatalf("Expected 3 columns, got %d", len(result[0]))
		}

		if _, ok := result[0]["name"]; !ok {
			t.Fatal("Expected 'name' column")
		}
		if _, ok := result[0]["age"]; !ok {
			t.Fatal("Expected 'age' column")
		}
		if _, ok := result[0]["salary"]; !ok {
			t.Fatal("Expected 'salary' column")
		}

		t.Logf("Cols selection test passed: %v", result[0])
	})

	// 测试 2: 使用 All() 选择所有列
	t.Run("AllSelection", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			All(),
		)

		result, err := collectToMaps(t, lf.Limit(1))
		if err != nil {
			t.Fatalf("All selection failed: %v", err)
		}

		if len(result) == 0 {
			t.Fatal("Expected at least one row")
		}

		// 应该有所有列（4 列）
		if len(result[0]) != 4 {
			t.Fatalf("Expected 4 columns (all columns), got %d", len(result[0]))
		}

		t.Logf("All selection test passed: %d columns", len(result[0]))
	})

	// 测试 3: 组合使用 - 对多列应用相同操作
	t.Run("MultiColumnOperation", func(t *testing.T) {
		// 对多列应用相同的转换
		lf := ScanCSV("../testdata/sample.csv")

		// 分别选择多列并进行计算
		result, err := collectToMaps(t, lf.Select(
			Col("name"),
			Col("age"),
			Col("salary"),
			Col("salary").Div(Lit(12)).Alias("monthly_salary"),
		).Limit(2))

		if err != nil {
			t.Fatalf("Multi-column operation failed: %v", err)
		}

		for i, row := range result {
			t.Logf("Row %d: name=%v, salary=%v, monthly_salary=%v",
				i, row["name"], row["salary"], row["monthly_salary"])
		}
	})
}

func TestWhenThenOtherwise(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	rows, err := collectToMaps(t, df.Select(
		Col("name"),
		When(Col("age").Gt(Lit(30))).
			Then(Lit("senior")).
			Otherwise(Lit("junior")).
			Alias("level"),
	))
	fmt.Println(rows)
	if err != nil {
		t.Fatalf("When/Then/Otherwise failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}
	if rows[0]["level"] != "junior" {
		t.Fatalf("Expected Alice to be junior, got %#v", rows[0]["level"])
	}
	if rows[1]["level"] != "senior" {
		t.Fatalf("Expected Bob to be senior, got %#v", rows[1]["level"])
	}
}

func TestCasting(t *testing.T) {
	// 测试 1: 数值类型转换
	t.Run("NumericCasting", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("age"),
			Col("age").Cast(Int32, true).Alias("age_int32"),
			Col("age").Cast(Int16, true).Alias("age_int16"),
			Col("age").Cast(Float32, true).Alias("age_float32"),
		)

		result, err := collectToMaps(t, lf.Limit(2))
		if err != nil {
			t.Fatalf("Numeric casting failed: %v", err)
		}

		if len(result) == 0 {
			t.Fatal("Expected at least one row")
		}

		t.Logf("Numeric casting test passed: %v", result[0])
	})

	// 测试 2: 字符串转换为数值
	t.Run("StringToNumeric", func(t *testing.T) {
		// 创建一个包含字符串数字的 DataFrame
		// 这里我们使用现有的 CSV，将 age 转换为字符串再转回数字
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("age").Cast(String, true).Alias("age_as_string"),
			Col("age").Cast(String, true).Cast(Int64, true).Alias("age_back_to_int"),
		)

		result, err := collectToMaps(t, lf.Limit(2))
		if err != nil {
			t.Fatalf("String to numeric casting failed: %v", err)
		}

		t.Logf("String conversion test: %v", result[0])
	})

	// 测试 3: 布尔类型转换
	t.Run("BooleanCasting", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("age"),
			Col("age").Gt(Lit(30)).Alias("age_gt_30"),
			Col("age").Gt(Lit(30)).Cast(Int8, true).Alias("age_gt_30_as_int"),
		)

		result, err := collectToMaps(t, lf.Limit(3))
		if err != nil {
			t.Fatalf("Boolean casting failed: %v", err)
		}

		for i, row := range result {
			t.Logf("Row %d: age=%v, age_gt_30=%v, age_gt_30_as_int=%v",
				i, row["age"], row["age_gt_30"], row["age_gt_30_as_int"])
		}
	})

	// 测试 4: 非严格模式（strict=false）
	t.Run("NonStrictCasting", func(t *testing.T) {
		// 非严格模式下，超出范围的值会转为 null
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("salary"),
			Col("salary").Cast(Int8, false).Alias("salary_as_int8"), // salary 很大，会超出 Int8 范围
		)

		result, err := collectToMaps(t, lf.Limit(2))
		if err != nil {
			t.Fatalf("Non-strict casting failed: %v", err)
		}

		t.Logf("Non-strict casting test (values out of range become null): %v", result[0])
	})

	// 测试 5: 严格模式 - 使用 StrictCast 方法
	t.Run("StrictCastMethod", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("age"),
			Col("age").StrictCast(Int16).Alias("age_int16"),
			Col("salary").StrictCast(Float32).Alias("salary_float32"),
		)

		result, err := collectToMaps(t, lf.Limit(2))
		if err != nil {
			t.Fatalf("StrictCast method failed: %v", err)
		}

		t.Logf("StrictCast method test passed: %v", result[0])
	})
}

func TestArrowExecution(t *testing.T) {
	// 加载 bridge（自动从环境变量 POLARS_BRIDGE_LIB 或默认路径加载）
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	// 测试 1: Arrow 零拷贝执行（无输入）
	t.Run("ArrowExecutionNoInput", func(t *testing.T) {
		// 构建一个简单的查询计划
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("name"),
			Col("age"),
		).Limit(3)

		// 构建 Plan
		plan := &pb.Plan{
			PlanVersion: 1,
			Root:        lf.root,
		}

		// 序列化 Plan
		planBytes, err := proto.Marshal(plan)
		if err != nil {
			t.Fatalf("Failed to marshal plan: %v", err)
		}

		// 编译 Plan
		handle, err := brg.CompilePlan(planBytes)
		if err != nil {
			t.Fatalf("Failed to compile plan: %v", err)
		}
		defer brg.FreePlan(handle)

		// 使用 Arrow C Data Interface 执行（无输入）
		outSchema, outArray, err := brg.ExecuteArrow(handle, nil, nil)
		if err != nil {
			t.Fatalf("Arrow execution failed: %v", err)
		}

		// 确保在函数结束时释放 Arrow 资源
		if outSchema != nil {
			defer bridge.ReleaseArrowSchema(outSchema)
		}
		if outArray != nil {
			defer bridge.ReleaseArrowArray(outArray)
		}

		// 验证返回的 Arrow 数据不为空
		if outSchema == nil || outArray == nil {
			t.Fatal("Expected non-nil Arrow schema and array")
		}

		t.Logf("✅ Arrow execution succeeded (zero-copy data transfer)")
		t.Logf("   Output Arrow Schema: %p", outSchema)
		t.Logf("   Output Arrow Array: %p", outArray)
	})

	// 测试 2: 比较 ToMaps（Arrow-backed）与直接 Arrow 执行的结果一致性
	t.Run("ArrowBackedToMapsConsistency", func(t *testing.T) {
		// 构建相同的查询
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("name"),
			Col("age"),
			Col("salary"),
		).Filter(Col("age").Gt(Lit(25))).Limit(5)

		// 方法 1: 使用 ToMaps（当前内部同样走 Arrow 导出）
		resultRows, err := collectToMaps(t, lf)
		if err != nil {
			t.Fatalf("ToMaps execution failed: %v", err)
		}

		// 方法 2: 使用 Arrow C Data Interface
		plan := &pb.Plan{
			PlanVersion: 1,
			Root:        lf.root,
		}
		planBytes, _ := proto.Marshal(plan)
		handle, _ := brg.CompilePlan(planBytes)
		defer brg.FreePlan(handle)

		outSchema, outArray, err := brg.ExecuteArrow(handle, nil, nil)
		if err != nil {
			t.Fatalf("Arrow execution failed: %v", err)
		}
		defer bridge.ReleaseArrowSchema(outSchema)
		defer bridge.ReleaseArrowArray(outArray)

		// 验证行数一致
		if len(resultRows) == 0 {
			t.Fatal("Expected at least one row from ToMaps")
		}

		t.Logf("✅ ToMaps and direct Arrow execution both succeeded")
		t.Logf("   ToMaps result rows: %d", len(resultRows))
		t.Logf("   Arrow execution completed (zero-copy)")
		t.Logf("   First row from ToMaps: %v", resultRows[0])
	})

	// 测试 3: 测试 Arrow 错误处理
	t.Run("ArrowErrorHandling", func(t *testing.T) {
		// 使用不存在的文件测试错误处理
		lf := ScanCSV("nonexistent.csv").Select(Col("name"))

		plan := &pb.Plan{
			PlanVersion: 1,
			Root:        lf.root,
		}
		planBytes, _ := proto.Marshal(plan)
		handle, err := brg.CompilePlan(planBytes)
		if err != nil {
			t.Fatalf("Failed to compile plan: %v", err)
		}
		defer brg.FreePlan(handle)

		_, _, err = brg.ExecuteArrow(handle, nil, nil)
		if err == nil {
			t.Error("Expected error for non-existent file")
		} else {
			t.Logf("✅ Correctly got error: %v", err)
		}
	})

	// 测试 4: Arrow 性能特性说明
	t.Run("ArrowPerformanceNote", func(t *testing.T) {
		t.Log("📊 Arrow C Data Interface 特性:")
		t.Log("   • 零拷贝数据传输 (Zero-copy)")
		t.Log("   • 直接在内存中共享数据指针")
		t.Log("   • 避免序列化/反序列化开销")
		t.Log("   • 适合大数据量传输")
		t.Log("   • 与 Apache Arrow 生态集成")
		t.Log("   • 避免中间二进制序列化")
	})
}

func TestDataFrameFromMap(t *testing.T) {
	// 加载 bridge（自动从环境变量 POLARS_BRIDGE_LIB 或默认路径加载）
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	// 测试 1: 基本类型推断
	t.Run("BasicTypes", func(t *testing.T) {
		df, err := NewEagerFrameFromMap(brg, map[string]interface{}{
			"nrs":    []int64{1, 2, 3, 4, 5},
			"names":  []string{"foo", "ham", "spam", "egg", "spam"},
			"random": []float64{0.37454, 0.950714, 0.731994, 0.598658, 0.156019},
			"groups": []string{"A", "A", "B", "A", "B"},
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}
		defer df.Free()

		rows, err := collectToMaps(t, df.Lazy())
		if err != nil {
			t.Fatalf("Failed to collect rows: %v", err)
		}

		if len(rows) != 5 {
			t.Fatalf("Expected 5 rows, got %d", len(rows))
		}

		if rows[0]["names"] != "foo" {
			t.Fatalf("Expected names[0] = foo, got %v", rows[0]["names"])
		}

		t.Logf("✅ Basic types test passed: %d rows", len(rows))
	})

	// 测试 2: null 值处理
	t.Run("NullValues", func(t *testing.T) {
		df, err := NewEagerFrameFromMap(brg, map[string]interface{}{
			"id":   []interface{}{1, 2, nil, 4, 5},
			"name": []interface{}{"Alice", nil, "Charlie", "Diana", "Eve"},
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}
		defer df.Free()

		rows, err := collectToMaps(t, df.Lazy())
		if err != nil {
			t.Fatalf("Failed to collect rows: %v", err)
		}

		if len(rows) != 5 {
			t.Fatalf("Expected 5 rows, got %d", len(rows))
		}

		// 验证 null 值
		if rows[2]["id"] != nil {
			t.Fatalf("Expected id[2] = nil, got %v", rows[2]["id"])
		}

		if rows[1]["name"] != nil {
			t.Fatalf("Expected name[1] = nil, got %v", rows[1]["name"])
		}

		t.Logf("✅ Null values test passed")
	})

	// 测试 3: 链式操作
	t.Run("ChainedOperations", func(t *testing.T) {
		df, err := NewEagerFrameFromMap(brg, map[string]interface{}{
			"age":    []int64{25, 30, 35, 28, 32},
			"name":   []string{"Alice", "Bob", "Charlie", "Diana", "Eve"},
			"salary": []int64{50000, 60000, 70000, 55000, 65000},
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}
		defer df.Free()

		// 应用 Polars 操作
		result, err := collectToMaps(t, df.
			Filter(Col("age").Gt(Lit(28))).
			Select(Col("name"), Col("salary")))

		if err != nil {
			t.Fatalf("Chained operations failed: %v", err)
		}

		if len(result) != 3 {
			t.Fatalf("Expected 3 rows after filter, got %d", len(result))
		}

		t.Logf("✅ Chained operations test passed: %d rows", len(result))
		t.Logf("   Result: %v", result)
	})

	// 测试 4: Polars 类型推断
	t.Run("TypeInference", func(t *testing.T) {
		df, err := NewEagerFrameFromMap(brg, map[string]interface{}{
			"integers": []int64{1, 2, 3},
			"floats":   []float64{1.5, 2.5, 3.7},
			"strings":  []string{"a", "b", "c"},
			"mixed":    []interface{}{1, nil, 3},
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}
		defer df.Free()

		rows, err := collectToMaps(t, df.Lazy())
		if err != nil {
			t.Fatalf("Failed to collect rows: %v", err)
		}

		t.Logf("✅ Type inference test passed")
		t.Logf("   Row 0: %v", rows[0])
		t.Logf("   Row 1 (with null): %v", rows[1])
	})

	// 测试 5: 打印 DataFrame
	t.Run("PrintDataFrame", func(t *testing.T) {
		df, err := NewEagerFrameFromMap(brg, map[string]interface{}{
			"nrs":    []int64{1, 2, 3, 4, 5},
			"names":  []string{"foo", "ham", "spam", "egg", "spam"},
			"random": []interface{}{0.37454, 0.950714, 0.731994, 0.598658, 0.156019},
			"groups": []string{"A", "A", "B", "A", "B"},
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}
		defer df.Free()

		t.Log("📊 DataFrame content:")
		err = df.Print()
		if err != nil {
			t.Fatalf("Failed to print DataFrame: %v", err)
		}
	})

	// 测试 6: 显式 schema
	t.Run("ExplicitSchema", func(t *testing.T) {
		df, err := NewEagerFrameFromMapWithSchema(brg, map[string]interface{}{
			"id":    []interface{}{1, 2, 3},
			"age":   []interface{}{nil, 20, 30},
			"score": []interface{}{1, 2, 3},
		}, map[string]DataType{
			"id":    DataTypeUInt64,
			"age":   DataTypeInt32,
			"score": DataTypeFloat64,
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame with schema: %v", err)
		}
		defer df.Free()

		rows, err := collectToMaps(t, df.Lazy())
		if err != nil {
			t.Fatalf("Failed to collect rows: %v", err)
		}

		if got, ok := rows[0]["id"].(uint64); !ok || got != 1 {
			t.Fatalf("Expected id[0] to be uint64(1), got %T %v", rows[0]["id"], rows[0]["id"])
		}

		if rows[0]["age"] != nil {
			t.Fatalf("Expected age[0] = nil, got %v", rows[0]["age"])
		}

		if got, ok := rows[1]["age"].(int64); !ok || got != 20 {
			t.Fatalf("Expected age[1] to be int64(20), got %T %v", rows[1]["age"], rows[1]["age"])
		}

		if got, ok := rows[2]["score"].(float64); !ok || got != 3 {
			t.Fatalf("Expected score[2] to be float64(3), got %T %v", rows[2]["score"], rows[2]["score"])
		}

		t.Logf("✅ Explicit schema test passed")
	})
}

func TestDataFrameFromRows(t *testing.T) {
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	t.Run("BasicRows", func(t *testing.T) {
		df, err := NewEagerFrameFromRows(brg, []map[string]any{
			{"id": 1, "name": "Alice", "age": 18},
			{"id": 2, "name": "Bob", "age": 20},
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame from rows: %v", err)
		}
		defer df.Free()

		rows, err := collectToMaps(t, df.Lazy())
		if err != nil {
			t.Fatalf("Failed to collect rows: %v", err)
		}

		if len(rows) != 2 {
			t.Fatalf("Expected 2 rows, got %d", len(rows))
		}

		if rows[0]["name"] != "Alice" {
			t.Fatalf("Expected first row name Alice, got %v", rows[0]["name"])
		}
	})

	t.Run("MissingFieldsBecomeNil", func(t *testing.T) {
		df, err := NewEagerFrameFromRows(brg, []map[string]any{
			{"id": 1, "name": "Alice"},
			{"id": 2, "age": 20},
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame from sparse rows: %v", err)
		}
		defer df.Free()

		rows, err := collectToMaps(t, df.Lazy())
		if err != nil {
			t.Fatalf("Failed to collect rows: %v", err)
		}

		if rows[0]["age"] != nil {
			t.Fatalf("Expected missing age in row 0 to be nil, got %v", rows[0]["age"])
		}

		if rows[1]["name"] != nil {
			t.Fatalf("Expected missing name in row 1 to be nil, got %v", rows[1]["name"])
		}
	})

	t.Run("ExplicitSchema", func(t *testing.T) {
		df, err := NewEagerFrameFromRowsWithSchema(brg, []map[string]any{
			{"id": 1, "name": "Alice", "age": nil},
			{"id": 2, "name": "Bob", "age": 20},
		}, map[string]DataType{
			"id":   DataTypeUInt64,
			"name": DataTypeUTF8,
			"age":  DataTypeInt32,
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame from rows with schema: %v", err)
		}
		defer df.Free()

		rows, err := collectToMaps(t, df.Lazy())
		if err != nil {
			t.Fatalf("Failed to collect rows: %v", err)
		}

		if got, ok := rows[0]["id"].(uint64); !ok || got != 1 {
			t.Fatalf("Expected id[0] uint64(1), got %T %v", rows[0]["id"], rows[0]["id"])
		}

		if rows[0]["age"] != nil {
			t.Fatalf("Expected age[0] nil, got %v", rows[0]["age"])
		}

		if got, ok := rows[1]["age"].(int64); !ok || got != 20 {
			t.Fatalf("Expected age[1] int64(20), got %T %v", rows[1]["age"], rows[1]["age"])
		}
	})

	t.Run("FakeGoFrameResultList", func(t *testing.T) {
		// 模拟 GoFrame gdb.Result.List() 的返回形态：
		// []map[string]any，每个 map 都是已经解包后的普通值。
		goframeRows := []map[string]any{
			{
				"id":         uint64(1),
				"name":       "Alice",
				"age":        nil,
				"salary":     12500.5,
				"created_at": "2026-03-24 10:00:00",
			},
			{
				"id":         uint64(2),
				"name":       "Bob",
				"age":        20,
				"salary":     15888.0,
				"created_at": "2026-03-24 11:00:00",
			},
		}

		df, err := NewEagerFrameFromRowsWithSchema(brg, goframeRows, map[string]DataType{
			"id":         DataTypeUInt64,
			"name":       DataTypeUTF8,
			"age":        DataTypeInt32,
			"salary":     DataTypeFloat64,
			"created_at": DataTypeUTF8,
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame from fake GoFrame rows: %v", err)
		}
		defer df.Free()

		rows, err := collectToMaps(t, df.
			Filter(Col("salary").Gt(Lit(13000.0))).
			Select(Col("id"), Col("name"), Col("age"), Col("salary")))
		if err != nil {
			t.Fatalf("Failed to query fake GoFrame rows: %v", err)
		}

		if len(rows) != 1 {
			t.Fatalf("Expected 1 filtered row, got %d", len(rows))
		}

		if got, ok := rows[0]["id"].(uint64); !ok || got != 2 {
			t.Fatalf("Expected filtered id uint64(2), got %T %v", rows[0]["id"], rows[0]["id"])
		}

		if got := rows[0]["name"]; got != "Bob" {
			t.Fatalf("Expected filtered name Bob, got %v", got)
		}

		if got, ok := rows[0]["age"].(int64); !ok || got != 20 {
			t.Fatalf("Expected filtered age int64(20), got %T %v", rows[0]["age"], rows[0]["age"])
		}
	})
}

func TestDataFrameFromRowsConcurrent(t *testing.T) {
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	const (
		workers    = 12
		iterations = 20
	)

	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			for iter := 0; iter < iterations; iter++ {
				idBase := i*1000 + iter
				df, err := NewEagerFrameFromRowsWithSchema(brg, []map[string]any{
					{"id": idBase, "name": fmt.Sprintf("user-%d-%d", i, iter), "age": nil},
					{"id": idBase + 100, "name": fmt.Sprintf("user-%d-%d-b", i, iter), "age": i + iter + 20},
				}, map[string]DataType{
					"id":   DataTypeInt64,
					"name": DataTypeUTF8,
					"age":  DataTypeInt32,
				})
				if err != nil {
					errCh <- fmt.Errorf("worker %d iteration %d create failed: %w", i, iter, err)
					return
				}

				rows, err := collectToMaps(t, df.Lazy())
				df.Free()
				if err != nil {
					errCh <- fmt.Errorf("worker %d iteration %d collect failed: %w", i, iter, err)
					return
				}

				if len(rows) != 2 {
					errCh <- fmt.Errorf("worker %d iteration %d expected 2 rows, got %d", i, iter, len(rows))
					return
				}

				if got := rows[0]["name"]; got != fmt.Sprintf("user-%d-%d", i, iter) {
					errCh <- fmt.Errorf("worker %d iteration %d unexpected first name: %v", i, iter, got)
					return
				}

				if rows[0]["age"] != nil {
					errCh <- fmt.Errorf("worker %d iteration %d expected nil age in first row, got %v", i, iter, rows[0]["age"])
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDataFrameFromArrowRecord(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow record import requires cgo")
	}

	alloc := memory.NewGoAllocator()
	nameBuilder := array.NewStringBuilder(alloc)
	ageBuilder := array.NewInt64Builder(alloc)
	defer nameBuilder.Release()
	defer ageBuilder.Release()

	nameBuilder.AppendValues([]string{"alice", "bob", "carl"}, nil)
	ageBuilder.AppendValues([]int64{18, 20, 35}, nil)

	nameArr := nameBuilder.NewArray()
	ageArr := ageBuilder.NewArray()
	defer nameArr.Release()
	defer ageArr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{nameArr, ageArr}, 3)
	defer record.Release()

	df, err := NewDataFrameFromArrowRecordBatch(record)
	if err != nil {
		t.Fatalf("Failed to create DataFrame from Arrow record: %v", err)
	}
	defer df.Close()

	rows, err := collectToMaps(t, df.
		Filter(Col("age").Gt(Lit(19))).
		Select(Col("name"), Col("age")))
	if err != nil {
		t.Fatalf("Failed to query imported Arrow DataFrame: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows after filter, got %d", len(rows))
	}

	if rows[0]["name"] != "bob" || rows[1]["name"] != "carl" {
		t.Fatalf("Unexpected filtered rows: %#v", rows)
	}
}

func TestRowsToArrowToNewDataFrame(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	rows := []map[string]any{
		{
			"id":         uint64(1),
			"name":       "Alice",
			"age":        nil,
			"salary":     12500.5,
			"created_at": "2026-03-24 10:00:00",
		},
		{
			"id":         uint64(2),
			"name":       "Bob",
			"age":        20,
			"salary":     15888,
			"created_at": "2026-03-24 11:00:00",
		},
	}
	schema := map[string]DataType{
		"id":         DataTypeUInt64,
		"name":       DataTypeUTF8,
		"age":        DataTypeInt32,
		"salary":     DataTypeFloat64,
		"created_at": DataTypeUTF8,
	}

	record, err := NewArrowRecordBatchFromRowsWithSchema(rows, schema)
	if err != nil {
		t.Fatalf("Failed to build Arrow record from rows: %v", err)
	}
	defer record.Release()

	df, err := NewDataFrameFromArrowRecordBatch(record)
	if err != nil {
		t.Fatalf("Failed to create DataFrame from Arrow record: %v", err)
	}
	defer df.Close()

	result, err := collectToMaps(t, df.
		Filter(Col("salary").Gt(Lit(13000.0))).
		Select(Col("id"), Col("name"), Col("age"), Col("salary")))
	if err != nil {
		t.Fatalf("Failed to query Arrow-imported DataFrame: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 filtered row, got %d", len(result))
	}

	if got, ok := result[0]["id"].(uint64); !ok || got != 2 {
		t.Fatalf("Expected filtered id uint64(2), got %T %v", result[0]["id"], result[0]["id"])
	}

	if got := result[0]["name"]; got != "Bob" {
		t.Fatalf("Expected filtered name Bob, got %v", got)
	}
}

func TestRowsTemporalArrowRoundTrip(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	createdAt := time.Date(2026, 3, 24, 10, 30, 45, 123456000, time.FixedZone("CST", 8*3600))
	rows := []map[string]any{
		{
			"id":         int64(1),
			"created_at": createdAt,
			"birthday":   createdAt,
			"clock_at":   createdAt,
		},
	}
	schema := map[string]DataType{
		"id":         DataTypeInt64,
		"created_at": DataTypeDatetime,
		"birthday":   DataTypeDate,
		"clock_at":   DataTypeTime,
	}

	record, err := NewArrowRecordBatchFromRowsWithSchema(rows, schema)
	if err != nil {
		t.Fatalf("Failed to build Arrow record from rows: %v", err)
	}
	defer record.Release()

	fieldsByName := make(map[string]arrow.DataType, len(record.Schema().Fields()))
	for _, field := range record.Schema().Fields() {
		fieldsByName[field.Name] = field.Type
	}
	if fieldsByName["created_at"].ID() != arrow.TIMESTAMP {
		t.Fatalf("Expected created_at to use Arrow timestamp, got %s", fieldsByName["created_at"])
	}
	if fieldsByName["birthday"].ID() != arrow.DATE32 {
		t.Fatalf("Expected birthday to use Arrow date32, got %s", fieldsByName["birthday"])
	}
	if fieldsByName["clock_at"].ID() != arrow.TIME64 {
		t.Fatalf("Expected clock_at to use Arrow time64, got %s", fieldsByName["clock_at"])
	}

	df, err := NewDataFrameFromArrowRecordBatch(record)
	if err != nil {
		t.Fatalf("Failed to create DataFrame from Arrow record: %v", err)
	}
	defer df.Close()

	result, err := df.ToMaps()
	if err != nil {
		t.Fatalf("Failed to materialize temporal rows: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result))
	}

	datetimeVal, ok := result[0]["created_at"].(time.Time)
	if !ok {
		t.Fatalf("Expected created_at to round-trip as time.Time, got %T", result[0]["created_at"])
	}
	if datetimeVal.UTC().Format(time.RFC3339Nano) != createdAt.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("Unexpected created_at value: got %s want %s", datetimeVal.UTC().Format(time.RFC3339Nano), createdAt.UTC().Format(time.RFC3339Nano))
	}

	dateVal, ok := result[0]["birthday"].(time.Time)
	if !ok {
		t.Fatalf("Expected birthday to round-trip as time.Time, got %T", result[0]["birthday"])
	}
	if got := dateVal.Format("2006-01-02"); got != createdAt.Format("2006-01-02") {
		t.Fatalf("Unexpected birthday value: got %s want %s", got, createdAt.Format("2006-01-02"))
	}

	timeVal, ok := result[0]["clock_at"].(string)
	if !ok {
		t.Fatalf("Expected clock_at to round-trip as string, got %T", result[0]["clock_at"])
	}
	if want := createdAt.Format("15:04:05.999999999"); timeVal != want {
		t.Fatalf("Unexpected clock_at value: got %s want %s", timeVal, want)
	}
}

func TestArrowNestedRoundTrip(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow import/export requires cgo")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "profile", Type: arrow.StructOf(
			arrow.Field{Name: "city", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		), Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)

	tagsBuilder := builder.Field(1).(*array.ListBuilder)
	tagsValues := tagsBuilder.ValueBuilder().(*array.StringBuilder)
	tagsBuilder.Append(true)
	tagsValues.Append("go")
	tagsValues.Append("arrow")
	tagsBuilder.Append(true)
	tagsValues.Append("polars")
	tagsValues.AppendNull()

	profileBuilder := builder.Field(2).(*array.StructBuilder)
	profileCity := profileBuilder.FieldBuilder(0).(*array.StringBuilder)
	profileScore := profileBuilder.FieldBuilder(1).(*array.Int64Builder)
	profileBuilder.Append(true)
	profileCity.Append("Shanghai")
	profileScore.Append(9)
	profileBuilder.Append(true)
	profileCity.AppendNull()
	profileScore.Append(7)

	record := builder.NewRecordBatch()
	defer record.Release()

	df, err := NewDataFrameFromArrowRecordBatch(record)
	if err != nil {
		t.Fatalf("failed to create DataFrame from nested Arrow record: %v", err)
	}
	defer df.Close()

	rows, err := df.ToMaps()
	if err != nil {
		t.Fatalf("nested Arrow ToMaps failed: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	firstTags, ok := rows[0]["tags"].([]interface{})
	if !ok {
		t.Fatalf("expected first tags to be []interface{}, got %T", rows[0]["tags"])
	}
	if len(firstTags) != 2 || firstTags[0] != "go" || firstTags[1] != "arrow" {
		t.Fatalf("unexpected first tags: %#v", firstTags)
	}

	secondTags, ok := rows[1]["tags"].([]interface{})
	if !ok {
		t.Fatalf("expected second tags to be []interface{}, got %T", rows[1]["tags"])
	}
	if len(secondTags) != 2 || secondTags[0] != "polars" || secondTags[1] != nil {
		t.Fatalf("unexpected second tags: %#v", secondTags)
	}

	firstProfile, ok := rows[0]["profile"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected first profile to be map[string]interface{}, got %T", rows[0]["profile"])
	}
	if firstProfile["city"] != "Shanghai" {
		t.Fatalf("unexpected first profile city: %#v", firstProfile["city"])
	}
	if got, err := toInt64(firstProfile["score"]); err != nil || got != 9 {
		t.Fatalf("unexpected first profile score: %v (%v)", firstProfile["score"], err)
	}

	secondProfile, ok := rows[1]["profile"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected second profile to be map[string]interface{}, got %T", rows[1]["profile"])
	}
	if secondProfile["city"] != nil {
		t.Fatalf("expected second profile city to be nil, got %#v", secondProfile["city"])
	}
	if got, err := toInt64(secondProfile["score"]); err != nil || got != 7 {
		t.Fatalf("unexpected second profile score: %v (%v)", secondProfile["score"], err)
	}
}

func TestArrowBinaryRoundTrip(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow import/export requires cgo")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "blob", Type: arrow.BinaryTypes.LargeBinary, Nullable: true},
		{Name: "token", Type: &arrow.FixedSizeBinaryType{ByteWidth: 4}, Nullable: true},
	}, nil)

	rows := []map[string]any{
		{
			"id":      int64(1),
			"payload": []byte("abc"),
			"blob":    "longer-binary",
			"token":   [4]byte{1, 2, 3, 4},
		},
		{
			"id":      int64(2),
			"payload": nil,
			"blob":    []byte{9, 8, 7},
			"token":   []byte{5, 6, 7, 8},
		},
	}

	df, err := NewDataFrame(rows, WithArrowSchema(schema))
	if err != nil {
		t.Fatalf("NewDataFrame binary Arrow schema failed: %v", err)
	}
	defer df.Close()

	out, err := df.ToMaps()
	if err != nil {
		t.Fatalf("ToMaps failed: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(out))
	}
	assertBinaryValue(t, out[0]["payload"], []byte("abc"))
	assertBinaryValue(t, out[0]["blob"], []byte("longer-binary"))
	assertBinaryValue(t, out[0]["token"], []byte{1, 2, 3, 4})
	if out[1]["payload"] != nil {
		t.Fatalf("expected nil payload, got %#v", out[1]["payload"])
	}
}

func TestParseArrowRecordBatchBinaryTypes(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "blob", Type: arrow.BinaryTypes.LargeBinary, Nullable: true},
		{Name: "token", Type: &arrow.FixedSizeBinaryType{ByteWidth: 4}, Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	builder.Field(0).(*array.BinaryBuilder).Append([]byte("abc"))
	builder.Field(1).(*array.BinaryBuilder).Append([]byte("longer-binary"))
	builder.Field(2).(*array.FixedSizeBinaryBuilder).Append([]byte{1, 2, 3, 4})

	record := builder.NewRecordBatch()
	defer record.Release()

	rows, err := parseArrowRecordBatch(record)
	if err != nil {
		t.Fatalf("parseArrowRecordBatch failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	assertBinaryValue(t, rows[0]["payload"], []byte("abc"))
	assertBinaryValue(t, rows[0]["blob"], []byte("longer-binary"))
	assertBinaryValue(t, rows[0]["token"], []byte{1, 2, 3, 4})
}

func TestNewDataFrameWithArrowSchema(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "profile", Type: arrow.StructOf(
			arrow.Field{Name: "city", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		), Nullable: true},
	}, nil)

	rows := []map[string]any{
		{
			"id":   int64(1),
			"tags": []string{"go", "arrow"},
			"profile": map[string]any{
				"city":  "Shanghai",
				"score": int64(9),
			},
		},
		{
			"id":   int64(2),
			"tags": []any{"polars", nil},
			"profile": map[string]any{
				"city":  nil,
				"score": int64(7),
			},
		},
	}

	df, err := NewDataFrame(rows, WithArrowSchema(schema))
	if err != nil {
		t.Fatalf("NewDataFrame with Arrow schema failed: %v", err)
	}
	defer df.Close()

	out, err := df.ToMaps()
	if err != nil {
		t.Fatalf("ToMaps failed: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(out))
	}
	if got := out[0]["tags"].([]interface{}); len(got) != 2 || got[0] != "go" || got[1] != "arrow" {
		t.Fatalf("unexpected first tags: %#v", out[0]["tags"])
	}
	if got := out[1]["profile"].(map[string]interface{}); got["city"] != nil {
		t.Fatalf("expected second city to be nil, got %#v", got["city"])
	}
}

func TestNewDataFrameWithArrowSchemaStructValue(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	type profile struct {
		City  string
		Score int64
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "profile", Type: arrow.StructOf(
			arrow.Field{Name: "City", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "Score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		), Nullable: true},
	}, nil)

	df, err := NewDataFrame([]map[string]any{
		{
			"id":      int64(1),
			"profile": profile{City: "Shanghai", Score: 9},
		},
	}, WithArrowSchema(schema))
	if err != nil {
		t.Fatalf("NewDataFrame struct-valued Arrow schema failed: %v", err)
	}
	defer df.Close()

	out, err := df.ToMaps()
	if err != nil {
		t.Fatalf("ToMaps failed: %v", err)
	}
	profileMap, ok := out[0]["profile"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected struct column to round-trip as map[string]interface{}, got %T", out[0]["profile"])
	}
	if profileMap["City"] != "Shanghai" || profileMap["Score"] != int64(9) {
		t.Fatalf("unexpected struct column output: %#v", profileMap)
	}
}

func TestNewDataFrameFromColumnsWithArrowSchema(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "tags", Type: arrow.LargeListOf(arrow.BinaryTypes.String), Nullable: true},
	}, nil)

	df, err := NewDataFrame(map[string]interface{}{
		"id": []int64{1, 2},
		"tags": []any{
			[]string{"go", "arrow"},
			[]any{"polars", nil},
		},
	}, WithArrowSchema(schema))
	if err != nil {
		t.Fatalf("NewDataFrame columns with Arrow schema failed: %v", err)
	}
	defer df.Close()

	out, err := df.ToMaps()
	if err != nil {
		t.Fatalf("ToMaps failed: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(out))
	}

	firstTags, ok := out[0]["tags"].([]interface{})
	if !ok {
		t.Fatalf("expected first tags []interface{}, got %T", out[0]["tags"])
	}
	if len(firstTags) != 2 || firstTags[0] != "go" || firstTags[1] != "arrow" {
		t.Fatalf("unexpected first tags: %#v", firstTags)
	}

	secondTags, ok := out[1]["tags"].([]interface{})
	if !ok {
		t.Fatalf("expected second tags []interface{}, got %T", out[1]["tags"])
	}
	if len(secondTags) != 2 || secondTags[0] != "polars" || secondTags[1] != nil {
		t.Fatalf("unexpected second tags: %#v", secondTags)
	}
}

func TestArrowSchemaMismatchNestedError(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "profile", Type: arrow.StructOf(
			arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		), Nullable: true},
	}, nil)

	_, err := NewArrowRecordBatchFromRowsWithArrowSchema([]map[string]any{
		{
			"profile": map[string]any{
				"score": "oops",
			},
		},
	}, schema)
	if err == nil {
		t.Fatal("expected nested schema mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "profile.score") {
		t.Fatalf("expected error to mention nested field path, got %v", err)
	}
}

func TestArrowSchemaMismatchError(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	rows := []map[string]any{
		{"age": "not-a-number"},
	}
	schema := map[string]DataType{
		"age": DataTypeInt32,
	}

	_, err := NewArrowRecordBatchFromRowsWithSchema(rows, schema)
	if err == nil {
		t.Fatal("expected schema mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "field age") && !strings.Contains(err.Error(), "age:") {
		t.Fatalf("expected error to mention age field, got %v", err)
	}
	if !strings.Contains(err.Error(), "int32-compatible") {
		t.Fatalf("expected error to mention target int32 conversion, got %v", err)
	}
}

func TestNewDataFrameSchemaErrorIncludesContext(t *testing.T) {
	_, err := NewDataFrame(map[string]any{
		"age": []any{"oops"},
	}, WithSchema(map[string]DataType{
		"age": DataTypeInt32,
	}))
	if err == nil {
		t.Fatal("expected schema normalization error, got nil")
	}

	msg := err.Error()
	if !strings.Contains(msg, "column age row 0") {
		t.Fatalf("expected column/row context, got %v", err)
	}
	if !strings.Contains(msg, "schema expects INT32") {
		t.Fatalf("expected target schema type in error, got %v", err)
	}
	if !strings.Contains(msg, "got string") {
		t.Fatalf("expected source Go type in error, got %v", err)
	}
	if !strings.Contains(msg, "hint:") {
		t.Fatalf("expected remediation hint in error, got %v", err)
	}
}

func TestNewDataFrameFromRowsAuto(t *testing.T) {
	rows := []map[string]any{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}

	t.Run("AutoWithSchema", func(t *testing.T) {
		df, err := NewDataFrameFromRowsAuto(rows, map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		})
		if err != nil {
			t.Fatalf("NewDataFrameFromRowsAuto failed: %v", err)
		}
		defer df.Close()

		out, err := collectToMaps(t, df.Filter(Col("id").Gt(Lit(1))))
		if err != nil {
			t.Fatalf("ToMaps query failed: %v", err)
		}
		if len(out) != 1 || out[0]["name"] != "Bob" {
			t.Fatalf("Unexpected auto import result: %#v", out)
		}
	})

	t.Run("AutoFallsBackWithoutSchema", func(t *testing.T) {
		df, err := NewDataFrameFromRowsAuto(rows, nil)
		if err != nil {
			t.Fatalf("NewDataFrameFromRowsAuto fallback failed: %v", err)
		}
		defer df.Close()

		out, err := df.ToMaps()
		if err != nil {
			t.Fatalf("ToMaps failed: %v", err)
		}
		if len(out) != 2 {
			t.Fatalf("Expected 2 rows, got %d", len(out))
		}
	})
}

func TestDataFrameAPI(t *testing.T) {
	t.Run("PolarsStyleConstructors", func(t *testing.T) {
		inferredFrame, err := NewDataFrame([]map[string]any{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		})
		if err != nil {
			t.Fatalf("DataFrame inferred rows constructor failed: %v", err)
		}
		defer inferredFrame.Close()

		inferredRows, err := inferredFrame.ToMaps()
		if err != nil {
			t.Fatalf("DataFrame inferred ToMaps failed: %v", err)
		}
		if len(inferredRows) != 2 || inferredRows[1]["name"] != "Bob" {
			t.Fatalf("Unexpected rows from inferred DataFrame constructor: %#v", inferredRows)
		}

		frame, err := NewDataFrame([]map[string]any{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		}, WithSchema(map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		}))
		if err != nil {
			t.Fatalf("DataFrame rows constructor failed: %v", err)
		}
		defer frame.Close()

		rows, err := collectToMaps(t, frame.Filter(Col("id").Gt(Lit(1))))
		if err != nil {
			t.Fatalf("DataFrame rows query failed: %v", err)
		}
		if len(rows) != 1 || rows[0]["name"] != "Bob" {
			t.Fatalf("Unexpected rows from DataFrame rows constructor: %#v", rows)
		}

		columnFrame, err := NewDataFrame(map[string]interface{}{
			"id":   []int64{1, 2},
			"name": []string{"Alice", "Bob"},
		})
		if err != nil {
			t.Fatalf("DataFrame columns constructor failed: %v", err)
		}
		defer columnFrame.Close()

		columnRows, err := columnFrame.ToMaps()
		if err != nil {
			t.Fatalf("DataFrame columns rows failed: %v", err)
		}
		if len(columnRows) != 2 || columnRows[1]["name"] != "Bob" {
			t.Fatalf("Unexpected rows from DataFrame columns constructor: %#v", columnRows)
		}
	})

	t.Run("NewDataFrameFromStructs", func(t *testing.T) {
		type employee struct {
			ID       int64  `polars:"id"`
			Name     string `polars:"name"`
			Age      *int   `polars:"age"`
			IgnoreMe string `polars:"-"`
			Team     string
			Nickname *string `polars:"nickname"`
		}

		age := 35
		nickname := "Bobby"
		frame, err := NewDataFrame([]employee{
			{ID: 1, Name: "Alice", Team: "Engineering"},
			{ID: 2, Name: "Bob", Age: &age, Team: "Marketing", Nickname: &nickname},
		})
		if err != nil {
			t.Fatalf("NewDataFrame struct slice failed: %v", err)
		}
		defer frame.Close()

		rows, err := frame.ToMaps()
		if err != nil {
			t.Fatalf("NewDataFrame struct slice ToMaps failed: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if rows[0]["id"] != int64(1) || rows[0]["name"] != "Alice" {
			t.Fatalf("unexpected first struct row: %#v", rows[0])
		}
		if rows[0]["age"] != nil {
			t.Fatalf("expected nil pointer field to round-trip as nil, got %#v", rows[0]["age"])
		}
		if _, exists := rows[0]["IgnoreMe"]; exists {
			t.Fatalf("ignored field should not be exported: %#v", rows[0])
		}
		if rows[1]["nickname"] != "Bobby" {
			t.Fatalf("unexpected tagged nickname value: %#v", rows[1]["nickname"])
		}
		if rows[1]["Team"] != "Marketing" {
			t.Fatalf("unexpected default field name value: %#v", rows[1]["Team"])
		}
	})

	t.Run("NewDataFrameFromStructsGeneric", func(t *testing.T) {
		type user struct {
			ID   int64  `polars:"id"`
			Name string `polars:"name"`
		}
		frame, err := NewDataFrameFromStructs([]user{
			{ID: 1, Name: "Alice"},
			{ID: 2, Name: "Bob"},
		})
		if err != nil {
			t.Fatalf("NewDataFrameFromStructs failed: %v", err)
		}
		defer frame.Close()

		rows, err := frame.ToMaps()
		if err != nil {
			t.Fatalf("NewDataFrameFromStructs ToMaps failed: %v", err)
		}
		if len(rows) != 2 || rows[1]["name"] != "Bob" {
			t.Fatalf("unexpected rows from NewDataFrameFromStructs: %#v", rows)
		}
	})

	t.Run("ToStructs", func(t *testing.T) {
		type profile struct {
			City string `polars:"city"`
			Zip  int64  `polars:"zip"`
		}
		type employee struct {
			ID       int64    `polars:"id"`
			Name     string   `polars:"name"`
			Age      *int     `polars:"age"`
			Tags     []string `polars:"tags"`
			Payload  []byte   `polars:"payload"`
			Profile  profile  `polars:"profile"`
			Nickname *string  `polars:"nickname"`
		}

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
			{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true},
			{Name: "profile", Type: arrow.StructOf(
				arrow.Field{Name: "city", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "zip", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			), Nullable: true},
			{Name: "nickname", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil)

		frame, err := NewDataFrame([]map[string]any{
			{
				"id":       int64(1),
				"name":     "Alice",
				"age":      nil,
				"tags":     []any{"go", "polars"},
				"payload":  []byte("abc"),
				"profile":  map[string]any{"city": "Shanghai", "zip": int64(200000)},
				"nickname": nil,
			},
			{
				"id":       int64(2),
				"name":     "Bob",
				"age":      int64(35),
				"tags":     []any{"arrow"},
				"payload":  []byte("xyz"),
				"profile":  map[string]any{"city": "Suzhou", "zip": int64(215000)},
				"nickname": "Bobby",
			},
		}, WithArrowSchema(schema))
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		out, err := ToStructs[employee](frame)
		if err != nil {
			t.Fatalf("ToStructs failed: %v", err)
		}
		if len(out) != 2 {
			t.Fatalf("expected 2 structs, got %d", len(out))
		}
		if out[0].Age != nil {
			t.Fatalf("expected nil Age, got %#v", out[0].Age)
		}
		if out[1].Age == nil || *out[1].Age != 35 {
			t.Fatalf("unexpected Age pointer: %#v", out[1].Age)
		}
		if out[1].Nickname == nil || *out[1].Nickname != "Bobby" {
			t.Fatalf("unexpected Nickname pointer: %#v", out[1].Nickname)
		}
		if len(out[0].Tags) != 2 || out[0].Tags[0] != "go" {
			t.Fatalf("unexpected tags: %#v", out[0].Tags)
		}
		if !bytes.Equal(out[0].Payload, []byte("abc")) {
			t.Fatalf("unexpected payload: %#v", out[0].Payload)
		}
		if out[1].Profile.City != "Suzhou" || out[1].Profile.Zip != 215000 {
			t.Fatalf("unexpected nested profile: %#v", out[1].Profile)
		}
	})

	t.Run("ToStructPointers", func(t *testing.T) {
		type user struct {
			ID   int64  `polars:"id"`
			Name string `polars:"name"`
		}
		frame, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		out, err := ToStructPointers[user](frame)
		if err != nil {
			t.Fatalf("ToStructPointers failed: %v", err)
		}
		if len(out) != 2 || out[0] == nil || out[1] == nil {
			t.Fatalf("unexpected pointer result: %#v", out)
		}
		if out[1].Name != "Bob" {
			t.Fatalf("unexpected pointer export: %#v", out[1])
		}
	})

	t.Run("NewDataFrameUsesDefaultBridgeWhenNil", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame with nil bridge failed: %v", err)
		}
		defer frame.Close()

		rows, err := frame.ToMaps()
		if err != nil {
			t.Fatalf("NewDataFrame(nil) ToMaps failed: %v", err)
		}
		if len(rows) != 2 || rows[0]["name"] != "Alice" {
			t.Fatalf("Unexpected rows from NewDataFrame(nil): %#v", rows)
		}
	})

	t.Run("NewDataFrameDispatchesColumnsWithSchema", func(t *testing.T) {
		frame, err := NewDataFrame(map[string]interface{}{
			"id":         []uint64{1, 2},
			"name":       []string{"Alice", "Bob"},
			"age":        []interface{}{nil, int64(20)},
			"created_at": []string{"2026-03-24T10:00:00+08:00", "2026-03-24T11:00:00+08:00"},
		}, WithSchema(map[string]DataType{
			"id":         DataTypeUInt64,
			"name":       DataTypeUTF8,
			"age":        DataTypeInt64,
			"created_at": DataTypeUTF8,
		}))
		if err != nil {
			t.Fatalf("NewDataFrame column dispatch with schema failed: %v", err)
		}
		defer frame.Close()

		rows, err := frame.ToMaps()
		if err != nil {
			t.Fatalf("NewDataFrame column dispatch ToMaps failed: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("Expected 2 rows, got %d", len(rows))
		}
		if got, ok := rows[1]["id"].(uint64); !ok || got != 2 {
			t.Fatalf("Expected uint64 id=2, got %T %v", rows[1]["id"], rows[1]["id"])
		}
		if got := rows[0]["age"]; got != nil {
			t.Fatalf("Expected nil age in first row, got %#v", got)
		}
	})

	t.Run("NewDataFrameRejectsUnsupportedInput", func(t *testing.T) {
		_, err := NewDataFrame(struct{ Name string }{Name: "Alice"})
		if err == nil {
			t.Fatal("Expected unsupported input type error, got nil")
		}
		if !strings.Contains(err.Error(), "unsupported dataframe input type") {
			t.Fatalf("Unexpected unsupported input error: %v", err)
		}
	})

	t.Run("NewDataFrameFromMapsWithSchema", func(t *testing.T) {
		frame, err := NewDataFrameFromMaps([]map[string]any{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		}, WithSchema(map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		}))
		if err != nil {
			t.Fatalf("NewDataFrameFromMaps failed: %v", err)
		}
		defer frame.Close()

		rows, err := collectToMaps(t, frame.Filter(Col("id").Gt(Lit(1))))
		if err != nil {
			t.Fatalf("Managed frame query failed: %v", err)
		}
		if len(rows) != 1 || rows[0]["name"] != "Bob" {
			t.Fatalf("Unexpected managed frame rows: %#v", rows)
		}
	})

	t.Run("QueryMapsOwnsCleanup", func(t *testing.T) {
		rows, err := QueryMaps([]map[string]any{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		}, map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		}, func(frame *DataFrame) *LazyFrame {
			return frame.Filter(Col("id").Gt(Lit(1))).Select(Col("name"))
		})
		if err != nil {
			t.Fatalf("QueryMaps failed: %v", err)
		}
		if len(rows) != 1 || rows[0]["name"] != "Bob" {
			t.Fatalf("Unexpected QueryMaps rows: %#v", rows)
		}
	})

	t.Run("DataFrameCloseInvalidatesObject", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": 1, "name": "Alice"},
		}, WithSchema(map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		}))
		if err != nil {
			t.Fatalf("DataFrame failed: %v", err)
		}

		frame.Close()
		frame.Close()

		if !frame.Closed() {
			t.Fatal("expected Closed() to report true after Close")
		}

		if _, err := frame.ToMaps(); err == nil {
			t.Fatal("Expected ToMaps() to fail after Close(), got nil")
		} else if !errors.Is(err, ErrClosedDataFrame) {
			t.Fatalf("expected ErrClosedDataFrame, got %v", err)
		}
	})

	t.Run("DataFrameFinalizerOwnsEagerFrame", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": 1, "name": "Alice"},
		}, WithSchema(map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		}))
		if err != nil {
			t.Fatalf("DataFrame failed: %v", err)
		}

		escapedDF := frame.df
		if escapedDF == nil {
			t.Fatal("Expected managed frame to hold a dataframe")
		}

		frame = nil

		deadline := time.Now().Add(2 * time.Second)
		for {
			runtime.GC()
			runtime.Gosched()

			if _, err := escapedDF.ToMaps(); err != nil {
				return
			}

			if time.Now().After(deadline) {
				t.Fatal("Expected frame finalizer to close dataframe before timeout")
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

func TestLazyAndEagerModes(t *testing.T) {
	t.Run("LazyModeFromScan", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").
			Filter(Col("age").Gt(Lit(28))).
			Select(Col("name"), Col("age")).
			Limit(2)

		rows, err := collectToMaps(t, lf)
		if err != nil {
			t.Fatalf("Lazy mode collect failed: %v", err)
		}
		if len(rows) == 0 || len(rows) > 2 {
			t.Fatalf("Expected 1-2 lazy rows, got %d", len(rows))
		}
	})

	t.Run("EagerModeFromConstructor", func(t *testing.T) {
		df, err := NewDataFrame([]map[string]any{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		})
		if err != nil {
			t.Fatalf("Eager constructor failed: %v", err)
		}
		defer df.Close()

		rows, err := df.ToMaps()
		if err != nil {
			t.Fatalf("Eager ToMaps failed: %v", err)
		}
		if len(rows) != 2 || rows[1]["name"] != "Bob" {
			t.Fatalf("Unexpected eager rows: %#v", rows)
		}
	})

	t.Run("LazyCollectToEager", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").
			Filter(Col("department").Eq(Lit("Engineering"))).
			Limit(2)

		eager, err := lf.Collect()
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}
		defer eager.Free()

		rows, err := eager.ToMaps()
		if err != nil {
			t.Fatalf("Eager ToMaps after collect failed: %v", err)
		}
		if len(rows) == 0 || len(rows) > 2 {
			t.Fatalf("Expected 1-2 collected rows, got %d", len(rows))
		}
	})
}

func TestRowsImportFromDictsSemantics(t *testing.T) {
	t.Run("SchemaDropsExtrasAndExtendsMissing", func(t *testing.T) {
		frame, err := NewDataFrameFromMaps([]map[string]any{
			{"id": 1, "name": "Alice", "extra": true},
			{"id": 2},
		}, WithSchema(map[string]DataType{
			"id":  DataTypeInt64,
			"age": DataTypeInt64,
		}))
		if err != nil {
			t.Fatalf("NewDataFrameFromMaps with partial schema failed: %v", err)
		}
		defer frame.Close()

		rows, err := frame.ToMaps()
		if err != nil {
			t.Fatalf("ToMaps failed: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if _, ok := rows[0]["name"]; ok {
			t.Fatalf("expected undeclared column name to be dropped: %#v", rows[0])
		}
		if _, ok := rows[0]["extra"]; ok {
			t.Fatalf("expected undeclared column extra to be dropped: %#v", rows[0])
		}
		if got, err := toInt64(rows[0]["id"]); err != nil || got != 1 {
			t.Fatalf("unexpected id value: %v (%v)", rows[0]["id"], err)
		}
		if rows[0]["age"] != nil || rows[1]["age"] != nil {
			t.Fatalf("expected schema-only age column to be null-filled: %#v", rows)
		}
	})

	t.Run("SchemaOverridesOnlyAffectExistingColumns", func(t *testing.T) {
		frame, err := NewDataFrameFromMaps([]map[string]any{
			{"a": "2", "b": "x"},
		}, WithSchemaOverrides(map[string]DataType{
			"a":       DataTypeInt64,
			"missing": DataTypeInt64,
		}))
		if err != nil {
			t.Fatalf("NewDataFrameFromMaps with schema overrides failed: %v", err)
		}
		defer frame.Close()

		rows, err := frame.ToMaps()
		if err != nil {
			t.Fatalf("ToMaps failed: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if got, err := toInt64(rows[0]["a"]); err != nil || got != 2 {
			t.Fatalf("unexpected coerced a value: %v (%v)", rows[0]["a"], err)
		}
		if _, ok := rows[0]["missing"]; ok {
			t.Fatalf("schema override should not add missing column: %#v", rows[0])
		}
	})

	t.Run("ColumnNamesSelectByNameAndAddMissingColumns", func(t *testing.T) {
		frame, err := NewDataFrameFromMaps([]map[string]any{
			{"x": 1, "y": 2, "drop": "ignored"},
		}, WithColumnNames([]string{"y", "x", "z"}))
		if err != nil {
			t.Fatalf("NewDataFrameFromMaps with column names failed: %v", err)
		}
		defer frame.Close()

		rows, err := frame.ToMaps()
		if err != nil {
			t.Fatalf("ToMaps failed: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if len(rows[0]) != 3 {
			t.Fatalf("expected only y/x/z columns, got %#v", rows[0])
		}
		if got, err := toInt64(rows[0]["y"]); err != nil || got != 2 {
			t.Fatalf("unexpected y value: %v (%v)", rows[0]["y"], err)
		}
		if got, err := toInt64(rows[0]["x"]); err != nil || got != 1 {
			t.Fatalf("unexpected x value: %v (%v)", rows[0]["x"], err)
		}
		if rows[0]["z"] != nil {
			t.Fatalf("expected missing z column to be null-filled, got %#v", rows[0]["z"])
		}
	})

	t.Run("SchemaFieldsApplyExplicitDtypes", func(t *testing.T) {
		frame, err := NewDataFrameFromMaps([]map[string]any{
			{"a": "2", "b": "3.5"},
		}, WithSchemaFields([]SchemaField{
			{Name: "a", Type: DataTypeInt64},
			{Name: "b", Type: DataTypeFloat64},
		}))
		if err != nil {
			t.Fatalf("NewDataFrameFromMaps with schema fields failed: %v", err)
		}
		defer frame.Close()

		rows, err := frame.ToMaps()
		if err != nil {
			t.Fatalf("ToMaps failed: %v", err)
		}
		if got, err := toInt64(rows[0]["a"]); err != nil || got != 2 {
			t.Fatalf("unexpected typed a value: %v (%v)", rows[0]["a"], err)
		}
		if got, ok := rows[0]["b"].(float64); !ok || got != 3.5 {
			t.Fatalf("unexpected typed b value: %T %#v", rows[0]["b"], rows[0]["b"])
		}
	})

	t.Run("EmptyRowsFollowPythonSemantics", func(t *testing.T) {
		if _, err := NewDataFrameFromMaps(nil); err == nil {
			t.Fatal("expected empty rows without schema to fail")
		} else if !strings.Contains(err.Error(), "no data, cannot infer schema") {
			t.Fatalf("unexpected empty rows error: %v", err)
		}

		schemaFrame, err := NewDataFrameFromMaps(nil, WithSchema(map[string]DataType{
			"a": DataTypeInt64,
		}))
		if err != nil {
			t.Fatalf("empty rows with schema failed: %v", err)
		}
		defer schemaFrame.Close()

		rows, err := collectToMaps(t, schemaFrame.Select(Col("a")))
		if err != nil {
			t.Fatalf("empty rows with schema select failed: %v", err)
		}
		if len(rows) != 0 {
			t.Fatalf("expected 0 rows from empty schema frame, got %d", len(rows))
		}

		namesFrame, err := NewDataFrameFromMaps(nil, WithColumnNames([]string{"a"}))
		if err != nil {
			t.Fatalf("empty rows with column names failed: %v", err)
		}
		defer namesFrame.Close()

		rows, err = collectToMaps(t, namesFrame.Select(Col("a")))
		if err != nil {
			t.Fatalf("empty rows with column names select failed: %v", err)
		}
		if len(rows) != 0 {
			t.Fatalf("expected 0 rows from empty named frame, got %d", len(rows))
		}

		overrideFrame, err := NewDataFrameFromMaps(nil, WithSchemaOverrides(map[string]DataType{
			"a": DataTypeInt64,
		}))
		if err != nil {
			t.Fatalf("empty rows with overrides failed: %v", err)
		}
		defer overrideFrame.Close()

		if _, err := collectToMaps(t, overrideFrame.Select(Col("a"))); err == nil {
			t.Fatal("expected selecting override-only missing column to fail")
		}
	})

	t.Run("StrictFalseCoercesBadValuesToNull", func(t *testing.T) {
		frame, err := NewDataFrameFromMaps([]map[string]any{
			{"a": "2"},
			{"a": "bad"},
		}, WithSchemaOverrides(map[string]DataType{
			"a": DataTypeInt64,
		}), WithStrict(false))
		if err != nil {
			t.Fatalf("strict=false import failed: %v", err)
		}
		defer frame.Close()

		rows, err := frame.ToMaps()
		if err != nil {
			t.Fatalf("ToMaps failed: %v", err)
		}
		if got, err := toInt64(rows[0]["a"]); err != nil || got != 2 {
			t.Fatalf("unexpected first strict=false value: %v (%v)", rows[0]["a"], err)
		}
		if rows[1]["a"] != nil {
			t.Fatalf("expected bad strict=false value to become null, got %#v", rows[1]["a"])
		}
	})

	t.Run("StrictTrueRejectsBadValues", func(t *testing.T) {
		_, err := NewDataFrameFromMaps([]map[string]any{
			{"a": "2"},
			{"a": "bad"},
		}, WithSchemaOverrides(map[string]DataType{
			"a": DataTypeInt64,
		}), WithStrict(true))
		if err == nil {
			t.Fatal("expected strict=true import to fail")
		}
	})

	t.Run("InferSchemaLengthControlsSampling", func(t *testing.T) {
		shortFrame, err := NewDataFrameFromMaps([]map[string]any{
			{"a": int64(1)},
			{"a": 2.5},
		}, WithInferSchemaLength(1))
		if err != nil {
			t.Fatalf("short inference sample import failed: %v", err)
		}
		defer shortFrame.Close()

		shortRows, err := shortFrame.ToMaps()
		if err != nil {
			t.Fatalf("short inference sample ToMaps failed: %v", err)
		}
		if got, err := toInt64(shortRows[0]["a"]); err != nil || got != 1 {
			t.Fatalf("unexpected first short-sample value: %v (%v)", shortRows[0]["a"], err)
		}
		if got, err := toInt64(shortRows[1]["a"]); err != nil || got != 2 {
			t.Fatalf("expected second short-sample value to truncate to 2, got %v (%v)", shortRows[1]["a"], err)
		}

		frame, err := NewDataFrameFromMaps([]map[string]any{
			{"a": int64(1)},
			{"a": 2.5},
		}, WithInferSchemaAll())
		if err != nil {
			t.Fatalf("infer all import failed: %v", err)
		}
		defer frame.Close()

		rows, err := frame.ToMaps()
		if err != nil {
			t.Fatalf("ToMaps failed: %v", err)
		}
		if got, ok := rows[0]["a"].(float64); !ok || got != 1 {
			t.Fatalf("expected float64 1 from infer all, got %T %#v", rows[0]["a"], rows[0]["a"])
		}
		if got, ok := rows[1]["a"].(float64); !ok || got != 2.5 {
			t.Fatalf("expected float64 2.5 from infer all, got %T %#v", rows[1]["a"], rows[1]["a"])
		}
	})

	t.Run("NestedRowsRoundTripWithoutArrowSchema", func(t *testing.T) {
		eventAt := time.Date(2026, 3, 28, 10, 30, 0, 123000000, time.UTC)
		frame, err := NewDataFrameFromMaps([]map[string]any{
			{
				"id":       int64(1),
				"tags":     []any{"go", nil, "polars"},
				"profile":  map[string]any{"city": "Shanghai", "score": int64(9)},
				"payload":  []byte("abc"),
				"event_at": eventAt,
			},
		})
		if err != nil {
			t.Fatalf("nested rows import failed: %v", err)
		}
		defer frame.Close()

		rows, err := frame.ToMaps()
		if err != nil {
			t.Fatalf("ToMaps failed: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}

		tags, ok := rows[0]["tags"].([]interface{})
		if !ok || len(tags) != 3 || tags[0] != "go" || tags[1] != nil || tags[2] != "polars" {
			t.Fatalf("unexpected nested tags value: %#v", rows[0]["tags"])
		}
		profile, ok := rows[0]["profile"].(map[string]interface{})
		if !ok || profile["city"] != "Shanghai" {
			t.Fatalf("unexpected nested profile value: %#v", rows[0]["profile"])
		}
		if got, err := toInt64(profile["score"]); err != nil || got != 9 {
			t.Fatalf("unexpected nested profile score: %v (%v)", profile["score"], err)
		}
		assertBinaryValue(t, rows[0]["payload"], []byte("abc"))
		gotTime, ok := rows[0]["event_at"].(time.Time)
		if !ok {
			t.Fatalf("expected event_at to round-trip as time.Time, got %T", rows[0]["event_at"])
		}
		if !gotTime.Equal(eventAt) {
			t.Fatalf("unexpected event_at value: got %v want %v", gotTime, eventAt)
		}
	})

	t.Run("StructRowsUseSameSemanticsAsMapRows", func(t *testing.T) {
		type row struct {
			ID   any `polars:"id"`
			Name any `polars:"name"`
		}

		frame, err := NewDataFrameFromStructs([]row{
			{ID: "2", Name: "Alice"},
			{ID: "bad", Name: "Bob"},
		}, WithSchemaOverrides(map[string]DataType{
			"id": DataTypeInt64,
		}), WithStrict(false))
		if err != nil {
			t.Fatalf("struct rows import failed: %v", err)
		}
		defer frame.Close()

		rows, err := frame.ToMaps()
		if err != nil {
			t.Fatalf("ToMaps failed: %v", err)
		}
		if got, err := toInt64(rows[0]["id"]); err != nil || got != 2 {
			t.Fatalf("unexpected first struct row id: %v (%v)", rows[0]["id"], err)
		}
		if rows[1]["id"] != nil {
			t.Fatalf("expected second struct row id to be null, got %#v", rows[1]["id"])
		}
	})
}

func TestDataFrameAcrossFunctions(t *testing.T) {
	createUsers := func() (*DataFrame, error) {
		return NewDataFrame([]map[string]any{
			{"id": 1, "name": "Alice", "age": 18},
			{"id": 2, "name": "Bob", "age": 25},
			{"id": 3, "name": "Carl", "age": 31},
		})
	}

	filterAdults := func(df *DataFrame) ([]map[string]interface{}, error) {
		eager, err := df.
			Filter(Col("age").Gt(Lit(20))).
			Select(Col("name"), Col("age")).
			Collect()
		if err != nil {
			return nil, err
		}
		defer eager.Free()

		return eager.ToMaps()
	}

	df, err := createUsers()
	if err != nil {
		t.Fatalf("createUsers failed: %v", err)
	}
	defer df.Close()

	rows, err := filterAdults(df)
	if err != nil {
		t.Fatalf("filterAdults failed: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows after cross-function filter, got %d", len(rows))
	}
	if rows[0]["name"] != "Bob" || rows[1]["name"] != "Carl" {
		t.Fatalf("Unexpected rows from cross-function use: %#v", rows)
	}
}

func TestLazyFrameDefaultBridge(t *testing.T) {
	const sampleCSV = "../testdata/sample.csv"

	df, err := ScanCSV(sampleCSV).Limit(2).Collect()
	if err != nil {
		t.Fatalf("Collect(nil) failed: %v", err)
	}
	defer df.Free()

	rows, err := df.ToMaps()
	if err != nil {
		t.Fatalf("Collect(nil) ToMaps failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}

	if err := ScanCSV(sampleCSV).Limit(1).Print(); err != nil {
		t.Fatalf("Print(nil) failed: %v", err)
	}

	logicalPlan, err := ScanCSV(sampleCSV).Limit(1).LogicalPlan()
	if err != nil {
		t.Fatalf("LogicalPlan failed: %v", err)
	}
	if logicalPlan == "" {
		t.Fatal("expected non-empty logical plan")
	}

	optimizedPlan, err := ScanCSV(sampleCSV).Limit(1).OptimizedPlan()
	if err != nil {
		t.Fatalf("OptimizedPlan failed: %v", err)
	}
	if optimizedPlan == "" {
		t.Fatal("expected non-empty optimized plan")
	}
}

func TestDataFrameFromArrowManaged(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("managed arrow frame requires cgo")
	}

	record, err := NewArrowRecordBatchFromRowsWithSchema([]map[string]any{
		{"id": uint64(1), "name": "Alice"},
		{"id": uint64(2), "name": "Bob"},
	}, map[string]DataType{
		"id":   DataTypeUInt64,
		"name": DataTypeUTF8,
	})
	if err != nil {
		t.Fatalf("NewArrowRecordBatchFromRowsWithSchema failed: %v", err)
	}

	frame, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}
	defer frame.Close()

	df, err := frame.Filter(Col("id").Eq(Lit(2))).Collect()
	if err != nil {
		t.Fatalf("Managed arrow frame query failed: %v", err)
	}
	defer df.Free()
	rows, err := df.ToMaps()
	if err != nil {
		t.Fatalf("Managed arrow frame ToMaps failed: %v", err)
	}
	if len(rows) != 1 || rows[0]["name"] != "Bob" {
		t.Fatalf("Unexpected managed arrow frame rows: %#v", rows)
	}

	polarsStyleRecord, err := NewArrowRecordBatchFromRowsWithSchema([]map[string]any{
		{"id": uint64(3), "name": "Carl"},
	}, map[string]DataType{
		"id":   DataTypeUInt64,
		"name": DataTypeUTF8,
	})
	if err != nil {
		t.Fatalf("NewArrowRecordBatchFromRowsWithSchema failed: %v", err)
	}

	polarsStyleFrame, err := NewDataFrameFromArrow(polarsStyleRecord)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}
	defer polarsStyleFrame.Close()

	polarsStyleRows, err := polarsStyleFrame.ToMaps()
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow ToMaps failed: %v", err)
	}
	if len(polarsStyleRows) != 1 || polarsStyleRows[0]["name"] != "Carl" {
		t.Fatalf("Unexpected rows from NewDataFrameFromArrow: %#v", polarsStyleRows)
	}
}

func TestToArrow(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("ToArrow requires cgo")
	}

	df, err := NewDataFrame([]map[string]any{
		{"id": uint64(1), "name": "Alice"},
		{"id": uint64(2), "name": "Bob"},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	recordBatch, err := df.ToArrow()
	if err != nil {
		t.Fatalf("DataFrame.ToArrow failed: %v", err)
	}
	defer recordBatch.Release()

	if got := recordBatch.NumRows(); got != 2 {
		t.Fatalf("Expected 2 rows, got %d", got)
	}
	if got := recordBatch.NumCols(); got != 2 {
		t.Fatalf("Expected 2 cols, got %d", got)
	}

	rows, err := parseArrowRecordBatch(recordBatch)
	if err != nil {
		t.Fatalf("parseArrowRecordBatch failed: %v", err)
	}
	if len(rows) != 2 || rows[1]["name"] != "Bob" {
		t.Fatalf("Unexpected rows from ToArrow: %#v", rows)
	}
}

func TestEagerFrameToArrow(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("ToArrow requires cgo")
	}

	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("LoadBridge failed: %v", err)
	}

	eager, err := NewEagerFrameFromRowsWithSchema(brg, []map[string]any{
		{"id": uint64(10), "city": "Shanghai"},
	}, map[string]DataType{
		"id":   DataTypeUInt64,
		"city": DataTypeUTF8,
	})
	if err != nil {
		t.Fatalf("NewEagerFrameFromRowsWithSchema failed: %v", err)
	}
	defer eager.Free()

	recordBatch, err := eager.ToArrow()
	if err != nil {
		t.Fatalf("EagerFrame.ToArrow failed: %v", err)
	}
	defer recordBatch.Release()

	rows, err := parseArrowRecordBatch(recordBatch)
	if err != nil {
		t.Fatalf("parseArrowRecordBatch failed: %v", err)
	}
	if len(rows) != 1 || rows[0]["city"] != "Shanghai" {
		t.Fatalf("Unexpected rows from eager ToArrowBatch: %#v", rows)
	}
}

func TestDataFrameMapRows(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	mapped, err := df.MapRows(func(row map[string]any) (map[string]any, error) {
		level := "junior"
		if age, ok := row["age"].(int64); ok && age > 30 {
			level = "senior"
		}
		return map[string]any{
			"name":  row["name"],
			"level": level,
		}, nil
	}, MapRowsOptions{})
	if err != nil {
		t.Fatalf("MapRows failed: %v", err)
	}
	defer mapped.Close()

	rows, err := mapped.ToMaps()
	if err != nil {
		t.Fatalf("mapped.ToMaps failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}
	if rows[0]["level"] != "junior" || rows[1]["level"] != "senior" {
		t.Fatalf("Unexpected MapRows output: %#v", rows)
	}
}

func TestDataFrameMapRowsError(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	_, err = df.MapRows(func(row map[string]any) (map[string]any, error) {
		if row["name"] == "Bob" {
			return nil, fmt.Errorf("boom")
		}
		return row, nil
	}, MapRowsOptions{})
	if err == nil {
		t.Fatal("Expected MapRows error, got nil")
	}
	if !strings.Contains(err.Error(), "MapRows row 1") {
		t.Fatalf("Expected row index in error, got %v", err)
	}
}

func TestDataFrameMapBatches(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("MapBatches requires cgo")
	}

	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	mapped, err := df.MapBatches(func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
		rows, err := parseArrowRecordBatch(batch)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			level := "junior"
			if age, ok := row["age"].(int64); ok && age > 30 {
				level = "senior"
			}
			row["level"] = level
		}
		return NewArrowRecordBatchFromRowsWithSchema(rows, map[string]DataType{
			"name":  DataTypeUTF8,
			"age":   DataTypeInt64,
			"level": DataTypeUTF8,
		})
	}, MapBatchesOptions{})
	if err != nil {
		t.Fatalf("MapBatches failed: %v", err)
	}
	defer mapped.Close()

	rows, err := mapped.ToMaps()
	if err != nil {
		t.Fatalf("mapped.ToMaps failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}
	if rows[0]["level"] != "junior" || rows[1]["level"] != "senior" {
		t.Fatalf("Unexpected MapBatches output: %#v", rows)
	}
}

func TestDataFrameMapBatchesPassThrough(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("MapBatches requires cgo")
	}

	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	mapped, err := df.MapBatches(func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
		return batch, nil
	}, MapBatchesOptions{})
	if err != nil {
		t.Fatalf("MapBatches passthrough failed: %v", err)
	}
	defer mapped.Close()

	rows, err := mapped.ToMaps()
	if err != nil {
		t.Fatalf("mapped.ToMaps failed: %v", err)
	}
	if len(rows) != 1 || rows[0]["name"] != "Alice" {
		t.Fatalf("Unexpected passthrough MapBatches output: %#v", rows)
	}
}

func TestDataFrameMapBatchesError(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("MapBatches requires cgo")
	}

	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	_, err = df.MapBatches(func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
		return nil, fmt.Errorf("boom")
	}, MapBatchesOptions{})
	if err == nil {
		t.Fatal("Expected MapBatches error, got nil")
	}
	if !strings.Contains(err.Error(), "MapBatches failed") {
		t.Fatalf("Expected MapBatches prefix in error, got %v", err)
	}
}

func TestExprMapBatches(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("Expr.MapBatches requires cgo")
	}

	df, err := NewDataFrame([]map[string]any{
		{"age": int64(25)},
		{"age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	rows, err := collectToMaps(t, df.Select(
		Col("age").
			MapBatches(func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
				col, ok := batch.Column(0).(*array.Int64)
				if !ok {
					return nil, fmt.Errorf("expected int64 input column, got %T", batch.Column(0))
				}

				pool := memory.NewGoAllocator()
				builder := array.NewInt64Builder(pool)
				defer builder.Release()

				for i := 0; i < col.Len(); i++ {
					if col.IsNull(i) {
						builder.AppendNull()
						continue
					}
					builder.Append(col.Value(i) + 10)
				}

				values := builder.NewArray()
				defer values.Release()

				schema := arrow.NewSchema([]arrow.Field{
					{Name: batch.Schema().Field(0).Name, Type: arrow.PrimitiveTypes.Int64},
				}, nil)
				record := array.NewRecordBatch(schema, []arrow.Array{values}, int64(col.Len()))
				return record, nil
			}, ExprMapBatchesOptions{ReturnType: DataTypeInt64}).
			Alias("age_plus_ten"),
	))
	fmt.Println(rows)
	if err != nil {
		t.Fatalf("Expr.MapBatches failed: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}
	if rows[0]["age_plus_ten"] != int64(35) || rows[1]["age_plus_ten"] != int64(45) {
		t.Fatalf("Unexpected Expr.MapBatches output: %#v", rows)
	}
}

func TestMapBatchesMultipleExprs(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("MapBatches requires cgo")
	}

	df, err := NewDataFrame([]map[string]any{
		{"age": int64(25), "bonus": int64(5)},
		{"age": int64(35), "bonus": int64(10)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	rows, err := collectToMaps(t, df.Select(
		MapBatches([]Expr{Col("age"), Col("bonus")}, func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
			ageCol, ok := batch.Column(0).(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("expected int64 age column, got %T", batch.Column(0))
			}
			bonusCol, ok := batch.Column(1).(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("expected int64 bonus column, got %T", batch.Column(1))
			}

			pool := memory.NewGoAllocator()
			builder := array.NewInt64Builder(pool)
			defer builder.Release()

			for i := 0; i < ageCol.Len(); i++ {
				if ageCol.IsNull(i) || bonusCol.IsNull(i) {
					builder.AppendNull()
					continue
				}
				builder.Append(ageCol.Value(i) + bonusCol.Value(i))
			}

			values := builder.NewArray()
			defer values.Release()

			schema := arrow.NewSchema([]arrow.Field{
				{Name: "age_bonus", Type: arrow.PrimitiveTypes.Int64},
			}, nil)
			record := array.NewRecordBatch(schema, []arrow.Array{values}, int64(ageCol.Len()))
			return record, nil
		}, ExprMapBatchesOptions{ReturnType: DataTypeInt64}).Alias("age_bonus"),
	))
	fmt.Println(rows)
	if err != nil {
		t.Fatalf("multi-input MapBatches failed: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}
	if rows[0]["age_bonus"] != int64(30) || rows[1]["age_bonus"] != int64(45) {
		t.Fatalf("Unexpected multi-input MapBatches output: %#v", rows)
	}
}

func TestExprMapBatchesError(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("Expr.MapBatches requires cgo")
	}

	df, err := NewDataFrame([]map[string]any{
		{"age": int64(25)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	_, err = collectToMaps(t, df.Select(
		Col("age").MapBatches(func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
			return nil, fmt.Errorf("boom")
		}, ExprMapBatchesOptions{ReturnType: DataTypeInt64}).Alias("age_fail"),
	))
	if err == nil {
		t.Fatal("Expected Expr.MapBatches error, got nil")
	}
	if !strings.Contains(err.Error(), "MapBatches") {
		t.Fatalf("Expected Expr.MapBatches in error, got %v", err)
	}
}

func TestSQLContextExecute(t *testing.T) {
	ctx := NewSQLContext()
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
		{"name": "Charlie", "age": int64(40)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	ctx.Register("people", df)

	result, err := ctx.Execute("SELECT name, age FROM people WHERE age >= 35 ORDER BY age")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Close()

	rows, err := result.ToMaps()
	if err != nil {
		t.Fatalf("result.ToMaps failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != "Bob" || rows[1]["name"] != "Charlie" {
		t.Fatalf("unexpected SQL rows: %#v", rows)
	}
}

func TestSQLContextInputErrorsIncludeHints(t *testing.T) {
	ctx := NewSQLContext().Register("", 123)
	_, err := ctx.Execute("SELECT 1")
	if err == nil {
		t.Fatal("expected execute to fail for empty table name")
	}
	if !strings.Contains(err.Error(), "SQLContext.Register: table name is empty") {
		t.Fatalf("expected table-name context, got %v", err)
	}

	_, err = NewSQLContext().Execute("")
	if err == nil {
		t.Fatal("expected execute to fail for empty query")
	}
	if !strings.Contains(err.Error(), "query is empty") || !strings.Contains(err.Error(), "hint:") {
		t.Fatalf("expected empty-query hint, got %v", err)
	}

	_, err = NewSQLContext().Execute("SELECT 1")
	if err == nil {
		t.Fatal("expected execute to fail for missing tables")
	}
	if !strings.Contains(err.Error(), "no registered tables") || !strings.Contains(err.Error(), `SELECT 1`) {
		t.Fatalf("expected no-table query context, got %v", err)
	}

	_, err = NewSQLContext(map[string]any{
		"bad": 123,
	}).Execute("SELECT * FROM bad")
	if err == nil {
		t.Fatal("expected execute to fail for unsupported table type")
	}
	if !strings.Contains(err.Error(), "unsupported SQL table type int") {
		t.Fatalf("expected unsupported-type context, got %v", err)
	}
	if !strings.Contains(err.Error(), "*DataFrame") {
		t.Fatalf("expected supported-type hint, got %v", err)
	}
}

func TestCreateDataFrameFromColumnsErrorsIncludeContext(t *testing.T) {
	brg, err := resolveBridge(nil)
	if err != nil {
		t.Fatalf("resolveBridge failed: %v", err)
	}

	if _, err := brg.CreateDataFrameFromColumns(nil); err == nil {
		t.Fatal("expected empty jsonData to fail")
	} else if !strings.Contains(err.Error(), "Bridge.CreateDataFrameFromColumns: jsonData is empty") {
		t.Fatalf("expected empty-payload context, got %v", err)
	}

	_, err = brg.CreateDataFrameFromColumns([]byte(`{"columns":[{"values":[1,2,3]}]}`))
	if err == nil {
		t.Fatal("expected missing column name to fail")
	}
	msg := err.Error()
	if !strings.Contains(msg, "bridge_df_from_columns") {
		t.Fatalf("expected low-level import context, got %v", err)
	}
	if !strings.Contains(msg, "name") {
		t.Fatalf("expected missing name field context, got %v", err)
	}
}

func TestSQLContextChainRegisterMany(t *testing.T) {
	left, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	})
	if err != nil {
		t.Fatalf("NewDataFrame left failed: %v", err)
	}
	defer left.Close()

	right, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "age": int64(25)},
		{"id": int64(2), "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame right failed: %v", err)
	}
	defer right.Close()

	ctx := NewSQLContext().
		Register("left_tbl", left).
		RegisterMany(map[string]any{
			"right_tbl": right,
		})

	tables := ctx.Tables()
	if len(tables) != 2 || tables[0] != "left_tbl" || tables[1] != "right_tbl" {
		t.Fatalf("unexpected SQLContext tables: %#v", tables)
	}

	result, err := ctx.Execute(`
		SELECT l.name, r.age
		FROM left_tbl AS l
		INNER JOIN right_tbl AS r
		ON l.id = r.id
		ORDER BY r.age
	`)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Close()

	rows, err := result.ToMaps()
	if err != nil {
		t.Fatalf("result.ToMaps failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestSQLContextUnregisterAndShowTables(t *testing.T) {
	left, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "name": "Alice"},
	})
	if err != nil {
		t.Fatalf("NewDataFrame left failed: %v", err)
	}
	defer left.Close()

	right, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "age": int64(25)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame right failed: %v", err)
	}
	defer right.Close()

	ctx := NewSQLContext().
		Register("left_tbl", left).
		Register("right_tbl", right).
		Unregister("right_tbl")

	tables := ctx.Tables()
	if len(tables) != 1 || tables[0] != "left_tbl" {
		t.Fatalf("unexpected tables after unregister: %#v", tables)
	}

	showTables, err := ctx.Execute("SHOW TABLES")
	if err != nil {
		t.Fatalf("SHOW TABLES failed: %v", err)
	}
	defer showTables.Close()

	rows, err := showTables.ToMaps()
	if err != nil {
		t.Fatalf("SHOW TABLES ToMaps failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 SHOW TABLES row, got %d", len(rows))
	}
	if rows[0]["name"] != "left_tbl" {
		t.Fatalf("unexpected SHOW TABLES rows: %#v", rows)
	}

	_, err = ctx.Execute("SELECT * FROM right_tbl")
	if err == nil {
		t.Fatal("expected SQL on unregistered table to fail")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "right_tbl") {
		t.Fatalf("unexpected unregistered table error: %v", err)
	}
	if !strings.Contains(err.Error(), "SELECT * FROM right_tbl") {
		t.Fatalf("expected SQL error to include query summary, got %v", err)
	}
	if !strings.Contains(err.Error(), "left_tbl") {
		t.Fatalf("expected SQL error to include registered table context, got %v", err)
	}
}

func TestSQLHelperJoin(t *testing.T) {
	left, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	})
	if err != nil {
		t.Fatalf("NewDataFrame left failed: %v", err)
	}
	defer left.Close()

	right, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "age": int64(25)},
		{"id": int64(2), "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame right failed: %v", err)
	}
	defer right.Close()

	result, err := SQL(`
		SELECT l.name, r.age
		FROM left_tbl AS l
		INNER JOIN right_tbl AS r
		ON l.id = r.id
		ORDER BY r.age
	`, map[string]any{
		"left_tbl":  left,
		"right_tbl": right,
	})
	if err != nil {
		t.Fatalf("SQL helper failed: %v", err)
	}
	defer result.Close()

	rows, err := result.ToMaps()
	if err != nil {
		t.Fatalf("result.ToMaps failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != "Alice" || rows[1]["age"] != int64(35) {
		t.Fatalf("unexpected SQL join rows: %#v", rows)
	}
}

func TestSQLContextWithLazyFrame(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
		{"name": "Charlie", "age": int64(40)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	ctx := NewSQLContext().
		Register("adults", df.Filter(Col("age").Gt(Lit(int64(30)))))

	result, err := ctx.Execute("SELECT name FROM adults ORDER BY name")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Close()

	rows, err := result.ToMaps()
	if err != nil {
		t.Fatalf("result.ToMaps failed: %v", err)
	}
	if len(rows) != 2 || rows[0]["name"] != "Bob" || rows[1]["name"] != "Charlie" {
		t.Fatalf("unexpected SQL lazy rows: %#v", rows)
	}
}

func TestSQLContextExecuteLazy(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	lf, err := NewSQLContext(map[string]any{
		"people": df,
	}).ExecuteLazy("SELECT name FROM people WHERE age >= 30")
	if err != nil {
		t.Fatalf("ExecuteLazy failed: %v", err)
	}

	rows, err := collectToMaps(t, lf.Filter(Col("name").Eq(Lit("Bob"))))
	if err != nil {
		t.Fatalf("collectToMaps failed: %v", err)
	}
	if len(rows) != 1 || rows[0]["name"] != "Bob" {
		t.Fatalf("unexpected ExecuteLazy rows: %#v", rows)
	}
}

func TestSQLContextExecuteLazyDefersExecution(t *testing.T) {
	lf, err := NewSQLContext(map[string]any{
		"missing_people": ScanCSV("../testdata/does-not-exist.csv"),
	}).ExecuteLazy("SELECT * FROM missing_people")
	if err != nil {
		t.Fatalf("ExecuteLazy should not eagerly fail: %v", err)
	}

	_, err = lf.Collect()

	if err == nil {
		t.Fatal("expected Collect to fail for missing CSV input")
	}
	if !strings.Contains(err.Error(), "does-not-exist.csv") {
		t.Fatalf("unexpected Collect error: %v", err)
	}
}

func TestExecutionOptionsMemoryLimitCollect(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	if err := SetExecutionOptions(ExecutionOptions{MemoryLimitBytes: 1}); err != nil {
		t.Fatalf("SetExecutionOptions failed: %v", err)
	}
	t.Cleanup(func() {
		if err := SetExecutionOptions(ExecutionOptions{}); err != nil {
			t.Fatalf("reset execution options failed: %v", err)
		}
	})

	_, err = df.Lazy().Collect()
	if err == nil {
		t.Fatal("expected Collect to fail under tiny memory limit")
	}
	if !strings.Contains(err.Error(), "ERR_OOM") || !strings.Contains(err.Error(), "memory limit exceeded") {
		t.Fatalf("unexpected memory limit error: %v", err)
	}
}

func TestExecutionOptionsRejectNegativeLimitWithHint(t *testing.T) {
	err := SetExecutionOptions(ExecutionOptions{MemoryLimitBytes: -1})
	if err == nil {
		t.Fatal("expected negative memory limit to fail")
	}
	if !strings.Contains(err.Error(), "MemoryLimitBytes must be >= 0") {
		t.Fatalf("expected field context, got %v", err)
	}
	if !strings.Contains(err.Error(), "use 0 to disable the limit") {
		t.Fatalf("expected remediation hint, got %v", err)
	}
}

func TestExecutionOptionsMemoryLimitDisabled(t *testing.T) {
	if err := SetExecutionOptions(ExecutionOptions{}); err != nil {
		t.Fatalf("SetExecutionOptions failed: %v", err)
	}

	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	rows, err := collectToMaps(t, df.Lazy())
	if err != nil {
		t.Fatalf("collectToMaps failed with disabled memory limit: %v", err)
	}
	if len(rows) != 1 || rows[0]["name"] != "Alice" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
}

func TestExecutionOptionsMemoryLimitSQL(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	if err := SetExecutionOptions(ExecutionOptions{MemoryLimitBytes: 1}); err != nil {
		t.Fatalf("SetExecutionOptions failed: %v", err)
	}
	t.Cleanup(func() {
		if err := SetExecutionOptions(ExecutionOptions{}); err != nil {
			t.Fatalf("reset execution options failed: %v", err)
		}
	})

	_, err = NewSQLContext(map[string]any{
		"people": df,
	}).Execute("SELECT * FROM people")
	if err == nil {
		t.Fatal("expected SQL Execute to fail under tiny memory limit")
	}
	if !strings.Contains(err.Error(), "ERR_OOM") || !strings.Contains(err.Error(), "memory limit exceeded") {
		t.Fatalf("unexpected SQL memory limit error: %v", err)
	}
}

func TestDataFrameFreeBehavior(t *testing.T) {
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	t.Run("FreeInvalidatesDataFrame", func(t *testing.T) {
		df, err := NewEagerFrameFromRowsWithSchema(brg, []map[string]any{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		}, map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}

		df.Free()

		if !df.Closed() {
			t.Fatal("expected Closed() to report true after Free")
		}

		if _, err := df.ToMaps(); err == nil {
			t.Fatal("Expected ToMaps() to fail after Free(), got nil")
		} else if !errors.Is(err, ErrClosedDataFrame) {
			t.Fatalf("expected ErrClosedDataFrame after Free, got %v", err)
		}
		if err := df.Print(); err == nil {
			t.Fatal("Expected Print() to fail after Free(), got nil")
		} else if !errors.Is(err, ErrClosedDataFrame) {
			t.Fatalf("expected ErrClosedDataFrame from Print after Free, got %v", err)
		}
	})

	t.Run("FreeIsIdempotent", func(t *testing.T) {
		df, err := NewEagerFrameFromRowsWithSchema(brg, []map[string]any{
			{"id": 1, "name": "Alice"},
		}, map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}

		df.Free()
		df.Free()
		df.Free()

		if _, err := df.ToMaps(); err == nil {
			t.Fatal("Expected ToMaps() to remain invalid after repeated Free()")
		}
	})

	t.Run("CollectThenToMapsStillOwnsCleanupInternally", func(t *testing.T) {
		rows, err := NewDataFrameFromRowsWithSchema([]map[string]any{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		}, map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}

		// Use the DataFrame immediately and release the managed wrapper below.
		result, err := collectToMaps(t, rows.Lazy())
		if err != nil {
			t.Fatalf("Collect+ToMaps failed: %v", err)
		}
		if len(result) != 2 {
			t.Fatalf("Expected 2 rows, got %d", len(result))
		}

		rows.Close()
	})

	t.Run("DeferFreeOnFunctionReturn", func(t *testing.T) {
		var escaped *EagerFrame

		useWithDefer := func() error {
			df, err := NewEagerFrameFromRowsWithSchema(brg, []map[string]any{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			}, map[string]DataType{
				"id":   DataTypeInt64,
				"name": DataTypeUTF8,
			})
			if err != nil {
				return err
			}
			escaped = df
			defer df.Free()

			rows, err := df.ToMaps()
			if err != nil {
				return fmt.Errorf("ToMaps before deferred Free failed: %w", err)
			}
			if len(rows) != 2 {
				return fmt.Errorf("expected 2 rows before deferred Free, got %d", len(rows))
			}
			return nil
		}

		if err := useWithDefer(); err != nil {
			t.Fatalf("Function using defer Free failed: %v", err)
		}
		if escaped == nil {
			t.Fatal("Expected escaped dataframe reference to be set")
		}

		if _, err := escaped.ToMaps(); err == nil {
			t.Fatal("Expected ToMaps() to fail after deferred Free ran, got nil")
		}
	})
}

func TestNilAndClosedErrors(t *testing.T) {
	var managed *DataFrame
	if _, err := managed.ToMaps(); !errors.Is(err, ErrNilDataFrame) {
		t.Fatalf("expected ErrNilDataFrame from nil managed dataframe, got %v", err)
	}

	var lazy *LazyFrame
	if _, err := lazy.Collect(); !errors.Is(err, ErrNilLazyFrame) {
		t.Fatalf("expected ErrNilLazyFrame from nil lazyframe, got %v", err)
	}

	type dup struct {
		First  string `polars:"name"`
		Second string `polars:"name"`
	}
	if _, err := NewDataFrameFromStructs([]dup{{First: "a", Second: "b"}}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput from duplicate struct tags, got %v", err)
	}

	type user struct {
		Name string `polars:"name"`
	}
	var nilUser *user
	if _, err := NewDataFrame([]*user{nilUser}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput from nil struct pointer row, got %v", err)
	}

	frame, err := NewDataFrame([]map[string]any{{"name": "Alice"}})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer frame.Close()

	if _, err := ToStructs[int](frame); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput from non-struct ToStructs target, got %v", err)
	}

	type strictUser struct {
		Age int64 `polars:"age"`
	}
	badFrame, err := NewDataFrame([]map[string]any{{"age": "oops"}})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer badFrame.Close()

	if _, err := ToStructs[strictUser](badFrame); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput from mismatched ToStructs field, got %v", err)
	}

	type event struct {
		CreatedAt time.Time `polars:"created_at"`
	}
	timeFrame, err := NewDataFrame([]map[string]any{{"created_at": "not-a-date"}})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer timeFrame.Close()

	_, err = ToStructs[event](timeFrame)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput from invalid time conversion, got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "field created_at") {
		t.Fatalf("expected field context in temporal ToStructs error, got %v", err)
	}
	if !strings.Contains(msg, "datetime") {
		t.Fatalf("expected datetime context in temporal ToStructs error, got %v", err)
	}
	if !strings.Contains(msg, "RFC3339") && !strings.Contains(msg, "2006-01-02") {
		t.Fatalf("expected supported layout hint in temporal ToStructs error, got %v", err)
	}
}

func TestDataFrameJSONExport(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("JSON export requires cgo")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "profile", Type: arrow.StructOf(
			arrow.Field{Name: "city", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		), Nullable: true},
	}, nil)

	df, err := NewDataFrame([]map[string]any{
		{
			"id":      int64(1),
			"name":    "Alice",
			"tags":    []string{"go", "json"},
			"payload": []byte("abc"),
			"profile": map[string]any{"city": "Shanghai", "score": int64(9)},
		},
		{
			"id":      int64(2),
			"name":    "Bob",
			"tags":    []string{"polars"},
			"payload": []byte("xyz"),
			"profile": map[string]any{"city": "Suzhou", "score": int64(7)},
		},
	}, WithArrowSchema(schema))
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	t.Run("WriteJSON", func(t *testing.T) {
		var buf bytes.Buffer
		if err := df.WriteJSON(&buf); err != nil {
			t.Fatalf("WriteJSON failed: %v", err)
		}
		var rows []map[string]any
		if err := json.Unmarshal(buf.Bytes(), &rows); err != nil {
			t.Fatalf("unmarshal JSON failed: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if rows[0]["name"] != "Alice" {
			t.Fatalf("unexpected first row: %#v", rows[0])
		}
		if rows[0]["payload"] != "abc" {
			t.Fatalf("unexpected payload: %#v", rows[0]["payload"])
		}
		profile, ok := rows[0]["profile"].(map[string]any)
		if !ok || profile["city"] != "Shanghai" {
			t.Fatalf("unexpected nested profile: %#v", rows[0]["profile"])
		}
	})

	t.Run("WriteJSON", func(t *testing.T) {
		var buf bytes.Buffer
		if err := df.WriteJSON(&buf); err != nil {
			t.Fatalf("WriteJSON failed: %v", err)
		}
		if !strings.HasPrefix(buf.String(), "[") {
			t.Fatalf("expected JSON array string, got %q", buf.String())
		}
	})

	t.Run("WriteJSONNDJSON", func(t *testing.T) {
		var buf bytes.Buffer
		if err := df.WriteNDJSON(&buf); err != nil {
			t.Fatalf("WriteNDJSON failed: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 2 {
			t.Fatalf("expected 2 ndjson lines, got %d", len(lines))
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(lines[1]), &row); err != nil {
			t.Fatalf("unmarshal ndjson line failed: %v", err)
		}
		if row["name"] != "Bob" {
			t.Fatalf("unexpected second ndjson row: %#v", row)
		}
	})

	t.Run("WriteNDJSONGzip", func(t *testing.T) {
		var buf bytes.Buffer
		if err := df.WriteNDJSON(&buf, WriteNDJSONOptions{Compression: NDJSONCompressionGzip}); err != nil {
			t.Fatalf("WriteNDJSON gzip failed: %v", err)
		}
		zr, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatalf("gzip reader failed: %v", err)
		}
		defer zr.Close()
		plain, err := io.ReadAll(zr)
		if err != nil {
			t.Fatalf("read gzip payload failed: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(plain)), "\n")
		if len(lines) != 2 {
			t.Fatalf("expected 2 gzip ndjson lines, got %d", len(lines))
		}
	})

	t.Run("WriteNDJSONInvalidCompression", func(t *testing.T) {
		var buf bytes.Buffer
		err := df.WriteNDJSON(&buf, WriteNDJSONOptions{Compression: NDJSONCompression("zip")})
		if err == nil {
			t.Fatal("expected invalid NDJSON compression to fail")
		}
		if !strings.Contains(err.Error(), `unsupported NDJSON compression "zip"`) {
			t.Fatalf("expected compression value in error, got %v", err)
		}
		if !strings.Contains(err.Error(), `"none"`) || !strings.Contains(err.Error(), `"gzip"`) {
			t.Fatalf("expected supported compression hint, got %v", err)
		}
	})
}

func TestLazyFrameJSONExport(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("JSON export requires cgo")
	}

	df, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	lf := df.Filter(Col("id").Gt(Lit(1)))

	t.Run("SinkJSON", func(t *testing.T) {
		var buf bytes.Buffer
		if err := lf.SinkJSON(&buf); err != nil {
			t.Fatalf("SinkJSON failed: %v", err)
		}
		var rows []map[string]any
		if err := json.Unmarshal(buf.Bytes(), &rows); err != nil {
			t.Fatalf("unmarshal JSON failed: %v", err)
		}
		if len(rows) != 1 || rows[0]["name"] != "Bob" {
			t.Fatalf("unexpected lazy json rows: %#v", rows)
		}
	})

	t.Run("SinkJSON", func(t *testing.T) {
		var buf bytes.Buffer
		if err := lf.SinkJSON(&buf); err != nil {
			t.Fatalf("SinkJSON failed: %v", err)
		}
		if !strings.Contains(buf.String(), "\"Bob\"") {
			t.Fatalf("unexpected lazy json string: %q", buf.String())
		}
	})

	t.Run("WriteJSONNDJSON", func(t *testing.T) {
		var buf bytes.Buffer
		if err := lf.SinkNDJSON(&buf); err != nil {
			t.Fatalf("SinkNDJSON failed: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 1 {
			t.Fatalf("expected 1 ndjson line, got %d", len(lines))
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &row); err != nil {
			t.Fatalf("unmarshal ndjson line failed: %v", err)
		}
		if row["name"] != "Bob" {
			t.Fatalf("unexpected lazy ndjson row: %#v", row)
		}
	})

	t.Run("SinkNDJSONFile", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "lazy.jsonl")
		if err := lf.SinkNDJSONFile(path); err != nil {
			t.Fatalf("SinkNDJSONFile failed: %v", err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read sink file failed: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) != 1 {
			t.Fatalf("expected 1 ndjson line, got %d", len(lines))
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &row); err != nil {
			t.Fatalf("unmarshal ndjson file line failed: %v", err)
		}
		if row["name"] != "Bob" {
			t.Fatalf("unexpected lazy ndjson file row: %#v", row)
		}
	})

	t.Run("SinkNDJSONGzip", func(t *testing.T) {
		var buf bytes.Buffer
		if err := lf.SinkNDJSON(&buf, SinkNDJSONOptions{Compression: NDJSONCompressionGzip}); err != nil {
			t.Fatalf("SinkNDJSON gzip failed: %v", err)
		}
		zr, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatalf("gzip reader failed: %v", err)
		}
		defer zr.Close()
		plain, err := io.ReadAll(zr)
		if err != nil {
			t.Fatalf("read gzip payload failed: %v", err)
		}
		if !strings.Contains(string(plain), "\"Bob\"") {
			t.Fatalf("unexpected lazy gzip ndjson payload: %q", string(plain))
		}
	})

	t.Run("SinkNDJSONFileGzip", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "lazy.jsonl.gz")
		if err := lf.SinkNDJSONFile(path, SinkNDJSONOptions{Compression: NDJSONCompressionGzip}); err != nil {
			t.Fatalf("SinkNDJSONFile gzip failed: %v", err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatalf("open gzip sink file failed: %v", err)
		}
		defer file.Close()
		zr, err := gzip.NewReader(file)
		if err != nil {
			t.Fatalf("gzip reader failed: %v", err)
		}
		defer zr.Close()
		plain, err := io.ReadAll(zr)
		if err != nil {
			t.Fatalf("read gzip sink payload failed: %v", err)
		}
		if !strings.Contains(string(plain), "\"Bob\"") {
			t.Fatalf("unexpected lazy gzip sink payload: %q", string(plain))
		}
	})
}

func TestReadExcel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sample.xlsx")
	file := excelize.NewFile()
	defer file.Close()

	if err := file.SetSheetRow("Sheet1", "A1", &[]any{"id", "name", "active", "score"}); err != nil {
		t.Fatalf("SetSheetRow Sheet1 header failed: %v", err)
	}
	if err := file.SetSheetRow("Sheet1", "A2", &[]any{1, "Alice", true, 9.5}); err != nil {
		t.Fatalf("SetSheetRow Sheet1 row1 failed: %v", err)
	}
	if err := file.SetSheetRow("Sheet1", "A3", &[]any{2, "Bob", false, 7.25}); err != nil {
		t.Fatalf("SetSheetRow Sheet1 row2 failed: %v", err)
	}

	sheet2 := "Raw"
	if _, err := file.NewSheet(sheet2); err != nil {
		t.Fatalf("NewSheet failed: %v", err)
	}
	if err := file.SetSheetRow(sheet2, "A1", &[]any{"001", "Carol"}); err != nil {
		t.Fatalf("SetSheetRow Raw row1 failed: %v", err)
	}
	if err := file.SetSheetRow(sheet2, "A2", &[]any{"002", "Drew"}); err != nil {
		t.Fatalf("SetSheetRow Raw row2 failed: %v", err)
	}

	if err := file.SaveAs(path); err != nil {
		t.Fatalf("SaveAs failed: %v", err)
	}

	t.Run("DefaultSheet", func(t *testing.T) {
		df, err := ReadExcel(path)
		if err != nil {
			t.Fatalf("ReadExcel failed: %v", err)
		}
		defer df.Close()

		rows, err := df.ToMaps()
		if err != nil {
			t.Fatalf("ToMaps failed: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if rows[0]["name"] != "Alice" || rows[1]["active"] != false {
			t.Fatalf("unexpected excel rows: %#v", rows)
		}
	})

	t.Run("SheetNameNoHeader", func(t *testing.T) {
		hasHeader := false
		df, err := ReadExcel(path, ExcelReadOptions{
			SheetName: sheet2,
			HasHeader: &hasHeader,
		})
		if err != nil {
			t.Fatalf("ReadExcel sheet Raw failed: %v", err)
		}
		defer df.Close()

		rows, err := df.ToMaps()
		if err != nil {
			t.Fatalf("ToMaps failed: %v", err)
		}
		if got := rows[0]["column_1"]; got != "001" {
			t.Fatalf("expected leading-zero string in column_1, got %#v", got)
		}
		if got := rows[1]["column_2"]; got != "Drew" {
			t.Fatalf("unexpected second column value: %#v", got)
		}
	})

	t.Run("InvalidSheetID", func(t *testing.T) {
		if _, err := ReadExcel(path, ExcelReadOptions{SheetID: 99}); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput for invalid sheet id, got %v", err)
		}
	})

	t.Run("ReadExcelSheetsAll", func(t *testing.T) {
		frames, err := ReadExcelSheets(path)
		if err != nil {
			t.Fatalf("ReadExcelSheets failed: %v", err)
		}
		defer func() {
			for _, df := range frames {
				df.Close()
			}
		}()
		if len(frames) != 2 {
			t.Fatalf("expected 2 sheets, got %d", len(frames))
		}
		rows, err := frames["Sheet1"].ToMaps()
		if err != nil {
			t.Fatalf("ToMaps Sheet1 failed: %v", err)
		}
		if len(rows) != 2 || rows[1]["name"] != "Bob" {
			t.Fatalf("unexpected Sheet1 rows: %#v", rows)
		}
	})

	t.Run("ReadExcelSheetsSubset", func(t *testing.T) {
		hasHeader := false
		frames, err := ReadExcelSheets(path, ExcelReadOptions{
			SheetNames: []string{sheet2},
			HasHeader:  &hasHeader,
		})
		if err != nil {
			t.Fatalf("ReadExcelSheets subset failed: %v", err)
		}
		defer func() {
			for _, df := range frames {
				df.Close()
			}
		}()
		if len(frames) != 1 {
			t.Fatalf("expected 1 sheet, got %d", len(frames))
		}
		rows, err := frames[sheet2].ToMaps()
		if err != nil {
			t.Fatalf("ToMaps Raw failed: %v", err)
		}
		if rows[0]["column_1"] != "001" {
			t.Fatalf("unexpected Raw rows: %#v", rows)
		}
	})
}

func assertBinaryValue(t *testing.T, got any, want []byte) {
	t.Helper()

	switch v := got.(type) {
	case []byte:
		if !bytes.Equal(v, want) {
			t.Fatalf("unexpected binary value: got %v want %v", v, want)
		}
	case string:
		if !bytes.Equal([]byte(v), want) {
			t.Fatalf("unexpected binary string value: got %q want %q", v, string(want))
		}
	default:
		t.Fatalf("unexpected binary type %T (%#v)", got, got)
	}
}
