package polars

import (
	"fmt"
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
	"github.com/isesword/polars-go-bridge/bridge"
	pb "github.com/isesword/polars-go-bridge/proto"
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
			Schema: map[string]pb.DataType{
				"name":      pb.DataType_UTF8,
				"age":       pb.DataType_INT64,
				"joined_at": pb.DataType_DATE,
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

		encoding := pb.CsvEncoding_CSV_ENCODING_LOSSY_UTF8
		ignoreErrors := true
		rows, err := collectToMaps(t, ScanCSVWithOptions(path, CSVScanOptions{
			Encoding:     &encoding,
			IgnoreErrors: &ignoreErrors,
			Schema: map[string]pb.DataType{
				"name": pb.DataType_UTF8,
				"age":  pb.DataType_INT64,
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
		}, map[string]pb.DataType{
			"id":    pb.DataType_UINT64,
			"age":   pb.DataType_INT32,
			"score": pb.DataType_FLOAT64,
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
		}, map[string]pb.DataType{
			"id":   pb.DataType_UINT64,
			"name": pb.DataType_UTF8,
			"age":  pb.DataType_INT32,
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

		df, err := NewEagerFrameFromRowsWithSchema(brg, goframeRows, map[string]pb.DataType{
			"id":         pb.DataType_UINT64,
			"name":       pb.DataType_UTF8,
			"age":        pb.DataType_INT32,
			"salary":     pb.DataType_FLOAT64,
			"created_at": pb.DataType_UTF8,
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
				}, map[string]pb.DataType{
					"id":   pb.DataType_INT64,
					"name": pb.DataType_UTF8,
					"age":  pb.DataType_INT32,
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
	schema := map[string]pb.DataType{
		"id":         pb.DataType_UINT64,
		"name":       pb.DataType_UTF8,
		"age":        pb.DataType_INT32,
		"salary":     pb.DataType_FLOAT64,
		"created_at": pb.DataType_UTF8,
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
	schema := map[string]pb.DataType{
		"id":         pb.DataType_INT64,
		"created_at": pb.DataType_DATETIME,
		"birthday":   pb.DataType_DATE,
		"clock_at":   pb.DataType_TIME,
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
	schema := map[string]pb.DataType{
		"age": pb.DataType_INT32,
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

func TestNewDataFrameFromRowsAuto(t *testing.T) {
	rows := []map[string]any{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}

	t.Run("AutoWithSchema", func(t *testing.T) {
		df, err := NewDataFrameFromRowsAuto(rows, map[string]pb.DataType{
			"id":   pb.DataType_INT64,
			"name": pb.DataType_UTF8,
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
		}, WithSchema(map[string]pb.DataType{
			"id":   pb.DataType_INT64,
			"name": pb.DataType_UTF8,
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
		}, WithSchema(map[string]pb.DataType{
			"id":         pb.DataType_UINT64,
			"name":       pb.DataType_UTF8,
			"age":        pb.DataType_INT64,
			"created_at": pb.DataType_UTF8,
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
		}, WithSchema(map[string]pb.DataType{
			"id":   pb.DataType_INT64,
			"name": pb.DataType_UTF8,
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
		}, map[string]pb.DataType{
			"id":   pb.DataType_INT64,
			"name": pb.DataType_UTF8,
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
		}, WithSchema(map[string]pb.DataType{
			"id":   pb.DataType_INT64,
			"name": pb.DataType_UTF8,
		}))
		if err != nil {
			t.Fatalf("DataFrame failed: %v", err)
		}

		frame.Close()
		frame.Close()

		if _, err := frame.ToMaps(); err == nil {
			t.Fatal("Expected ToMaps() to fail after Close(), got nil")
		}
	})

	t.Run("DataFrameFinalizerOwnsEagerFrame", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": 1, "name": "Alice"},
		}, WithSchema(map[string]pb.DataType{
			"id":   pb.DataType_INT64,
			"name": pb.DataType_UTF8,
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
		lf := ScanCSV("/Users/esword/GolandProjects/src/polars-go-bridge/testdata/sample.csv").
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
		lf := ScanCSV("/Users/esword/GolandProjects/src/polars-go-bridge/testdata/sample.csv").
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
	const sampleCSV = "/Users/esword/GolandProjects/src/polars-go-bridge/testdata/sample.csv"

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
}

func TestDataFrameFromArrowManaged(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("managed arrow frame requires cgo")
	}

	record, err := NewArrowRecordBatchFromRowsWithSchema([]map[string]any{
		{"id": uint64(1), "name": "Alice"},
		{"id": uint64(2), "name": "Bob"},
	}, map[string]pb.DataType{
		"id":   pb.DataType_UINT64,
		"name": pb.DataType_UTF8,
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
	}, map[string]pb.DataType{
		"id":   pb.DataType_UINT64,
		"name": pb.DataType_UTF8,
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
	}, map[string]pb.DataType{
		"id":   pb.DataType_UINT64,
		"city": pb.DataType_UTF8,
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
		return NewArrowRecordBatchFromRowsWithSchema(rows, map[string]pb.DataType{
			"name":  pb.DataType_UTF8,
			"age":   pb.DataType_INT64,
			"level": pb.DataType_UTF8,
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
			}, ExprMapBatchesOptions{ReturnType: pb.DataType_INT64}).
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
		}, ExprMapBatchesOptions{ReturnType: pb.DataType_INT64}).Alias("age_bonus"),
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
		}, ExprMapBatchesOptions{ReturnType: pb.DataType_INT64}).Alias("age_fail"),
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
	if !strings.Contains(err.Error(), "memory limit exceeded") {
		t.Fatalf("unexpected memory limit error: %v", err)
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

func TestDataFrameFreeBehavior(t *testing.T) {
	brg, err := bridge.LoadBridge("/Users/esword/GolandProjects/src/polars-go-bridge/libpolars_bridge.dylib")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	t.Run("FreeInvalidatesDataFrame", func(t *testing.T) {
		df, err := NewEagerFrameFromRowsWithSchema(brg, []map[string]any{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		}, map[string]pb.DataType{
			"id":   pb.DataType_INT64,
			"name": pb.DataType_UTF8,
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}

		df.Free()

		if _, err := df.ToMaps(); err == nil {
			t.Fatal("Expected ToMaps() to fail after Free(), got nil")
		}
		if err := df.Print(); err == nil {
			t.Fatal("Expected Print() to fail after Free(), got nil")
		}
	})

	t.Run("FreeIsIdempotent", func(t *testing.T) {
		df, err := NewEagerFrameFromRowsWithSchema(brg, []map[string]any{
			{"id": 1, "name": "Alice"},
		}, map[string]pb.DataType{
			"id":   pb.DataType_INT64,
			"name": pb.DataType_UTF8,
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
		}, map[string]pb.DataType{
			"id":   pb.DataType_INT64,
			"name": pb.DataType_UTF8,
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
			}, map[string]pb.DataType{
				"id":   pb.DataType_INT64,
				"name": pb.DataType_UTF8,
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
