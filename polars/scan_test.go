package polars

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

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
		fmt.Println(rows)

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
