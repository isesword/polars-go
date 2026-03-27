package main

import (
	"fmt"
	"log"

	"github.com/isesword/polars-go/polars"
)

func main() {
	fmt.Println("=== Polars Go Bridge - CSV Scan Example ===")

	// 示例 1: 基本 CSV 扫描（Print 模式不需要 Free）
	fmt.Println("📖 示例 1: 基本 CSV 扫描")
	fmt.Println("代码: ScanCSV(\"testdata/sample.csv\").Print()")
	lf := polars.ScanCSV("testdata/sample.csv")
	err := lf.Print()
	if err != nil {
		log.Fatalf("Failed to print: %v", err)
	}

	// 示例 2: Filter - 筛选年龄大于 28 的记录
	fmt.Println("\n📖 示例 2: Filter - 筛选年龄大于 28 的记录")
	fmt.Println("代码: ScanCSV().Filter(Col(\"age\").Gt(Lit(28))).Print()")
	lf2 := polars.ScanCSV("testdata/sample.csv").
		Filter(polars.Col("age").Gt(polars.Lit(28)))
	err = lf2.Print()
	if err != nil {
		log.Fatalf("Failed to print: %v", err)
	}

	// 示例 3: Select - 只选择特定列
	fmt.Println("\n📖 示例 3: Select - 只选择 name 和 age 列")
	fmt.Println("代码: ScanCSV().Select(Col(\"name\"), Col(\"age\")).Print()")
	lf3 := polars.ScanCSV("testdata/sample.csv").
		Select(polars.Col("name"), polars.Col("age"))
	err = lf3.Print()
	if err != nil {
		log.Fatalf("Failed to print: %v", err)
	}

	// 示例 4: 组合操作 - Filter + Select + WithColumns + Limit
	fmt.Println("\n📖 示例 4: 组合操作 - Filter + Select + WithColumns + Limit")
	fmt.Println("代码: ScanCSV().Filter(...).Select(...).WithColumns(...).Limit(3).Print()")
	query := polars.ScanCSV("testdata/sample.csv").
		Filter(polars.Col("age").Gt(polars.Lit(25))).
		Select(polars.Col("name"), polars.Col("age"), polars.Col("salary"), polars.Col("department")).
		WithColumns(
			polars.Col("age").Add(polars.Lit(1)).Alias("next_year_age"),
		).
		Limit(3)
	err = query.Print()
	if err != nil {
		log.Fatalf("Failed to print: %v", err)
	}

	// 示例 5: GroupBy + Aggregation
	fmt.Println("\n📖 示例 5: GroupBy + Aggregation")
	fmt.Println("代码: ScanCSV().GroupBy(\"department\").Agg(...).Print()")
	summary := polars.ScanCSV("testdata/sample.csv").
		GroupBy("department").
		Agg(
			polars.Col("salary").Sum().Alias("total_salary"),
			polars.Col("name").Count().Alias("employee_count"),
		).
		Sort(polars.SortOptions{
			By: []polars.Expr{polars.Col("department")},
		})
	err = summary.Print()
	if err != nil {
		log.Fatalf("Failed to print summary: %v", err)
	}

	// 示例 6: Sort / Unique
	fmt.Println("\n📖 示例 6: Sort / Unique")
	fmt.Println("代码: ScanCSV().Select(...).Unique(...).Sort(...).Print()")
	uniqueDepartments := polars.ScanCSV("testdata/sample.csv").
		Select(polars.Col("department")).
		Unique(polars.UniqueOptions{
			Subset: []string{"department"},
			Keep:   "first",
		}).
		Sort(polars.SortOptions{
			By: []polars.Expr{polars.Col("department")},
		})
	err = uniqueDepartments.Print()
	if err != nil {
		log.Fatalf("Failed to print unique departments: %v", err)
	}

	// 示例 7: 使用 Collect 获取 DataFrame（需要显式 Free）
	fmt.Println("\n📖 示例 7: 资源管理 - 使用 Collect 必须调用 Free")
	fmt.Println("代码: df, _ := query.Collect(); defer df.Free(); rows, _ := df.ToMaps()")
	resultDF, err := query.Collect()
	if err != nil {
		log.Fatalf("Failed to collect: %v", err)
	}
	defer resultDF.Free() // ⚠️ 重要：必须调用 Free 释放资源
	rows, err := resultDF.ToMaps()
	if err != nil {
		log.Fatalf("Failed to convert to maps: %v", err)
	}
	fmt.Printf("Rows: %#v\n", rows)
	err = resultDF.Print()
	if err != nil {
		log.Fatalf("Failed to print: %v", err)
	}

	// 示例 8: 复杂过滤 - 工程部门且工资大于 60000
	fmt.Println("\n📖 示例 8: 复杂过滤 - Engineering 部门且工资大于 60000")
	fmt.Println("代码: ScanCSV().Filter(Col(\"department\").Eq(Lit(\"Engineering\")).And(Col(\"salary\").Gt(Lit(60000)))).Print()")
	lf5 := polars.ScanCSV("testdata/sample.csv").
		Filter(
			polars.Col("department").Eq(polars.Lit("Engineering")).
				And(polars.Col("salary").Gt(polars.Lit(60000))),
		)
	err = lf5.Print()
	if err != nil {
		log.Fatalf("Failed to print: %v", err)
	}

	fmt.Println("\n✅ 所有示例执行成功！")
}
