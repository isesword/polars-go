package polars

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/xuri/excelize/v2"
)

type benchmarkStructRow struct {
	ID         int64  `polars:"id"`
	Name       string `polars:"name"`
	Age        int64  `polars:"age"`
	Salary     int64  `polars:"salary"`
	Department string `polars:"department"`
}

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

func benchmarkStructRows(n int) []benchmarkStructRow {
	rows := benchmarkRows(n)
	out := make([]benchmarkStructRow, len(rows))
	for i, row := range rows {
		out[i] = benchmarkStructRow{
			ID:         row["id"].(int64),
			Name:       row["name"].(string),
			Age:        row["age"].(int64),
			Salary:     row["salary"].(int64),
			Department: row["department"].(string),
		}
	}
	return out
}

func benchmarkJoinFrames(b *testing.B, n int) (*DataFrame, *DataFrame) {
	b.Helper()
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
	left, err := NewDataFrame(leftRows, WithArrowSchema(arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "department", Type: arrow.BinaryTypes.String},
		{Name: "salary", Type: arrow.PrimitiveTypes.Int64},
	}, nil)))
	if err != nil {
		b.Fatalf("failed to create left join dataframe: %v", err)
	}
	right, err := NewDataFrame(rightRows, WithArrowSchema(arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "level", Type: arrow.BinaryTypes.String},
		{Name: "bonus", Type: arrow.PrimitiveTypes.Int64},
	}, nil)))
	if err != nil {
		left.Close()
		b.Fatalf("failed to create right join dataframe: %v", err)
	}
	return left, right
}

func benchmarkExcelFixture(b *testing.B, rows []map[string]any) string {
	b.Helper()
	path := filepath.Join(b.TempDir(), "bench.xlsx")
	file := excelize.NewFile()
	defer file.Close()

	if err := file.SetSheetRow("Sheet1", "A1", &[]any{"id", "name", "age", "salary", "department"}); err != nil {
		b.Fatalf("failed to write excel header: %v", err)
	}
	for i, row := range rows {
		cell := fmt.Sprintf("A%d", i+2)
		record := []any{
			row["id"],
			row["name"],
			row["age"],
			row["salary"],
			row["department"],
		}
		if err := file.SetSheetRow("Sheet1", cell, &record); err != nil {
			b.Fatalf("failed to write excel row %d: %v", i, err)
		}
	}
	if err := file.SaveAs(path); err != nil {
		b.Fatalf("failed to save excel benchmark fixture: %v", err)
	}
	return path
}

func benchmarkPrimitiveSchema() map[string]DataType {
	return map[string]DataType{
		"id":         DataTypeInt64,
		"name":       DataTypeUTF8,
		"age":        DataTypeInt64,
		"salary":     DataTypeInt64,
		"department": DataTypeUTF8,
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

func benchmarkCSVFixture(b *testing.B, rows []map[string]any) string {
	b.Helper()
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.csv")

	var sb strings.Builder
	sb.WriteString("id,name,age,salary,department\n")
	for _, row := range rows {
		sb.WriteString(fmt.Sprintf("%d,%s,%d,%d,%s\n",
			row["id"].(int64),
			row["name"].(string),
			row["age"].(int64),
			row["salary"].(int64),
			row["department"].(string),
		))
	}

	if err := os.WriteFile(path, []byte(sb.String()), 0o600); err != nil {
		b.Fatalf("failed to write benchmark csv fixture: %v", err)
	}
	return path
}

func requireBenchmarkBridge(b *testing.B) {
	b.Helper()
	if _, err := resolveBridge(nil); err != nil {
		b.Skipf("bridge unavailable for benchmark: %v", err)
	}
}

func benchmarkSizeLabel(n int) string {
	return "Rows" + strconv.Itoa(n)
}

func BenchmarkImportRows(b *testing.B) {
	requireBenchmarkBridge(b)
	for _, size := range []int{200, 2000, 10000} {
		rows := benchmarkRows(size)
		schema := benchmarkPrimitiveSchema()
		arrowSchema := benchmarkArrowSchema()

		b.Run(benchmarkSizeLabel(size), func(b *testing.B) {
			b.Run("JSONWithSchema", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					df, err := NewDataFrameFromRowsWithSchema(rows, schema)
					if err != nil {
						b.Fatalf("json import failed: %v", err)
					}
					df.Close()
				}
			})

			b.Run("ArrowSchema", func(b *testing.B) {
				if !zeroCopySupported() {
					b.Skip("arrow row conversion requires cgo")
				}
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					df, err := NewDataFrameFromRowsWithArrowSchema(rows, arrowSchema)
					if err != nil {
						b.Fatalf("arrow import failed: %v", err)
					}
					df.Close()
				}
			})
		})
	}
}

func BenchmarkQueryCollect(b *testing.B) {
	requireBenchmarkBridge(b)
	for _, size := range []int{1000, 4000, 16000} {
		rows := benchmarkRows(size)
		csvPath := benchmarkCSVFixture(b, rows)

		b.Run(benchmarkSizeLabel(size), func(b *testing.B) {
			b.Run("InMemoryArrowBacked", func(b *testing.B) {
				if !zeroCopySupported() {
					b.Skip("arrow row conversion requires cgo")
				}
				df, err := NewDataFrame(rows, WithArrowSchema(benchmarkArrowSchema()))
				if err != nil {
					b.Fatalf("failed to create in-memory dataframe: %v", err)
				}
				defer df.Close()

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := collectToMapsBenchmark(df.
						Filter(Col("age").Gt(Lit(30))).
						Select(Col("name"), Col("salary"), Col("department")))
					if err != nil {
						b.Fatalf("in-memory collect failed: %v", err)
					}
					if len(out) == 0 {
						b.Fatal("expected non-empty in-memory benchmark result")
					}
				}
			})

			b.Run("CSVScan", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := collectToMapsBenchmark(ScanCSV(csvPath).
						Filter(Col("age").Gt(Lit(30))).
						Select(Col("name"), Col("salary"), Col("department")))
					if err != nil {
						b.Fatalf("csv collect failed: %v", err)
					}
					if len(out) == 0 {
						b.Fatal("expected non-empty csv benchmark result")
					}
				}
			})
		})
	}
}

func BenchmarkToMaps(b *testing.B) {
	requireBenchmarkBridge(b)
	if !zeroCopySupported() {
		b.Skip("arrow row conversion requires cgo")
	}
	for _, size := range []int{1000, 4000, 16000} {
		df, err := NewDataFrame(benchmarkRows(size), WithArrowSchema(benchmarkArrowSchema()))
		if err != nil {
			b.Fatalf("failed to create dataframe: %v", err)
		}
		defer df.Close()

		b.Run(benchmarkSizeLabel(size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := df.ToMaps()
				if err != nil {
					b.Fatalf("ToMaps failed: %v", err)
				}
				if len(rows) == 0 {
					b.Fatal("expected non-empty ToMaps result")
				}
			}
		})
	}
}

func BenchmarkToStructs(b *testing.B) {
	requireBenchmarkBridge(b)
	if !zeroCopySupported() {
		b.Skip("struct export requires cgo")
	}

	for _, size := range []int{1000, 4000, 16000} {
		df, err := NewDataFrame(benchmarkRows(size), WithArrowSchema(benchmarkArrowSchema()))
		if err != nil {
			b.Fatalf("failed to create dataframe: %v", err)
		}
		defer df.Close()

		b.Run(benchmarkSizeLabel(size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := ToStructs[benchmarkStructRow](df)
				if err != nil {
					b.Fatalf("ToStructs failed: %v", err)
				}
				if len(rows) == 0 {
					b.Fatal("expected non-empty ToStructs result")
				}
			}
		})
	}
}

func BenchmarkToStructPointers(b *testing.B) {
	requireBenchmarkBridge(b)
	if !zeroCopySupported() {
		b.Skip("struct pointer export requires cgo")
	}

	for _, size := range []int{1000, 4000, 16000} {
		df, err := NewDataFrame(benchmarkRows(size), WithArrowSchema(benchmarkArrowSchema()))
		if err != nil {
			b.Fatalf("failed to create dataframe: %v", err)
		}
		defer df.Close()

		b.Run(benchmarkSizeLabel(size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := ToStructPointers[benchmarkStructRow](df)
				if err != nil {
					b.Fatalf("ToStructPointers failed: %v", err)
				}
				if len(rows) == 0 || rows[0] == nil {
					b.Fatal("expected non-empty ToStructPointers result")
				}
			}
		})
	}
}

func BenchmarkStructImport(b *testing.B) {
	requireBenchmarkBridge(b)
	for _, size := range []int{1000, 4000, 16000} {
		rows := benchmarkStructRows(size)

		b.Run(benchmarkSizeLabel(size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				df, err := NewDataFrame(rows)
				if err != nil {
					b.Fatalf("struct import failed: %v", err)
				}
				df.Close()
			}
		})
	}
}

func BenchmarkJoin(b *testing.B) {
	requireBenchmarkBridge(b)
	if !zeroCopySupported() {
		b.Skip("join benchmark requires cgo-backed frames")
	}
	for _, size := range []int{1000, 4000, 16000} {
		left, right := benchmarkJoinFrames(b, size)
		defer left.Close()
		defer right.Close()

		b.Run(benchmarkSizeLabel(size), func(b *testing.B) {
			b.Run("Inner", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					rows, err := collectToMapsBenchmark(left.Join(right, JoinOptions{
						On:  []string{"id"},
						How: JoinInner,
					}).Select(Col("id"), Col("salary"), Col("bonus")))
					if err != nil {
						b.Fatalf("inner join failed: %v", err)
					}
					if len(rows) == 0 {
						b.Fatal("expected non-empty inner join result")
					}
				}
			})

			b.Run("Left", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					rows, err := collectToMapsBenchmark(left.Join(right, JoinOptions{
						On:  []string{"id"},
						How: JoinLeft,
					}).Select(Col("id"), Col("salary"), Col("bonus")))
					if err != nil {
						b.Fatalf("left join failed: %v", err)
					}
					if len(rows) == 0 {
						b.Fatal("expected non-empty left join result")
					}
				}
			})
		})
	}
}

func BenchmarkGroupBy(b *testing.B) {
	requireBenchmarkBridge(b)
	for _, size := range []int{1000, 4000, 16000} {
		df, err := NewDataFrame(benchmarkRows(size), WithArrowSchema(benchmarkArrowSchema()))
		if err != nil {
			b.Fatalf("failed to create dataframe: %v", err)
		}
		defer df.Close()

		b.Run(benchmarkSizeLabel(size), func(b *testing.B) {
			b.Run("DepartmentAgg", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					rows, err := collectToMapsBenchmark(df.GroupBy("department").Agg(
						Col("salary").Sum().Alias("salary_sum"),
						Col("id").Count().Alias("employee_count"),
						Col("age").Mean().Alias("avg_age"),
					))
					if err != nil {
						b.Fatalf("groupby agg failed: %v", err)
					}
					if len(rows) == 0 {
						b.Fatal("expected non-empty groupby result")
					}
				}
			})

			b.Run("DepartmentValueCounts", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					rows, err := collectToMapsBenchmark(df.GroupBy("department").Agg(
						Col("name").NUnique().Alias("unique_names"),
						Col("salary").Max().Alias("max_salary"),
					))
					if err != nil {
						b.Fatalf("groupby n_unique failed: %v", err)
					}
					if len(rows) == 0 {
						b.Fatal("expected non-empty groupby n_unique result")
					}
				}
			})
		})
	}
}

func BenchmarkNDJSON(b *testing.B) {
	requireBenchmarkBridge(b)
	for _, size := range []int{1000, 4000, 16000} {
		df, err := NewDataFrame(benchmarkRows(size), WithArrowSchema(benchmarkArrowSchema()))
		if err != nil {
			b.Fatalf("failed to create dataframe: %v", err)
		}
		defer df.Close()
		lf := df.Filter(Col("age").Gt(Lit(30))).Select(Col("id"), Col("name"), Col("department"))
		sinkPath := filepath.Join(b.TempDir(), fmt.Sprintf("bench-%d.jsonl", size))
		sinkGzipPath := filepath.Join(b.TempDir(), fmt.Sprintf("bench-%d.jsonl.gz", size))

		b.Run(benchmarkSizeLabel(size), func(b *testing.B) {
			b.Run("WriteNDJSON", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var buf bytes.Buffer
					if err := df.WriteNDJSON(&buf); err != nil {
						b.Fatalf("WriteNDJSON failed: %v", err)
					}
					if buf.Len() == 0 {
						b.Fatal("expected non-empty WriteNDJSON output")
					}
				}
			})

			b.Run("WriteNDJSONGzip", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var buf bytes.Buffer
					if err := df.WriteNDJSON(&buf, WriteNDJSONOptions{Compression: NDJSONCompressionGzip}); err != nil {
						b.Fatalf("WriteNDJSON gzip failed: %v", err)
					}
					zr, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
					if err != nil {
						b.Fatalf("gzip reader failed: %v", err)
					}
					plain, err := io.ReadAll(zr)
					_ = zr.Close()
					if err != nil {
						b.Fatalf("read gzip payload failed: %v", err)
					}
					if len(plain) == 0 {
						b.Fatal("expected non-empty gzip ndjson output")
					}
				}
			})

			b.Run("SinkNDJSONFile", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := lf.SinkNDJSONFile(sinkPath); err != nil {
						b.Fatalf("SinkNDJSONFile failed: %v", err)
					}
					info, err := os.Stat(sinkPath)
					if err != nil {
						b.Fatalf("stat SinkNDJSONFile output failed: %v", err)
					}
					if info.Size() == 0 {
						b.Fatal("expected non-empty SinkNDJSONFile output")
					}
				}
			})

			b.Run("SinkNDJSONFileGzip", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := lf.SinkNDJSONFile(sinkGzipPath, SinkNDJSONOptions{Compression: NDJSONCompressionGzip}); err != nil {
						b.Fatalf("SinkNDJSONFile gzip failed: %v", err)
					}
					info, err := os.Stat(sinkGzipPath)
					if err != nil {
						b.Fatalf("stat gzip SinkNDJSONFile output failed: %v", err)
					}
					if info.Size() == 0 {
						b.Fatal("expected non-empty gzip SinkNDJSONFile output")
					}
				}
			})
		})
	}
}

func BenchmarkExcel(b *testing.B) {
	requireBenchmarkBridge(b)
	for _, size := range []int{200, 1000, 4000} {
		path := benchmarkExcelFixture(b, benchmarkRows(size))

		b.Run(benchmarkSizeLabel(size), func(b *testing.B) {
			b.Run("ReadExcel", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					df, err := ReadExcel(path)
					if err != nil {
						b.Fatalf("ReadExcel failed: %v", err)
					}
					rows, err := df.ToMaps()
					df.Close()
					if err != nil {
						b.Fatalf("ReadExcel ToMaps failed: %v", err)
					}
					if len(rows) == 0 {
						b.Fatal("expected non-empty ReadExcel result")
					}
				}
			})

			b.Run("ReadExcelWithSchema", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					df, err := ReadExcel(path, ExcelReadOptions{
						Schema: benchmarkPrimitiveSchema(),
					})
					if err != nil {
						b.Fatalf("ReadExcel with schema failed: %v", err)
					}
					rows, err := df.ToMaps()
					df.Close()
					if err != nil {
						b.Fatalf("ReadExcel with schema ToMaps failed: %v", err)
					}
					if len(rows) == 0 {
						b.Fatal("expected non-empty ReadExcel with schema result")
					}
				}
			})
		})
	}
}

func BenchmarkExprMapBatches(b *testing.B) {
	requireBenchmarkBridge(b)
	if !zeroCopySupported() {
		b.Skip("Expr.MapBatches requires cgo")
	}

	for _, size := range []int{1000, 4000} {
		df, err := NewDataFrame(benchmarkRows(size), WithArrowSchema(benchmarkArrowSchema()))
		if err != nil {
			b.Fatalf("failed to create dataframe: %v", err)
		}
		defer df.Close()

		b.Run(benchmarkSizeLabel(size), func(b *testing.B) {
			b.Run("SingleInput", func(b *testing.B) {
				expr := Col("age").MapBatches(func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
					col := batch.Column(0).(*array.Int64)
					builder := array.NewInt64Builder(memory.NewGoAllocator())
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
					return array.NewRecordBatch(schema, []arrow.Array{values}, int64(col.Len())), nil
				}, ExprMapBatchesOptions{ReturnType: DataTypeInt64}).Alias("age_plus_ten")

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := collectToMapsBenchmark(df.Select(expr))
					if err != nil {
						b.Fatalf("single-input Expr.MapBatches failed: %v", err)
					}
					if len(out) == 0 {
						b.Fatal("expected non-empty single-input Expr.MapBatches result")
					}
				}
			})

			b.Run("MultiInput", func(b *testing.B) {
				expr := MapBatches([]Expr{Col("age"), Col("salary")}, func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
					ageCol := batch.Column(0).(*array.Int64)
					salaryCol := batch.Column(1).(*array.Int64)
					builder := array.NewInt64Builder(memory.NewGoAllocator())
					defer builder.Release()
					for i := 0; i < ageCol.Len(); i++ {
						if ageCol.IsNull(i) || salaryCol.IsNull(i) {
							builder.AppendNull()
							continue
						}
						builder.Append(ageCol.Value(i) + salaryCol.Value(i))
					}
					values := builder.NewArray()
					defer values.Release()
					schema := arrow.NewSchema([]arrow.Field{
						{Name: "age_salary", Type: arrow.PrimitiveTypes.Int64},
					}, nil)
					return array.NewRecordBatch(schema, []arrow.Array{values}, int64(ageCol.Len())), nil
				}, ExprMapBatchesOptions{ReturnType: DataTypeInt64}).Alias("age_salary")

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := collectToMapsBenchmark(df.Select(expr))
					if err != nil {
						b.Fatalf("multi-input MapBatches failed: %v", err)
					}
					if len(out) == 0 {
						b.Fatal("expected non-empty multi-input MapBatches result")
					}
				}
			})
		})
	}
}

func BenchmarkExprVsMapBatches(b *testing.B) {
	requireBenchmarkBridge(b)
	if !zeroCopySupported() {
		b.Skip("Expr.MapBatches requires cgo")
	}

	for _, size := range []int{1000, 4000} {
		df, err := NewDataFrame(benchmarkRows(size), WithArrowSchema(benchmarkArrowSchema()))
		if err != nil {
			b.Fatalf("failed to create dataframe: %v", err)
		}
		defer df.Close()

		b.Run(benchmarkSizeLabel(size), func(b *testing.B) {
			nativeSingle := Col("age").Add(Lit(int64(10))).Alias("age_plus_ten")
			mapSingle := Col("age").MapBatches(func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
				col := batch.Column(0).(*array.Int64)
				builder := array.NewInt64Builder(memory.NewGoAllocator())
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
				return array.NewRecordBatch(schema, []arrow.Array{values}, int64(col.Len())), nil
			}, ExprMapBatchesOptions{ReturnType: DataTypeInt64}).Alias("age_plus_ten")
			mapMulti := MapBatches([]Expr{Col("age"), Col("salary")}, func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
				ageCol := batch.Column(0).(*array.Int64)
				salaryCol := batch.Column(1).(*array.Int64)
				builder := array.NewInt64Builder(memory.NewGoAllocator())
				defer builder.Release()
				for i := 0; i < ageCol.Len(); i++ {
					if ageCol.IsNull(i) || salaryCol.IsNull(i) {
						builder.AppendNull()
						continue
					}
					builder.Append(ageCol.Value(i) + salaryCol.Value(i))
				}
				values := builder.NewArray()
				defer values.Release()
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "age_salary", Type: arrow.PrimitiveTypes.Int64},
				}, nil)
				return array.NewRecordBatch(schema, []arrow.Array{values}, int64(ageCol.Len())), nil
			}, ExprMapBatchesOptions{ReturnType: DataTypeInt64}).Alias("age_salary")
			nativeMulti := Col("age").Add(Col("salary")).Alias("age_salary")

			b.Run("NativeSingle", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := collectToMapsBenchmark(df.Select(nativeSingle))
					if err != nil {
						b.Fatalf("native single expr failed: %v", err)
					}
					if len(out) == 0 {
						b.Fatal("expected non-empty native single result")
					}
				}
			})

			b.Run("MapBatchesSingle", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := collectToMapsBenchmark(df.Select(mapSingle))
					if err != nil {
						b.Fatalf("map single expr failed: %v", err)
					}
					if len(out) == 0 {
						b.Fatal("expected non-empty map single result")
					}
				}
			})

			b.Run("NativeMulti", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := collectToMapsBenchmark(df.Select(nativeMulti))
					if err != nil {
						b.Fatalf("native multi expr failed: %v", err)
					}
					if len(out) == 0 {
						b.Fatal("expected non-empty native multi result")
					}
				}
			})

			b.Run("MapBatchesMulti", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := collectToMapsBenchmark(df.Select(mapMulti))
					if err != nil {
						b.Fatalf("map multi expr failed: %v", err)
					}
					if len(out) == 0 {
						b.Fatal("expected non-empty map multi result")
					}
				}
			})
		})
	}
}

func collectToMapsBenchmark(lf *LazyFrame) ([]map[string]interface{}, error) {
	df, err := lf.Collect()
	if err != nil {
		return nil, err
	}
	defer df.Free()
	return df.ToMaps()
}
