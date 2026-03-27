package polars

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

	type benchRow struct {
		ID         int64   `polars:"id"`
		Name       string  `polars:"name"`
		Age        int64   `polars:"age"`
		Salary     float64 `polars:"salary"`
		Department string  `polars:"department"`
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
				rows, err := ToStructs[benchRow](df)
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

	type benchRow struct {
		ID         int64   `polars:"id"`
		Name       string  `polars:"name"`
		Age        int64   `polars:"age"`
		Salary     float64 `polars:"salary"`
		Department string  `polars:"department"`
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
				rows, err := ToStructPointers[benchRow](df)
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
