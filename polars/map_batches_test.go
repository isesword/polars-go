package polars

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

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
