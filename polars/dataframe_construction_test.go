package polars

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/isesword/polars-go/bridge"
)

func TestDataFrameFromMap(t *testing.T) {
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

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
	})

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
		if rows[2]["id"] != nil {
			t.Fatalf("Expected id[2] = nil, got %v", rows[2]["id"])
		}
		if rows[1]["name"] != nil {
			t.Fatalf("Expected name[1] = nil, got %v", rows[1]["name"])
		}
	})

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

		result, err := collectToMaps(t, df.
			Filter(Col("age").Gt(Lit(28))).
			Select(Col("name"), Col("salary")))
		if err != nil {
			t.Fatalf("Chained operations failed: %v", err)
		}

		if len(result) != 3 {
			t.Fatalf("Expected 3 rows after filter, got %d", len(result))
		}
	})

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

		if _, err := collectToMaps(t, df.Lazy()); err != nil {
			t.Fatalf("Failed to collect rows: %v", err)
		}
	})

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

		if err := df.Print(); err != nil {
			t.Fatalf("Failed to print DataFrame: %v", err)
		}
	})

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
