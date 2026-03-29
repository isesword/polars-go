package polars

import (
	"testing"
	"time"
)

func TestCollectHelpers(t *testing.T) {
	rows := []map[string]any{
		{"id": int64(1), "name": "Alice", "score": float64(1.5)},
		{"id": int64(2), "name": "Bob", "score": float64(2.5)},
		{"id": int64(3), "name": "Alice", "score": float64(3.5)},
	}
	schema := map[string]DataType{
		"id":    DataTypeInt64,
		"name":  DataTypeUTF8,
		"score": DataTypeFloat64,
	}

	t.Run("CollectMaps", func(t *testing.T) {
		out, err := CollectMaps(rows, schema, func(df *DataFrame) *LazyFrame {
			return df.Filter(Col("id").Gt(Lit(1))).Select(Col("name"))
		})
		if err != nil {
			t.Fatalf("CollectMaps failed: %v", err)
		}
		if len(out) != 2 || out[0]["name"] != "Bob" || out[1]["name"] != "Alice" {
			t.Fatalf("unexpected CollectMaps result: %#v", out)
		}
	})

	t.Run("CollectSliceString", func(t *testing.T) {
		out, err := CollectSliceString(rows, schema, func(df *DataFrame) *LazyFrame {
			return df.Select(Col("name")).Unique(UniqueOptions{
				Subset:        []string{"name"},
				MaintainOrder: true,
			})
		}, "")
		if err != nil {
			t.Fatalf("CollectSliceString failed: %v", err)
		}
		if len(out) != 2 || out[0] != "Alice" || out[1] != "Bob" {
			t.Fatalf("unexpected CollectSliceString result: %#v", out)
		}
	})

	t.Run("CollectSliceInt64", func(t *testing.T) {
		out, err := CollectSliceInt64(rows, schema, func(df *DataFrame) *LazyFrame {
			return df.Select(Col("id"))
		}, "")
		if err != nil {
			t.Fatalf("CollectSliceInt64 failed: %v", err)
		}
		if len(out) != 3 || out[0] != 1 || out[2] != 3 {
			t.Fatalf("unexpected CollectSliceInt64 result: %#v", out)
		}
	})

	t.Run("CollectSliceFloat64", func(t *testing.T) {
		out, err := CollectSliceFloat64(rows, schema, func(df *DataFrame) *LazyFrame {
			return df.Select(Col("score"))
		}, "")
		if err != nil {
			t.Fatalf("CollectSliceFloat64 failed: %v", err)
		}
		if len(out) != 3 || out[0] != 1.5 || out[2] != 3.5 {
			t.Fatalf("unexpected CollectSliceFloat64 result: %#v", out)
		}
	})

	t.Run("CollectSliceUInt64", func(t *testing.T) {
		uintRows := []map[string]any{
			{"id": uint64(1)},
			{"id": uint64(2)},
		}
		uintSchema := map[string]DataType{"id": DataTypeUInt64}
		out, err := CollectSliceUInt64(uintRows, uintSchema, func(df *DataFrame) *LazyFrame {
			return df.Select(Col("id"))
		}, "")
		if err != nil {
			t.Fatalf("CollectSliceUInt64 failed: %v", err)
		}
		if len(out) != 2 || out[0] != 1 || out[1] != 2 {
			t.Fatalf("unexpected CollectSliceUInt64 result: %#v", out)
		}
	})

	t.Run("CollectSliceBool", func(t *testing.T) {
		boolRows := []map[string]any{
			{"active": true},
			{"active": false},
		}
		boolSchema := map[string]DataType{"active": DataTypeBool}
		out, err := CollectSliceBool(boolRows, boolSchema, func(df *DataFrame) *LazyFrame {
			return df.Select(Col("active"))
		}, "")
		if err != nil {
			t.Fatalf("CollectSliceBool failed: %v", err)
		}
		if len(out) != 2 || out[0] != true || out[1] != false {
			t.Fatalf("unexpected CollectSliceBool result: %#v", out)
		}
	})

	t.Run("CollectSliceTime", func(t *testing.T) {
		ts1 := time.Date(2026, 3, 30, 10, 0, 0, 0, time.UTC)
		ts2 := time.Date(2026, 3, 30, 11, 0, 0, 0, time.UTC)
		timeRows := []map[string]any{
			{"created_at": ts1},
			{"created_at": ts2},
		}
		timeSchema := map[string]DataType{"created_at": DataTypeDatetime}
		out, err := CollectSliceTime(timeRows, timeSchema, func(df *DataFrame) *LazyFrame {
			return df.Select(Col("created_at"))
		}, "")
		if err != nil {
			t.Fatalf("CollectSliceTime failed: %v", err)
		}
		if len(out) != 2 || !out[0].Equal(ts1) || !out[1].Equal(ts2) {
			t.Fatalf("unexpected CollectSliceTime result: %#v", out)
		}
	})

	t.Run("CollectSliceBytes", func(t *testing.T) {
		byteRows := []map[string]any{
			{"payload": []byte("abc")},
			{"payload": []byte("xyz")},
		}
		out, err := CollectSliceBytes(byteRows, nil, func(df *DataFrame) *LazyFrame {
			return df.Select(Col("payload"))
		}, "")
		if err != nil {
			t.Fatalf("CollectSliceBytes failed: %v", err)
		}
		if len(out) != 2 || string(out[0]) != "abc" || string(out[1]) != "xyz" {
			t.Fatalf("unexpected CollectSliceBytes result: %#v", out)
		}
	})

	t.Run("CollectSliceInt32", func(t *testing.T) {
		rows32 := []map[string]any{
			{"value": int32(10)},
			{"value": int32(20)},
		}
		schema32 := map[string]DataType{"value": DataTypeInt32}
		out, err := CollectSliceInt32(rows32, schema32, func(df *DataFrame) *LazyFrame {
			return df.Select(Col("value"))
		}, "")
		if err != nil {
			t.Fatalf("CollectSliceInt32 failed: %v", err)
		}
		if len(out) != 2 || out[0] != 10 || out[1] != 20 {
			t.Fatalf("unexpected CollectSliceInt32 result: %#v", out)
		}
	})

	t.Run("CollectSliceUInt32", func(t *testing.T) {
		rows32 := []map[string]any{
			{"value": uint32(10)},
			{"value": uint32(20)},
		}
		schema32 := map[string]DataType{"value": DataTypeUInt32}
		out, err := CollectSliceUInt32(rows32, schema32, func(df *DataFrame) *LazyFrame {
			return df.Select(Col("value"))
		}, "")
		if err != nil {
			t.Fatalf("CollectSliceUInt32 failed: %v", err)
		}
		if len(out) != 2 || out[0] != 10 || out[1] != 20 {
			t.Fatalf("unexpected CollectSliceUInt32 result: %#v", out)
		}
	})

	t.Run("CollectSliceFloat32", func(t *testing.T) {
		rows32 := []map[string]any{
			{"value": float32(1.5)},
			{"value": float32(2.5)},
		}
		schema32 := map[string]DataType{"value": DataTypeFloat32}
		out, err := CollectSliceFloat32(rows32, schema32, func(df *DataFrame) *LazyFrame {
			return df.Select(Col("value"))
		}, "")
		if err != nil {
			t.Fatalf("CollectSliceFloat32 failed: %v", err)
		}
		if len(out) != 2 || out[0] != float32(1.5) || out[1] != float32(2.5) {
			t.Fatalf("unexpected CollectSliceFloat32 result: %#v", out)
		}
	})

	t.Run("CollectSlice", func(t *testing.T) {
		out, err := CollectSlice(rows, schema, func(df *DataFrame) *LazyFrame {
			return df.Select(Col("name"))
		}, "")
		if err != nil {
			t.Fatalf("CollectSlice failed: %v", err)
		}
		if len(out) != 3 || out[0] != "Alice" || out[1] != "Bob" {
			t.Fatalf("unexpected CollectSlice result: %#v", out)
		}
	})

	t.Run("CollectStructs", func(t *testing.T) {
		type row struct {
			ID    int64   `polars:"id"`
			Name  string  `polars:"name"`
			Score float64 `polars:"score"`
		}
		out, err := CollectStructs[row](rows, schema, func(df *DataFrame) *LazyFrame {
			return df.Select(Col("id"), Col("name"), Col("score"))
		})
		if err != nil {
			t.Fatalf("CollectStructs failed: %v", err)
		}
		if len(out) != 3 || out[1].Name != "Bob" || out[2].Score != 3.5 {
			t.Fatalf("unexpected CollectStructs result: %#v", out)
		}
	})

	t.Run("CollectStructPointers", func(t *testing.T) {
		type row struct {
			ID   int64  `polars:"id"`
			Name string `polars:"name"`
		}
		out, err := CollectStructPointers[row](rows, schema, func(df *DataFrame) *LazyFrame {
			return df.Select(Col("id"), Col("name"))
		})
		if err != nil {
			t.Fatalf("CollectStructPointers failed: %v", err)
		}
		if len(out) != 3 || out[0] == nil || out[2] == nil || out[2].ID != 3 {
			t.Fatalf("unexpected CollectStructPointers result: %#v", out)
		}
	})
}
