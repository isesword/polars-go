package polars

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestToArrayTyped(t *testing.T) {
	t.Run("ToArrayStringFromDataFrame", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"name": "Alice"},
			{"name": "Bob"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		col, err := frame.ToArrayString("name")
		if err != nil {
			t.Fatalf("ToArrayString failed: %v", err)
		}
		defer col.Release()

		if col.Len() != 2 || col.Value(0) != "Alice" || col.Value(1) != "Bob" {
			t.Fatalf("unexpected string array values: %v, %q, %q", col.Len(), col.Value(0), col.Value(1))
		}
	})

	t.Run("ToArrayStringFromEagerFrame", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"name": "Alice"},
			{"name": "Bob"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		eager, err := frame.Select(Col("name")).Collect()
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}
		defer eager.Free()

		col, err := eager.ToArrayString()
		if err != nil {
			t.Fatalf("ToArrayString failed: %v", err)
		}
		defer col.Release()

		if col.Len() != 2 || col.Value(0) != "Alice" || col.Value(1) != "Bob" {
			t.Fatalf("unexpected eager string array values: %v, %q, %q", col.Len(), col.Value(0), col.Value(1))
		}
	})

	t.Run("ToArrayInt64", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"score": int64(10)},
			{"score": int64(20)},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		col, err := frame.ToArrayInt64("score")
		if err != nil {
			t.Fatalf("ToArrayInt64 failed: %v", err)
		}
		defer col.Release()

		if col.Len() != 2 || col.Value(0) != 10 || col.Value(1) != 20 {
			t.Fatalf("unexpected int64 array values: %v, %v, %v", col.Len(), col.Value(0), col.Value(1))
		}
	})

	t.Run("ToArrayUInt64", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": uint64(1)},
			{"id": uint64(2)},
		}, WithSchema(map[string]DataType{
			"id": DataTypeUInt64,
		}))
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		col, err := frame.ToArrayUInt64("id")
		if err != nil {
			t.Fatalf("ToArrayUInt64 failed: %v", err)
		}
		defer col.Release()

		if col.Len() != 2 || col.Value(0) != 1 || col.Value(1) != 2 {
			t.Fatalf("unexpected uint64 array values: %v, %v, %v", col.Len(), col.Value(0), col.Value(1))
		}
	})

	t.Run("ToArrayFloat64", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"score": 1.25},
			{"score": 2.5},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		col, err := frame.ToArrayFloat64("score")
		if err != nil {
			t.Fatalf("ToArrayFloat64 failed: %v", err)
		}
		defer col.Release()

		if col.Len() != 2 || col.Value(0) != 1.25 || col.Value(1) != 2.5 {
			t.Fatalf("unexpected float64 array values: %v, %v, %v", col.Len(), col.Value(0), col.Value(1))
		}
	})

	t.Run("ToArrayBool", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"active": true},
			{"active": false},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		col, err := frame.ToArrayBool("active")
		if err != nil {
			t.Fatalf("ToArrayBool failed: %v", err)
		}
		defer col.Release()

		if col.Len() != 2 || col.Value(0) != true || col.Value(1) != false {
			t.Fatalf("unexpected bool array values: %v, %v, %v", col.Len(), col.Value(0), col.Value(1))
		}
	})

	t.Run("ToArrayInt32", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "value", Type: arrow.PrimitiveTypes.Int32},
		}, nil)
		frame, err := NewDataFrame([]map[string]any{
			{"value": int32(10)},
			{"value": int32(20)},
		}, WithArrowSchema(schema))
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		col, err := frame.ToArrayInt32("value")
		if err != nil {
			t.Fatalf("ToArrayInt32 failed: %v", err)
		}
		defer col.Release()

		if col.Len() != 2 || col.Value(0) != 10 || col.Value(1) != 20 {
			t.Fatalf("unexpected int32 array values: %v, %v, %v", col.Len(), col.Value(0), col.Value(1))
		}
	})

	t.Run("ToArrayUInt32", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "value", Type: arrow.PrimitiveTypes.Uint32},
		}, nil)
		frame, err := NewDataFrame([]map[string]any{
			{"value": uint32(10)},
			{"value": uint32(20)},
		}, WithArrowSchema(schema))
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		col, err := frame.ToArrayUInt32("value")
		if err != nil {
			t.Fatalf("ToArrayUInt32 failed: %v", err)
		}
		defer col.Release()

		if col.Len() != 2 || col.Value(0) != 10 || col.Value(1) != 20 {
			t.Fatalf("unexpected uint32 array values: %v, %v, %v", col.Len(), col.Value(0), col.Value(1))
		}
	})

	t.Run("ToArrayFloat32", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "value", Type: arrow.PrimitiveTypes.Float32},
		}, nil)
		frame, err := NewDataFrame([]map[string]any{
			{"value": float32(1.5)},
			{"value": float32(2.5)},
		}, WithArrowSchema(schema))
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		col, err := frame.ToArrayFloat32("value")
		if err != nil {
			t.Fatalf("ToArrayFloat32 failed: %v", err)
		}
		defer col.Release()

		if col.Len() != 2 || col.Value(0) != float32(1.5) || col.Value(1) != float32(2.5) {
			t.Fatalf("unexpected float32 array values: %v, %v, %v", col.Len(), col.Value(0), col.Value(1))
		}
	})

	t.Run("TypeMismatch", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"name": "Alice"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		if _, err := frame.ToArrayInt64("name"); err == nil {
			t.Fatal("expected ToArrayInt64 on string column to fail")
		}
	})

	t.Run("RetainedArraySurvivesFrameClose", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"name": "Alice"},
			{"name": "Bob"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}

		col, err := frame.ToArrayString("name")
		if err != nil {
			t.Fatalf("ToArrayString failed: %v", err)
		}
		frame.Close()
		defer col.Release()

		if col.Len() != 2 || col.Value(1) != "Bob" {
			t.Fatalf("retained string array became invalid after frame close: len=%d val=%q", col.Len(), col.Value(1))
		}
	})

	t.Run("NormalizedStringArrayType", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"name": "Alice"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		col, err := frame.ToArrayString("name")
		if err != nil {
			t.Fatalf("ToArrayString failed: %v", err)
		}
		defer col.Release()

		var _ *array.String = col
	})
}
