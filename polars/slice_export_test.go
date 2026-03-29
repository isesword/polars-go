package polars

import (
	"errors"
	"testing"
)

func TestToSlice(t *testing.T) {
	t.Run("UniqueSingleSelectedColumn", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
			{"id": int64(3), "name": "Alice"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		unique, err := frame.Select(Col("name")).Unique(UniqueOptions{
			Subset:        []string{"name"},
			MaintainOrder: true,
		}).Collect()
		if err != nil {
			t.Fatalf("Collect unique failed: %v", err)
		}
		defer unique.Free()

		values, err := ToSlice[string](unique, "")
		if err != nil {
			t.Fatalf("ToSlice failed: %v", err)
		}
		if len(values) != 2 {
			t.Fatalf("expected 2 values, got %d", len(values))
		}
		if values[0] != "Alice" || values[1] != "Bob" {
			t.Fatalf("unexpected unique values: %#v", values)
		}
	})

	t.Run("ExplicitColumnName", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "score": int64(10)},
			{"id": int64(2), "score": int64(20)},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		values, err := ToSlice[int64](frame, "score")
		if err != nil {
			t.Fatalf("ToSlice failed: %v", err)
		}
		if len(values) != 2 || values[0] != 10 || values[1] != 20 {
			t.Fatalf("unexpected score slice: %#v", values)
		}
	})

	t.Run("NullablePointers", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"age": nil},
			{"age": int64(35)},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		values, err := ToSlice[*int](frame, "age")
		if err != nil {
			t.Fatalf("ToSlice failed: %v", err)
		}
		if len(values) != 2 {
			t.Fatalf("expected 2 values, got %d", len(values))
		}
		if values[0] != nil {
			t.Fatalf("expected nil pointer for first value, got %#v", values[0])
		}
		if values[1] == nil || *values[1] != 35 {
			t.Fatalf("unexpected second pointer value: %#v", values[1])
		}
	})

	t.Run("EmptyColumnNameRequiresSingleColumn", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "Alice"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		_, err = ToSlice[string](frame, "")
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput, got %v", err)
		}
	})

	t.Run("MissingColumn", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "Alice"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		_, err = ToSlice[string](frame, "missing")
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput, got %v", err)
		}
	})
}
