package polars

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
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

	t.Run("EagerFrameToSlice", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		values, err := frame.Select(Col("name")).Collect()
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}
		defer values.Free()

		names, err := values.ToSlice()
		if err != nil {
			t.Fatalf("ToSlice failed: %v", err)
		}
		if len(names) != 2 || names[0] != "Alice" || names[1] != "Bob" {
			t.Fatalf("unexpected ToSlice values: %#v", names)
		}
	})

	t.Run("DataFrameToSliceWithColumnName", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		names, err := frame.ToSlice("name")
		if err != nil {
			t.Fatalf("ToSlice failed: %v", err)
		}
		if len(names) != 2 || names[0] != "Alice" || names[1] != "Bob" {
			t.Fatalf("unexpected ToSlice values: %#v", names)
		}
	})

	t.Run("DataFrameToSliceStringWithColumnName", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		names, err := frame.ToSliceString("name")
		if err != nil {
			t.Fatalf("ToSliceString failed: %v", err)
		}
		if len(names) != 2 || names[0] != "Alice" || names[1] != "Bob" {
			t.Fatalf("unexpected ToSliceString values: %#v", names)
		}
	})

	t.Run("EagerFrameToSliceString", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		values, err := frame.Select(Col("name")).Collect()
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}
		defer values.Free()

		names, err := values.ToSliceString()
		if err != nil {
			t.Fatalf("ToSliceString failed: %v", err)
		}
		if len(names) != 2 || names[0] != "Alice" || names[1] != "Bob" {
			t.Fatalf("unexpected ToSliceString values: %#v", names)
		}
	})

	t.Run("DataFrameToSliceInt64", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"score": int64(10)},
			{"score": int64(20)},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		values, err := frame.ToSliceInt64("score")
		if err != nil {
			t.Fatalf("ToSliceInt64 failed: %v", err)
		}
		if len(values) != 2 || values[0] != 10 || values[1] != 20 {
			t.Fatalf("unexpected ToSliceInt64 values: %#v", values)
		}
	})

	t.Run("DataFrameToSliceUInt64", func(t *testing.T) {
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

		values, err := frame.ToSliceUInt64("id")
		if err != nil {
			t.Fatalf("ToSliceUInt64 failed: %v", err)
		}
		if len(values) != 2 || values[0] != 1 || values[1] != 2 {
			t.Fatalf("unexpected ToSliceUInt64 values: %#v", values)
		}
	})

	t.Run("DataFrameToSliceInt32", func(t *testing.T) {
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

		values, err := frame.ToSliceInt32("value")
		if err != nil {
			t.Fatalf("ToSliceInt32 failed: %v", err)
		}
		if len(values) != 2 || values[0] != 10 || values[1] != 20 {
			t.Fatalf("unexpected ToSliceInt32 values: %#v", values)
		}
	})

	t.Run("DataFrameToSliceUInt32", func(t *testing.T) {
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

		values, err := frame.ToSliceUInt32("value")
		if err != nil {
			t.Fatalf("ToSliceUInt32 failed: %v", err)
		}
		if len(values) != 2 || values[0] != 10 || values[1] != 20 {
			t.Fatalf("unexpected ToSliceUInt32 values: %#v", values)
		}
	})

	t.Run("DataFrameToSliceFloat64", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"score": 1.25},
			{"score": 2.5},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		values, err := frame.ToSliceFloat64("score")
		if err != nil {
			t.Fatalf("ToSliceFloat64 failed: %v", err)
		}
		if len(values) != 2 || values[0] != 1.25 || values[1] != 2.5 {
			t.Fatalf("unexpected ToSliceFloat64 values: %#v", values)
		}
	})

	t.Run("DataFrameToSliceFloat32", func(t *testing.T) {
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

		values, err := frame.ToSliceFloat32("value")
		if err != nil {
			t.Fatalf("ToSliceFloat32 failed: %v", err)
		}
		if len(values) != 2 || values[0] != float32(1.5) || values[1] != float32(2.5) {
			t.Fatalf("unexpected ToSliceFloat32 values: %#v", values)
		}
	})

	t.Run("DataFrameToSliceBool", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"active": true},
			{"active": false},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		values, err := frame.ToSliceBool("active")
		if err != nil {
			t.Fatalf("ToSliceBool failed: %v", err)
		}
		if len(values) != 2 || values[0] != true || values[1] != false {
			t.Fatalf("unexpected ToSliceBool values: %#v", values)
		}
	})

	t.Run("DataFrameToSliceTime", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
		}, nil)
		ts1 := time.Date(2026, 3, 30, 10, 0, 0, 0, time.UTC)
		ts2 := time.Date(2026, 3, 30, 11, 0, 0, 0, time.UTC)
		frame, err := NewDataFrame([]map[string]any{
			{"created_at": ts1},
			{"created_at": ts2},
		}, WithArrowSchema(schema))
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		values, err := frame.ToSliceTime("created_at")
		if err != nil {
			t.Fatalf("ToSliceTime failed: %v", err)
		}
		if len(values) != 2 || !values[0].Equal(ts1) || !values[1].Equal(ts2) {
			t.Fatalf("unexpected ToSliceTime values: %#v", values)
		}
	})

	t.Run("DataFrameToSliceBytes", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"payload": []byte("abc")},
			{"payload": []byte("xyz")},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		values, err := frame.ToSliceBytes("payload")
		if err != nil {
			t.Fatalf("ToSliceBytes failed: %v", err)
		}
		if len(values) != 2 || string(values[0]) != "abc" || string(values[1]) != "xyz" {
			t.Fatalf("unexpected ToSliceBytes values: %#v", values)
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

	t.Run("ToSliceRejectsMultipleColumnArgs", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "Alice"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		_, err = frame.ToSlice("id", "name")
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput, got %v", err)
		}
	})

	t.Run("ToListAlias", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		names, err := frame.ToList("name")
		if err != nil {
			t.Fatalf("ToList alias failed: %v", err)
		}
		if len(names) != 2 || names[0] != "Alice" || names[1] != "Bob" {
			t.Fatalf("unexpected ToList alias values: %#v", names)
		}
	})

	t.Run("CollectThenToSliceUnderGCPressure", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
			{"id": int64(3), "name": "Alice"},
			{"id": int64(4), "name": "Cara"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		for i := 0; i < 500; i++ {
			unique, err := frame.Select(Col("name")).Unique(UniqueOptions{
				Subset:        []string{"name"},
				MaintainOrder: true,
			}).Collect()
			if err != nil {
				t.Fatalf("Collect unique failed at iter %d: %v", i, err)
			}

			values, err := ToSlice[string](unique, "")
			unique.Free()
			if err != nil {
				t.Fatalf("ToSlice failed at iter %d: %v", i, err)
			}
			if len(values) != 3 {
				t.Fatalf("expected 3 values at iter %d, got %d", i, len(values))
			}

			if i%25 == 0 {
				runtime.GC()
				runtime.GC()
			}
		}
	})

	t.Run("ConcurrentCollectThenToSliceUnderGCPressure", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
			{"id": int64(3), "name": "Alice"},
			{"id": int64(4), "name": "Cara"},
			{"id": int64(5), "name": "Bob"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		const workers = 8
		const iterations = 100

		errCh := make(chan error, workers)
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for i := 0; i < iterations; i++ {
					unique, err := frame.Select(Col("name")).Unique(UniqueOptions{
						Subset:        []string{"name"},
						MaintainOrder: true,
					}).Collect()
					if err != nil {
						errCh <- err
						return
					}

					values, err := ToSlice[string](unique, "")
					unique.Free()
					if err != nil {
						errCh <- err
						return
					}
					if len(values) != 3 {
						errCh <- errors.New("unexpected unique slice length")
						return
					}

					if (workerID+i)%20 == 0 {
						runtime.GC()
					}
				}
			}(worker)
		}

		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				t.Fatalf("concurrent Collect->ToSlice failed: %v", err)
			}
		}
	})
}
