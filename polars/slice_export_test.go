package polars

import (
	"errors"
	"runtime"
	"sync"
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
