package polars

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/isesword/polars-go/bridge"
)

func TestLazyFrameDefaultBridge(t *testing.T) {
	const sampleCSV = "../testdata/sample.csv"

	df, err := ScanCSV(sampleCSV).Limit(2).Collect()
	if err != nil {
		t.Fatalf("Collect(nil) failed: %v", err)
	}
	defer df.Free()

	rows, err := df.ToMaps()
	if err != nil {
		t.Fatalf("Collect(nil) ToMaps failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}

	if err := ScanCSV(sampleCSV).Limit(1).Print(); err != nil {
		t.Fatalf("Print(nil) failed: %v", err)
	}

	logicalPlan, err := ScanCSV(sampleCSV).Limit(1).LogicalPlan()
	if err != nil {
		t.Fatalf("LogicalPlan failed: %v", err)
	}
	if logicalPlan == "" {
		t.Fatal("expected non-empty logical plan")
	}

	optimizedPlan, err := ScanCSV(sampleCSV).Limit(1).OptimizedPlan()
	if err != nil {
		t.Fatalf("OptimizedPlan failed: %v", err)
	}
	if optimizedPlan == "" {
		t.Fatal("expected non-empty optimized plan")
	}
}

func TestDataFrameFreeBehavior(t *testing.T) {
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	t.Run("FreeInvalidatesDataFrame", func(t *testing.T) {
		df, err := NewEagerFrameFromRowsWithSchema(brg, []map[string]any{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		}, map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}

		df.Free()

		if !df.Closed() {
			t.Fatal("expected Closed() to report true after Free")
		}

		if _, err := df.ToMaps(); err == nil {
			t.Fatal("Expected ToMaps() to fail after Free(), got nil")
		} else if !errors.Is(err, ErrClosedDataFrame) {
			t.Fatalf("expected ErrClosedDataFrame after Free, got %v", err)
		}
		if err := df.Print(); err == nil {
			t.Fatal("Expected Print() to fail after Free(), got nil")
		} else if !errors.Is(err, ErrClosedDataFrame) {
			t.Fatalf("expected ErrClosedDataFrame from Print after Free, got %v", err)
		}
	})

	t.Run("FreeIsIdempotent", func(t *testing.T) {
		df, err := NewEagerFrameFromRowsWithSchema(brg, []map[string]any{
			{"id": 1, "name": "Alice"},
		}, map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}

		df.Free()
		df.Free()
		df.Free()

		if _, err := df.ToMaps(); err == nil {
			t.Fatal("Expected ToMaps() to remain invalid after repeated Free()")
		}
	})

	t.Run("CollectThenToMapsStillOwnsCleanupInternally", func(t *testing.T) {
		rows, err := NewDataFrameFromRowsWithSchema([]map[string]any{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		}, map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}

		result, err := collectToMaps(t, rows.Lazy())
		if err != nil {
			t.Fatalf("Collect+ToMaps failed: %v", err)
		}
		if len(result) != 2 {
			t.Fatalf("Expected 2 rows, got %d", len(result))
		}

		rows.Close()
	})

	t.Run("DeferFreeOnFunctionReturn", func(t *testing.T) {
		var escaped *EagerFrame

		useWithDefer := func() error {
			df, err := NewEagerFrameFromRowsWithSchema(brg, []map[string]any{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			}, map[string]DataType{
				"id":   DataTypeInt64,
				"name": DataTypeUTF8,
			})
			if err != nil {
				return err
			}
			escaped = df
			defer df.Free()

			rows, err := df.ToMaps()
			if err != nil {
				return fmt.Errorf("ToMaps before deferred Free failed: %w", err)
			}
			if len(rows) != 2 {
				return fmt.Errorf("expected 2 rows before deferred Free, got %d", len(rows))
			}
			return nil
		}

		if err := useWithDefer(); err != nil {
			t.Fatalf("Function using defer Free failed: %v", err)
		}
		if escaped == nil {
			t.Fatal("Expected escaped dataframe reference to be set")
		}

		if _, err := escaped.ToMaps(); err == nil {
			t.Fatal("Expected ToMaps() to fail after deferred Free ran, got nil")
		}
	})

	t.Run("ManagedDataFrameFinalizerReleasesAfterFunctionReturn", func(t *testing.T) {
		var escaped *EagerFrame

		createFrame := func() error {
			frame, err := NewDataFrame([]map[string]any{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			}, WithSchema(map[string]DataType{
				"id":   DataTypeInt64,
				"name": DataTypeUTF8,
			}))
			if err != nil {
				return err
			}

			escaped = frame.df
			if escaped == nil {
				return fmt.Errorf("expected managed frame to hold eager dataframe")
			}

			rows, err := frame.ToMaps()
			if err != nil {
				return fmt.Errorf("ToMaps before function return failed: %w", err)
			}
			if len(rows) != 2 {
				return fmt.Errorf("expected 2 rows before function return, got %d", len(rows))
			}
			return nil
		}

		if err := createFrame(); err != nil {
			t.Fatalf("managed frame function failed: %v", err)
		}
		if escaped == nil {
			t.Fatal("expected escaped eager frame reference to be set")
		}

		deadline := time.Now().Add(2 * time.Second)
		for {
			runtime.GC()
			runtime.Gosched()

			if escaped.Closed() {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("expected managed dataframe finalizer to release eager frame before timeout")
			}
			time.Sleep(10 * time.Millisecond)
		}

		if _, err := escaped.ToMaps(); err == nil {
			t.Fatal("expected escaped eager frame to fail after managed dataframe finalizer ran")
		} else if !errors.Is(err, ErrClosedDataFrame) {
			t.Fatalf("expected ErrClosedDataFrame after managed finalizer, got %v", err)
		}
	})

	t.Run("ManagedDataFrameStaysAliveWhileReferencedAfterFunctionReturn", func(t *testing.T) {
		var escaped *DataFrame

		createFrame := func() error {
			frame, err := NewDataFrame([]map[string]any{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			}, WithSchema(map[string]DataType{
				"id":   DataTypeInt64,
				"name": DataTypeUTF8,
			}))
			if err != nil {
				return err
			}

			escaped = frame

			rows, err := frame.ToMaps()
			if err != nil {
				return fmt.Errorf("ToMaps before function return failed: %w", err)
			}
			if len(rows) != 2 {
				return fmt.Errorf("expected 2 rows before function return, got %d", len(rows))
			}
			return nil
		}

		if err := createFrame(); err != nil {
			t.Fatalf("managed frame function failed: %v", err)
		}
		if escaped == nil {
			t.Fatal("expected escaped managed dataframe reference to be set")
		}

		for i := 0; i < 5; i++ {
			runtime.GC()
			runtime.Gosched()
		}

		if escaped.Closed() {
			t.Fatal("expected managed dataframe to stay alive while caller still holds a reference")
		}

		rows, err := escaped.ToMaps()
		if err != nil {
			t.Fatalf("expected managed dataframe ToMaps to succeed while referenced, got %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows while managed dataframe is still referenced, got %d", len(rows))
		}

		escaped.Close()
	})

	t.Run("ManagedDataFrameReturnedFromFunctionRemainsUsableUntilClose", func(t *testing.T) {
		buildFrame := func() (*DataFrame, error) {
			return NewDataFrame([]map[string]any{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
				{"id": 3, "name": "Cara"},
			}, WithSchema(map[string]DataType{
				"id":   DataTypeInt64,
				"name": DataTypeUTF8,
			}))
		}

		frame, err := buildFrame()
		if err != nil {
			t.Fatalf("buildFrame failed: %v", err)
		}

		rows, err := collectToMaps(t, frame.Filter(Col("id").Gt(Lit(1))))
		if err != nil {
			t.Fatalf("query on returned managed dataframe failed: %v", err)
		}
		if len(rows) != 2 || rows[0]["name"] != "Bob" || rows[1]["name"] != "Cara" {
			t.Fatalf("unexpected rows from returned managed dataframe: %#v", rows)
		}

		frame.Close()
		if _, err := frame.ToMaps(); err == nil {
			t.Fatal("expected ToMaps to fail after explicit Close on returned dataframe")
		} else if !errors.Is(err, ErrClosedDataFrame) {
			t.Fatalf("expected ErrClosedDataFrame after explicit Close, got %v", err)
		}
	})

	t.Run("TemporaryManagedDataFramesCanBeCollectedAfterFunctionReturns", func(t *testing.T) {
		const iterations = 64

		makeTemporaryFrame := func(i int) error {
			frame, err := NewDataFrame([]map[string]any{
				{"id": i, "name": fmt.Sprintf("user-%d", i)},
				{"id": i + 1000, "name": fmt.Sprintf("user-%d-b", i)},
			}, WithSchema(map[string]DataType{
				"id":   DataTypeInt64,
				"name": DataTypeUTF8,
			}))
			if err != nil {
				return err
			}

			rows, err := frame.ToMaps()
			if err != nil {
				return fmt.Errorf("temporary frame ToMaps failed: %w", err)
			}
			if len(rows) != 2 {
				return fmt.Errorf("expected 2 rows from temporary frame, got %d", len(rows))
			}
			return nil
		}

		for i := 0; i < iterations; i++ {
			if err := makeTemporaryFrame(i); err != nil {
				t.Fatalf("temporary frame iteration %d failed: %v", i, err)
			}
		}

		for i := 0; i < 5; i++ {
			runtime.GC()
			runtime.Gosched()
		}

		verify, err := NewDataFrame([]map[string]any{
			{"id": 1, "name": "still-works"},
		}, WithSchema(map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		}))
		if err != nil {
			t.Fatalf("verification dataframe creation failed after temporary frames: %v", err)
		}
		defer verify.Close()

		rows, err := verify.ToMaps()
		if err != nil {
			t.Fatalf("verification dataframe ToMaps failed: %v", err)
		}
		if len(rows) != 1 || rows[0]["name"] != "still-works" {
			t.Fatalf("unexpected verification rows after temporary frames: %#v", rows)
		}
	})

	t.Run("TemporaryManagedDataFramesCreatedInGoroutinesCanBeCollected", func(t *testing.T) {
		const (
			workers    = 8
			iterations = 16
		)

		errCh := make(chan error, workers)
		var wg sync.WaitGroup

		makeTemporaryFrame := func(workerID, iter int) error {
			frame, err := NewDataFrame([]map[string]any{
				{"id": int64(workerID*1000 + iter), "name": fmt.Sprintf("worker-%d-%d", workerID, iter)},
				{"id": int64(workerID*1000 + iter + 100), "name": fmt.Sprintf("worker-%d-%d-b", workerID, iter)},
			}, WithSchema(map[string]DataType{
				"id":   DataTypeInt64,
				"name": DataTypeUTF8,
			}))
			if err != nil {
				return err
			}

			rows, err := frame.ToMaps()
			if err != nil {
				return fmt.Errorf("worker %d iteration %d ToMaps failed: %w", workerID, iter, err)
			}
			if len(rows) != 2 {
				return fmt.Errorf("worker %d iteration %d expected 2 rows, got %d", workerID, iter, len(rows))
			}
			return nil
		}

		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for iter := 0; iter < iterations; iter++ {
					if err := makeTemporaryFrame(workerID, iter); err != nil {
						errCh <- err
						return
					}
				}
			}(worker)
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			if err != nil {
				t.Fatal(err)
			}
		}

		for i := 0; i < 5; i++ {
			runtime.GC()
			runtime.Gosched()
		}

		verify, err := NewDataFrame([]map[string]any{
			{"id": int64(1), "name": "post-goroutine-check"},
		}, WithSchema(map[string]DataType{
			"id":   DataTypeInt64,
			"name": DataTypeUTF8,
		}))
		if err != nil {
			t.Fatalf("verification dataframe creation failed after goroutine frames: %v", err)
		}
		defer verify.Close()

		rows, err := verify.ToMaps()
		if err != nil {
			t.Fatalf("verification dataframe ToMaps failed after goroutine frames: %v", err)
		}
		if len(rows) != 1 || rows[0]["name"] != "post-goroutine-check" {
			t.Fatalf("unexpected verification rows after goroutine frames: %#v", rows)
		}
	})

	t.Run("TemporaryManagedDataFramesMemoryTrendUnderLoad", func(t *testing.T) {
		const iterations = 2000

		makeTemporaryFrame := func(i int) error {
			frame, err := NewDataFrame([]map[string]any{
				{"id": int64(i), "name": fmt.Sprintf("user-%d", i), "score": float64(i) * 1.5},
				{"id": int64(i + 1), "name": fmt.Sprintf("user-%d-b", i), "score": float64(i+1) * 1.5},
				{"id": int64(i + 2), "name": fmt.Sprintf("user-%d-c", i), "score": float64(i+2) * 1.5},
			}, WithSchema(map[string]DataType{
				"id":    DataTypeInt64,
				"name":  DataTypeUTF8,
				"score": DataTypeFloat64,
			}))
			if err != nil {
				return err
			}

			rows, err := frame.ToMaps()
			if err != nil {
				return fmt.Errorf("temporary frame ToMaps failed: %w", err)
			}
			if len(rows) != 3 {
				return fmt.Errorf("expected 3 rows from temporary frame, got %d", len(rows))
			}
			return nil
		}

		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		for i := 0; i < iterations; i++ {
			if err := makeTemporaryFrame(i); err != nil {
				t.Fatalf("temporary frame iteration %d failed: %v", i, err)
			}
		}

		for i := 0; i < 8; i++ {
			runtime.GC()
			runtime.Gosched()
			time.Sleep(10 * time.Millisecond)
		}

		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		heapBeforeMiB := float64(before.HeapAlloc) / (1024 * 1024)
		heapAfterMiB := float64(after.HeapAlloc) / (1024 * 1024)
		sysBeforeMiB := float64(before.Sys) / (1024 * 1024)
		sysAfterMiB := float64(after.Sys) / (1024 * 1024)
		heapGrowthMiB := heapAfterMiB - heapBeforeMiB

		t.Logf("temporary dataframe load: iterations=%d heap_before=%.2fMiB heap_after=%.2fMiB heap_growth=%.2fMiB sys_before=%.2fMiB sys_after=%.2fMiB",
			iterations, heapBeforeMiB, heapAfterMiB, heapGrowthMiB, sysBeforeMiB, sysAfterMiB)

		if heapGrowthMiB > 32 {
			t.Fatalf("unexpected heap growth after temporary dataframe load: before=%.2fMiB after=%.2fMiB growth=%.2fMiB",
				heapBeforeMiB, heapAfterMiB, heapGrowthMiB)
		}
	})

	t.Run("TemporaryManagedDataFramesMemoryTrendUnderHeavierLoad", func(t *testing.T) {
		const iterations = 20000

		makeTemporaryFrame := func(i int) error {
			frame, err := NewDataFrame([]map[string]any{
				{"id": int64(i), "name": fmt.Sprintf("user-%d", i), "score": float64(i) * 1.5},
				{"id": int64(i + 1), "name": fmt.Sprintf("user-%d-b", i), "score": float64(i+1) * 1.5},
				{"id": int64(i + 2), "name": fmt.Sprintf("user-%d-c", i), "score": float64(i+2) * 1.5},
			}, WithSchema(map[string]DataType{
				"id":    DataTypeInt64,
				"name":  DataTypeUTF8,
				"score": DataTypeFloat64,
			}))
			if err != nil {
				return err
			}

			rows, err := frame.ToMaps()
			if err != nil {
				return fmt.Errorf("temporary frame ToMaps failed: %w", err)
			}
			if len(rows) != 3 {
				return fmt.Errorf("expected 3 rows from temporary frame, got %d", len(rows))
			}
			return nil
		}

		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		for i := 0; i < iterations; i++ {
			if err := makeTemporaryFrame(i); err != nil {
				t.Fatalf("temporary frame iteration %d failed: %v", i, err)
			}
		}

		for i := 0; i < 10; i++ {
			runtime.GC()
			runtime.Gosched()
			time.Sleep(10 * time.Millisecond)
		}

		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		heapBeforeMiB := float64(before.HeapAlloc) / (1024 * 1024)
		heapAfterMiB := float64(after.HeapAlloc) / (1024 * 1024)
		sysBeforeMiB := float64(before.Sys) / (1024 * 1024)
		sysAfterMiB := float64(after.Sys) / (1024 * 1024)
		heapGrowthMiB := heapAfterMiB - heapBeforeMiB

		t.Logf("heavy temporary dataframe load: iterations=%d heap_before=%.2fMiB heap_after=%.2fMiB heap_growth=%.2fMiB sys_before=%.2fMiB sys_after=%.2fMiB",
			iterations, heapBeforeMiB, heapAfterMiB, heapGrowthMiB, sysBeforeMiB, sysAfterMiB)

		if heapGrowthMiB > 48 {
			t.Fatalf("unexpected heap growth after heavy temporary dataframe load: before=%.2fMiB after=%.2fMiB growth=%.2fMiB",
				heapBeforeMiB, heapAfterMiB, heapGrowthMiB)
		}
	})

	t.Run("TemporaryManagedDataFramesMemoryTrendUnderConcurrentLoad", func(t *testing.T) {
		const (
			workers    = 8
			iterations = 2000
		)

		makeTemporaryFrame := func(workerID, iter int) error {
			base := workerID*100000 + iter*10
			frame, err := NewDataFrame([]map[string]any{
				{"id": int64(base), "name": fmt.Sprintf("worker-%d-%d-a", workerID, iter), "score": float64(base) * 1.25},
				{"id": int64(base + 1), "name": fmt.Sprintf("worker-%d-%d-b", workerID, iter), "score": float64(base+1) * 1.25},
				{"id": int64(base + 2), "name": fmt.Sprintf("worker-%d-%d-c", workerID, iter), "score": float64(base+2) * 1.25},
			}, WithSchema(map[string]DataType{
				"id":    DataTypeInt64,
				"name":  DataTypeUTF8,
				"score": DataTypeFloat64,
			}))
			if err != nil {
				return err
			}

			rows, err := frame.ToMaps()
			if err != nil {
				return fmt.Errorf("worker %d iteration %d ToMaps failed: %w", workerID, iter, err)
			}
			if len(rows) != 3 {
				return fmt.Errorf("worker %d iteration %d expected 3 rows, got %d", workerID, iter, len(rows))
			}
			return nil
		}

		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		errCh := make(chan error, workers)
		var wg sync.WaitGroup

		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for iter := 0; iter < iterations; iter++ {
					if err := makeTemporaryFrame(workerID, iter); err != nil {
						errCh <- err
						return
					}
				}
			}(worker)
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			if err != nil {
				t.Fatal(err)
			}
		}

		for i := 0; i < 10; i++ {
			runtime.GC()
			runtime.Gosched()
			time.Sleep(10 * time.Millisecond)
		}

		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		heapBeforeMiB := float64(before.HeapAlloc) / (1024 * 1024)
		heapAfterMiB := float64(after.HeapAlloc) / (1024 * 1024)
		sysBeforeMiB := float64(before.Sys) / (1024 * 1024)
		sysAfterMiB := float64(after.Sys) / (1024 * 1024)
		heapGrowthMiB := heapAfterMiB - heapBeforeMiB

		t.Logf("concurrent temporary dataframe load: workers=%d iterations=%d total=%d heap_before=%.2fMiB heap_after=%.2fMiB heap_growth=%.2fMiB sys_before=%.2fMiB sys_after=%.2fMiB",
			workers, iterations, workers*iterations, heapBeforeMiB, heapAfterMiB, heapGrowthMiB, sysBeforeMiB, sysAfterMiB)

		if heapGrowthMiB > 64 {
			t.Fatalf("unexpected heap growth after concurrent temporary dataframe load: before=%.2fMiB after=%.2fMiB growth=%.2fMiB",
				heapBeforeMiB, heapAfterMiB, heapGrowthMiB)
		}
	})
}

func TestNilAndClosedErrors(t *testing.T) {
	var managed *DataFrame
	if _, err := managed.ToMaps(); !errors.Is(err, ErrNilDataFrame) {
		t.Fatalf("expected ErrNilDataFrame from nil managed dataframe, got %v", err)
	}

	var lazy *LazyFrame
	if _, err := lazy.Collect(); !errors.Is(err, ErrNilLazyFrame) {
		t.Fatalf("expected ErrNilLazyFrame from nil lazyframe, got %v", err)
	}

	type dup struct {
		First  string `polars:"name"`
		Second string `polars:"name"`
	}
	if _, err := NewDataFrameFromStructs([]dup{{First: "a", Second: "b"}}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput from duplicate struct tags, got %v", err)
	}

	type user struct {
		Name string `polars:"name"`
	}
	var nilUser *user
	if _, err := NewDataFrame([]*user{nilUser}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput from nil struct pointer row, got %v", err)
	}

	frame, err := NewDataFrame([]map[string]any{{"name": "Alice"}})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer frame.Close()

	if _, err := ToStructs[int](frame); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput from non-struct ToStructs target, got %v", err)
	}

	type strictUser struct {
		Age int64 `polars:"age"`
	}
	badFrame, err := NewDataFrame([]map[string]any{{"age": "oops"}})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer badFrame.Close()

	if _, err := ToStructs[strictUser](badFrame); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput from mismatched ToStructs field, got %v", err)
	}

	type event struct {
		CreatedAt time.Time `polars:"created_at"`
	}
	timeFrame, err := NewDataFrame([]map[string]any{{"created_at": "not-a-date"}})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer timeFrame.Close()

	_, err = ToStructs[event](timeFrame)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput from invalid time conversion, got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "field created_at") {
		t.Fatalf("expected field context in temporal ToStructs error, got %v", err)
	}
	if !strings.Contains(msg, "datetime") {
		t.Fatalf("expected datetime context in temporal ToStructs error, got %v", err)
	}
	if !strings.Contains(msg, "RFC3339") && !strings.Contains(msg, "2006-01-02") {
		t.Fatalf("expected supported layout hint in temporal ToStructs error, got %v", err)
	}
}
