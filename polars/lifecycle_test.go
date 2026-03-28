package polars

import (
	"errors"
	"fmt"
	"strings"
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
