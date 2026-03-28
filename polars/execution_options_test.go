package polars

import (
	"strings"
	"testing"
)

func TestExecutionOptionsMemoryLimitCollect(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	if err := SetExecutionOptions(ExecutionOptions{MemoryLimitBytes: 1}); err != nil {
		t.Fatalf("SetExecutionOptions failed: %v", err)
	}
	t.Cleanup(func() {
		if err := SetExecutionOptions(ExecutionOptions{}); err != nil {
			t.Fatalf("reset execution options failed: %v", err)
		}
	})

	_, err = df.Lazy().Collect()
	if err == nil {
		t.Fatal("expected Collect to fail under tiny memory limit")
	}
	if !strings.Contains(err.Error(), "ERR_OOM") || !strings.Contains(err.Error(), "memory limit exceeded") {
		t.Fatalf("unexpected memory limit error: %v", err)
	}
}

func TestExecutionOptionsRejectNegativeLimitWithHint(t *testing.T) {
	err := SetExecutionOptions(ExecutionOptions{MemoryLimitBytes: -1})
	if err == nil {
		t.Fatal("expected negative memory limit to fail")
	}
	if !strings.Contains(err.Error(), "MemoryLimitBytes must be >= 0") {
		t.Fatalf("expected field context, got %v", err)
	}
	if !strings.Contains(err.Error(), "use 0 to disable the limit") {
		t.Fatalf("expected remediation hint, got %v", err)
	}
}

func TestExecutionOptionsMemoryLimitDisabled(t *testing.T) {
	if err := SetExecutionOptions(ExecutionOptions{}); err != nil {
		t.Fatalf("SetExecutionOptions failed: %v", err)
	}

	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	rows, err := collectToMaps(t, df.Lazy())
	if err != nil {
		t.Fatalf("collectToMaps failed with disabled memory limit: %v", err)
	}
	if len(rows) != 1 || rows[0]["name"] != "Alice" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
}

func TestExecutionOptionsMemoryLimitSQL(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	if err := SetExecutionOptions(ExecutionOptions{MemoryLimitBytes: 1}); err != nil {
		t.Fatalf("SetExecutionOptions failed: %v", err)
	}
	t.Cleanup(func() {
		if err := SetExecutionOptions(ExecutionOptions{}); err != nil {
			t.Fatalf("reset execution options failed: %v", err)
		}
	})

	_, err = NewSQLContext(map[string]any{
		"people": df,
	}).Execute("SELECT * FROM people")
	if err == nil {
		t.Fatal("expected SQL Execute to fail under tiny memory limit")
	}
	if !strings.Contains(err.Error(), "ERR_OOM") || !strings.Contains(err.Error(), "memory limit exceeded") {
		t.Fatalf("unexpected SQL memory limit error: %v", err)
	}
}
