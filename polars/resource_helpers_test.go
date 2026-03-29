package polars

import (
	"testing"
)

func TestLazyCollectHelpers(t *testing.T) {
	frame, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "name": "Alice", "score": float64(1.5)},
		{"id": int64(2), "name": "Bob", "score": float64(2.5)},
		{"id": int64(3), "name": "Alice", "score": float64(3.5)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer frame.Close()

	t.Run("CollectMaps", func(t *testing.T) {
		rows, err := frame.Filter(Col("id").Gt(Lit(1))).Select(Col("name")).CollectMaps()
		if err != nil {
			t.Fatalf("CollectMaps failed: %v", err)
		}
		if len(rows) != 2 || rows[0]["name"] != "Bob" || rows[1]["name"] != "Alice" {
			t.Fatalf("unexpected CollectMaps result: %#v", rows)
		}
	})

	t.Run("CollectSliceString", func(t *testing.T) {
		names, err := frame.Select(Col("name")).Unique(UniqueOptions{
			Subset:        []string{"name"},
			MaintainOrder: true,
		}).CollectSliceString("")
		if err != nil {
			t.Fatalf("CollectSliceString failed: %v", err)
		}
		if len(names) != 2 || names[0] != "Alice" || names[1] != "Bob" {
			t.Fatalf("unexpected CollectSliceString result: %#v", names)
		}
	})

	t.Run("CollectSliceFloat64", func(t *testing.T) {
		scores, err := frame.Select(Col("score")).CollectSliceFloat64("")
		if err != nil {
			t.Fatalf("CollectSliceFloat64 failed: %v", err)
		}
		if len(scores) != 3 || scores[0] != 1.5 || scores[2] != 3.5 {
			t.Fatalf("unexpected CollectSliceFloat64 result: %#v", scores)
		}
	})
}

func TestResourceHelpers(t *testing.T) {
	t.Run("WithDataFrame", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"name": "Alice"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}

		if err := WithDataFrame(frame, func(df *DataFrame) error {
			rows, err := df.ToMaps()
			if err != nil {
				return err
			}
			if len(rows) != 1 || rows[0]["name"] != "Alice" {
				t.Fatalf("unexpected rows: %#v", rows)
			}
			return nil
		}); err != nil {
			t.Fatalf("WithDataFrame failed: %v", err)
		}

		if !frame.Closed() {
			t.Fatal("expected WithDataFrame to close frame")
		}
	})

	t.Run("WithCollect", func(t *testing.T) {
		frame, err := NewDataFrame([]map[string]any{
			{"name": "Alice"},
			{"name": "Bob"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		defer frame.Close()

		var escaped *EagerFrame
		if err := WithCollect(frame.Select(Col("name")), func(df *EagerFrame) error {
			escaped = df
			names, err := df.ToSliceString()
			if err != nil {
				return err
			}
			if len(names) != 2 || names[1] != "Bob" {
				t.Fatalf("unexpected names: %#v", names)
			}
			return nil
		}); err != nil {
			t.Fatalf("WithCollect failed: %v", err)
		}

		if escaped == nil || !escaped.Closed() {
			t.Fatal("expected WithCollect to free eager frame")
		}
	})

	t.Run("Releaser", func(t *testing.T) {
		r := NewReleaser()
		frame, err := NewDataFrame([]map[string]any{
			{"name": "Alice"},
		})
		if err != nil {
			t.Fatalf("NewDataFrame failed: %v", err)
		}
		frame = r.DataFrame(frame)

		eager, err := frame.Select(Col("name")).Collect()
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}
		eager = r.EagerFrame(eager)

		col, err := eager.ToArrayString()
		if err != nil {
			t.Fatalf("ToArrayString failed: %v", err)
		}
		r.Array(col)

		if col.Len() != 1 || col.Value(0) != "Alice" {
			t.Fatalf("unexpected array values: len=%d value=%q", col.Len(), col.Value(0))
		}

		r.Release()

		if !frame.Closed() || !eager.Closed() {
			t.Fatal("expected Releaser to close frame and eager frame")
		}
	})
}
