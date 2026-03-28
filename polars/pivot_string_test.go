package polars

import (
	"strings"
	"testing"
)

func TestDataFramePivot(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "subject": "math", "score": int64(90)},
		{"name": "Alice", "subject": "english", "score": int64(85)},
		{"name": "Bob", "subject": "math", "score": int64(70)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	pivoted, err := df.Pivot(PivotOptions{
		On:            []string{"subject"},
		Index:         []string{"name"},
		Values:        []string{"score"},
		Aggregate:     "first",
		MaintainOrder: true,
	})
	if err != nil {
		t.Fatalf("Pivot failed: %v", err)
	}
	defer pivoted.Close()

	rows, err := pivoted.ToMaps()
	if err != nil {
		t.Fatalf("pivoted.ToMaps failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != "Alice" || rows[0]["math"] != int64(90) || rows[0]["english"] != int64(85) {
		t.Fatalf("unexpected pivot rows: %#v", rows)
	}

	_, err = df.Pivot(PivotOptions{
		On:        []string{"subject"},
		Index:     []string{"name"},
		Values:    []string{"score"},
		Aggregate: "bogus",
	})
	if err == nil {
		t.Fatal("expected pivot with invalid aggregate to fail")
	}
	if !strings.Contains(err.Error(), `aggregate="bogus"`) {
		t.Fatalf("expected pivot error to include aggregate context, got %v", err)
	}
	if !strings.Contains(err.Error(), "unsupported pivot aggregate") {
		t.Fatalf("expected pivot error to include aggregate hint, got %v", err)
	}
}

func TestLazyFramePivot(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "subject": "math", "score": int64(90)},
		{"name": "Alice", "subject": "english", "score": int64(85)},
		{"name": "Bob", "subject": "math", "score": int64(70)},
		{"name": "Bob", "subject": "english", "score": int64(80)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	rows, err := collectToMaps(t, df.Filter(Col("name").IsNotNull()).Pivot(LazyPivotOptions{
		On:            []string{"subject"},
		OnColumns:     []any{"math", "english"},
		Index:         []string{"name"},
		Values:        []string{"score"},
		Aggregate:     "first",
		MaintainOrder: true,
	}))
	if err != nil {
		t.Fatalf("lazy pivot collect failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 lazy pivot rows, got %d", len(rows))
	}
	if rows[0]["name"] != "Alice" || rows[0]["math"] != int64(90) || rows[0]["english"] != int64(85) {
		t.Fatalf("unexpected lazy pivot rows: %#v", rows)
	}

	t.Run("PivotLazyFromDataFrame", func(t *testing.T) {
		rows, err := collectToMaps(t, df.PivotLazy(LazyPivotOptions{
			On:            []string{"subject"},
			OnColumns:     []any{"math", "english"},
			Index:         []string{"name"},
			Values:        []string{"score"},
			Aggregate:     "first",
			MaintainOrder: true,
		}))
		if err != nil {
			t.Fatalf("DataFrame.PivotLazy collect failed: %v", err)
		}
		if len(rows) != 2 || rows[1]["name"] != "Bob" {
			t.Fatalf("unexpected DataFrame.PivotLazy rows: %#v", rows)
		}
	})

	t.Run("MissingOnColumns", func(t *testing.T) {
		_, err := collectToMaps(t, df.PivotLazy(LazyPivotOptions{
			On:        []string{"subject"},
			Index:     []string{"name"},
			Values:    []string{"score"},
			Aggregate: "first",
		}))
		if err == nil {
			t.Fatal("expected missing OnColumns to fail")
		}
		if !strings.Contains(err.Error(), "explicit OnColumns") {
			t.Fatalf("expected missing OnColumns hint, got %v", err)
		}
	})

	t.Run("InvalidAggregate", func(t *testing.T) {
		_, err := collectToMaps(t, df.PivotLazy(LazyPivotOptions{
			On:        []string{"subject"},
			OnColumns: []any{"math", "english"},
			Index:     []string{"name"},
			Values:    []string{"score"},
			Aggregate: "bogus",
		}))
		if err == nil {
			t.Fatal("expected lazy pivot with invalid aggregate to fail")
		}
		if !strings.Contains(err.Error(), "unsupported pivot aggregate") {
			t.Fatalf("expected aggregate hint, got %v", err)
		}
	})
}

func TestStringOperations(t *testing.T) {
	t.Run("StringOpsBasic", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("name"),
			Col("department"),
			Col("name").StrLenChars().Alias("name_len"),
			Col("name").StrLenBytes().Alias("name_len_bytes"),
			Col("name").StrContains("li", true).Alias("name_contains_li"),
			Col("name").StrStartsWith("A").Alias("name_starts_a"),
			Col("name").StrEndsWith("e").Alias("name_ends_e"),
			Col("department").StrExtract("(Eng)", 1).Alias("dept_extract"),
			Col("name").StrReplace("i", "I", true).Alias("name_replace"),
			Col("name").StrReplaceAll("i", "I", true).Alias("name_replace_all"),
			Col("name").StrToLowercase().Alias("name_lower"),
			Col("name").StrToUppercase().Alias("name_upper"),
			Col("name").StrStripChars("A").Alias("name_strip_a"),
			Col("name").StrSlice(1, 3).Alias("name_slice"),
			Col("name").StrPadStart(7, "_").Alias("name_pad_start"),
			Col("name").StrPadEnd(7, "_").Alias("name_pad_end"),
		)

		lf.Print()

		result, err := collectToMaps(t, lf.Limit(1))
		if err != nil {
			t.Fatalf("String operations failed: %v", err)
		}

		if len(result) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result))
		}

		row := result[0]
		if row["name"] != "Alice" {
			t.Fatalf("Expected name Alice, got %v", row["name"])
		}

		if v, err := toInt64(row["name_len"]); err != nil || v != 5 {
			t.Fatalf("Expected name_len 5, got %#v", row["name_len"])
		}
		if v, err := toInt64(row["name_len_bytes"]); err != nil || v != 5 {
			t.Fatalf("Expected name_len_bytes 5, got %#v", row["name_len_bytes"])
		}
		if row["name_contains_li"] != true {
			t.Fatalf("Expected name_contains_li true, got %#v", row["name_contains_li"])
		}
		if row["name_starts_a"] != true {
			t.Fatalf("Expected name_starts_a true, got %#v", row["name_starts_a"])
		}
		if row["name_ends_e"] != true {
			t.Fatalf("Expected name_ends_e true, got %#v", row["name_ends_e"])
		}
		if row["dept_extract"] != "Eng" {
			t.Fatalf("Expected dept_extract Eng, got %#v", row["dept_extract"])
		}
		if row["name_replace"] != "AlIce" {
			t.Fatalf("Expected name_replace AlIce, got %#v", row["name_replace"])
		}
		if row["name_replace_all"] != "AlIce" {
			t.Fatalf("Expected name_replace_all AlIce, got %#v", row["name_replace_all"])
		}
		if row["name_lower"] != "alice" {
			t.Fatalf("Expected name_lower alice, got %#v", row["name_lower"])
		}
		if row["name_upper"] != "ALICE" {
			t.Fatalf("Expected name_upper ALICE, got %#v", row["name_upper"])
		}
		if row["name_strip_a"] != "lice" {
			t.Fatalf("Expected name_strip_a lice, got %#v", row["name_strip_a"])
		}
		if row["name_slice"] != "lic" {
			t.Fatalf("Expected name_slice lic, got %#v", row["name_slice"])
		}
		if row["name_pad_start"] != "__Alice" {
			t.Fatalf("Expected name_pad_start __Alice, got %#v", row["name_pad_start"])
		}
		if row["name_pad_end"] != "Alice__" {
			t.Fatalf("Expected name_pad_end Alice__, got %#v", row["name_pad_end"])
		}
	})
}
