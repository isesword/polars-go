package polars

import (
	"strings"
	"testing"
)

func TestSQLContextExecute(t *testing.T) {
	ctx := NewSQLContext()
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
		{"name": "Charlie", "age": int64(40)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	ctx.Register("people", df)

	result, err := ctx.Execute("SELECT name, age FROM people WHERE age >= 35 ORDER BY age")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Close()

	rows, err := result.ToMaps()
	if err != nil {
		t.Fatalf("result.ToMaps failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != "Bob" || rows[1]["name"] != "Charlie" {
		t.Fatalf("unexpected SQL rows: %#v", rows)
	}
}

func TestSQLContextInputErrorsIncludeHints(t *testing.T) {
	ctx := NewSQLContext().Register("", 123)
	_, err := ctx.Execute("SELECT 1")
	if err == nil {
		t.Fatal("expected execute to fail for empty table name")
	}
	if !strings.Contains(err.Error(), "SQLContext.Register: table name is empty") {
		t.Fatalf("expected table-name context, got %v", err)
	}

	_, err = NewSQLContext().Execute("")
	if err == nil {
		t.Fatal("expected execute to fail for empty query")
	}
	if !strings.Contains(err.Error(), "query is empty") || !strings.Contains(err.Error(), "hint:") {
		t.Fatalf("expected empty-query hint, got %v", err)
	}

	_, err = NewSQLContext().Execute("SELECT 1")
	if err == nil {
		t.Fatal("expected execute to fail for missing tables")
	}
	if !strings.Contains(err.Error(), "no registered tables") || !strings.Contains(err.Error(), `SELECT 1`) {
		t.Fatalf("expected no-table query context, got %v", err)
	}

	_, err = NewSQLContext(map[string]any{
		"bad": 123,
	}).Execute("SELECT * FROM bad")
	if err == nil {
		t.Fatal("expected execute to fail for unsupported table type")
	}
	if !strings.Contains(err.Error(), "unsupported SQL table type int") {
		t.Fatalf("expected unsupported-type context, got %v", err)
	}
	if !strings.Contains(err.Error(), "*DataFrame") {
		t.Fatalf("expected supported-type hint, got %v", err)
	}
}

func TestSQLContextChainRegisterMany(t *testing.T) {
	left, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	})
	if err != nil {
		t.Fatalf("NewDataFrame left failed: %v", err)
	}
	defer left.Close()

	right, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "age": int64(25)},
		{"id": int64(2), "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame right failed: %v", err)
	}
	defer right.Close()

	ctx := NewSQLContext().
		Register("left_tbl", left).
		RegisterMany(map[string]any{
			"right_tbl": right,
		})

	tables := ctx.Tables()
	if len(tables) != 2 || tables[0] != "left_tbl" || tables[1] != "right_tbl" {
		t.Fatalf("unexpected SQLContext tables: %#v", tables)
	}

	result, err := ctx.Execute(`
		SELECT l.name, r.age
		FROM left_tbl AS l
		INNER JOIN right_tbl AS r
		ON l.id = r.id
		ORDER BY r.age
	`)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Close()

	rows, err := result.ToMaps()
	if err != nil {
		t.Fatalf("result.ToMaps failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestSQLContextUnregisterAndShowTables(t *testing.T) {
	left, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "name": "Alice"},
	})
	if err != nil {
		t.Fatalf("NewDataFrame left failed: %v", err)
	}
	defer left.Close()

	right, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "age": int64(25)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame right failed: %v", err)
	}
	defer right.Close()

	ctx := NewSQLContext().
		Register("left_tbl", left).
		Register("right_tbl", right).
		Unregister("right_tbl")

	tables := ctx.Tables()
	if len(tables) != 1 || tables[0] != "left_tbl" {
		t.Fatalf("unexpected tables after unregister: %#v", tables)
	}

	showTables, err := ctx.Execute("SHOW TABLES")
	if err != nil {
		t.Fatalf("SHOW TABLES failed: %v", err)
	}
	defer showTables.Close()

	rows, err := showTables.ToMaps()
	if err != nil {
		t.Fatalf("SHOW TABLES ToMaps failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 SHOW TABLES row, got %d", len(rows))
	}
	if rows[0]["name"] != "left_tbl" {
		t.Fatalf("unexpected SHOW TABLES rows: %#v", rows)
	}

	_, err = ctx.Execute("SELECT * FROM right_tbl")
	if err == nil {
		t.Fatal("expected SQL on unregistered table to fail")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "right_tbl") {
		t.Fatalf("unexpected unregistered table error: %v", err)
	}
	if !strings.Contains(err.Error(), "SELECT * FROM right_tbl") {
		t.Fatalf("expected SQL error to include query summary, got %v", err)
	}
	if !strings.Contains(err.Error(), "left_tbl") {
		t.Fatalf("expected SQL error to include registered table context, got %v", err)
	}
}

func TestSQLHelperJoin(t *testing.T) {
	left, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	})
	if err != nil {
		t.Fatalf("NewDataFrame left failed: %v", err)
	}
	defer left.Close()

	right, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "age": int64(25)},
		{"id": int64(2), "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame right failed: %v", err)
	}
	defer right.Close()

	result, err := SQL(`
		SELECT l.name, r.age
		FROM left_tbl AS l
		INNER JOIN right_tbl AS r
		ON l.id = r.id
		ORDER BY r.age
	`, map[string]any{
		"left_tbl":  left,
		"right_tbl": right,
	})
	if err != nil {
		t.Fatalf("SQL helper failed: %v", err)
	}
	defer result.Close()

	rows, err := result.ToMaps()
	if err != nil {
		t.Fatalf("result.ToMaps failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != "Alice" || rows[1]["age"] != int64(35) {
		t.Fatalf("unexpected SQL join rows: %#v", rows)
	}
}

func TestSQLContextWithLazyFrame(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
		{"name": "Charlie", "age": int64(40)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	ctx := NewSQLContext().
		Register("adults", df.Filter(Col("age").Gt(Lit(int64(30)))))

	result, err := ctx.Execute("SELECT name FROM adults ORDER BY name")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Close()

	rows, err := result.ToMaps()
	if err != nil {
		t.Fatalf("result.ToMaps failed: %v", err)
	}
	if len(rows) != 2 || rows[0]["name"] != "Bob" || rows[1]["name"] != "Charlie" {
		t.Fatalf("unexpected SQL lazy rows: %#v", rows)
	}
}

func TestSQLContextExecuteLazy(t *testing.T) {
	df, err := NewDataFrame([]map[string]any{
		{"name": "Alice", "age": int64(25)},
		{"name": "Bob", "age": int64(35)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	lf, err := NewSQLContext(map[string]any{
		"people": df,
	}).ExecuteLazy("SELECT name FROM people WHERE age >= 30")
	if err != nil {
		t.Fatalf("ExecuteLazy failed: %v", err)
	}

	rows, err := collectToMaps(t, lf.Filter(Col("name").Eq(Lit("Bob"))))
	if err != nil {
		t.Fatalf("collectToMaps failed: %v", err)
	}
	if len(rows) != 1 || rows[0]["name"] != "Bob" {
		t.Fatalf("unexpected ExecuteLazy rows: %#v", rows)
	}
}

func TestSQLContextExecuteLazyDefersExecution(t *testing.T) {
	lf, err := NewSQLContext(map[string]any{
		"missing_people": ScanCSV("../testdata/does-not-exist.csv"),
	}).ExecuteLazy("SELECT * FROM missing_people")
	if err != nil {
		t.Fatalf("ExecuteLazy should not eagerly fail: %v", err)
	}

	_, err = lf.Collect()
	if err == nil {
		t.Fatal("expected Collect to fail for missing CSV input")
	}
	if !strings.Contains(err.Error(), "does-not-exist.csv") {
		t.Fatalf("unexpected Collect error: %v", err)
	}
}
