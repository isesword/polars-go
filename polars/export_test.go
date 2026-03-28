package polars

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestDataFrameJSONExport(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("JSON export requires cgo")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "profile", Type: arrow.StructOf(
			arrow.Field{Name: "city", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		), Nullable: true},
	}, nil)

	df, err := NewDataFrame([]map[string]any{
		{
			"id":      int64(1),
			"name":    "Alice",
			"tags":    []string{"go", "json"},
			"payload": []byte("abc"),
			"profile": map[string]any{"city": "Shanghai", "score": int64(9)},
		},
		{
			"id":      int64(2),
			"name":    "Bob",
			"tags":    []string{"polars"},
			"payload": []byte("xyz"),
			"profile": map[string]any{"city": "Suzhou", "score": int64(7)},
		},
	}, WithArrowSchema(schema))
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	t.Run("WriteJSON", func(t *testing.T) {
		var buf bytes.Buffer
		if err := df.WriteJSON(&buf); err != nil {
			t.Fatalf("WriteJSON failed: %v", err)
		}
		var rows []map[string]any
		if err := json.Unmarshal(buf.Bytes(), &rows); err != nil {
			t.Fatalf("unmarshal JSON failed: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if rows[0]["name"] != "Alice" {
			t.Fatalf("unexpected first row: %#v", rows[0])
		}
		if rows[0]["payload"] != "abc" {
			t.Fatalf("unexpected payload: %#v", rows[0]["payload"])
		}
		profile, ok := rows[0]["profile"].(map[string]any)
		if !ok || profile["city"] != "Shanghai" {
			t.Fatalf("unexpected nested profile: %#v", rows[0]["profile"])
		}
	})

	t.Run("WriteJSONArray", func(t *testing.T) {
		var buf bytes.Buffer
		if err := df.WriteJSON(&buf); err != nil {
			t.Fatalf("WriteJSON failed: %v", err)
		}
		if !strings.HasPrefix(buf.String(), "[") {
			t.Fatalf("expected JSON array string, got %q", buf.String())
		}
	})

	t.Run("WriteNDJSON", func(t *testing.T) {
		var buf bytes.Buffer
		if err := df.WriteNDJSON(&buf); err != nil {
			t.Fatalf("WriteNDJSON failed: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 2 {
			t.Fatalf("expected 2 ndjson lines, got %d", len(lines))
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(lines[1]), &row); err != nil {
			t.Fatalf("unmarshal ndjson line failed: %v", err)
		}
		if row["name"] != "Bob" {
			t.Fatalf("unexpected second ndjson row: %#v", row)
		}
	})

	t.Run("WriteNDJSONGzip", func(t *testing.T) {
		var buf bytes.Buffer
		if err := df.WriteNDJSON(&buf, WriteNDJSONOptions{Compression: NDJSONCompressionGzip}); err != nil {
			t.Fatalf("WriteNDJSON gzip failed: %v", err)
		}
		zr, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatalf("gzip reader failed: %v", err)
		}
		defer zr.Close()
		plain, err := io.ReadAll(zr)
		if err != nil {
			t.Fatalf("read gzip payload failed: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(plain)), "\n")
		if len(lines) != 2 {
			t.Fatalf("expected 2 gzip ndjson lines, got %d", len(lines))
		}
	})

	t.Run("WriteNDJSONInvalidCompression", func(t *testing.T) {
		var buf bytes.Buffer
		err := df.WriteNDJSON(&buf, WriteNDJSONOptions{Compression: NDJSONCompression("zip")})
		if err == nil {
			t.Fatal("expected invalid NDJSON compression to fail")
		}
		if !strings.Contains(err.Error(), `unsupported NDJSON compression "zip"`) {
			t.Fatalf("expected compression value in error, got %v", err)
		}
		if !strings.Contains(err.Error(), `"none"`) || !strings.Contains(err.Error(), `"gzip"`) {
			t.Fatalf("expected supported compression hint, got %v", err)
		}
	})
}

func TestLazyFrameJSONExport(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("JSON export requires cgo")
	}

	df, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	lf := df.Filter(Col("id").Gt(Lit(1)))

	t.Run("SinkJSON", func(t *testing.T) {
		var buf bytes.Buffer
		if err := lf.SinkJSON(&buf); err != nil {
			t.Fatalf("SinkJSON failed: %v", err)
		}
		var rows []map[string]any
		if err := json.Unmarshal(buf.Bytes(), &rows); err != nil {
			t.Fatalf("unmarshal JSON failed: %v", err)
		}
		if len(rows) != 1 || rows[0]["name"] != "Bob" {
			t.Fatalf("unexpected lazy json rows: %#v", rows)
		}
	})

	t.Run("SinkJSONString", func(t *testing.T) {
		var buf bytes.Buffer
		if err := lf.SinkJSON(&buf); err != nil {
			t.Fatalf("SinkJSON failed: %v", err)
		}
		if !strings.Contains(buf.String(), "\"Bob\"") {
			t.Fatalf("unexpected lazy json string: %q", buf.String())
		}
	})

	t.Run("SinkNDJSON", func(t *testing.T) {
		var buf bytes.Buffer
		if err := lf.SinkNDJSON(&buf); err != nil {
			t.Fatalf("SinkNDJSON failed: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 1 {
			t.Fatalf("expected 1 ndjson line, got %d", len(lines))
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &row); err != nil {
			t.Fatalf("unmarshal ndjson line failed: %v", err)
		}
		if row["name"] != "Bob" {
			t.Fatalf("unexpected lazy ndjson row: %#v", row)
		}
	})

	t.Run("SinkNDJSONFile", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "lazy.jsonl")
		if err := lf.SinkNDJSONFile(path); err != nil {
			t.Fatalf("SinkNDJSONFile failed: %v", err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read sink file failed: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) != 1 {
			t.Fatalf("expected 1 ndjson line, got %d", len(lines))
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &row); err != nil {
			t.Fatalf("unmarshal ndjson file line failed: %v", err)
		}
		if row["name"] != "Bob" {
			t.Fatalf("unexpected lazy ndjson file row: %#v", row)
		}
	})

	t.Run("SinkNDJSONGzip", func(t *testing.T) {
		var buf bytes.Buffer
		if err := lf.SinkNDJSON(&buf, SinkNDJSONOptions{Compression: NDJSONCompressionGzip}); err != nil {
			t.Fatalf("SinkNDJSON gzip failed: %v", err)
		}
		zr, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatalf("gzip reader failed: %v", err)
		}
		defer zr.Close()
		plain, err := io.ReadAll(zr)
		if err != nil {
			t.Fatalf("read gzip payload failed: %v", err)
		}
		if !strings.Contains(string(plain), "\"Bob\"") {
			t.Fatalf("unexpected lazy gzip ndjson payload: %q", string(plain))
		}
	})

	t.Run("SinkNDJSONFileGzip", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "lazy.jsonl.gz")
		if err := lf.SinkNDJSONFile(path, SinkNDJSONOptions{Compression: NDJSONCompressionGzip}); err != nil {
			t.Fatalf("SinkNDJSONFile gzip failed: %v", err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatalf("open gzip sink file failed: %v", err)
		}
		defer file.Close()
		zr, err := gzip.NewReader(file)
		if err != nil {
			t.Fatalf("gzip reader failed: %v", err)
		}
		defer zr.Close()
		plain, err := io.ReadAll(zr)
		if err != nil {
			t.Fatalf("read gzip sink payload failed: %v", err)
		}
		if !strings.Contains(string(plain), "\"Bob\"") {
			t.Fatalf("unexpected lazy gzip sink payload: %q", string(plain))
		}
	})
}

func TestDataFrameParquetExport(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("Parquet export requires cgo")
	}

	df, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "name": "Alice", "age": int64(30)},
		{"id": int64(2), "name": "Bob", "age": int64(31)},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	path := filepath.Join(t.TempDir(), "eager.parquet")
	if err := df.WriteParquetFile(path); err != nil {
		t.Fatalf("WriteParquetFile failed: %v", err)
	}

	rows, err := collectToMaps(t, ScanParquet(path).Sort(SortOptions{By: []Expr{Col("id")}}))
	if err != nil {
		t.Fatalf("ScanParquet collect failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != "Alice" || rows[1]["name"] != "Bob" {
		t.Fatalf("unexpected rows after parquet round trip: %#v", rows)
	}
}

func TestLazyFrameParquetExport(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("Parquet export requires cgo")
	}

	df, err := NewDataFrame([]map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
		{"id": int64(3), "name": "Carol"},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	path := filepath.Join(t.TempDir(), "lazy.parquet")
	if err := df.Filter(Col("id").Gt(Lit(1))).SinkParquetFile(path); err != nil {
		t.Fatalf("SinkParquetFile failed: %v", err)
	}

	rows, err := collectToMaps(t, ScanParquet(path).Sort(SortOptions{By: []Expr{Col("id")}}))
	if err != nil {
		t.Fatalf("ScanParquet collect failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != "Bob" || rows[1]["name"] != "Carol" {
		t.Fatalf("unexpected rows after lazy parquet round trip: %#v", rows)
	}
}
