package polars

import "testing"

func TestWithSchemaAcceptsPolarsDataType(t *testing.T) {
	cfg := applyImportOptions(WithSchema(map[string]DataType{
		"id":   UInt64,
		"name": String,
	}))

	if got := cfg.schema["id"]; got != UInt64 {
		t.Fatalf("expected UInt64 schema, got %v", got)
	}
	if got := cfg.schema["name"]; got != String {
		t.Fatalf("expected String schema, got %v", got)
	}
}

func TestCSVScanOptionsAcceptPolarsAliases(t *testing.T) {
	encoding := CSVEncodingLossyUTF8
	lf := ScanCSVWithOptions("dummy.csv", CSVScanOptions{
		Schema: map[string]DataType{
			"id": UInt64,
		},
		Encoding: &encoding,
	})

	if lf == nil || lf.root == nil {
		t.Fatal("expected lazy frame root to be initialized")
	}

	scan := lf.root.GetCsvScan()
	if scan == nil {
		t.Fatal("expected csv scan node")
	}
	if got := DataType(scan.Schema["id"]); got != UInt64 {
		t.Fatalf("expected UInt64 schema, got %v", got)
	}
	if scan.Encoding == nil || CsvEncoding(*scan.Encoding) != CSVEncodingLossyUTF8 {
		t.Fatalf("expected lossy utf8 encoding, got %v", scan.Encoding)
	}
}
