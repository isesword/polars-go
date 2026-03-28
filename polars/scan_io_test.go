package polars

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

func collectToMaps(t *testing.T, lf *LazyFrame) ([]map[string]interface{}, error) {
	t.Helper()

	df, err := lf.Collect()
	if err != nil {
		return nil, err
	}
	defer df.Free()

	return df.ToMaps()
}

func TestScanCSV(t *testing.T) {
	t.Run("BasicCSVScan", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv")
		if lf == nil {
			t.Fatal("ScanCSV returned nil")
		}

		result, err := collectToMaps(t, lf)
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}

		fmt.Println(result)

		if len(result) != 7 {
			t.Fatalf("expected 7 rows, got %d", len(result))
		}

		if result[0]["name"] != "Alice" {
			t.Fatalf("unexpected first row name: %#v", result[0]["name"])
		}
	})

	t.Run("CSVScanWithLimit", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Limit(5)

		result, err := collectToMaps(t, lf)
		if err != nil {
			t.Fatalf("Collect with limit failed: %v", err)
		}

		if len(result) != 5 {
			t.Fatalf("expected 5 rows, got %d", len(result))
		}
	})

	t.Run("CSVScanWithFilterSelect", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").
			Filter(Col("age").Gt(Lit(25))).
			Select(Col("name"), Col("age")).
			Limit(3)

		result, err := collectToMaps(t, lf)
		if err != nil {
			t.Fatalf("Collect with filter+select failed: %v", err)
		}

		if len(result) == 0 || len(result) > 3 {
			t.Fatalf("expected 1-3 rows, got %d", len(result))
		}

		for i, row := range result {
			ageVal, ok := row["age"].(int64)
			if !ok {
				t.Fatalf("row %d: age is not int64: %#v", i, row["age"])
			}
			if ageVal <= 25 {
				t.Fatalf("row %d: expected age > 25, got %d", i, ageVal)
			}
			if _, ok := row["name"].(string); !ok {
				t.Fatalf("row %d: name is not string: %#v", i, row["name"])
			}
		}
	})

	t.Run("NonExistentFile", func(t *testing.T) {
		lf := ScanCSV("nonexistent.csv")

		_, err := collectToMaps(t, lf)
		if err == nil {
			t.Error("Expected error for non-existent file, got nil")
		} else {
			t.Logf("Correctly got error for non-existent file: %v", err)
		}
	})

	t.Run("DataFrameChaining", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv")
		df, err := lf.Collect()
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}
		defer df.Free()

		result, err := collectToMaps(t, df.Filter(Col("age").Gt(Lit(28))).
			Select(Col("name"), Col("age")).
			Limit(2))

		if err != nil {
			t.Fatalf("DataFrame chaining failed: %v", err)
		}

		if len(result) > 2 {
			t.Fatalf("expected at most 2 rows, got %d", len(result))
		}

		for i, row := range result {
			age := row["age"].(int64)
			if age <= 28 {
				t.Fatalf("row %d: expected age > 28, got %d", i, age)
			}
			t.Logf("Row %d: name=%v, age=%d", i, row["name"], age)
		}
	})

	t.Run("CSVScanWithOptions", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "custom.csv")
		content := "# comment line\nname;age;joined_at\n'Alice';25;2026-03-24\n'Bob';NA;2026-03-25\n"
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("failed to write csv fixture: %v", err)
		}

		hasHeader := true
		separator := byte(';')
		quoteChar := byte('\'')
		inferSchemaLength := uint64(10)
		nullValue := "NA"
		tryParseDates := true
		commentPrefix := "#"

		rows, err := collectToMaps(t, ScanCSVWithOptions(path, CSVScanOptions{
			HasHeader:         &hasHeader,
			Separator:         &separator,
			InferSchemaLength: &inferSchemaLength,
			NullValue:         &nullValue,
			TryParseDates:     &tryParseDates,
			QuoteChar:         &quoteChar,
			CommentPrefix:     &commentPrefix,
			Schema: map[string]DataType{
				"name":      DataTypeUTF8,
				"age":       DataTypeInt64,
				"joined_at": DataTypeDate,
			},
		}))
		if err != nil {
			t.Fatalf("csv scan with options failed: %v", err)
		}

		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if rows[0]["name"] != "Alice" {
			t.Fatalf("unexpected first name: %#v", rows[0]["name"])
		}
		if rows[1]["age"] != nil {
			t.Fatalf("expected second age to be nil, got %#v", rows[1]["age"])
		}
		joinedAt, ok := rows[0]["joined_at"].(time.Time)
		if !ok {
			t.Fatalf("expected joined_at to parse as time.Time, got %T", rows[0]["joined_at"])
		}
		if got := joinedAt.Format("2006-01-02"); got != "2026-03-24" {
			t.Fatalf("unexpected joined_at: %s", got)
		}
	})

	t.Run("CSVScanWithLossyEncodingAndIgnoreErrors", func(t *testing.T) {
		tempDir := t.TempDir()
		path := filepath.Join(tempDir, "invalid_utf8.csv")
		content := []byte("name,age\nAlice,20\nBad\xffName,not-a-number\n")
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("failed to write csv fixture: %v", err)
		}

		encoding := CSVEncodingLossyUTF8
		ignoreErrors := true
		rows, err := collectToMaps(t, ScanCSVWithOptions(path, CSVScanOptions{
			Encoding:     &encoding,
			IgnoreErrors: &ignoreErrors,
			Schema: map[string]DataType{
				"name": DataTypeUTF8,
				"age":  DataTypeInt64,
			},
		}))
		if err != nil {
			t.Fatalf("csv scan with lossy encoding failed: %v", err)
		}

		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if rows[0]["name"] != "Alice" {
			t.Fatalf("unexpected first name: %#v", rows[0]["name"])
		}
		if rows[1]["age"] != nil {
			t.Fatalf("expected second age to be nil after ignore_errors, got %#v", rows[1]["age"])
		}
		if got, ok := rows[1]["name"].(string); !ok || !strings.Contains(got, "Bad") {
			t.Fatalf("expected lossy UTF-8 string, got %T %#v", rows[1]["name"], rows[1]["name"])
		}
	})
}

func TestScanParquet(t *testing.T) {
	t.Run("BasicParquetScan", func(t *testing.T) {
		lf := ScanParquet("../testdata/fortune1000_2024.parquet").Limit(3)

		rows, err := collectToMaps(t, lf)
		if err != nil {
			t.Fatalf("Parquet scan failed: %v", err)
		}

		if len(rows) != 3 {
			t.Fatalf("Expected 3 rows, got %d", len(rows))
		}

		if len(rows[0]) == 0 {
			t.Fatal("Expected parquet row to contain columns")
		}
	})
}

func TestReadExcel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sample.xlsx")
	file := excelize.NewFile()
	defer file.Close()

	if err := file.SetSheetRow("Sheet1", "A1", &[]any{"id", "name", "active", "score"}); err != nil {
		t.Fatalf("SetSheetRow Sheet1 header failed: %v", err)
	}
	if err := file.SetSheetRow("Sheet1", "A2", &[]any{1, "Alice", true, 9.5}); err != nil {
		t.Fatalf("SetSheetRow Sheet1 row1 failed: %v", err)
	}
	if err := file.SetSheetRow("Sheet1", "A3", &[]any{2, "Bob", false, 7.25}); err != nil {
		t.Fatalf("SetSheetRow Sheet1 row2 failed: %v", err)
	}

	sheet2 := "Raw"
	if _, err := file.NewSheet(sheet2); err != nil {
		t.Fatalf("NewSheet failed: %v", err)
	}
	if err := file.SetSheetRow(sheet2, "A1", &[]any{"001", "Carol"}); err != nil {
		t.Fatalf("SetSheetRow Raw row1 failed: %v", err)
	}
	if err := file.SetSheetRow(sheet2, "A2", &[]any{"002", "Drew"}); err != nil {
		t.Fatalf("SetSheetRow Raw row2 failed: %v", err)
	}

	if err := file.SaveAs(path); err != nil {
		t.Fatalf("SaveAs failed: %v", err)
	}

	t.Run("DefaultSheet", func(t *testing.T) {
		df, err := ReadExcel(path)
		if err != nil {
			t.Fatalf("ReadExcel failed: %v", err)
		}
		defer df.Close()

		rows, err := df.ToMaps()
		if err != nil {
			t.Fatalf("ToMaps failed: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if rows[0]["name"] != "Alice" || rows[1]["active"] != false {
			t.Fatalf("unexpected excel rows: %#v", rows)
		}
	})

	t.Run("SheetNameNoHeader", func(t *testing.T) {
		hasHeader := false
		df, err := ReadExcel(path, ExcelReadOptions{
			SheetName: sheet2,
			HasHeader: &hasHeader,
		})
		if err != nil {
			t.Fatalf("ReadExcel sheet Raw failed: %v", err)
		}
		defer df.Close()

		rows, err := df.ToMaps()
		if err != nil {
			t.Fatalf("ToMaps failed: %v", err)
		}
		if got := rows[0]["column_1"]; got != "001" {
			t.Fatalf("expected leading-zero string in column_1, got %#v", got)
		}
		if got := rows[1]["column_2"]; got != "Drew" {
			t.Fatalf("unexpected second column value: %#v", got)
		}
	})

	t.Run("InvalidSheetID", func(t *testing.T) {
		if _, err := ReadExcel(path, ExcelReadOptions{SheetID: 99}); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput for invalid sheet id, got %v", err)
		}
	})

	t.Run("ReadExcelSheetsAll", func(t *testing.T) {
		frames, err := ReadExcelSheets(path)
		if err != nil {
			t.Fatalf("ReadExcelSheets failed: %v", err)
		}
		defer func() {
			for _, df := range frames {
				df.Close()
			}
		}()
		if len(frames) != 2 {
			t.Fatalf("expected 2 sheets, got %d", len(frames))
		}
		rows, err := frames["Sheet1"].ToMaps()
		if err != nil {
			t.Fatalf("ToMaps Sheet1 failed: %v", err)
		}
		if len(rows) != 2 || rows[1]["name"] != "Bob" {
			t.Fatalf("unexpected Sheet1 rows: %#v", rows)
		}
	})

	t.Run("ReadExcelSheetsSubset", func(t *testing.T) {
		hasHeader := false
		frames, err := ReadExcelSheets(path, ExcelReadOptions{
			SheetNames: []string{sheet2},
			HasHeader:  &hasHeader,
		})
		if err != nil {
			t.Fatalf("ReadExcelSheets subset failed: %v", err)
		}
		defer func() {
			for _, df := range frames {
				df.Close()
			}
		}()
		if len(frames) != 1 {
			t.Fatalf("expected 1 sheet, got %d", len(frames))
		}
		rows, err := frames[sheet2].ToMaps()
		if err != nil {
			t.Fatalf("ToMaps Raw failed: %v", err)
		}
		if rows[0]["column_1"] != "001" {
			t.Fatalf("unexpected Raw rows: %#v", rows)
		}
	})
}
