package polars

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	pb "github.com/isesword/polars-go-bridge/proto"
	"github.com/xuri/excelize/v2"
)

type ExcelReadOptions struct {
	SheetName    string
	SheetID      int
	SheetNames   []string
	SheetIDs     []int
	HasHeader    *bool
	RaiseIfEmpty bool
	Schema       map[string]pb.DataType
	ArrowSchema  *arrow.Schema
}

func normalizeExcelReadOptions(opts []ExcelReadOptions) ExcelReadOptions {
	out := ExcelReadOptions{SheetID: 1, RaiseIfEmpty: true}
	if len(opts) == 0 {
		return out
	}
	in := opts[0]
	if in.SheetName != "" {
		out.SheetName = in.SheetName
	}
	if in.SheetID != 0 {
		out.SheetID = in.SheetID
	}
	if len(in.SheetNames) > 0 {
		out.SheetNames = append([]string(nil), in.SheetNames...)
	}
	if len(in.SheetIDs) > 0 {
		out.SheetIDs = append([]int(nil), in.SheetIDs...)
	}
	if in.HasHeader != nil {
		out.HasHeader = in.HasHeader
	}
	if !in.RaiseIfEmpty {
		out.RaiseIfEmpty = false
	}
	if in.Schema != nil {
		out.Schema = in.Schema
	}
	if in.ArrowSchema != nil {
		out.ArrowSchema = in.ArrowSchema
	}
	return out
}

func (o ExcelReadOptions) hasHeader() bool {
	if o.HasHeader == nil {
		return true
	}
	return *o.HasHeader
}

func ReadExcel(path string, opts ...ExcelReadOptions) (*DataFrame, error) {
	cfg := normalizeExcelReadOptions(opts)

	file, err := excelize.OpenFile(path)
	if err != nil {
		return nil, &ValidationError{Op: "ReadExcel", Message: "failed to open workbook", Err: err}
	}
	defer file.Close()

	sheetName, err := resolveExcelSheet(file, cfg)
	if err != nil {
		return nil, err
	}
	return readExcelSheetAsDataFrame(file, sheetName, cfg)
}

func ReadExcelSheets(path string, opts ...ExcelReadOptions) (map[string]*DataFrame, error) {
	var cfg ExcelReadOptions
	if len(opts) > 0 {
		cfg = opts[0]
	}
	if len(opts) == 0 {
		cfg.RaiseIfEmpty = true
	}

	file, err := excelize.OpenFile(path)
	if err != nil {
		return nil, &ValidationError{Op: "ReadExcelSheets", Message: "failed to open workbook", Err: err}
	}
	defer file.Close()

	sheetNames, err := resolveExcelSheets(file, cfg)
	if err != nil {
		return nil, err
	}

	frames := make(map[string]*DataFrame, len(sheetNames))
	for _, sheetName := range sheetNames {
		df, err := readExcelSheetAsDataFrame(file, sheetName, cfg)
		if err != nil {
			for _, frame := range frames {
				frame.Close()
			}
			return nil, err
		}
		frames[sheetName] = df
	}
	return frames, nil
}

func readExcelSheetAsDataFrame(file *excelize.File, sheetName string, cfg ExcelReadOptions) (*DataFrame, error) {
	rows, err := file.GetRows(sheetName)
	if err != nil {
		return nil, &ValidationError{Op: "ReadExcel", Field: sheetName, Message: "failed to read worksheet rows", Err: err}
	}
	if len(rows) == 0 {
		if cfg.RaiseIfEmpty {
			return nil, &ValidationError{Op: "ReadExcel", Field: sheetName, Message: "worksheet is empty"}
		}
		importOpts := makeExcelImportOptions(cfg)
		return NewDataFrame([]map[string]any{}, importOpts...)
	}

	headers, dataRows, err := normalizeExcelRows(rows, cfg)
	if err != nil {
		return nil, err
	}
	if len(dataRows) == 0 && cfg.RaiseIfEmpty {
		return nil, &ValidationError{Op: "ReadExcel", Field: sheetName, Message: "worksheet has no data rows"}
	}

	records := make([]map[string]any, 0, len(dataRows))
	columnKinds := inferExcelColumnKinds(headers, dataRows)
	for rowIdx, row := range dataRows {
		record := make(map[string]any, len(headers))
		for colIdx, header := range headers {
			if colIdx >= len(row) {
				record[header] = nil
				continue
			}
			record[header] = parseExcelCellValue(row[colIdx], columnKinds[colIdx])
		}
		records = append(records, record)
		if rowIdx == 0 && len(headers) == 0 {
			break
		}
	}

	return NewDataFrame(records, makeExcelImportOptions(cfg)...)
}

func makeExcelImportOptions(cfg ExcelReadOptions) []ImportOption {
	importOpts := make([]ImportOption, 0, 2)
	if cfg.Schema != nil {
		importOpts = append(importOpts, WithSchema(cfg.Schema))
	}
	if cfg.ArrowSchema != nil {
		importOpts = append(importOpts, WithArrowSchema(cfg.ArrowSchema))
	}
	return importOpts
}

func resolveExcelSheet(file *excelize.File, opts ExcelReadOptions) (string, error) {
	if opts.SheetName != "" {
		if idx, err := file.GetSheetIndex(opts.SheetName); err != nil || idx == -1 {
			return "", &ValidationError{Op: "ReadExcel", Field: opts.SheetName, Message: "worksheet does not exist"}
		}
		return opts.SheetName, nil
	}

	sheets := file.GetSheetList()
	if len(sheets) == 0 {
		return "", &ValidationError{Op: "ReadExcel", Message: "workbook has no worksheets"}
	}
	if opts.SheetID < 1 || opts.SheetID > len(sheets) {
		return "", &ValidationError{
			Op:      "ReadExcel",
			Field:   "SheetID",
			Message: fmt.Sprintf("sheet id %d out of range (1..%d)", opts.SheetID, len(sheets)),
		}
	}
	return sheets[opts.SheetID-1], nil
}

func resolveExcelSheets(file *excelize.File, opts ExcelReadOptions) ([]string, error) {
	if opts.SheetName != "" || (opts.SheetID != 0 && len(opts.SheetNames) == 0 && len(opts.SheetIDs) == 0) {
		sheetName, err := resolveExcelSheet(file, opts)
		if err != nil {
			return nil, err
		}
		return []string{sheetName}, nil
	}

	if len(opts.SheetNames) > 0 && len(opts.SheetIDs) > 0 {
		return nil, &ValidationError{Op: "ReadExcelSheets", Message: "SheetNames and SheetIDs cannot be used together"}
	}

	if len(opts.SheetNames) > 0 {
		out := make([]string, 0, len(opts.SheetNames))
		seen := make(map[string]struct{}, len(opts.SheetNames))
		for _, sheetName := range opts.SheetNames {
			if _, exists := seen[sheetName]; exists {
				continue
			}
			single := normalizeExcelReadOptions(nil)
			single.HasHeader = opts.HasHeader
			single.RaiseIfEmpty = opts.RaiseIfEmpty
			single.Schema = opts.Schema
			single.ArrowSchema = opts.ArrowSchema
			single.SheetName = sheetName
			single.SheetID = 0
			name, err := resolveExcelSheet(file, single)
			if err != nil {
				return nil, err
			}
			seen[name] = struct{}{}
			out = append(out, name)
		}
		return out, nil
	}

	if len(opts.SheetIDs) > 0 {
		out := make([]string, 0, len(opts.SheetIDs))
		seen := make(map[string]struct{}, len(opts.SheetIDs))
		for _, sheetID := range opts.SheetIDs {
			single := normalizeExcelReadOptions(nil)
			single.HasHeader = opts.HasHeader
			single.RaiseIfEmpty = opts.RaiseIfEmpty
			single.Schema = opts.Schema
			single.ArrowSchema = opts.ArrowSchema
			single.SheetName = ""
			single.SheetID = sheetID
			name, err := resolveExcelSheet(file, single)
			if err != nil {
				return nil, err
			}
			if _, exists := seen[name]; exists {
				continue
			}
			seen[name] = struct{}{}
			out = append(out, name)
		}
		return out, nil
	}

	sheets := file.GetSheetList()
	if len(sheets) == 0 {
		return nil, &ValidationError{Op: "ReadExcelSheets", Message: "workbook has no worksheets"}
	}
	return sheets, nil
}

func normalizeExcelRows(rows [][]string, opts ExcelReadOptions) ([]string, [][]string, error) {
	if opts.hasHeader() {
		headers, err := normalizeExcelHeaders(rows[0])
		if err != nil {
			return nil, nil, err
		}
		return headers, rows[1:], nil
	}

	width := maxExcelRowWidth(rows)
	headers := make([]string, width)
	for i := range headers {
		headers[i] = fmt.Sprintf("column_%d", i+1)
	}
	return headers, rows, nil
}

func normalizeExcelHeaders(row []string) ([]string, error) {
	headers := make([]string, len(row))
	seen := make(map[string]struct{}, len(row))
	for i, raw := range row {
		name := strings.TrimSpace(raw)
		if name == "" {
			name = fmt.Sprintf("column_%d", i+1)
		}
		if _, exists := seen[name]; exists {
			return nil, &ValidationError{Op: "ReadExcel", Field: name, Message: "duplicate column name"}
		}
		seen[name] = struct{}{}
		headers[i] = name
	}
	return headers, nil
}

func maxExcelRowWidth(rows [][]string) int {
	width := 0
	for _, row := range rows {
		if len(row) > width {
			width = len(row)
		}
	}
	return width
}

type excelColumnKind int

const (
	excelColumnString excelColumnKind = iota
	excelColumnBool
	excelColumnInt
	excelColumnFloat
)

func inferExcelColumnKinds(headers []string, rows [][]string) []excelColumnKind {
	kinds := make([]excelColumnKind, len(headers))
	for colIdx := range headers {
		kinds[colIdx] = inferExcelColumnKind(colIdx, rows)
	}
	return kinds
}

func inferExcelColumnKind(colIdx int, rows [][]string) excelColumnKind {
	boolCandidate := true
	intCandidate := true
	floatCandidate := true
	seenValue := false

	for _, row := range rows {
		if colIdx >= len(row) {
			continue
		}
		value := strings.TrimSpace(row[colIdx])
		if value == "" {
			continue
		}
		seenValue = true
		if _, err := strconv.ParseBool(strings.ToLower(value)); err != nil {
			boolCandidate = false
		}
		if !shouldParseExcelInt(value) {
			intCandidate = false
		}
		if !(shouldParseExcelFloat(value) || shouldParseExcelInt(value)) {
			floatCandidate = false
		}
	}

	if !seenValue {
		return excelColumnString
	}
	if boolCandidate {
		return excelColumnBool
	}
	if intCandidate {
		return excelColumnInt
	}
	if floatCandidate {
		return excelColumnFloat
	}
	return excelColumnString
}

func parseExcelCellValue(raw string, kind excelColumnKind) any {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil
	}

	switch kind {
	case excelColumnString:
		return value
	case excelColumnBool:
		if b, err := strconv.ParseBool(strings.ToLower(value)); err == nil {
			return b
		}
	case excelColumnInt:
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i
		}
		if u, err := strconv.ParseUint(value, 10, 64); err == nil {
			return u
		}
	case excelColumnFloat:
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
	}

	return value
}

func shouldParseExcelInt(value string) bool {
	if value == "" {
		return false
	}
	if strings.ContainsAny(value, ".eE") {
		return false
	}
	trimmed := value
	if trimmed[0] == '+' || trimmed[0] == '-' {
		trimmed = trimmed[1:]
	}
	if trimmed == "" {
		return false
	}
	if len(trimmed) > 1 && trimmed[0] == '0' {
		return false
	}
	for _, r := range trimmed {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func shouldParseExcelFloat(value string) bool {
	if value == "" {
		return false
	}
	if !strings.ContainsAny(value, ".eE") {
		return false
	}
	if len(value) > 1 {
		unsigned := value
		if unsigned[0] == '+' || unsigned[0] == '-' {
			unsigned = unsigned[1:]
		}
		if len(unsigned) > 1 && unsigned[0] == '0' && unsigned[1] != '.' {
			return false
		}
	}
	return true
}
