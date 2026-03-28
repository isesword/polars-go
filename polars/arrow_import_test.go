package polars

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/isesword/polars-go/bridge"
	pb "github.com/isesword/polars-go/proto"
	"google.golang.org/protobuf/proto"
)

func TestArrowZeroCopy(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("zero-copy requires cgo")
	}

	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	lf := NewLazyFrame(nil).Select(
		Col("name"),
		Col("age"),
		Col("age").Add(Lit(1)).Alias("age_plus"),
		Col("name").StrToUppercase().Alias("name_upper"),
	)

	plan := &pb.Plan{
		PlanVersion: 1,
		Root:        lf.root,
	}
	planBytes, err := proto.Marshal(plan)
	if err != nil {
		t.Fatalf("Failed to marshal plan: %v", err)
	}

	handle, err := brg.CompilePlan(planBytes)
	if err != nil {
		t.Fatalf("Failed to compile plan: %v", err)
	}
	defer brg.FreePlan(handle)

	inSchema, inArray, cleanupInput, err := buildArrowInput()
	if err != nil {
		t.Fatalf("Failed to build Arrow input: %v", err)
	}
	defer cleanupInput()

	outSchema, outArray, err := brg.ExecuteArrow(handle, inSchema, inArray)
	if err != nil {
		t.Fatalf("ExecuteArrow failed: %v", err)
	}

	rec, err := importArrowRecordBatch(outSchema, outArray)
	if err != nil {
		bridge.ReleaseArrowSchema(outSchema)
		bridge.ReleaseArrowArray(outArray)
		t.Fatalf("Failed to import Arrow result: %v", err)
	}
	bridge.ReleaseArrowSchema(outSchema)
	bridge.ReleaseArrowArray(outArray)
	defer rec.Release()

	if rec.NumRows() != 3 {
		t.Fatalf("Expected 3 rows, got %d", rec.NumRows())
	}
	if rec.NumCols() != 4 {
		t.Fatalf("Expected 4 columns, got %d", rec.NumCols())
	}

	fields := rec.Schema().Fields()
	if fields[0].Name != "name" || fields[1].Name != "age" ||
		fields[2].Name != "age_plus" || fields[3].Name != "name_upper" {
		t.Fatalf("Unexpected schema fields: %#v", fields)
	}

	readString := func(col arrow.Array, idx int) (string, bool) {
		if col.IsNull(idx) {
			return "", false
		}
		switch c := col.(type) {
		case *array.String:
			return c.Value(idx), true
		case *array.LargeString:
			return c.Value(idx), true
		case *array.BinaryView:
			return string(c.Value(idx)), true
		case *array.StringView:
			return c.Value(idx), true
		default:
			return "", false
		}
	}

	readInt64 := func(col arrow.Array, idx int) (int64, bool) {
		if col.IsNull(idx) {
			return 0, false
		}
		switch c := col.(type) {
		case *array.Int64:
			return c.Value(idx), true
		case *array.Int32:
			return int64(c.Value(idx)), true
		case *array.Int16:
			return int64(c.Value(idx)), true
		case *array.Int8:
			return int64(c.Value(idx)), true
		case *array.Uint64:
			return int64(c.Value(idx)), true
		case *array.Uint32:
			return int64(c.Value(idx)), true
		case *array.Uint16:
			return int64(c.Value(idx)), true
		case *array.Uint8:
			return int64(c.Value(idx)), true
		default:
			return 0, false
		}
	}

	nameCol := rec.Column(0)
	ageCol := rec.Column(1)
	agePlusCol := rec.Column(2)
	upperCol := rec.Column(3)

	if v, ok := readString(nameCol, 0); !ok || v != "alice" {
		t.Fatalf("Expected name alice, got %#v", v)
	}
	if v, ok := readInt64(ageCol, 1); !ok || v != 21 {
		t.Fatalf("Expected age 21, got %#v", v)
	}
	if v, ok := readInt64(agePlusCol, 2); !ok || v != 46 {
		t.Fatalf("Expected age_plus 46, got %#v", v)
	}
	if v, ok := readString(upperCol, 2); !ok || v != "CARL" {
		t.Fatalf("Expected name_upper CARL, got %#v", v)
	}
}

func TestArrowExecution(t *testing.T) {
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	t.Run("ArrowExecutionNoInput", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("name"),
			Col("age"),
		).Limit(3)

		plan := &pb.Plan{
			PlanVersion: 1,
			Root:        lf.root,
		}

		planBytes, err := proto.Marshal(plan)
		if err != nil {
			t.Fatalf("Failed to marshal plan: %v", err)
		}

		handle, err := brg.CompilePlan(planBytes)
		if err != nil {
			t.Fatalf("Failed to compile plan: %v", err)
		}
		defer brg.FreePlan(handle)

		outSchema, outArray, err := brg.ExecuteArrow(handle, nil, nil)
		if err != nil {
			t.Fatalf("Arrow execution failed: %v", err)
		}

		if outSchema != nil {
			defer bridge.ReleaseArrowSchema(outSchema)
		}
		if outArray != nil {
			defer bridge.ReleaseArrowArray(outArray)
		}

		if outSchema == nil || outArray == nil {
			t.Fatal("Expected non-nil Arrow schema and array")
		}
	})

	t.Run("ArrowBackedToMapsConsistency", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("name"),
			Col("age"),
			Col("salary"),
		).Filter(Col("age").Gt(Lit(25))).Limit(5)

		resultRows, err := collectToMaps(t, lf)
		if err != nil {
			t.Fatalf("ToMaps execution failed: %v", err)
		}

		plan := &pb.Plan{
			PlanVersion: 1,
			Root:        lf.root,
		}
		planBytes, _ := proto.Marshal(plan)
		handle, _ := brg.CompilePlan(planBytes)
		defer brg.FreePlan(handle)

		outSchema, outArray, err := brg.ExecuteArrow(handle, nil, nil)
		if err != nil {
			t.Fatalf("Arrow execution failed: %v", err)
		}
		defer bridge.ReleaseArrowSchema(outSchema)
		defer bridge.ReleaseArrowArray(outArray)

		if len(resultRows) == 0 {
			t.Fatal("Expected at least one row from ToMaps")
		}
	})

	t.Run("ArrowErrorHandling", func(t *testing.T) {
		lf := ScanCSV("nonexistent.csv").Select(Col("name"))

		plan := &pb.Plan{
			PlanVersion: 1,
			Root:        lf.root,
		}
		planBytes, _ := proto.Marshal(plan)
		handle, err := brg.CompilePlan(planBytes)
		if err != nil {
			t.Fatalf("Failed to compile plan: %v", err)
		}
		defer brg.FreePlan(handle)

		_, _, err = brg.ExecuteArrow(handle, nil, nil)
		if err == nil {
			t.Error("Expected error for non-existent file")
		}
	})
}

func TestDataFrameFromArrowRecord(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow record import requires cgo")
	}

	alloc := memory.NewGoAllocator()
	nameBuilder := array.NewStringBuilder(alloc)
	ageBuilder := array.NewInt64Builder(alloc)
	defer nameBuilder.Release()
	defer ageBuilder.Release()

	nameBuilder.AppendValues([]string{"alice", "bob", "carl"}, nil)
	ageBuilder.AppendValues([]int64{18, 20, 35}, nil)

	nameArr := nameBuilder.NewArray()
	ageArr := ageBuilder.NewArray()
	defer nameArr.Release()
	defer ageArr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{nameArr, ageArr}, 3)
	defer record.Release()

	df, err := NewDataFrameFromArrowRecordBatch(record)
	if err != nil {
		t.Fatalf("Failed to create DataFrame from Arrow record: %v", err)
	}
	defer df.Close()

	rows, err := collectToMaps(t, df.
		Filter(Col("age").Gt(Lit(19))).
		Select(Col("name"), Col("age")))
	if err != nil {
		t.Fatalf("Failed to query imported Arrow DataFrame: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows after filter, got %d", len(rows))
	}

	if rows[0]["name"] != "bob" || rows[1]["name"] != "carl" {
		t.Fatalf("Unexpected filtered rows: %#v", rows)
	}
}

func TestRowsToArrowToNewDataFrame(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	rows := []map[string]any{
		{
			"id":         uint64(1),
			"name":       "Alice",
			"age":        nil,
			"salary":     12500.5,
			"created_at": "2026-03-24 10:00:00",
		},
		{
			"id":         uint64(2),
			"name":       "Bob",
			"age":        20,
			"salary":     15888,
			"created_at": "2026-03-24 11:00:00",
		},
	}
	schema := map[string]DataType{
		"id":         DataTypeUInt64,
		"name":       DataTypeUTF8,
		"age":        DataTypeInt32,
		"salary":     DataTypeFloat64,
		"created_at": DataTypeUTF8,
	}

	record, err := NewArrowRecordBatchFromRowsWithSchema(rows, schema)
	if err != nil {
		t.Fatalf("Failed to build Arrow record from rows: %v", err)
	}
	defer record.Release()

	df, err := NewDataFrameFromArrowRecordBatch(record)
	if err != nil {
		t.Fatalf("Failed to create DataFrame from Arrow record: %v", err)
	}
	defer df.Close()

	result, err := collectToMaps(t, df.
		Filter(Col("salary").Gt(Lit(13000.0))).
		Select(Col("id"), Col("name"), Col("age"), Col("salary")))
	if err != nil {
		t.Fatalf("Failed to query Arrow-imported DataFrame: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 filtered row, got %d", len(result))
	}

	if got, ok := result[0]["id"].(uint64); !ok || got != 2 {
		t.Fatalf("Expected filtered id uint64(2), got %T %v", result[0]["id"], result[0]["id"])
	}

	if got := result[0]["name"]; got != "Bob" {
		t.Fatalf("Expected filtered name Bob, got %v", got)
	}
}

func TestRowsTemporalArrowRoundTrip(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	createdAt := time.Date(2026, 3, 24, 10, 30, 45, 123456000, time.FixedZone("CST", 8*3600))
	rows := []map[string]any{
		{
			"id":         int64(1),
			"created_at": createdAt,
			"birthday":   createdAt,
			"clock_at":   createdAt,
		},
	}
	schema := map[string]DataType{
		"id":         DataTypeInt64,
		"created_at": DataTypeDatetime,
		"birthday":   DataTypeDate,
		"clock_at":   DataTypeTime,
	}

	record, err := NewArrowRecordBatchFromRowsWithSchema(rows, schema)
	if err != nil {
		t.Fatalf("Failed to build Arrow record from rows: %v", err)
	}
	defer record.Release()

	fieldsByName := make(map[string]arrow.DataType, len(record.Schema().Fields()))
	for _, field := range record.Schema().Fields() {
		fieldsByName[field.Name] = field.Type
	}
	if fieldsByName["created_at"].ID() != arrow.TIMESTAMP {
		t.Fatalf("Expected created_at to use Arrow timestamp, got %s", fieldsByName["created_at"])
	}
	if fieldsByName["birthday"].ID() != arrow.DATE32 {
		t.Fatalf("Expected birthday to use Arrow date32, got %s", fieldsByName["birthday"])
	}
	if fieldsByName["clock_at"].ID() != arrow.TIME64 {
		t.Fatalf("Expected clock_at to use Arrow time64, got %s", fieldsByName["clock_at"])
	}

	df, err := NewDataFrameFromArrowRecordBatch(record)
	if err != nil {
		t.Fatalf("Failed to create DataFrame from Arrow record: %v", err)
	}
	defer df.Close()

	result, err := df.ToMaps()
	if err != nil {
		t.Fatalf("Failed to materialize temporal rows: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result))
	}

	datetimeVal, ok := result[0]["created_at"].(time.Time)
	if !ok {
		t.Fatalf("Expected created_at to round-trip as time.Time, got %T", result[0]["created_at"])
	}
	if datetimeVal.UTC().Format(time.RFC3339Nano) != createdAt.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("Unexpected created_at value: got %s want %s", datetimeVal.UTC().Format(time.RFC3339Nano), createdAt.UTC().Format(time.RFC3339Nano))
	}

	dateVal, ok := result[0]["birthday"].(time.Time)
	if !ok {
		t.Fatalf("Expected birthday to round-trip as time.Time, got %T", result[0]["birthday"])
	}
	if got := dateVal.Format("2006-01-02"); got != createdAt.Format("2006-01-02") {
		t.Fatalf("Unexpected birthday value: got %s want %s", got, createdAt.Format("2006-01-02"))
	}

	timeVal, ok := result[0]["clock_at"].(string)
	if !ok {
		t.Fatalf("Expected clock_at to round-trip as string, got %T", result[0]["clock_at"])
	}
	if want := createdAt.Format("15:04:05.999999999"); timeVal != want {
		t.Fatalf("Unexpected clock_at value: got %s want %s", timeVal, want)
	}
}

func TestArrowNestedRoundTrip(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow import/export requires cgo")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "profile", Type: arrow.StructOf(
			arrow.Field{Name: "city", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		), Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)

	tagsBuilder := builder.Field(1).(*array.ListBuilder)
	tagsValues := tagsBuilder.ValueBuilder().(*array.StringBuilder)
	tagsBuilder.Append(true)
	tagsValues.Append("go")
	tagsValues.Append("arrow")
	tagsBuilder.Append(true)
	tagsValues.Append("polars")
	tagsValues.AppendNull()

	profileBuilder := builder.Field(2).(*array.StructBuilder)
	profileCity := profileBuilder.FieldBuilder(0).(*array.StringBuilder)
	profileScore := profileBuilder.FieldBuilder(1).(*array.Int64Builder)
	profileBuilder.Append(true)
	profileCity.Append("Shanghai")
	profileScore.Append(9)
	profileBuilder.Append(true)
	profileCity.AppendNull()
	profileScore.Append(7)

	record := builder.NewRecordBatch()
	defer record.Release()

	df, err := NewDataFrameFromArrowRecordBatch(record)
	if err != nil {
		t.Fatalf("failed to create DataFrame from nested Arrow record: %v", err)
	}
	defer df.Close()

	rows, err := df.ToMaps()
	if err != nil {
		t.Fatalf("nested Arrow ToMaps failed: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	firstTags, ok := rows[0]["tags"].([]interface{})
	if !ok {
		t.Fatalf("expected first tags to be []interface{}, got %T", rows[0]["tags"])
	}
	if len(firstTags) != 2 || firstTags[0] != "go" || firstTags[1] != "arrow" {
		t.Fatalf("unexpected first tags: %#v", firstTags)
	}

	secondTags, ok := rows[1]["tags"].([]interface{})
	if !ok {
		t.Fatalf("expected second tags to be []interface{}, got %T", rows[1]["tags"])
	}
	if len(secondTags) != 2 || secondTags[0] != "polars" || secondTags[1] != nil {
		t.Fatalf("unexpected second tags: %#v", secondTags)
	}

	firstProfile, ok := rows[0]["profile"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected first profile to be map[string]interface{}, got %T", rows[0]["profile"])
	}
	if firstProfile["city"] != "Shanghai" {
		t.Fatalf("unexpected first profile city: %#v", firstProfile["city"])
	}
	if got, err := toInt64(firstProfile["score"]); err != nil || got != 9 {
		t.Fatalf("unexpected first profile score: %v (%v)", firstProfile["score"], err)
	}

	secondProfile, ok := rows[1]["profile"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected second profile to be map[string]interface{}, got %T", rows[1]["profile"])
	}
	if secondProfile["city"] != nil {
		t.Fatalf("expected second profile city to be nil, got %#v", secondProfile["city"])
	}
	if got, err := toInt64(secondProfile["score"]); err != nil || got != 7 {
		t.Fatalf("unexpected second profile score: %v (%v)", secondProfile["score"], err)
	}
}

func TestArrowBinaryRoundTrip(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow import/export requires cgo")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "blob", Type: arrow.BinaryTypes.LargeBinary, Nullable: true},
		{Name: "token", Type: &arrow.FixedSizeBinaryType{ByteWidth: 4}, Nullable: true},
	}, nil)

	rows := []map[string]any{
		{
			"id":      int64(1),
			"payload": []byte("abc"),
			"blob":    "longer-binary",
			"token":   [4]byte{1, 2, 3, 4},
		},
		{
			"id":      int64(2),
			"payload": nil,
			"blob":    []byte{9, 8, 7},
			"token":   []byte{5, 6, 7, 8},
		},
	}

	df, err := NewDataFrame(rows, WithArrowSchema(schema))
	if err != nil {
		t.Fatalf("NewDataFrame binary Arrow schema failed: %v", err)
	}
	defer df.Close()

	out, err := df.ToMaps()
	if err != nil {
		t.Fatalf("ToMaps failed: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(out))
	}
	assertBinaryValue(t, out[0]["payload"], []byte("abc"))
	assertBinaryValue(t, out[0]["blob"], []byte("longer-binary"))
	assertBinaryValue(t, out[0]["token"], []byte{1, 2, 3, 4})
	if out[1]["payload"] != nil {
		t.Fatalf("expected nil payload, got %#v", out[1]["payload"])
	}
}

func TestParseArrowRecordBatchBinaryTypes(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "blob", Type: arrow.BinaryTypes.LargeBinary, Nullable: true},
		{Name: "token", Type: &arrow.FixedSizeBinaryType{ByteWidth: 4}, Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	builder.Field(0).(*array.BinaryBuilder).Append([]byte("abc"))
	builder.Field(1).(*array.BinaryBuilder).Append([]byte("longer-binary"))
	builder.Field(2).(*array.FixedSizeBinaryBuilder).Append([]byte{1, 2, 3, 4})

	record := builder.NewRecordBatch()
	defer record.Release()

	rows, err := parseArrowRecordBatch(record)
	if err != nil {
		t.Fatalf("parseArrowRecordBatch failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	assertBinaryValue(t, rows[0]["payload"], []byte("abc"))
	assertBinaryValue(t, rows[0]["blob"], []byte("longer-binary"))
	assertBinaryValue(t, rows[0]["token"], []byte{1, 2, 3, 4})
}

func TestNewDataFrameWithArrowSchema(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "profile", Type: arrow.StructOf(
			arrow.Field{Name: "city", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		), Nullable: true},
	}, nil)

	rows := []map[string]any{
		{
			"id":   int64(1),
			"tags": []string{"go", "arrow"},
			"profile": map[string]any{
				"city":  "Shanghai",
				"score": int64(9),
			},
		},
		{
			"id":   int64(2),
			"tags": []any{"polars", nil},
			"profile": map[string]any{
				"city":  nil,
				"score": int64(7),
			},
		},
	}

	df, err := NewDataFrame(rows, WithArrowSchema(schema))
	if err != nil {
		t.Fatalf("NewDataFrame with Arrow schema failed: %v", err)
	}
	defer df.Close()

	out, err := df.ToMaps()
	if err != nil {
		t.Fatalf("ToMaps failed: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(out))
	}
	if got := out[0]["tags"].([]interface{}); len(got) != 2 || got[0] != "go" || got[1] != "arrow" {
		t.Fatalf("unexpected first tags: %#v", out[0]["tags"])
	}
	if got := out[1]["profile"].(map[string]interface{}); got["city"] != nil {
		t.Fatalf("expected second city to be nil, got %#v", got["city"])
	}
}

func TestNewDataFrameWithArrowSchemaStructValue(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	type profile struct {
		City  string
		Score int64
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "profile", Type: arrow.StructOf(
			arrow.Field{Name: "City", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "Score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		), Nullable: true},
	}, nil)

	df, err := NewDataFrame([]map[string]any{
		{
			"id":      int64(1),
			"profile": profile{City: "Shanghai", Score: 9},
		},
	}, WithArrowSchema(schema))
	if err != nil {
		t.Fatalf("NewDataFrame struct-valued Arrow schema failed: %v", err)
	}
	defer df.Close()

	out, err := df.ToMaps()
	if err != nil {
		t.Fatalf("ToMaps failed: %v", err)
	}
	profileMap, ok := out[0]["profile"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected struct column to round-trip as map[string]interface{}, got %T", out[0]["profile"])
	}
	if profileMap["City"] != "Shanghai" || profileMap["Score"] != int64(9) {
		t.Fatalf("unexpected struct column output: %#v", profileMap)
	}
}

func TestNewDataFrameFromColumnsWithArrowSchema(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "tags", Type: arrow.LargeListOf(arrow.BinaryTypes.String), Nullable: true},
	}, nil)

	df, err := NewDataFrame(map[string]interface{}{
		"id": []int64{1, 2},
		"tags": []any{
			[]string{"go", "arrow"},
			[]any{"polars", nil},
		},
	}, WithArrowSchema(schema))
	if err != nil {
		t.Fatalf("NewDataFrame columns with Arrow schema failed: %v", err)
	}
	defer df.Close()

	out, err := df.ToMaps()
	if err != nil {
		t.Fatalf("ToMaps failed: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(out))
	}

	firstTags, ok := out[0]["tags"].([]interface{})
	if !ok {
		t.Fatalf("expected first tags []interface{}, got %T", out[0]["tags"])
	}
	if len(firstTags) != 2 || firstTags[0] != "go" || firstTags[1] != "arrow" {
		t.Fatalf("unexpected first tags: %#v", firstTags)
	}

	secondTags, ok := out[1]["tags"].([]interface{})
	if !ok {
		t.Fatalf("expected second tags []interface{}, got %T", out[1]["tags"])
	}
	if len(secondTags) != 2 || secondTags[0] != "polars" || secondTags[1] != nil {
		t.Fatalf("unexpected second tags: %#v", secondTags)
	}
}

func TestArrowSchemaMismatchNestedError(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "profile", Type: arrow.StructOf(
			arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		), Nullable: true},
	}, nil)

	_, err := NewArrowRecordBatchFromRowsWithArrowSchema([]map[string]any{
		{
			"profile": map[string]any{
				"score": "oops",
			},
		},
	}, schema)
	if err == nil {
		t.Fatal("expected nested schema mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "profile.score") {
		t.Fatalf("expected error to mention nested field path, got %v", err)
	}
}

func TestArrowSchemaMismatchError(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("arrow row conversion requires cgo")
	}

	rows := []map[string]any{
		{"age": "not-a-number"},
	}
	schema := map[string]DataType{
		"age": DataTypeInt32,
	}

	_, err := NewArrowRecordBatchFromRowsWithSchema(rows, schema)
	if err == nil {
		t.Fatal("expected schema mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "field age") && !strings.Contains(err.Error(), "age:") {
		t.Fatalf("expected error to mention age field, got %v", err)
	}
	if !strings.Contains(err.Error(), "int32-compatible") {
		t.Fatalf("expected error to mention target int32 conversion, got %v", err)
	}
}

func TestDataFrameFromArrowManaged(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("managed arrow frame requires cgo")
	}

	record, err := NewArrowRecordBatchFromRowsWithSchema([]map[string]any{
		{"id": uint64(1), "name": "Alice"},
		{"id": uint64(2), "name": "Bob"},
	}, map[string]DataType{
		"id":   DataTypeUInt64,
		"name": DataTypeUTF8,
	})
	if err != nil {
		t.Fatalf("NewArrowRecordBatchFromRowsWithSchema failed: %v", err)
	}

	frame, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}
	defer frame.Close()

	df, err := frame.Filter(Col("id").Eq(Lit(2))).Collect()
	if err != nil {
		t.Fatalf("Managed arrow frame query failed: %v", err)
	}
	defer df.Free()
	rows, err := df.ToMaps()
	if err != nil {
		t.Fatalf("Managed arrow frame ToMaps failed: %v", err)
	}
	if len(rows) != 1 || rows[0]["name"] != "Bob" {
		t.Fatalf("Unexpected managed arrow frame rows: %#v", rows)
	}

	polarsStyleRecord, err := NewArrowRecordBatchFromRowsWithSchema([]map[string]any{
		{"id": uint64(3), "name": "Carl"},
	}, map[string]DataType{
		"id":   DataTypeUInt64,
		"name": DataTypeUTF8,
	})
	if err != nil {
		t.Fatalf("NewArrowRecordBatchFromRowsWithSchema failed: %v", err)
	}

	polarsStyleFrame, err := NewDataFrameFromArrow(polarsStyleRecord)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}
	defer polarsStyleFrame.Close()

	polarsStyleRows, err := polarsStyleFrame.ToMaps()
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow ToMaps failed: %v", err)
	}
	if len(polarsStyleRows) != 1 || polarsStyleRows[0]["name"] != "Carl" {
		t.Fatalf("Unexpected rows from NewDataFrameFromArrow: %#v", polarsStyleRows)
	}
}

func TestToArrow(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("ToArrow requires cgo")
	}

	df, err := NewDataFrame([]map[string]any{
		{"id": uint64(1), "name": "Alice"},
		{"id": uint64(2), "name": "Bob"},
	})
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}
	defer df.Close()

	recordBatch, err := df.ToArrow()
	if err != nil {
		t.Fatalf("DataFrame.ToArrow failed: %v", err)
	}
	defer recordBatch.Release()

	if got := recordBatch.NumRows(); got != 2 {
		t.Fatalf("Expected 2 rows, got %d", got)
	}
	if got := recordBatch.NumCols(); got != 2 {
		t.Fatalf("Expected 2 cols, got %d", got)
	}

	rows, err := parseArrowRecordBatch(recordBatch)
	if err != nil {
		t.Fatalf("parseArrowRecordBatch failed: %v", err)
	}
	if len(rows) != 2 || rows[1]["name"] != "Bob" {
		t.Fatalf("Unexpected rows from ToArrow: %#v", rows)
	}
}

func TestEagerFrameToArrow(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("ToArrow requires cgo")
	}

	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("LoadBridge failed: %v", err)
	}

	eager, err := NewEagerFrameFromRowsWithSchema(brg, []map[string]any{
		{"id": uint64(10), "city": "Shanghai"},
	}, map[string]DataType{
		"id":   DataTypeUInt64,
		"city": DataTypeUTF8,
	})
	if err != nil {
		t.Fatalf("NewEagerFrameFromRowsWithSchema failed: %v", err)
	}
	defer eager.Free()

	recordBatch, err := eager.ToArrow()
	if err != nil {
		t.Fatalf("EagerFrame.ToArrow failed: %v", err)
	}
	defer recordBatch.Release()

	rows, err := parseArrowRecordBatch(recordBatch)
	if err != nil {
		t.Fatalf("parseArrowRecordBatch failed: %v", err)
	}
	if len(rows) != 1 || rows[0]["city"] != "Shanghai" {
		t.Fatalf("Unexpected rows from eager ToArrowBatch: %#v", rows)
	}
}
