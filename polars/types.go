package polars

import pb "github.com/isesword/polars-go/proto"

// DataType re-exports the public schema enum from the proto layer.
type DataType = pb.DataType

// CsvEncoding re-exports CSV encoding options from the proto layer.
type CsvEncoding = pb.CsvEncoding

const (
	DataTypeInt64    DataType = pb.DataType_INT64
	DataTypeInt32    DataType = pb.DataType_INT32
	DataTypeInt16    DataType = pb.DataType_INT16
	DataTypeInt8     DataType = pb.DataType_INT8
	DataTypeUInt64   DataType = pb.DataType_UINT64
	DataTypeUInt32   DataType = pb.DataType_UINT32
	DataTypeUInt16   DataType = pb.DataType_UINT16
	DataTypeUInt8    DataType = pb.DataType_UINT8
	DataTypeFloat64  DataType = pb.DataType_FLOAT64
	DataTypeFloat32  DataType = pb.DataType_FLOAT32
	DataTypeBool     DataType = pb.DataType_BOOL
	DataTypeUTF8     DataType = pb.DataType_UTF8
	DataTypeDate     DataType = pb.DataType_DATE
	DataTypeDatetime DataType = pb.DataType_DATETIME
	DataTypeTime     DataType = pb.DataType_TIME

	CSVEncodingUTF8      CsvEncoding = pb.CsvEncoding_CSV_ENCODING_UTF8
	CSVEncodingLossyUTF8 CsvEncoding = pb.CsvEncoding_CSV_ENCODING_LOSSY_UTF8
)
