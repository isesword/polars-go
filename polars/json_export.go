package polars

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
)

const (
	jsonFormatCodeArray  int32 = 0
	jsonFormatCodeNDJSON int32 = 1
)

type NDJSONCompression string

const (
	NDJSONCompressionNone NDJSONCompression = "none"
	NDJSONCompressionGzip NDJSONCompression = "gzip"
)

type WriteNDJSONOptions struct {
	Compression      NDJSONCompression
	CompressionLevel int
}

type SinkNDJSONOptions = WriteNDJSONOptions

func normalizeWriteNDJSONOptions(opts []WriteNDJSONOptions) WriteNDJSONOptions {
	if len(opts) == 0 {
		return WriteNDJSONOptions{Compression: NDJSONCompressionNone}
	}
	out := opts[0]
	if out.Compression == "" {
		out.Compression = NDJSONCompressionNone
	}
	return out
}

func ndjsonCompressionCode(compression NDJSONCompression) (int32, error) {
	switch compression {
	case NDJSONCompressionNone:
		return 0, nil
	case NDJSONCompressionGzip:
		return 1, nil
	default:
		return 0, fmt.Errorf(
			"unsupported NDJSON compression %q; supported values: %q, %q",
			compression,
			NDJSONCompressionNone,
			NDJSONCompressionGzip,
		)
	}
}

func validateNDJSONCompressionLevel(opts WriteNDJSONOptions) error {
	if opts.Compression != NDJSONCompressionGzip {
		return nil
	}
	level := opts.CompressionLevel
	if level == 0 {
		return nil
	}
	if _, err := gzip.NewWriterLevel(io.Discard, level); err != nil {
		return fmt.Errorf(
			"invalid gzip compression level %d: %w; hint: use gzip.DefaultCompression (0) or a value between %d and %d",
			level,
			err,
			gzip.HuffmanOnly,
			gzip.BestCompression,
		)
	}
	return nil
}

func ndjsonTempFilePattern(opts WriteNDJSONOptions) string {
	if opts.Compression == NDJSONCompressionGzip {
		return "polars-*.jsonl.gz"
	}
	return "polars-*.jsonl"
}

func openNDJSONTempFile(opts WriteNDJSONOptions) (*os.File, error) {
	pattern := ndjsonTempFilePattern(opts)
	file, err := os.CreateTemp("", pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary NDJSON file: %w", err)
	}
	return file, nil
}

func wrapNDJSONWriter(w io.Writer, opts WriteNDJSONOptions) (io.Writer, func() error, error) {
	switch opts.Compression {
	case NDJSONCompressionNone:
		return w, func() error { return nil }, nil
	case NDJSONCompressionGzip:
		level := opts.CompressionLevel
		if level == 0 {
			level = gzip.DefaultCompression
		}
		zw, err := gzip.NewWriterLevel(w, level)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"invalid gzip compression level %d: %w; hint: use gzip.DefaultCompression (0) or a value between %d and %d",
				level,
				err,
				gzip.HuffmanOnly,
				gzip.BestCompression,
			)
		}
		return zw, zw.Close, nil
	default:
		return nil, nil, fmt.Errorf(
			"unsupported NDJSON compression %q; supported values: %q, %q",
			opts.Compression,
			NDJSONCompressionNone,
			NDJSONCompressionGzip,
		)
	}
}

func (f *DataFrame) WriteJSON(w io.Writer) error {
	if err := invalidDataFrameError(f); err != nil {
		return wrapOp("DataFrame.WriteJSON", err)
	}
	return f.df.WriteJSON(w)
}

func (f *DataFrame) WriteNDJSON(w io.Writer, opts ...WriteNDJSONOptions) error {
	if err := invalidDataFrameError(f); err != nil {
		return wrapOp("DataFrame.WriteNDJSON", err)
	}
	return f.df.WriteNDJSON(w, opts...)
}

func (df *EagerFrame) exportJSON(formatCode int32, op string) ([]byte, error) {
	if err := invalidEagerFrameError(df); err != nil {
		return nil, wrapOp(op, err)
	}
	data, err := df.brg.DataFrameToJSON(df.handle, formatCode)
	if err != nil {
		return nil, wrapOp(op, err)
	}
	return data, nil
}

func (df *EagerFrame) WriteJSON(w io.Writer) error {
	if err := invalidEagerFrameError(df); err != nil {
		return wrapOp("EagerFrame.WriteJSON", err)
	}
	data, err := df.exportJSON(jsonFormatCodeArray, "EagerFrame.WriteJSON")
	if err != nil {
		return err
	}
	if _, err := bytes.NewBuffer(data).WriteTo(w); err != nil {
		return wrapOp("EagerFrame.WriteJSON", err)
	}
	return nil
}

func (df *EagerFrame) WriteNDJSON(w io.Writer, opts ...WriteNDJSONOptions) error {
	if err := invalidEagerFrameError(df); err != nil {
		return wrapOp("EagerFrame.WriteNDJSON", err)
	}
	cfg := normalizeWriteNDJSONOptions(opts)
	out, closeFn, err := wrapNDJSONWriter(w, cfg)
	if err != nil {
		return wrapOp("EagerFrame.WriteNDJSON", err)
	}
	defer closeFn()
	data, err := df.exportJSON(jsonFormatCodeNDJSON, "EagerFrame.WriteNDJSON")
	if err != nil {
		return err
	}
	if _, err := bytes.NewBuffer(data).WriteTo(out); err != nil {
		return wrapOp("EagerFrame.WriteNDJSON", err)
	}
	return nil
}
