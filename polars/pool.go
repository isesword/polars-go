package polars

import "sync"

var (
	stringSlicePool = sync.Pool{
		New: func() any {
			return make([]string, 0, 16)
		},
	}
	columnDecoderSlicePool = sync.Pool{
		New: func() any {
			return make([]arrowColumnDecoder, 0, 16)
		},
	}
)

func getStringSlice(size int) []string {
	buf := stringSlicePool.Get().([]string)
	if cap(buf) < size {
		return make([]string, size)
	}
	return buf[:size]
}

func putStringSlice(buf []string) {
	for i := range buf {
		buf[i] = ""
	}
	stringSlicePool.Put(buf[:0])
}

func getArrowColumnDecoders(size int) []arrowColumnDecoder {
	buf := columnDecoderSlicePool.Get().([]arrowColumnDecoder)
	if cap(buf) < size {
		return make([]arrowColumnDecoder, size)
	}
	return buf[:size]
}

func putArrowColumnDecoders(buf []arrowColumnDecoder) {
	for i := range buf {
		buf[i] = nil
	}
	columnDecoderSlicePool.Put(buf[:0])
}
