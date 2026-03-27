package polars

import (
	"testing"
	"time"
)

func TestToDurationSupportsAdditionalNumericTypes(t *testing.T) {
	cases := []struct {
		name  string
		input any
		want  time.Duration
	}{
		{name: "int8", input: int8(3), want: 3},
		{name: "int16", input: int16(4), want: 4},
		{name: "int32", input: int32(5), want: 5},
		{name: "uint8", input: uint8(6), want: 6},
		{name: "uint16", input: uint16(7), want: 7},
		{name: "uint32", input: uint32(8), want: 8},
		{name: "uintptr", input: uintptr(9), want: 9},
		{name: "float32", input: float32(10), want: 10},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := toDuration(tc.input)
			if err != nil {
				t.Fatalf("toDuration(%T) returned error: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("toDuration(%T) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestNumericConversionsSupportUintptr(t *testing.T) {
	input := uintptr(12)

	if got, err := toInt64(input); err != nil || got != 12 {
		t.Fatalf("toInt64(uintptr) = %v, %v; want 12, nil", got, err)
	}
	if got, err := toUint64(input); err != nil || got != 12 {
		t.Fatalf("toUint64(uintptr) = %v, %v; want 12, nil", got, err)
	}
	if got, err := toFloat64(input); err != nil || got != 12 {
		t.Fatalf("toFloat64(uintptr) = %v, %v; want 12, nil", got, err)
	}
}
