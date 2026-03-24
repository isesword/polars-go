//go:build cgo
// +build cgo

package polars

func arrowRowsPathSupported() bool {
	return true
}
