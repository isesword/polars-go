//go:build !cgo
// +build !cgo

package polars

func bridgeArrowExportSupported() bool {
	return false
}
