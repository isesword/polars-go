//go:build !cgo
// +build !cgo

package polars

import (
	"fmt"

	"github.com/isesword/polars-go-bridge/bridge"
)

func registerExprMapBatchesBridgeCallback(_ *bridge.Bridge) error {
	return fmt.Errorf("Expr.MapBatches requires cgo (set CGO_ENABLED=1)")
}
