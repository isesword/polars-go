package polars

import "github.com/isesword/polars-go-bridge/bridge"

func resolveBridge(brg *bridge.Bridge) (*bridge.Bridge, error) {
	if brg != nil {
		return brg, nil
	}
	return bridge.DefaultBridge()
}
