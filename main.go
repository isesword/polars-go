package main

import (
	"fmt"
	"log"

	"github.com/isesword/polars-go-bridge/bridge"
	"github.com/isesword/polars-go-bridge/polars"
)

func main() {
	// 加载动态库
	brg, err := bridge.LoadBridge("")
	if err != nil {
		log.Fatalf("Failed to load bridge: %v", err)
	}

	// 获取版本信息
	abiVer := brg.AbiVersion()
	fmt.Printf("ABI Version: %d\n", abiVer)

	engineVer, err := brg.EngineVersion()
	if err != nil {
		log.Fatalf("Failed to get engine version: %v", err)
	}
	fmt.Printf("Engine Version: %s\n", engineVer)

	caps, err := brg.Capabilities()
	if err != nil {
		log.Fatalf("Failed to get capabilities: %v", err)
	}
	fmt.Printf("Capabilities:\n%s\n", caps)

	fmt.Println("\n=== Testing CSV Scan (Mode A: ScanCSV) ===")

	lf := polars.ScanCSV("testdata/sample.csv")
	if err := lf.Print(); err != nil {
		log.Fatalf("Failed to execute CSV scan: %v", err)
	}

	fmt.Println("\n✅ CSV scan finished!")
}
