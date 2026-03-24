package bridge

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadBridge(t *testing.T) {
	// 跳过如果没有设置库路径
	libPath := os.Getenv("POLARS_BRIDGE_LIB")
	if libPath == "" {
		t.Skip("POLARS_BRIDGE_LIB not set, skipping test")
	}

	brg, err := LoadBridge(libPath)
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	// 测试 ABI 版本
	abiVer := brg.AbiVersion()
	if abiVer != 1 {
		t.Errorf("Expected ABI version 1, got %d", abiVer)
	}

	t.Logf("✅ ABI Version: %d", abiVer)
}

func TestLoadBridgeDefaultCached(t *testing.T) {
	libPath := os.Getenv("POLARS_BRIDGE_LIB")
	if libPath == "" {
		t.Skip("POLARS_BRIDGE_LIB not set, skipping test")
	}

	t.Setenv("POLARS_BRIDGE_LIB", libPath)

	brg1, err := LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load default bridge first time: %v", err)
	}

	brg2, err := LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load default bridge second time: %v", err)
	}

	if brg1 != brg2 {
		t.Fatal("Expected default bridge loader to reuse the same instance")
	}
}

func TestLoadBridgeExplicitPathCached(t *testing.T) {
	libPath := os.Getenv("POLARS_BRIDGE_LIB")
	if libPath == "" {
		t.Skip("POLARS_BRIDGE_LIB not set, skipping test")
	}

	absPath, err := filepath.Abs(libPath)
	if err != nil {
		t.Fatalf("Failed to resolve library path: %v", err)
	}

	brg1, err := LoadBridge(libPath)
	if err != nil {
		t.Fatalf("Failed to load bridge by original path: %v", err)
	}

	brg2, err := LoadBridge(absPath)
	if err != nil {
		t.Fatalf("Failed to load bridge by absolute path: %v", err)
	}

	if brg1 != brg2 {
		t.Fatal("Expected explicit path loader to reuse the same instance for the same dylib")
	}
}

func TestEngineVersion(t *testing.T) {
	libPath := os.Getenv("POLARS_BRIDGE_LIB")
	if libPath == "" {
		t.Skip("POLARS_BRIDGE_LIB not set, skipping test")
	}

	brg, err := LoadBridge(libPath)
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	version, err := brg.EngineVersion()
	if err != nil {
		t.Fatalf("Failed to get engine version: %v", err)
	}

	if version == "" {
		t.Error("Engine version is empty")
	}

	t.Logf("✅ Engine Version: %s", version)
}

func TestCapabilities(t *testing.T) {
	libPath := os.Getenv("POLARS_BRIDGE_LIB")
	if libPath == "" {
		t.Skip("POLARS_BRIDGE_LIB not set, skipping test")
	}

	brg, err := LoadBridge(libPath)
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	caps, err := brg.Capabilities()
	if err != nil {
		t.Fatalf("Failed to get capabilities: %v", err)
	}

	if caps == "" {
		t.Error("Capabilities is empty")
	}

	t.Logf("✅ Capabilities:\n%s", caps)
}

func TestConcurrentLastError(t *testing.T) {
	libPath := os.Getenv("POLARS_BRIDGE_LIB")
	if libPath == "" {
		t.Skip("POLARS_BRIDGE_LIB not set, skipping test")
	}

	brg, err := LoadBridge(libPath)
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	// 测试并发调用不会互相干扰
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			// 触发一些调用
			_, _ = brg.EngineVersion()
			_, _ = brg.Capabilities()
		}(i)
	}

	// 等待所有 goroutine 完成
	for i := 0; i < 10; i++ {
		<-done
	}

	t.Log("✅ Concurrent calls completed without error")
}

func TestInvalidPlanCompile(t *testing.T) {
	libPath := os.Getenv("POLARS_BRIDGE_LIB")
	if libPath == "" {
		t.Skip("POLARS_BRIDGE_LIB not set, skipping test")
	}

	brg, err := LoadBridge(libPath)
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	// 尝试编译无效的计划
	invalidPlan := []byte("invalid protobuf data")
	_, err = brg.CompilePlan(invalidPlan)
	if err == nil {
		t.Error("Expected error for invalid plan, got nil")
	}

	t.Logf("✅ Invalid plan correctly rejected: %v", err)
}
