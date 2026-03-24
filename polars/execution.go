package polars

import "fmt"

// ExecutionOptions configures runtime execution behavior shared by collect and SQL paths.
type ExecutionOptions struct {
	MemoryLimitBytes int64
}

var currentExecutionOptions ExecutionOptions

// SetExecutionOptions applies execution settings to the active bridge.
//
// MemoryLimitBytes <= 0 disables the limit.
func SetExecutionOptions(opts ExecutionOptions) error {
	if opts.MemoryLimitBytes < 0 {
		return fmt.Errorf("memory limit must be >= 0")
	}
	brg, err := resolveBridge(nil)
	if err != nil {
		return err
	}
	if err := brg.SetMemoryLimitBytes(opts.MemoryLimitBytes); err != nil {
		return err
	}
	currentExecutionOptions = opts
	return nil
}

// ExecutionConfig returns the last execution options applied through SetExecutionOptions.
func ExecutionConfig() ExecutionOptions {
	return currentExecutionOptions
}
