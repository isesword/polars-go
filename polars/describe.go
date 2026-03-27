package polars

import (
	"fmt"
	"math"
)

func describeEagerFrame(df *EagerFrame) (*DataFrame, error) {
	if df == nil {
		return nil, fmt.Errorf("dataframe is nil")
	}

	recordBatch, err := df.ToArrow()
	if err != nil {
		return nil, err
	}
	columnNames := make([]string, len(recordBatch.Schema().Fields()))
	for i, field := range recordBatch.Schema().Fields() {
		columnNames[i] = field.Name
	}
	recordBatch.Release()

	rows, err := df.ToMaps()
	if err != nil {
		return nil, err
	}

	summaryRows := buildDescribeRows(rows, columnNames)
	return NewDataFrame(summaryRows)
}

func buildDescribeRows(rows []map[string]interface{}, columnNames []string) []map[string]any {
	stats := []string{"count", "null_count", "mean", "std", "min", "max"}
	out := make([]map[string]any, 0, len(stats))
	for _, stat := range stats {
		row := map[string]any{"statistic": stat}
		for _, name := range columnNames {
			row[name] = describeValueString(describeStat(rows, name, stat))
		}
		out = append(out, row)
	}
	return out
}

func describeValueString(value any) any {
	if value == nil {
		return nil
	}
	return fmt.Sprintf("%v", value)
}

func describeStat(rows []map[string]interface{}, name string, stat string) any {
	var (
		count     int64
		nullCount int64
		minNum    float64
		maxNum    float64
		sum       float64
		sumSq     float64
		hasNum    bool
		minStr    string
		maxStr    string
		hasStr    bool
	)

	for _, row := range rows {
		value, ok := row[name]
		if !ok || value == nil {
			nullCount++
			continue
		}
		count++
		if n, ok := asFloat64(value); ok {
			if !hasNum {
				minNum, maxNum = n, n
				hasNum = true
			} else {
				minNum = min(minNum, n)
				maxNum = max(maxNum, n)
			}
			sum += n
			sumSq += n * n
			continue
		}
		if s, ok := value.(string); ok {
			if !hasStr {
				minStr, maxStr = s, s
				hasStr = true
			} else {
				if s < minStr {
					minStr = s
				}
				if s > maxStr {
					maxStr = s
				}
			}
		}
	}

	switch stat {
	case "count":
		return count
	case "null_count":
		return nullCount
	case "mean":
		if hasNum && count > 0 {
			return sum / float64(count)
		}
	case "std":
		if hasNum && count > 1 {
			mean := sum / float64(count)
			variance := (sumSq / float64(count)) - (mean * mean)
			if variance < 0 {
				variance = 0
			}
			return math.Sqrt(variance)
		}
	case "min":
		if hasNum {
			return minNum
		}
		if hasStr {
			return minStr
		}
	case "max":
		if hasNum {
			return maxNum
		}
		if hasStr {
			return maxStr
		}
	}
	return nil
}

func asFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}
