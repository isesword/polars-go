package polars

import (
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

var temporalLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02",
}

var timeOnlyLayouts = []string{
	"15:04:05.999999999",
	"15:04:05.999999",
	"15:04:05",
	time.RFC3339Nano,
	time.RFC3339,
}

func normalizeTemporalValueForJSON(value any, dataType DataType) (string, error) {
	switch dataType {
	case DataTypeDate:
		t, err := toDateTime(value)
		if err != nil {
			return "", err
		}
		return t.Format("2006-01-02"), nil
	case DataTypeDatetime:
		t, err := toDateTime(value)
		if err != nil {
			return "", err
		}
		return t.Format(time.RFC3339Nano), nil
	case DataTypeTime:
		t, err := toTimeOfDay(value)
		if err != nil {
			return "", err
		}
		return t.Format("15:04:05.999999999"), nil
	default:
		return "", fmt.Errorf("unsupported temporal data type: %s", dataType.String())
	}
}

func toArrowDate32(value any) (arrow.Date32, error) {
	t, err := toDateTime(value)
	if err != nil {
		return 0, err
	}
	return arrow.Date32FromTime(t), nil
}

func toArrowTimestamp(value any) (time.Time, error) {
	return toDateTime(value)
}

func toArrowTime64Micro(value any) (arrow.Time64, error) {
	t, err := toTimeOfDay(value)
	if err != nil {
		return 0, err
	}

	duration := time.Duration(t.Hour())*time.Hour +
		time.Duration(t.Minute())*time.Minute +
		time.Duration(t.Second())*time.Second +
		time.Duration(t.Nanosecond())

	return arrow.Time64(duration / time.Microsecond), nil
}

func toDateTime(value any) (time.Time, error) {
	if value == nil {
		return time.Time{}, fmt.Errorf("datetime value is nil; hint: %s", temporalValueHint("datetime", temporalLayouts))
	}

	switch v := value.(type) {
	case time.Time:
		return v, nil
	case *time.Time:
		if v == nil {
			return time.Time{}, fmt.Errorf("datetime value is nil; hint: %s", temporalValueHint("datetime", temporalLayouts))
		}
		return *v, nil
	case string:
		return parseTemporalString(v, temporalLayouts, "datetime")
	case []byte:
		return parseTemporalString(string(v), temporalLayouts, "datetime")
	default:
		return time.Time{}, fmt.Errorf(
			"cannot convert %s to datetime (value=%s); hint: %s",
			describeValueType(value),
			describeValue(value),
			temporalValueHint("datetime", temporalLayouts),
		)
	}
}

func toTimeOfDay(value any) (time.Time, error) {
	if value == nil {
		return time.Time{}, fmt.Errorf("time value is nil; hint: %s", temporalValueHint("time", timeOnlyLayouts))
	}

	switch v := value.(type) {
	case time.Time:
		return v, nil
	case *time.Time:
		if v == nil {
			return time.Time{}, fmt.Errorf("time value is nil; hint: %s", temporalValueHint("time", timeOnlyLayouts))
		}
		return *v, nil
	case string:
		return parseTemporalString(v, timeOnlyLayouts, "time")
	case []byte:
		return parseTemporalString(string(v), timeOnlyLayouts, "time")
	default:
		return time.Time{}, fmt.Errorf(
			"cannot convert %s to time (value=%s); hint: %s",
			describeValueType(value),
			describeValue(value),
			temporalValueHint("time", timeOnlyLayouts),
		)
	}
}

func parseTemporalString(value string, layouts []string, kind string) (time.Time, error) {
	value = strings.TrimSpace(value)
	for _, layout := range layouts {
		if t, err := time.Parse(layout, value); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf(
		"cannot parse %s value %q; hint: %s",
		kind,
		value,
		temporalValueHint(kind, layouts),
	)
}
