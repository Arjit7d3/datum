package datum

import (
	"fmt"
	"time"
)

// QueryOption is a function that modifies query parameters
type QueryOption func(params map[string]string)

// Limit sets the limit parameter for a query
func Limit(n int) QueryOption {
	return func(params map[string]string) {
		params["limit"] = fmt.Sprintf("%d", n)
	}
}

// StartTime sets the startTime parameter for a query
func StartTime(t time.Time) QueryOption {
	return func(params map[string]string) {
		params["startTime"] = fmt.Sprintf("%d", t.UnixMilli())
	}
}

// EndTime sets the endTime parameter for a query
func EndTime(t time.Time) QueryOption {
	return func(params map[string]string) {
		params["endTime"] = fmt.Sprintf("%d", t.UnixMilli())
	}
}
