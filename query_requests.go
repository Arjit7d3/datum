package datum

import "github.com/Arjit7d3/datum/internal/core"

// CandlesticksQuery creates a candlestick query request
func CandlesticksQuery(symbol, interval string) QueryRequest[[]core.RawCandlestick] {
	return newQueryRequest(func(p core.Provider) core.IQuery[[]core.RawCandlestick] {
		return p.NewCandlestickQuery(symbol, interval)
	})
}
