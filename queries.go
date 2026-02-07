package datum

import "github.com/Arjit7d3/datum/internal/core"

// CandlesticksQuery defines a request for a candlestick query
type CandlesticksQuery struct {
	Symbol    string
	Interval  string
	StartTime int64
	EndTime   int64
}

// CreateQuery implements the QueryRequest interface
func (r CandlesticksQuery) CreateQuery(p core.Provider) core.IQuery[[]core.Candlestick] {
	args := core.CandlestickQueryArgs{
		Symbol:    r.Symbol,
		Interval:  r.Interval,
		StartTime: r.StartTime,
		EndTime:   r.EndTime,
	}

	return p.NewCandlestickQuery(args)
}
