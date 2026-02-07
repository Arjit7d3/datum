package datum

import "github.com/Arjit7d3/datum/internal/core"

// TradesStream defines a request for a trade stream
type TradesStream struct {
	Symbol string
}

func (r TradesStream) CreateStream(p core.Provider) core.IStream[core.Trade] {
	return p.NewTradeStream(r.Symbol)
}

// CandlesticksStream defines a request for a candlestick stream
type CandlesticksStream struct {
	Symbol   string
	Interval string
}

func (r CandlesticksStream) CreateStream(p core.Provider) core.IStream[core.Candlestick] {
	return p.NewCandlestickStream(r.Symbol, r.Interval)
}
