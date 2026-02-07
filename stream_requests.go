package datum

import "github.com/Arjit7d3/datum/internal/core"

// Trades creates a trade stream request
func Trades(symbol string) StreamRequest[core.TradeData] {
	return newStreamRequest(func(p core.Provider) core.IStream[core.TradeData] {
		return p.NewTradeStream(symbol)
	})
}

// Candlesticks creates a candlestick stream request
func Candlesticks(symbol, interval string) StreamRequest[core.CandlestickData] {
	return newStreamRequest(func(p core.Provider) core.IStream[core.CandlestickData] {
		return p.NewCandlestickStream(symbol, interval)
	})
}
