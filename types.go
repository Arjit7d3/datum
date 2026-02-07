package datum

import "github.com/Arjit7d3/datum/internal/core"

// Re-export core types for convenience
type (
	TradeData       = core.TradeData
	CandlestickData = core.CandlestickData
	RawCandlestick  = core.RawCandlestick
)
