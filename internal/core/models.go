package core

// TradeData is the domain model for trade events
type TradeData struct {
	Symbol    string
	Price     float64
	Quantity  float64
	Timestamp int64
}

// CandlestickData is the domain model for candlestick/kline data
type CandlestickData struct {
	Symbol     string
	Interval   string
	StartTime  int64
	CloseTime  int64
	OpenPrice  float64
	ClosePrice float64
	HighPrice  float64
	LowPrice   float64
	Volume     float64
}

// RawCandlestick represents raw candlestick data as array
type RawCandlestick []any
