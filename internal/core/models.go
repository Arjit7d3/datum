package core

// Trade is the domain model for trade events
type Trade struct {
	Symbol    string
	Price     float64
	Quantity  float64
	Timestamp int64
}

// Candlestick is the domain model for candlestick/kline data
type Candlestick struct {
	Symbol     string
	Interval   string
	StartTime  int64
	OpenPrice  float64
	ClosePrice float64
	HighPrice  float64
	LowPrice   float64
	Volume     float64
	CloseTime  int64
}

// CandlestickQueryArgs represents arguments for a candlestick query
type CandlestickQueryArgs struct {
	Symbol    string
	Interval  string
	StartTime int64
	EndTime   int64
}

// DepthLevel represents a single price/quantity level in the order book
type DepthLevel struct {
	Price    float64
	Quantity float64
}

// Depth is the domain model for diff depth stream updates
type Depth struct {
	Symbol        string
	Bids          []DepthLevel
	Asks          []DepthLevel
	FirstUpdateID int64
	FinalUpdateID int64
	Timestamp     int64
}
