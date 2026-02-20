package core

// IStream represents a data stream that yields items of type T
type IStream[T any] interface {
	OnMessage(callback func(T))
}

// Provider defines the interface for market data providers
type Provider interface {
	// Factory methods for creating streams
	NewTradeStream(symbol string) (IStream[Trade], error)
	NewCandlestickStream(symbol, interval string) (IStream[Candlestick], error)
	NewDepthStream(symbol string) (IStream[Depth], error)
}
