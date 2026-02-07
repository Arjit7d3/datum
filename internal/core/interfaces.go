package core

import "context"

// IStream represents a data stream that yields items of type T
type IStream[T any] interface {
	GetStreamParams() (symbol string, streamName string)
	Decode(data []byte) (T, error)
}

// IQuery represents a data query that returns type T
type IQuery[T any] interface {
	GetEndpoint() string
	GetQueryParameters() map[string]string
	Decode(data []byte) (T, error)
}

// Provider defines the interface for market data providers
type Provider interface {
	// Subscribe subscribes to a stream and returns a channel of raw bytes
	Subscribe(ctx context.Context, symbol string, streamName string) (<-chan []byte, error)

	// Query executes a query and returns raw bytes
	Query(ctx context.Context, endpoint string, params map[string]string) ([]byte, error)

	// Factory methods for creating streams
	NewTradeStream(symbol string) IStream[TradeData]
	NewCandlestickStream(symbol, interval string) IStream[CandlestickData]

	// Factory methods for creating queries
	NewCandlestickQuery(symbol, interval string) IQuery[[]RawCandlestick]
}
