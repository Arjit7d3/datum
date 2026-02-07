package datum

import (
	"context"
	"fmt"
	"log"

	"github.com/Arjit7d3/datum/internal/core"
	"github.com/Arjit7d3/datum/internal/providers"
)

// Client is the main entry point for interacting with market data providers
type Client struct {
	provider core.Provider
}

// NewClient creates a new client for the specified provider
func NewClient(providerName string) (*Client, error) {
	provider, err := providers.NewProvider(providerName)
	if err != nil {
		return nil, fmt.Errorf("failed to create provider: %w", err)
	}

	return &Client{
		provider: provider,
	}, nil
}

// StreamBuilder builds and executes stream subscriptions
type StreamBuilder[T any] struct {
	provider core.Provider
	request  StreamRequest[T]
}

// Subscribe subscribes to the stream and returns a channel of typed responses
func (sb *StreamBuilder[T]) Subscribe(ctx context.Context) (<-chan T, error) {
	stream := sb.request.CreateStream(sb.provider)
	symbol, streamName := stream.GetStreamParams()

	rawCh, err := sb.provider.Subscribe(ctx, symbol, streamName)
	if err != nil {
		return nil, err
	}

	typedCh := make(chan T)

	go func() {
		defer close(typedCh)
		for data := range rawCh {
			item, err := stream.Decode(data)
			if err != nil {
				log.Println("Error decoding stream data:", err)
				continue
			}
			typedCh <- item
		}
	}()

	return typedCh, nil
}

// QueryBuilder builds and executes queries
type QueryBuilder[T any] struct {
	provider core.Provider
	request  QueryRequest[T]
}

// Execute executes the query and returns the result
func (qb *QueryBuilder[T]) Execute(ctx context.Context) (T, error) {
	var zero T

	query := qb.request.CreateQuery(qb.provider)
	endpoint := query.GetEndpoint()
	params := query.GetQueryParameters()

	raw, err := qb.provider.Query(ctx, endpoint, params)
	if err != nil {
		return zero, err
	}

	return query.Decode(raw)
}
