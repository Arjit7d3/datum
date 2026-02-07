package datum

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Arjit7d3/datum/internal/core"
	"github.com/Arjit7d3/datum/internal/providers"
)

// Client is the main entry point for interacting with market data providers
type Client struct {
	provider     core.Provider
	providerName string
}

// NewClient creates a new client for the specified provider
func NewClient(providerName string) (*Client, error) {
	provider, err := providers.NewProvider(providerName)
	if err != nil {
		return nil, fmt.Errorf("failed to create provider: %w", err)
	}

	return &Client{
		provider:     provider,
		providerName: providerName,
	}, nil
}

// StreamBuilder builds and executes stream subscriptions
type StreamBuilder[T any] struct {
	provider core.Provider
	request  StreamRequest[T]
}

// Subscribe subscribes to the stream and returns a channel of typed responses
func (sb *StreamBuilder[T]) Subscribe(ctx context.Context) (<-chan T, error) {
	stream := sb.request.factory(sb.provider)
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
	options  []QueryOption
}

// Limit sets the limit for the query
func (qb *QueryBuilder[T]) Limit(n int) *QueryBuilder[T] {
	qb.options = append(qb.options, Limit(n))
	return qb
}

// StartTime sets the start time for the query
func (qb *QueryBuilder[T]) StartTime(t time.Time) *QueryBuilder[T] {
	qb.options = append(qb.options, StartTime(t))
	return qb
}

// EndTime sets the end time for the query
func (qb *QueryBuilder[T]) EndTime(t time.Time) *QueryBuilder[T] {
	qb.options = append(qb.options, EndTime(t))
	return qb
}

// Execute executes the query and returns the result
func (qb *QueryBuilder[T]) Execute(ctx context.Context) (T, error) {
	var zero T

	query := qb.request.factory(qb.provider)
	endpoint := query.GetEndpoint()
	params := query.GetQueryParameters()

	// Apply options
	for _, opt := range qb.options {
		opt(params)
	}

	raw, err := qb.provider.Query(ctx, endpoint, params)
	if err != nil {
		return zero, err
	}

	return query.Decode(raw)
}
