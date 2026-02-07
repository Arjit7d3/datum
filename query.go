package datum

import "github.com/Arjit7d3/datum/internal/core"

// QueryRequest encapsulates a query request
type QueryRequest[T any] struct {
	factory func(core.Provider) core.IQuery[T]
}

// Query creates a query builder for the given query request
func Query[T any](c *Client, req QueryRequest[T]) *QueryBuilder[T] {
	return &QueryBuilder[T]{
		provider: c.provider,
		request:  req,
	}
}

// newQueryRequest creates a new query request with the given factory
func newQueryRequest[T any](factory func(core.Provider) core.IQuery[T]) QueryRequest[T] {
	return QueryRequest[T]{factory: factory}
}
