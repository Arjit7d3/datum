package datum

// Query creates a query builder for the given query request
func Query[T any](c *Client, req QueryRequest[T]) *QueryBuilder[T] {
	return &QueryBuilder[T]{
		provider: c.provider,
		request:  req,
	}
}
