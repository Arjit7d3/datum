package datum

import "context"

// Query executes a query and returns the result directly
func Query[T any](ctx context.Context, c *Client, req QueryRequest[T]) (T, error) {
	var zero T

	query := req.CreateQuery(c.provider)
	endpoint := query.GetEndpoint()
	params := query.GetQueryParameters()

	raw, err := c.provider.Query(ctx, endpoint, params)
	if err != nil {
		return zero, err
	}

	return query.Decode(raw)
}
