package datum

// Stream creates a stream builder for the given stream request
func Stream[T any](c *Client, req StreamRequest[T]) *StreamBuilder[T] {
	return &StreamBuilder[T]{
		provider: c.provider,
		request:  req,
	}
}
