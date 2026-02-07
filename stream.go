package datum

import "github.com/Arjit7d3/datum/internal/core"

// StreamRequest encapsulates a stream subscription request
type StreamRequest[T any] struct {
	factory func(core.Provider) core.IStream[T]
}

// Stream creates a stream builder for the given stream request
func Stream[T any](c *Client, req StreamRequest[T]) *StreamBuilder[T] {
	return &StreamBuilder[T]{
		provider: c.provider,
		request:  req,
	}
}

// newStreamRequest creates a new stream request with the given factory
func newStreamRequest[T any](factory func(core.Provider) core.IStream[T]) StreamRequest[T] {
	return StreamRequest[T]{factory: factory}
}
