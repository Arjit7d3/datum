package datum

import (
	"context"
	"log"
)

// Stream creates a stream builder for the given stream request
func Stream[T any](ctx context.Context, c *Client, req StreamRequest[T]) (<-chan T, error) {
	stream := req.CreateStream(c.provider)
	symbol, streamName := stream.GetStreamParams()

	rawCh, err := c.provider.Subscribe(ctx, symbol, streamName)
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
