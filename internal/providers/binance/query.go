package binance

import (
	"context"
	"fmt"
	"time"
)

func (b *Binance) Query(ctx context.Context, endpoint string, params map[string]string) ([]byte, error) {
	var raw []byte
	var err error

	backoff := 500 * time.Millisecond
	const maxRetries = 5

	for i := 0; i < maxRetries; i++ {
		// Check for cancellation before attempt
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// We need to use a method that returns raw bytes.
		raw, err = b.httpClient.Get(ctx, endpoint, params)
		if err == nil {
			return raw, nil
		}

		// If this was the last attempt, don't sleep
		if i == maxRetries-1 {
			break
		}

		// Log and wait
		// In a real logger we would use log.Warn
		fmt.Printf("Query to %s failed (attempt %d/%d): %v. Retrying in %v...\n", endpoint, i+1, maxRetries, err, backoff)

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("query failed after %d attempts: %w", maxRetries, err)
}
