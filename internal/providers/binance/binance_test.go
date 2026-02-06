package binance

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSubscribe(t *testing.T) {
	tests := []struct {
		name       string
		symbol     string
		streamName string
	}{
		{
			name:       "BTC USDT Trade Stream",
			symbol:     "btcusdt",
			streamName: "trade",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a context that cancels after 5 seconds
			// so the test doesn't run forever.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			t.Logf("Testing stream for %s/%s (5s timeout)", tt.symbol, tt.streamName)

			// Note: This still connects to the real Binance API.
			// In a real production environment, you might want to mock the WebSocket.
			b := NewProvider()
			ch, err := b.Subscribe(ctx, tt.symbol, tt.streamName)

			for msg := range ch {
				fmt.Println(string(msg))
				t.Logf("Received message: %s", string(msg))
			}

			if err != nil {
				t.Fatalf("Failed to subscribe: %v", err)
			}
			if ch == nil {
				t.Fatal("Expected non-nil channel")
			}
		})
	}
}
