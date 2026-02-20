package datum_test

import (
	"context"
	"testing"
	"time"

	"github.com/Arjit7d3/datum"
	"github.com/Arjit7d3/datum/internal/core"
)

func TestGenericSubscription(t *testing.T) {
	client, err := datum.NewClient(context.Background(), "binance")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	t.Run("Trade Stream", func(t *testing.T) {
		stream, err := client.NewTradeStream("BTCUSDT")
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done := make(chan struct{})

		stream.OnMessage(func(trade core.Trade) {
			t.Logf("Received trade: %+v", trade)
			if trade.Symbol != "btcusdt" {
				t.Errorf("Expected symbol btcusdt, got %s", trade.Symbol)
			}
			select {
			case <-done:
			default:
				close(done)
			}
		})

		select {
		case <-done:
			t.Log("Successfully received a trade event")
		case <-ctx.Done():
			t.Fatal("Timeout waiting for trade")
		}
	})
}
