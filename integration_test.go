package datum_test

import (
	"context"
	"testing"
	"time"

	"github.com/Arjit7d3/datum"
)

func TestGenericSubscription(t *testing.T) {
	client, err := datum.NewClient("binance")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	t.Run("Trade Stream", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		ch, err := datum.Stream(client, datum.Trades("BTCUSDT")).Subscribe(ctx)
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		select {
		case trade := <-ch:
			t.Logf("Received trade: %+v", trade)
			if trade.Symbol != "BTCUSDT" {
				t.Errorf("Expected symbol BTCUSDT, got %s", trade.Symbol)
			}
		case <-ctx.Done():
			t.Fatal("Timeout waiting for trade")
		}
	})

	t.Run("Candlestick Stream", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		ch, err := datum.Stream(client, datum.Candlesticks("ETHUSDT", "1m")).Subscribe(ctx)
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		select {
		case candle := <-ch:
			t.Logf("Received candlestick: %+v", candle)
			if candle.Symbol != "ETHUSDT" {
				t.Errorf("Expected symbol ETHUSDT, got %s", candle.Symbol)
			}
		case <-ctx.Done():
			t.Fatal("Timeout waiting for candlestick")
		}
	})
}

func TestGenericQuery(t *testing.T) {
	client, err := datum.NewClient("binance")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	t.Run("Candlestick Query", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		candlesticks, err := datum.Query(client, datum.CandlesticksQuery("BTCUSDT", "1h")).
			Limit(5).
			Execute(ctx)
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}

		t.Logf("Received %d candlesticks", len(candlesticks))
		if len(candlesticks) == 0 {
			t.Fatal("Expected at least one candlestick")
		}

		if len(candlesticks) > 0 {
			t.Logf("First candlestick: %+v", candlesticks[0])
		}
	})
}
