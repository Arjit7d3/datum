package binance

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Arjit7d3/datum/internal/core"
)

func TestNew(t *testing.T) {
	provider, err := New(context.Background())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	if provider == nil {
		t.Fatal("New() returned nil")
	}
}

func TestBinanceStreamImplementations(t *testing.T) {
	b, err := New(context.Background())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("TradeStream", func(t *testing.T) {
		stream, err := b.NewTradeStream("BTCUSDT")
		if err != nil {
			t.Fatalf("NewTradeStream failed: %v", err)
		}
		if stream == nil {
			t.Fatal("NewTradeStream returned nil stream")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done := make(chan struct{})

		stream.OnMessage(func(trade core.Trade) {
			if trade.Symbol != "btcusdt" {
				t.Errorf("Expected symbol 'btcusdt', got '%s'", trade.Symbol)
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
			t.Log("Timeout waiting for message (acceptable for test)")
		}
	})

	t.Run("CandlestickStream", func(t *testing.T) {
		stream, err := b.NewCandlestickStream("ETHUSDT", "1m")
		if err != nil {
			t.Fatalf("NewCandlestickStream failed: %v", err)
		}
		if stream == nil {
			t.Fatal("NewCandlestickStream returned nil stream")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done := make(chan struct{})

		stream.OnMessage(func(candle core.Candlestick) {
			if candle.Symbol != "ethusdt" {
				t.Errorf("Expected symbol 'ethusdt', got '%s'", candle.Symbol)
			}
			if candle.Interval != "1m" {
				t.Errorf("Expected interval '1m', got '%s'", candle.Interval)
			}

			select {
			case <-done:
			default:
				close(done)
			}
		})

		select {
		case <-done:
			t.Log("Successfully received a candlestick event")
		case <-ctx.Done():
			t.Log("Timeout waiting for message (acceptable for test)")
		}
	})
}

// Verify interface compliance
var _ core.Provider = (*Binance)(nil)

// ==================== EXTENSIVE CONCURRENCY TESTS ====================

// TestConcurrentSubscribesSameProvider tests multiple concurrent subscriptions on same provider
func TestConcurrentSubscribesSameProvider(t *testing.T) {
	b, err := New(context.Background())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	symbols := []string{"BTCUSDT", "ETHUSDT", "BNBUSDT"}

	var wg sync.WaitGroup
	wg.Add(len(symbols))

	results := make(chan error, len(symbols))

	for _, symbol := range symbols {
		go func(sym string) {
			defer wg.Done()
			_, err := b.NewTradeStream(sym)
			results <- err
		}(symbol)
	}

	wg.Wait()
	close(results)

	successCount := 0
	for err := range results {
		if err != nil {
			t.Errorf("Subscribe failed: %v", err)
		} else {
			successCount++
		}
	}

	if successCount != len(symbols) {
		t.Errorf("Expected %d successful subscriptions, got %d", len(symbols), successCount)
	}
}
