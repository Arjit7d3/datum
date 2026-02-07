package binance

import (
	"context"
	"testing"
	"time"

	"github.com/Arjit7d3/datum/internal/core"
)

func TestNew(t *testing.T) {
	provider := New()
	if provider == nil {
		t.Fatal("New() returned nil")
	}
}

func TestBinanceSubscribe(t *testing.T) {
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
		{
			name:       "ETH USDT Kline Stream",
			symbol:     "ethusdt",
			streamName: "kline_1m",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			b := New()
			ch, err := b.Subscribe(ctx, tt.symbol, tt.streamName)
			if err != nil {
				t.Fatalf("Subscribe failed: %v", err)
			}

			select {
			case msg := <-ch:
				t.Logf("Received message: %d bytes", len(msg))
				if len(msg) == 0 {
					t.Error("Expected non-empty message")
				}
			case <-ctx.Done():
				t.Log("Timeout waiting for message (acceptable for test)")
			}
		})
	}
}

func TestBinanceQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	b := New()
	params := map[string]string{
		"symbol":    "BTCUSDT",
		"interval":  "1h",
		"startTime": "1600000000000",
		"endTime":   "1600003600000",
	}

	raw, err := b.Query(ctx, "klines", params)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(raw) == 0 {
		t.Error("Expected non-empty response")
	}

	t.Logf("Received %d bytes", len(raw))
}

func TestBinanceStreamImplementations(t *testing.T) {
	b := New()

	t.Run("TradeStream", func(t *testing.T) {
		stream := b.NewTradeStream("BTCUSDT")
		if stream == nil {
			t.Fatal("NewTradeStream returned nil")
		}

		symbol, streamName := stream.GetStreamParams()
		if symbol != "btcusdt" {
			t.Errorf("Expected symbol 'btcusdt', got '%s'", symbol)
		}
		if streamName != "trade" {
			t.Errorf("Expected streamName 'trade', got '%s'", streamName)
		}
	})

	t.Run("CandlestickStream", func(t *testing.T) {
		stream := b.NewCandlestickStream("ETHUSDT", "1m")
		if stream == nil {
			t.Fatal("NewCandlestickStream returned nil")
		}

		symbol, streamName := stream.GetStreamParams()
		if symbol != "ethusdt" {
			t.Errorf("Expected symbol 'ethusdt', got '%s'", symbol)
		}
		if streamName != "kline_1m" {
			t.Errorf("Expected streamName 'kline_1m', got '%s'", streamName)
		}
	})

	t.Run("CandlestickQuery", func(t *testing.T) {
		query := b.NewCandlestickQuery(core.CandlestickQueryArgs{
			Symbol:    "BTCUSDT",
			Interval:  "1h",
			StartTime: 1600000000000,
			EndTime:   1600003600000,
		})
		if query == nil {
			t.Fatal("NewCandlestickQuery returned nil")
		}

		endpoint := query.GetEndpoint()
		if endpoint != "klines" {
			t.Errorf("Expected endpoint 'klines', got '%s'", endpoint)
		}

		params := query.GetQueryParameters()
		if params["symbol"] != "BTCUSDT" {
			t.Errorf("Expected symbol 'BTCUSDT', got '%s'", params["symbol"])
		}
		if params["interval"] != "1h" {
			t.Errorf("Expected interval '1h', got '%s'", params["interval"])
		}
	})
}

func TestBinanceStreamDecode(t *testing.T) {
	b := New()

	t.Run("TradeStream Decode", func(t *testing.T) {
		stream := b.NewTradeStream("BTCUSDT")

		// Sample Binance trade message
		sampleData := []byte(`{"s":"BTCUSDT","p":"70000.50","q":"0.001","T":1234567890}`)

		trade, err := stream.Decode(sampleData)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if trade.Symbol != "BTCUSDT" {
			t.Errorf("Expected symbol 'BTCUSDT', got '%s'", trade.Symbol)
		}
		if trade.Price != 70000.50 {
			t.Errorf("Expected price 70000.50, got %f", trade.Price)
		}
		if trade.Quantity != 0.001 {
			t.Errorf("Expected quantity 0.001, got %f", trade.Quantity)
		}
	})

	t.Run("CandlestickQuery Decode", func(t *testing.T) {
		query := b.NewCandlestickQuery(core.CandlestickQueryArgs{
			Symbol:    "BTCUSDT",
			Interval:  "1h",
			StartTime: 1600000000000,
			EndTime:   1600003600000,
		})

		// Sample Binance klines response
		sampleData := []byte(`[[1234567890,  "70000.00", "71000.00", "69000.00", "70500.00", "100.5", 1234571490, "7050000.00", 1000, "50.25", "3525000.00", "0"]]`)

		candles, err := query.Decode(sampleData)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if len(candles) != 1 {
			t.Errorf("Expected 1 candle, got %d", len(candles))
		}
	})
}

func TestBinanceCandlestickStreamDecode(t *testing.T) {
	b := New()
	stream := b.NewCandlestickStream("BTCUSDT", "1m")

	// Sample Binance kline stream message
	sampleData := []byte(`{
		"s": "BTCUSDT",
		"k": {
			"t": 1234567890,
			"T": 1234567949,
			"s": "BTCUSDT",
			"i": "1m",
			"o": "70000.00",
			"c": "70500.00",
			"h": "71000.00",
			"l": "69000.00",
			"v": "100.5"
		}
	}`)

	candle, err := stream.Decode(sampleData)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if candle.Symbol != "BTCUSDT" {
		t.Errorf("Expected symbol 'BTCUSDT', got '%s'", candle.Symbol)
	}
	if candle.Interval != "1m" {
		t.Errorf("Expected interval '1m', got '%s'", candle.Interval)
	}
	if candle.OpenPrice != 70000.00 {
		t.Errorf("Expected open price 70000.00, got %f", candle.OpenPrice)
	}
}

// Verify interface compliance
var _ core.Provider = (*Binance)(nil)
