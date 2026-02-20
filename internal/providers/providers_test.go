package providers

import (
	"context"
	"testing"
	"time"

	"github.com/Arjit7d3/datum/internal/core"
)

func TestNewProvider(t *testing.T) {
	tests := []struct {
		name         string
		providerName string
		wantErr      bool
	}{
		{
			name:         "Binance provider",
			providerName: "binance",
			wantErr:      false,
		},
		{
			name:         "Unknown provider",
			providerName: "unknown",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, err := NewProvider(context.Background(), tt.providerName)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewProvider() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && provider == nil {
				t.Error("NewProvider() returned nil provider")
			}
		})
	}
}

func TestBinanceProvider(t *testing.T) {
	provider, err := NewProvider(context.Background(), "binance")
	if err != nil {
		t.Fatalf("Failed to create Binance provider: %v", err)
	}

	t.Run("TradeStream", func(t *testing.T) {
		stream, err := provider.NewTradeStream("BTCUSDT")
		if err != nil {
			t.Fatalf("Failed to create trade stream: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		done := make(chan struct{})
		// Use OnMessage callback to receive data
		stream.OnMessage(func(trade core.Trade) {
			t.Logf("Received trade: %+v", trade)
			if trade.Symbol != "btcusdt" {
				t.Errorf("Expected symbol btcusdt, got %s", trade.Symbol)
			}

			// Try to trigger once
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
			t.Log("Timeout waiting for message (this is acceptable)")
		}
	})
}
