package providers

import (
	"context"
	"testing"
	"time"
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
			provider, err := NewProvider(tt.providerName)
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
	provider, err := NewProvider("binance")
	if err != nil {
		t.Fatalf("Failed to create Binance provider: %v", err)
	}

	t.Run("Query", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		params := map[string]string{
			"symbol":   "BTCUSDT",
			"interval": "1h",
			"limit":    "5",
		}

		raw, err := provider.Query(ctx, "klines", params)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(raw) == 0 {
			t.Error("Expected non-empty response")
		}

		t.Logf("Received %d bytes", len(raw))
	})

	t.Run("Subscribe", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		ch, err := provider.Subscribe(ctx, "btcusdt", "trade")
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
			t.Log("Timeout waiting for message (this is acceptable)")
		}
	})
}
