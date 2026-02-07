package providers

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestProviders(t *testing.T) {
	tests := []struct {
		title   string
		setup   func() (Provider, error)
		symbol  string
		timeout time.Duration
	}{
		{
			title: "Verify Default Provider (Binance) functionality",
			setup: func() (Provider, error) {
				return NewDefaultProvider()
			},
			symbol:  "BTCUSDT",
			timeout: 10 * time.Second,
		},
		{
			title: "Verify Explicit Binance Provider functionality",
			setup: func() (Provider, error) {
				return NewProvider(Binance)
			},
			symbol:  "ETHUSDT",
			timeout: 5 * time.Second,
		},
		{
			title: "Verify Connection Multiplexing (Multiple subscribers to same symbol)",
			setup: func() (Provider, error) {
				return NewProvider(Binance)
			},
			symbol:  "BTCUSDT",
			timeout: 10 * time.Second,
		},
	}

	for i, tt := range tests {
		testName := fmt.Sprintf("[Test %d] %s", i+1, tt.title)
		t.Run(testName, func(t *testing.T) {
			provider, err := tt.setup()
			if err != nil {
				t.Fatalf("Failed to setup provider: %v", err)
			}

			t.Logf("Running test for symbol: %s", tt.symbol)

			// 1. Query Test
			t.Log("Execution: Querying klines")
			end := time.Now().UTC()
			start := end.Add(-5 * time.Minute)
			params := map[string]string{
				"symbol":    tt.symbol,
				"interval":  "1m",
				"startTime": fmt.Sprint(start.UnixMilli()),
				"endTime":   fmt.Sprint(end.UnixMilli()),
				"limit":     "10",
			}
			raw, err := provider.Query(context.Background(), "uiKlines", params)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			t.Logf("Result: Received %d klines", len(raw))

			// 2. Subscription Test
			t.Logf("Execution: Testing subscription (timeout: %v)", tt.timeout)
			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()

			if tt.title == "Verify Connection Multiplexing (Multiple subscribers to same symbol)" {
				// Special case for multiplexing: subscribe twice
				s1, err := provider.Subscribe(ctx, tt.symbol, "kline_1m")
				if err != nil {
					t.Fatalf("First subscribe failed: %v", err)
				}
				s2, err := provider.Subscribe(ctx, tt.symbol, "kline_1m")
				if err != nil {
					t.Fatalf("Second subscribe failed: %v", err)
				}

				count1, count2 := 0, 0
				done := false
				for !done {
					select {
					case msg, ok := <-s1:
						if !ok {
							s1 = nil
						} else {
							count1++
							t.Logf("Result: [Subscriber 1] Received message: %v bytes", len(msg))
						}
					case msg, ok := <-s2:
						if !ok {
							s2 = nil
						} else {
							count2++
							t.Logf("Result: [Subscriber 2] Received message: %v bytes", len(msg))
						}
					case <-ctx.Done():
						done = true
					}
					// Exit if both channels closed or both got at least one message
					if (s1 == nil && s2 == nil) || (count1 > 0 && count2 > 0) {
						done = true
					}
				}
				t.Logf("Result: Multiplexing status - Sub1: %d, Sub2: %d", count1, count2)
				if count1 == 0 || count2 == 0 {
					t.Errorf("Multiplexing failed: one or more subscribers didn't receive data")
				}
			} else {
				// Standard single subscription test
				stream, err := provider.Subscribe(ctx, tt.symbol, "kline_1m")
				if err != nil {
					t.Fatalf("Subscribe failed: %v", err)
				}

				select {
				case msg := <-stream:
					t.Logf("Result: Received stream message: %s", string(msg))
				case <-ctx.Done():
					t.Log("Result: Timeout waiting for message")
				}
			}
		})
	}
}
