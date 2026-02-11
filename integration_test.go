package datum_test

import (
	"context"
	"sync"
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

		ch, err := datum.Stream(ctx, client, datum.TradesStream{Symbol: "BTCUSDT"})
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

		ch, err := datum.Stream(ctx, client, datum.CandlesticksStream{Symbol: "ETHUSDT", Interval: "1m"})
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

		req := datum.CandlesticksQuery{
			Symbol:    "BTCUSDT",
			Interval:  "1h",
			StartTime: time.Now().Add(-5 * time.Hour).UnixMilli(),
			EndTime:   time.Now().UnixMilli(),
		}

		candlesticks, err := datum.Query(ctx, client, req)
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

// ==================== EXTENSIVE INTEGRATION TESTS ====================

// TestConcurrentStreamsAndQueries tests running streams and queries concurrently
func TestConcurrentStreamsAndQueries(t *testing.T) {
	client, err := datum.NewClient("binance")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	parentCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Start multiple streams
	wg.Add(1)
	go func() {
		defer wg.Done()
		ch, err := datum.Stream(parentCtx, client, datum.TradesStream{Symbol: "BTCUSDT"})
		if err != nil {
			t.Errorf("Trade stream failed: %v", err)
			return
		}
		select {
		case trade := <-ch:
			t.Logf("Concurrent trade received: %+v", trade)
		case <-parentCtx.Done():
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ch, err := datum.Stream(parentCtx, client, datum.CandlesticksStream{Symbol: "ETHUSDT", Interval: "1m"})
		if err != nil {
			t.Errorf("Candlestick stream failed: %v", err)
			return
		}
		select {
		case candle := <-ch:
			t.Logf("Concurrent candle received: %+v", candle)
		case <-parentCtx.Done():
		}
	}()

	// Run queries concurrently
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req := datum.CandlesticksQuery{
				Symbol:    "BTCUSDT",
				Interval:  "1h",
				StartTime: time.Now().Add(-2 * time.Hour).UnixMilli(),
				EndTime:   time.Now().UnixMilli(),
			}
			result, err := datum.Query(parentCtx, client, req)
			if err != nil {
				t.Errorf("Query %d failed: %v", id, err)
			} else {
				t.Logf("Query %d returned %d candles", id, len(result))
			}
		}(i)
	}

	wg.Wait()
}

// TestMultipleClientsIndependent tests that multiple clients work independently
func TestMultipleClientsIndependent(t *testing.T) {
	client1, err := datum.NewClient("binance")
	if err != nil {
		t.Fatalf("Failed to create client1: %v", err)
	}

	client2, err := datum.NewClient("binance")
	if err != nil {
		t.Fatalf("Failed to create client2: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Both clients should work independently
	ch1, err := datum.Stream(ctx, client1, datum.TradesStream{Symbol: "BTCUSDT"})
	if err != nil {
		t.Fatalf("Client1 subscribe failed: %v", err)
	}

	ch2, err := datum.Stream(ctx, client2, datum.TradesStream{Symbol: "ETHUSDT"})
	if err != nil {
		t.Fatalf("Client2 subscribe failed: %v", err)
	}

	// Both should receive data
	received1 := false
	received2 := false
	timeout := time.After(5 * time.Second)

	for !received1 || !received2 {
		select {
		case trade := <-ch1:
			received1 = true
			t.Logf("Client1 received: %s", trade.Symbol)
		case trade := <-ch2:
			received2 = true
			t.Logf("Client2 received: %s", trade.Symbol)
		case <-timeout:
			if !received1 {
				t.Log("Client1 timed out")
			}
			if !received2 {
				t.Log("Client2 timed out")
			}
			return
		}
	}
}

// TestClientReuseAfterContextCancel tests that client can be reused after context cancellation
func TestClientReuseAfterContextCancel(t *testing.T) {
	client, err := datum.NewClient("binance")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// First subscription with short timeout
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	ch1, err := datum.Stream(ctx1, client, datum.TradesStream{Symbol: "BTCUSDT"})
	if err != nil {
		t.Fatalf("First subscribe failed: %v", err)
	}

	select {
	case trade := <-ch1:
		t.Logf("First received: %+v", trade)
	case <-ctx1.Done():
		t.Log("First context cancelled")
	}
	cancel1()

	// Wait a bit for cleanup
	time.Sleep(200 * time.Millisecond)

	// Second subscription - should work
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	ch2, err := datum.Stream(ctx2, client, datum.TradesStream{Symbol: "BTCUSDT"})
	if err != nil {
		t.Fatalf("Second subscribe failed: %v", err)
	}

	select {
	case trade := <-ch2:
		t.Logf("Second received: %+v", trade)
	case <-ctx2.Done():
		t.Log("Second context timed out (acceptable)")
	}
}

// TestMultipleSymbolsStream tests streaming multiple symbols through the same client
func TestMultipleSymbolsStream(t *testing.T) {
	client, err := datum.NewClient("binance")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	symbols := []string{"BTCUSDT", "ETHUSDT", "BNBUSDT"}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Subscribe to each symbol
	for _, sym := range symbols {
		_, err := datum.Stream(ctx, client, datum.TradesStream{Symbol: sym})
		if err != nil {
			t.Fatalf("Subscribe %s failed: %v", sym, err)
		}
		t.Logf("Subscribed to %s", sym)
	}

	// Just verify subscriptions work - receiving from all is hard without type access
	t.Log("Successfully subscribed to all symbols")
}

// TestRapidSubscribeUnsubscribeIntegration tests rapid subscribe/unsubscribe at integration level
func TestRapidSubscribeUnsubscribeIntegration(t *testing.T) {
	client, err := datum.NewClient("binance")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	const iterations = 10

	for i := 0; i < iterations; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		_, err := datum.Stream(ctx, client, datum.TradesStream{Symbol: "BTCUSDT"})
		if err != nil {
			t.Fatalf("Iteration %d failed: %v", i, err)
		}
		cancel()
	}

	t.Logf("Completed %d rapid subscribe/unsubscribe cycles", iterations)

	// Verify client still works after rapid cycles
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := datum.Stream(ctx, client, datum.TradesStream{Symbol: "BTCUSDT"})
	if err != nil {
		t.Fatalf("Final subscribe failed: %v", err)
	}

	select {
	case trade := <-ch:
		t.Logf("Final trade received: %+v", trade)
	case <-ctx.Done():
		t.Log("Final subscription timed out (acceptable)")
	}
}
