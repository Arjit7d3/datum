package subscription

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestConcurrentSubscribeSameStream tests multiple goroutines subscribing to the same stream simultaneously
func TestConcurrentSubscribeSameStream(t *testing.T) {
	client := NewClient()
	defer client.cancel()

	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Use a long-lived parent context that we control
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	errors := make(chan error, numGoroutines)
	channels := make(chan (<-chan []byte), numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			// Use parent context so channels stay subscribed
			ch, err := client.Subscribe(parentCtx, "btcusdt", "trade")
			if err != nil {
				errors <- err
				return
			}
			channels <- ch
		}()
	}

	wg.Wait()
	close(errors)
	close(channels)

	for err := range errors {
		t.Errorf("Subscribe failed: %v", err)
	}

	// All should succeed
	count := 0
	for range channels {
		count++
	}
	if count != numGoroutines {
		t.Errorf("Expected %d successful subscribes, got %d", numGoroutines, count)
	}

	// Verify ref count BEFORE cancelling context
	client.mu.Lock()
	refCount := client.streamRefs["btcusdt@trade"]
	client.mu.Unlock()

	if refCount != numGoroutines {
		t.Errorf("Expected ref count %d, got %d", numGoroutines, refCount)
	}
}

// TestConcurrentSubscribeDifferentStreams tests subscribing to different streams concurrently
func TestConcurrentSubscribeDifferentStreams(t *testing.T) {
	client := NewClient()
	defer client.cancel()

	streams := []struct {
		symbol, stream string
	}{
		{"btcusdt", "trade"},
		{"ethusdt", "trade"},
		{"bnbusdt", "trade"},
		{"xrpusdt", "trade"},
		{"adausdt", "trade"},
	}

	// Use a long-lived parent context
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	var wg sync.WaitGroup
	wg.Add(len(streams))

	errors := make(chan error, len(streams))

	for _, s := range streams {
		go func(symbol, stream string) {
			defer wg.Done()
			// Use parent context so subscriptions stay active
			_, err := client.Subscribe(parentCtx, symbol, stream)
			if err != nil {
				errors <- err
			}
		}(s.symbol, s.stream)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Subscribe failed: %v", err)
	}

	// Verify all streams exist BEFORE cancelling context
	client.mu.Lock()
	streamCount := len(client.streams)
	client.mu.Unlock()

	if streamCount != len(streams) {
		t.Errorf("Expected %d streams, got %d", len(streams), streamCount)
	}
}

// TestSubscribeUnsubscribeRace tests subscribe and unsubscribe happening concurrently
func TestSubscribeUnsubscribeRace(t *testing.T) {
	client := NewClient()
	defer client.cancel()

	const iterations = 50

	for i := 0; i < iterations; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		ch, err := client.Subscribe(ctx, "btcusdt", "trade")
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		// Note: We need to access the underlying bidirectional channel for Unsubscribe
		// This is a test-only workaround since Subscribe returns <-chan []byte
		client.mu.Lock()
		var biCh chan []byte
		for c := range client.subToStream {
			if c == ch {
				biCh = c
				break
			}
		}
		client.mu.Unlock()

		// Start unsubscribe in goroutine while potentially still in subscribe flow
		go func() {
			time.Sleep(time.Millisecond) // tiny delay
			if biCh != nil {
				client.Unsubscribe(biCh)
			}
		}()

		// Also cancel context which triggers another unsubscribe path
		go func() {
			time.Sleep(2 * time.Millisecond)
			cancel()
		}()

		time.Sleep(5 * time.Millisecond) // let goroutines complete
	}

	// Should not panic or deadlock
}

// TestDoubleUnsubscribe tests calling Unsubscribe twice for the same channel
func TestDoubleUnsubscribe(t *testing.T) {
	client := NewClient()
	defer client.cancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := client.Subscribe(ctx, "btcusdt", "trade")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Get bidrectional channel for Unsubscribe
	client.mu.Lock()
	var biCh chan []byte
	for c := range client.subToStream {
		if c == ch {
			biCh = c
			break
		}
	}
	client.mu.Unlock()

	// Unsubscribe twice - should not panic
	client.Unsubscribe(biCh)
	client.Unsubscribe(biCh) // second call should be no-op
}

// TestContextCancellationDuringSubscribe tests context being cancelled while subscribe is in progress
func TestContextCancellationDuringSubscribe(t *testing.T) {
	client := NewClient()
	defer client.cancel()

	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())

			// Cancel immediately for half, delay for other half
			if id%2 == 0 {
				cancel()
			} else {
				go func() {
					time.Sleep(time.Millisecond)
					cancel()
				}()
			}

			_, _ = client.Subscribe(ctx, "btcusdt", "trade")
		}(i)
	}

	wg.Wait()
	// Should not panic or deadlock
}

// TestRapidSubscribeUnsubscribeCycle tests rapid subscribe/unsubscribe in a tight loop
func TestRapidSubscribeUnsubscribeCycle(t *testing.T) {
	client := NewClient()
	defer client.cancel()

	const iterations = 100

	for i := 0; i < iterations; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		ch, err := client.Subscribe(ctx, "btcusdt", "trade")
		if err != nil {
			cancel()
			t.Fatalf("Iteration %d: Subscribe failed: %v", i, err)
		}

		// Get bidirectional channel for Unsubscribe
		client.mu.Lock()
		var biCh chan []byte
		for c := range client.subToStream {
			if c == ch {
				biCh = c
				break
			}
		}
		client.mu.Unlock()

		if biCh != nil {
			client.Unsubscribe(biCh)
		}
		cancel()
	}

	// Final state: no streams should remain
	client.mu.Lock()
	streamCount := len(client.streams)
	refCount := client.streamRefs["btcusdt@trade"]
	client.mu.Unlock()

	if streamCount != 0 {
		t.Errorf("Expected 0 streams after all unsubscribes, got %d", streamCount)
	}
	if refCount != 0 {
		t.Errorf("Expected 0 ref count, got %d", refCount)
	}
}

// TestConcurrentReadLoopAndSubscribe tests that readLoop and subscribe don't deadlock
func TestConcurrentReadLoopAndSubscribe(t *testing.T) {
	client := NewClient()
	defer client.cancel()

	// First subscribe to trigger readLoop start
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	_, err := client.Subscribe(ctx1, "btcusdt", "trade")
	if err != nil {
		t.Fatalf("First subscribe failed: %v", err)
	}

	// Now hammer with more subscribes while readLoop is running
	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			symbol := "btcusdt"
			if id%2 == 0 {
				symbol = "ethusdt"
			}

			_, err := client.Subscribe(ctx, symbol, "trade")
			if err != nil {
				t.Logf("Subscribe %d failed: %v", id, err)
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out - possible deadlock")
	}
}

// TestHubMessageBroadcast tests that messages are properly broadcast to all subscribers
func TestHubMessageBroadcast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := newHub(ctx, cancel)

	const numSubs = 5
	channels := make([]chan []byte, numSubs)

	for i := 0; i < numSubs; i++ {
		ch, err := h.subscribe(ctx)
		if err != nil {
			t.Fatalf("subscribe failed: %v", err)
		}
		channels[i] = ch
	}

	// Send a message
	testMsg := []byte("test message")
	select {
	case h.in <- testMsg:
	case <-time.After(time.Second):
		t.Fatal("Failed to send message to hub")
	}

	// All channels should receive the message
	for i, ch := range channels {
		select {
		case msg := <-ch:
			if string(msg) != string(testMsg) {
				t.Errorf("Channel %d: expected %q, got %q", i, testMsg, msg)
			}
		case <-time.After(time.Second):
			t.Errorf("Channel %d: did not receive message", i)
		}
	}
}

// TestHubUnsubscribeClosesChannel tests that unsubscribe properly closes the channel
func TestHubUnsubscribeClosesChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := newHub(ctx, cancel)

	ch, err := h.subscribe(ctx)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	h.unsubscribe(ch)

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("Expected channel to be closed, but received value")
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Channel was not closed after unsubscribe")
	}
}

// TestHubStopClosesAllChannels tests that stopping the hub closes all subscriber channels
func TestHubStopClosesAllChannels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	h := newHub(ctx, cancel)

	const numSubs = 5
	channels := make([]chan []byte, numSubs)

	for i := 0; i < numSubs; i++ {
		ch, err := h.subscribe(ctx)
		if err != nil {
			t.Fatalf("subscribe failed: %v", err)
		}
		channels[i] = ch
	}

	// Stop the hub
	h.stop()

	// All channels should be closed
	for i, ch := range channels {
		select {
		case _, ok := <-ch:
			if ok {
				// Might get a value if there was one buffered, try again
				select {
				case _, ok := <-ch:
					if ok {
						t.Errorf("Channel %d: expected to be closed", i)
					}
				case <-time.After(200 * time.Millisecond):
					t.Errorf("Channel %d: not closed after hub stop", i)
				}
			}
		case <-time.After(200 * time.Millisecond):
			t.Errorf("Channel %d: not closed after hub stop", i)
		}
	}
}
