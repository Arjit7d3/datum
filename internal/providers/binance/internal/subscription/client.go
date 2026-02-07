package subscription

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Arjit7d3/datum/internal/providers/binance/internal/ws"
)

type Client struct {
	hubs map[string]*hub
	mu   sync.RWMutex
}

func NewClient() *Client {
	return &Client{
		hubs: make(map[string]*hub),
	}
}

func (c *Client) Subscribe(ctx context.Context, symbol string, streamName string) (<-chan []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	url := fmt.Sprintf("%s/ws/%s@%s", ws.BaseURL, strings.ToLower(symbol), streamName)

	if h, ok := c.hubs[url]; ok {
		ch, err := h.subscribe(ctx)
		if err != nil {
			return nil, err
		}

		// Ensure unsubscription on context cancellation
		go func() {
			<-ctx.Done()
			c.Unsubscribe(ch)
		}()

		return ch, nil
	}

	// Create a long-lived context for the hub and its worker
	hubCtx, cancel := context.WithCancel(context.Background())
	h := newHub(hubCtx, cancel)
	c.hubs[url] = h

	go func() {
		// Self-Cleanup: Remove hub from map if it dies unexpectedly
		defer func() {
			c.mu.Lock()
			if existingH, ok := c.hubs[url]; ok && existingH == h {
				delete(c.hubs, url)
			}
			c.mu.Unlock()
		}()

		// The WS package now handles connection lifecycle and backoff internally.
		// We just create the object and read.
		conn := ws.NewConnection(url)
		defer conn.Close()

		for {
			// Read blocks until a message is available or context is cancelled.
			// Reconnection is handled automatically inside Read.
			_, msg, err := conn.Read(hubCtx)
			if err != nil {
				// The only error returned by Read is if context is cancelled,
				// so we just return.
				return
			}

			select {
			case h.in <- msg:
			case <-hubCtx.Done():
				return
			}
		}
	}()

	ch, err := h.subscribe(hubCtx)
	if err != nil {
		return nil, err
	}

	// Ensure unsubscription on context cancellation
	go func() {
		<-ctx.Done()
		c.Unsubscribe(ch)
	}()

	return ch, nil
}

func (c *Client) Unsubscribe(ch chan []byte) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, h := range c.hubs {
		h.unsubscribe(ch)
	}
}

func (c *Client) UnsubscribeAll(ctx context.Context, url string) {
	c.mu.Lock()
	h, ok := c.hubs[url]
	if ok {
		delete(c.hubs, url)
	}
	c.mu.Unlock()

	if ok {
		h.stop()
	}
}
