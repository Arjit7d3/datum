package subscription

import (
	"context"
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

func (c *Client) Subscribe(ctx context.Context, url string) (<-chan []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if h, ok := c.hubs[url]; ok {
		return h.subscribe(), nil
	}

	h := newHub()
	c.hubs[url] = h

	// Connection worker
	go func() {
		// Use Background context for the shared connection
		conn, err := ws.NewConnection(context.Background(), url)
		if err != nil {
			h.stopHub()
			return
		}
		defer conn.Close()

		for {
			select {
			case <-h.stop:
				return
			default:
				// Read blocks, so we don't need a select here unless we want to interrupt it.
				// However, if h.stop is closed, conn.Close() will eventually trigger an error in Read.
				_, msg, err := conn.Read(context.Background())
				if err != nil {
					h.stopHub()
					return
				}

				// Try to send to hub, but abort if hub stopped
				select {
				case h.in <- msg:
				case <-h.stop:
					return
				}
			}
		}
	}()

	return h.subscribe(), nil
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
		h.stopHub()
	}
}
