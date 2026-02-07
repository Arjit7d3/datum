package subscription

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Arjit7d3/datum/internal/providers/binance/internal/ws"
)

type Client struct {
	conn        *ws.Connection
	streams     map[string]*hub
	subToStream map[chan []byte]string
	streamRefs  map[string]int
	mu          sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	initOnce    sync.Once
}

type combinedStreamEvent struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

func NewClient() *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		conn:        ws.NewConnection(ws.BaseURL),
		streams:     make(map[string]*hub),
		subToStream: make(map[chan []byte]string),
		streamRefs:  make(map[string]int),
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (c *Client) readLoop() {
	defer c.cancel()

	for {
		_, msg, err := c.conn.Read(c.ctx)
		if err != nil {
			return
		}

		var event combinedStreamEvent
		if err := json.Unmarshal(msg, &event); err != nil {
			// Ignore malformed messages
			continue
		}
		if event.Stream == "" {
			// Non-stream messages like subscribe/unsubscribe responses
			continue
		}

		c.mu.Lock()
		h, ok := c.streams[event.Stream]
		c.mu.Unlock()

		if ok {
			select {
			case h.in <- event.Data:
			case <-c.ctx.Done():
				return
			}
		}
	}
}

func (c *Client) Subscribe(ctx context.Context, symbol string, streamName string) (<-chan []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.initOnce.Do(func() {
		go c.readLoop()
	})

	stream := fmt.Sprintf("%s@%s", strings.ToLower(symbol), streamName)

	h, ok := c.streams[stream]
	if !ok {
		hubCtx, hubCancel := context.WithCancel(c.ctx)
		h = newHub(hubCtx, hubCancel)
		c.streams[stream] = h

		// Send Subscribe (lock held - WriteJSON uses separate ws.mu)
		err := c.conn.WriteJSON(c.ctx, map[string]any{
			"method": "SUBSCRIBE",
			"params": []string{stream},
			"id":     time.Now().UnixNano(),
		})
		if err != nil {
			delete(c.streams, stream)
			h.stop()
			return nil, err
		}
	}

	ch, err := h.subscribe(ctx)
	if err != nil {
		return nil, err
	}

	c.subToStream[ch] = stream
	c.streamRefs[stream]++

	// Ensure unsubscription on context cancellation
	go func() {
		<-ctx.Done()
		c.Unsubscribe(ch)
	}()

	return ch, nil
}

func (c *Client) Unsubscribe(ch chan []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stream, ok := c.subToStream[ch]
	if !ok {
		return
	}

	if h, ok := c.streams[stream]; ok {
		h.unsubscribe(ch)
	}
	delete(c.subToStream, ch)

	c.streamRefs[stream]--
	if c.streamRefs[stream] <= 0 {
		if h, ok := c.streams[stream]; ok {
			h.stop()
		}
		delete(c.streams, stream)
		delete(c.streamRefs, stream)

		// Send Unsubscribe (best effort, lock held)
		if c.ctx.Err() == nil {
			_ = c.conn.WriteJSON(c.ctx, map[string]any{
				"method": "UNSUBSCRIBE",
				"params": []string{stream},
				"id":     time.Now().UnixNano(),
			})
		}
	}
}

func (c *Client) UnsubscribeAll(ctx context.Context, url string) {
	// No-op
}
