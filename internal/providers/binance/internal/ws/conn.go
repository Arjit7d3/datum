package ws

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const BaseURL = "wss://stream.binance.com:9443/stream"

type Connection struct {
	url  string
	conn *websocket.Conn
	mu   sync.Mutex
}

func NewConnection(url string) *Connection {
	return &Connection{
		url: url,
	}
}

func (c *Connection) dial(ctx context.Context) error {
	if c.conn != nil {
		c.conn.Close(websocket.StatusNormalClosure, "")
		c.conn = nil
	}

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(dialCtx, c.url, nil)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close(websocket.StatusNormalClosure, "")
	}
	return nil
}

// Read reads a message from the connection.
// It automatically handles reconnection with exponential backoff if the connection drops.
// It blocks until a message is successfully read or the context is cancelled.
func (c *Connection) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}

		// Ensure connected
		c.mu.Lock()
		if c.conn == nil {
			if err := c.dial(ctx); err != nil {
				c.mu.Unlock()
				fmt.Printf("Connection to %s failed: %v. Retrying in %v...\n", c.url, err, backoff)
				select {
				case <-time.After(backoff):
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
					continue
				case <-ctx.Done():
					return 0, nil, ctx.Err()
				}
			}
			// Connected
			backoff = time.Second
			fmt.Printf("Connected to %s\n", c.url)
		}
		conn := c.conn
		c.mu.Unlock()

		// Try reading
		typ, msg, err := conn.Read(ctx)
		if err != nil {
			fmt.Printf("Connection to %s dropped: %v. Reconnecting...\n", c.url, err)
			c.mu.Lock()
			if c.conn == conn { // Check if it hasn't been replaced already
				c.conn.Close(websocket.StatusNormalClosure, "")
				c.conn = nil
			}
			c.mu.Unlock()
			continue // Loop back to reconnect
		}

		return typ, msg, nil
	}
}

// WriteJSON sends a JSON message to the connection.
func (c *Connection) WriteJSON(ctx context.Context, v interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		if err := c.dial(ctx); err != nil {
			return fmt.Errorf("failed to dial: %w", err)
		}
	}

	return wsjson.Write(ctx, c.conn, v)
}
