package ws

import (
	"context"
	"fmt"
	"time"

	"github.com/coder/websocket"
)

const BaseURL = "wss://stream.binance.com:9443"

type Connection struct {
	url  string
	conn *websocket.Conn
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
		if c.conn == nil {
			if err := c.dial(ctx); err != nil {
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

		// Try reading
		typ, msg, err := c.conn.Read(ctx)
		if err != nil {
			fmt.Printf("Connection to %s dropped: %v. Reconnecting...\n", c.url, err)
			c.conn.Close(websocket.StatusNormalClosure, "")
			c.conn = nil
			continue // Loop back to reconnect
		}

		return typ, msg, nil
	}
}
