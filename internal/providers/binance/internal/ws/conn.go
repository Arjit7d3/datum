package ws

import (
	"context"
	"time"

	"github.com/coder/websocket"
)

type Connection struct {
	conn *websocket.Conn
}

func NewConnection(ctx context.Context, url string) (*Connection, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	c, _, err := websocket.Dial(dialCtx, url, nil)
	if err != nil {
		return nil, err
	}
	return &Connection{conn: c}, nil
}

func (c *Connection) Close() error {
	return c.conn.Close(websocket.StatusNormalClosure, "")
}

func (c *Connection) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	return c.conn.Read(ctx)
}
