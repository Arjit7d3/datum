package binance

import (
	"context"
)

func (p *Binance) Subscribe(ctx context.Context, symbol string, streamName string) (<-chan []byte, error) {
	return p.subscriptionClient.Subscribe(ctx, symbol, streamName)
}
