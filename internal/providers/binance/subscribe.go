package binance

import (
	"context"
	"fmt"
	"strings"

	"github.com/Arjit7d3/datum/internal/providers/binance/internal/ws"
)

func getURL(symbol, streamName string) string {
	return fmt.Sprintf("%s/ws/%s@%s", ws.BaseURL, strings.ToLower(symbol), streamName)
}

func (p *Binance) Subscribe(ctx context.Context, symbol string, streamName string) (<-chan []byte, error) {
	url := getURL(symbol, streamName)
	return p.subscriptionClient.Subscribe(ctx, url)
}
