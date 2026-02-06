package binance

import (
	"context"
	"fmt"
)

func (b *Binance) Query(ctx context.Context, symbol string, interval string, startTime int64, endTime int64, limit int) ([][]any, error) {
	var raw [][]any
	err := b.httpClient.Get(ctx, "uiKlines", map[string]string{
		"symbol":    symbol,
		"interval":  interval,
		"startTime": fmt.Sprint(startTime),
		"endTime":   fmt.Sprint(endTime),
		"limit":     fmt.Sprint(limit),
	}, &raw)
	if err != nil {
		return nil, err
	}
	return raw, nil
}
