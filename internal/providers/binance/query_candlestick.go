package binance

import (
	"encoding/json"

	"github.com/Arjit7d3/datum/internal/core"
)

type candlestickQuery struct {
	symbol   string
	interval string
}

func (b *Binance) NewCandlestickQuery(symbol, interval string) core.IQuery[[]core.RawCandlestick] {
	return &candlestickQuery{symbol: symbol, interval: interval}
}

func (cq *candlestickQuery) GetEndpoint() string {
	return "klines"
}

func (cq *candlestickQuery) GetQueryParameters() map[string]string {
	return map[string]string{
		"symbol":   cq.symbol,
		"interval": cq.interval,
	}
}

func (cq *candlestickQuery) Decode(data []byte) ([]core.RawCandlestick, error) {
	var raw [][]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	result := make([]core.RawCandlestick, len(raw))
	for i, item := range raw {
		result[i] = core.RawCandlestick(item)
	}
	return result, nil
}
