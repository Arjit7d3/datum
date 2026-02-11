package binance

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/Arjit7d3/datum/internal/core"
)

type candlestick struct {
	StartTime  int64
	OpenPrice  float64
	ClosePrice float64
	HighPrice  float64
	LowPrice   float64
	Volume     float64
	CloseTime  int64
}

func (c *candlestick) UnmarshalJSON(data []byte) error {
	var a struct {
		V [12]json.RawMessage
	}

	if err := json.Unmarshal(data, &a.V); err != nil {
		return err
	}

	// Helper to parse string-encoded float from Binance API
	parseFloat := func(raw json.RawMessage) float64 {
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		}
		return 0
	}

	json.Unmarshal(a.V[0], &c.StartTime)
	c.OpenPrice = parseFloat(a.V[1])
	c.HighPrice = parseFloat(a.V[2])
	c.LowPrice = parseFloat(a.V[3])
	c.ClosePrice = parseFloat(a.V[4])
	c.Volume = parseFloat(a.V[5])
	json.Unmarshal(a.V[6], &c.CloseTime)

	return nil
}

type candlestickQuery struct {
	args core.CandlestickQueryArgs
}

func (b *Binance) NewCandlestickQuery(args core.CandlestickQueryArgs) core.IQuery[[]core.Candlestick] {
	return &candlestickQuery{args: args}
}

func (cq *candlestickQuery) GetEndpoint() string {
	return "klines"
}

func (cq *candlestickQuery) GetQueryParameters() map[string]string {
	return map[string]string{
		"symbol":    cq.args.Symbol,
		"interval":  cq.args.Interval,
		"startTime": fmt.Sprintf("%d", cq.args.StartTime),
		"endTime":   fmt.Sprintf("%d", cq.args.EndTime),
	}
}

func (cq *candlestickQuery) Decode(data []byte) ([]core.Candlestick, error) {
	var binanceCandlesticks []candlestick
	if err := json.Unmarshal(data, &binanceCandlesticks); err != nil {
		return nil, err
	}

	candlesticks := make([]core.Candlestick, len(binanceCandlesticks))
	for i := range binanceCandlesticks {
		candlesticks[i].Symbol = cq.args.Symbol
		candlesticks[i].Interval = cq.args.Interval
		candlesticks[i].StartTime = binanceCandlesticks[i].StartTime
		candlesticks[i].OpenPrice = binanceCandlesticks[i].OpenPrice
		candlesticks[i].HighPrice = binanceCandlesticks[i].HighPrice
		candlesticks[i].LowPrice = binanceCandlesticks[i].LowPrice
		candlesticks[i].ClosePrice = binanceCandlesticks[i].ClosePrice
		candlesticks[i].CloseTime = binanceCandlesticks[i].CloseTime
		candlesticks[i].Volume = binanceCandlesticks[i].Volume
	}

	return candlesticks, nil
}
