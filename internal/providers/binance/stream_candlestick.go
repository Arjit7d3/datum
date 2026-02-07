package binance

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Arjit7d3/datum/internal/core"
)

type candlestickStream struct {
	symbol   string
	interval string
}

func (b *Binance) NewCandlestickStream(symbol, interval string) core.IStream[core.CandlestickData] {
	return &candlestickStream{symbol: symbol, interval: interval}
}

func (cs *candlestickStream) GetStreamParams() (string, string) {
	suffix := fmt.Sprintf("kline_%s", cs.interval)
	return strings.ToLower(cs.symbol), suffix
}

func (cs *candlestickStream) Decode(data []byte) (core.CandlestickData, error) {
	var wire struct {
		Symbol string `json:"s"`
		Kline  struct {
			StartTime  int64       `json:"t"`
			CloseTime  int64       `json:"T"`
			Symbol     string      `json:"s"`
			Interval   string      `json:"i"`
			OpenPrice  interface{} `json:"o"`
			ClosePrice interface{} `json:"c"`
			HighPrice  interface{} `json:"h"`
			LowPrice   interface{} `json:"l"`
			Volume     interface{} `json:"v"`
		} `json:"k"`
	}

	if err := json.Unmarshal(data, &wire); err != nil {
		return core.CandlestickData{}, err
	}

	return core.CandlestickData{
		Symbol:     wire.Symbol,
		Interval:   wire.Kline.Interval,
		StartTime:  wire.Kline.StartTime,
		CloseTime:  wire.Kline.CloseTime,
		OpenPrice:  toFloat64(wire.Kline.OpenPrice),
		ClosePrice: toFloat64(wire.Kline.ClosePrice),
		HighPrice:  toFloat64(wire.Kline.HighPrice),
		LowPrice:   toFloat64(wire.Kline.LowPrice),
		Volume:     toFloat64(wire.Kline.Volume),
	}, nil
}
