package binance

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/Arjit7d3/datum/internal/core"
)

type tradeStream struct {
	symbol string
}

func (b *Binance) NewTradeStream(symbol string) core.IStream[core.TradeData] {
	return &tradeStream{symbol: symbol}
}

func (ts *tradeStream) GetStreamParams() (string, string) {
	return strings.ToLower(ts.symbol), "trade"
}

func (ts *tradeStream) Decode(data []byte) (core.TradeData, error) {
	var wire struct {
		Symbol    string `json:"s"`
		Price     string `json:"p"`
		Qty       string `json:"q"`
		Timestamp int64  `json:"T"`
	}

	if err := json.Unmarshal(data, &wire); err != nil {
		return core.TradeData{}, err
	}

	price, _ := strconv.ParseFloat(wire.Price, 64)
	qty, _ := strconv.ParseFloat(wire.Qty, 64)

	return core.TradeData{
		Symbol:    wire.Symbol,
		Price:     price,
		Quantity:  qty,
		Timestamp: wire.Timestamp,
	}, nil
}
