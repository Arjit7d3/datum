package binance

import (
	"strconv"
	"strings"

	"github.com/Arjit7d3/datum/internal/core"
	gobinance "github.com/arjit7d3/go-binance"
)

type tradeStreamWrapper struct {
	symbol string
	stream *gobinance.Stream[gobinance.TradeResponse]
}

func (b *Binance) NewTradeStream(symbol string) (core.IStream[core.Trade], error) {
	stream, err := b.client.Trade(symbol)
	if err != nil {
		return nil, err
	}
	return &tradeStreamWrapper{
		symbol: symbol,
		stream: stream,
	}, nil
}

func (ts *tradeStreamWrapper) OnMessage(callback func(core.Trade)) {
	ts.stream.OnMessage(func(resp gobinance.TradeResponse) {
		price, _ := strconv.ParseFloat(resp.Price, 64)
		qty, _ := strconv.ParseFloat(resp.Quantity, 64)

		trade := core.Trade{
			Symbol:    strings.ToLower(resp.Symbol),
			Price:     price,
			Quantity:  qty,
			Timestamp: resp.TradeTime,
		}
		callback(trade)
	})
}
