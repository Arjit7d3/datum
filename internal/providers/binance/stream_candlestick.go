package binance

import (
	"strconv"
	"strings"

	"github.com/Arjit7d3/datum/internal/core"
	gobinance "github.com/arjit7d3/go-binance"
)

type candlestickStreamWrapper struct {
	symbol   string
	interval string
	stream   *gobinance.Stream[gobinance.KlineResponse]
}

func (b *Binance) NewCandlestickStream(symbol, interval string) (core.IStream[core.Candlestick], error) {
	stream, err := b.client.Kline(symbol, interval)
	if err != nil {
		return nil, err
	}
	return &candlestickStreamWrapper{
		symbol:   symbol,
		interval: interval,
		stream:   stream,
	}, nil
}

func (cs *candlestickStreamWrapper) OnMessage(callback func(core.Candlestick)) {
	cs.stream.OnMessage(func(resp gobinance.KlineResponse) {
		openPrice, _ := strconv.ParseFloat(resp.Kline.OpenPrice, 64)
		closePrice, _ := strconv.ParseFloat(resp.Kline.ClosePrice, 64)
		highPrice, _ := strconv.ParseFloat(resp.Kline.HighPrice, 64)
		lowPrice, _ := strconv.ParseFloat(resp.Kline.LowPrice, 64)
		volume, _ := strconv.ParseFloat(resp.Kline.BaseAssetVolume, 64)

		candle := core.Candlestick{
			Symbol:     strings.ToLower(resp.Symbol),
			Interval:   resp.Kline.Interval,
			StartTime:  resp.Kline.StartTime,
			OpenPrice:  openPrice,
			ClosePrice: closePrice,
			HighPrice:  highPrice,
			LowPrice:   lowPrice,
			Volume:     volume,
			CloseTime:  resp.Kline.CloseTime,
		}
		callback(candle)
	})
}
