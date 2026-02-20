package binance

import (
	"strconv"
	"strings"

	"github.com/Arjit7d3/datum/internal/core"
	gobinance "github.com/arjit7d3/go-binance"
)

type depthStreamWrapper struct {
	stream *gobinance.Stream[gobinance.DepthResponse]
}

func (w *depthStreamWrapper) OnMessage(callback func(core.Depth)) {
	w.stream.OnMessage(func(resp gobinance.DepthResponse) {
		bids := make([]core.DepthLevel, 0, len(resp.Bids))
		for _, b := range resp.Bids {
			if len(b) >= 2 {
				price, _ := strconv.ParseFloat(b[0], 64)
				qty, _ := strconv.ParseFloat(b[1], 64)
				bids = append(bids, core.DepthLevel{Price: price, Quantity: qty})
			}
		}

		asks := make([]core.DepthLevel, 0, len(resp.Asks))
		for _, a := range resp.Asks {
			if len(a) >= 2 {
				price, _ := strconv.ParseFloat(a[0], 64)
				qty, _ := strconv.ParseFloat(a[1], 64)
				asks = append(asks, core.DepthLevel{Price: price, Quantity: qty})
			}
		}

		depth := core.Depth{
			Symbol:        strings.ToLower(resp.Symbol),
			Bids:          bids,
			Asks:          asks,
			FirstUpdateID: resp.FirstUpdateID,
			FinalUpdateID: resp.FinalUpdateID,
			Timestamp:     resp.EventTime,
		}

		callback(depth)
	})
}

// NewDepthStream creates a new 100ms order book depth wrapper stream
func (b *Binance) NewDepthStream(symbol string) (core.IStream[core.Depth], error) {
	stream, err := b.client.Depth100ms(symbol)
	if err != nil {
		return nil, err
	}
	return &depthStreamWrapper{stream: stream}, nil
}
