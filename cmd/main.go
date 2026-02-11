package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Arjit7d3/datum"
)

func main() {
	client, err := datum.NewClient("binance")
	if err != nil {
		log.Printf("Error: %v", err)
	}
	data, err := datum.Query(context.Background(), client, datum.CandlesticksQuery{
		Symbol:    "BTCUSDT",
		Interval:  "1m",
		StartTime: time.Now().Add(-24 * time.Hour).UnixMilli(),
		EndTime:   time.Now().UnixMilli(),
	})
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	candleSticks, err := datum.Stream(context.Background(), client, datum.CandlesticksStream{
		Symbol:   "BTCUSDT",
		Interval: "1m",
	})
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	for i, d := range data {
		fmt.Printf("%v: symbol: %v, timestamp: %v, open: %v, high: %v, low: %v, close: %v\n", i, d.Symbol, time.UnixMilli(d.StartTime), d.OpenPrice, d.HighPrice, d.LowPrice, d.ClosePrice)
	}

	for candlestick := range candleSticks {
		fmt.Printf("%v", candlestick)
	}
}
