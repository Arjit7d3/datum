# datum

[![Go](https://img.shields.io/badge/Go-1.24-00ADD8?logo=go&logoColor=white)](https://go.dev)

A provider-agnostic Go library for streaming real-time cryptocurrency market data.

## Features

- **Trade Stream** -- Individual trade events (price, quantity, timestamp)
- **Candlestick Stream** -- OHLCV data at configurable intervals
- **Depth Stream** -- Order book diff updates (100ms)
- Automatic WebSocket reconnection with exponential backoff
- Interface-based design -- implement `core.Provider` to add new exchanges

## Installation

```bash
go get github.com/Arjit7d3/datum
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Arjit7d3/datum"
	"github.com/Arjit7d3/datum/internal/core"
)

func main() {
	client, err := datum.NewClient(context.Background(), "binance")
	if err != nil {
		panic(err)
	}

	stream, err := client.NewTradeStream("btcusdt")
	if err != nil {
		panic(err)
	}

	stream.OnMessage(func(trade core.Trade) {
		fmt.Printf("%s %.2f x %.4f\n", trade.Symbol, trade.Price, trade.Quantity)
	})

	time.Sleep(10 * time.Second)
}
```

## Supported Providers

| Provider | Status |
|----------|--------|
| Binance  | Stable |

## Adding a Provider

Implement the `core.Provider` interface:

```go
type Provider interface {
	NewTradeStream(symbol string) (IStream[Trade], error)
	NewCandlestickStream(symbol, interval string) (IStream[Candlestick], error)
	NewDepthStream(symbol string) (IStream[Depth], error)
}
```

Then register it in `internal/providers/providers.go`.

## Examples

The `depth/` directory contains a reference application that records order book data to Parquet files, with Go and Python replay scripts. See [`depth/README.md`](depth/README.md) for details.
