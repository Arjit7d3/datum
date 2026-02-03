# datum

### Introduction

**datum** is a unified market data service that provies a clean, consistent interface for accessing prices and historical data across multiple exchanges.

It abstracts away exchange-specific APIs and formats, exposing a simple API for:

- Current prices (spot/instruments)
- Historical data (ticks, candles, snapshots)

Internally, datum subscribes to upstream exchanges (e.g. Binance), normalizes incoming data, and serves it as a single source of truth for downstream services.

### Goals

- One interface for all market data
- Real-time + historical access
- Deterministic, normalized outpus
- Exchange-agnostic consumers
- Low-latency reads
- Simple integration
