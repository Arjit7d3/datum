package providers

import (
	"context"
	"fmt"

	"github.com/Arjit7d3/datum/internal/providers/binance"
)

type ProviderName string

const (
	Binance ProviderName = "binance"
	Default ProviderName = Binance
)

type Provider interface {
	Subscribe(ctx context.Context, symbol string, streamName string) (<-chan []byte, error)
	Query(ctx context.Context, symbol string, interval string, startTime int64, endTime int64, limit int) ([][]any, error)
}

func NewDefaultProvider() (Provider, error) {
	return NewProvider(Default)
}

func NewProvider(name ProviderName) (Provider, error) {
	switch name {
	case Binance:
		return binance.NewProvider(), nil
	default:
		return nil, fmt.Errorf("provider %s not found", name)
	}
}
