package binance

import (
	"context"

	binance "github.com/arjit7d3/go-binance"
)

type Binance struct {
	client *binance.Client
}

func New(ctx context.Context) (*Binance, error) {
	c, err := binance.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return &Binance{
		client: c,
	}, nil
}
