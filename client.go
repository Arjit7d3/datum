package datum

import (
	"context"
	"fmt"
	"strings"

	"github.com/Arjit7d3/datum/internal/core"
	"github.com/Arjit7d3/datum/internal/providers"
)

// Client is the main entry point for interacting with market data providers
type Client struct {
	provider core.Provider
}

// NewClient creates a new client for the specified provider
func NewClient(ctx context.Context, providerName string) (*Client, error) {
	provider, err := providers.NewProvider(ctx, providerName)
	if err != nil {
		return nil, fmt.Errorf("failed to create provider: %w", err)
	}

	return &Client{
		provider: provider,
	}, nil
}

const defaultProvider = "binance"

func DefaultClient(ctx context.Context) (*Client, error) {
	return NewClient(ctx, defaultProvider)
}

func (c *Client) NewTradeStream(symbol string) (core.IStream[core.Trade], error) {
	symbol = strings.ToLower(symbol)
	return c.provider.NewTradeStream(symbol)
}
