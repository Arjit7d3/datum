package datum

import (
	"fmt"

	"github.com/Arjit7d3/datum/internal/core"
	"github.com/Arjit7d3/datum/internal/providers"
)

// Client is the main entry point for interacting with market data providers
type Client struct {
	provider core.Provider
}

// NewClient creates a new client for the specified provider
func NewClient(providerName string) (*Client, error) {
	provider, err := providers.NewProvider(providerName)
	if err != nil {
		return nil, fmt.Errorf("failed to create provider: %w", err)
	}

	return &Client{
		provider: provider,
	}, nil
}

const defaultProvider = "binance"

func DefaultClient() (*Client, error) {
	return NewClient(defaultProvider)
}
