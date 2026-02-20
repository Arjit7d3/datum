package providers

import (
	"context"
	"fmt"

	"github.com/Arjit7d3/datum/internal/core"
	"github.com/Arjit7d3/datum/internal/providers/binance"
)

// NewProvider creates a new provider instance by name
func NewProvider(ctx context.Context, name string) (core.Provider, error) {
	switch name {
	case "binance":
		return binance.New(ctx)
	default:
		return nil, fmt.Errorf("unknown provider: %s", name)
	}
}
