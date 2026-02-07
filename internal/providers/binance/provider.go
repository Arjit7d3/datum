package binance

import (
	"github.com/Arjit7d3/datum/internal/providers/binance/internal/http"
	"github.com/Arjit7d3/datum/internal/providers/binance/internal/subscription"
)

type Binance struct {
	httpClient         *http.Client
	subscriptionClient *subscription.Client
}

func New() *Binance {
	return &Binance{
		httpClient:         http.NewClient(),
		subscriptionClient: subscription.NewClient(),
	}
}
