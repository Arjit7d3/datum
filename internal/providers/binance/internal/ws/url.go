package ws

import (
	"fmt"
	"strings"
)

const BaseURL = "wss://stream.binance.com:9443"

func GetURL(symbol string, stream string) string {
	return fmt.Sprintf("%s/ws/%s@%s", BaseURL, strings.ToLower(symbol), stream)
}
