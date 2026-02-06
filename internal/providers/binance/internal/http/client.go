package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type Client struct {
	client  *http.Client
	baseURL string
}

func NewClient() *Client {
	return &Client{
		client:  &http.Client{},
		baseURL: "https://api.binance.com/api/v3",
	}
}

func (c *Client) Get(ctx context.Context, endpoint string, queryParams map[string]string, target any) error {
	url := GetURL(c.baseURL, endpoint, queryParams)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("bad status code: %d", resp.StatusCode)
	}

	if target != nil {
		if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
	}

	return nil
}
