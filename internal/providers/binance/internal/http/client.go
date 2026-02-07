package http

import (
	"context"
	"fmt"
	"io"
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

// Get performs a GET request and returns the raw response body
func (c *Client) Get(ctx context.Context, endpoint string, queryParams map[string]string) ([]byte, error) {
	url := GetURL(c.baseURL, endpoint, queryParams)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("bad status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return body, nil
}
