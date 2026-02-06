package http

import (
	"fmt"
	"net/url"
	"strings"
)

func GetURL(baseURL string, endpoint string, queryParams map[string]string) string {
	endpoint = strings.TrimPrefix(endpoint, "/")
	if len(queryParams) == 0 {
		return fmt.Sprintf("%s/%s", baseURL, endpoint)
	}
	q := url.Values{}
	for key, value := range queryParams {
		q.Add(key, value)
	}
	return fmt.Sprintf("%s/%s?%s", baseURL, endpoint, q.Encode())
}
