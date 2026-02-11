package httpx

import (
	"net/http"
	"time"
)

const (
	defaultMaxIdleConns        = 2048
	defaultMaxIdleConnsPerHost = 512
)

// NewPooledClient returns an HTTP client tuned for high-throughput local proxying.
func NewPooledClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = defaultMaxIdleConns
	transport.MaxIdleConnsPerHost = defaultMaxIdleConnsPerHost
	transport.DisableCompression = true

	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}
}
