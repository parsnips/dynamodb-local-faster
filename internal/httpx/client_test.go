package httpx

import (
	"net/http"
	"testing"
	"time"
)

func TestNewPooledClient(t *testing.T) {
	timeout := 7 * time.Second
	client := NewPooledClient(timeout)
	if client == nil {
		t.Fatalf("NewPooledClient returned nil")
	}
	if client.Timeout != timeout {
		t.Fatalf("client.Timeout = %v, want %v", client.Timeout, timeout)
	}

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("client.Transport type = %T, want *http.Transport", client.Transport)
	}
	if transport.MaxIdleConns != defaultMaxIdleConns {
		t.Fatalf("transport.MaxIdleConns = %d, want %d", transport.MaxIdleConns, defaultMaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != defaultMaxIdleConnsPerHost {
		t.Fatalf("transport.MaxIdleConnsPerHost = %d, want %d", transport.MaxIdleConnsPerHost, defaultMaxIdleConnsPerHost)
	}
	if !transport.DisableCompression {
		t.Fatalf("transport.DisableCompression = false, want true")
	}
}
