package backends

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

type AttachedManager struct {
	endpoints []string
	timeout   time.Duration
}

func NewAttachedManager(endpoints []string) *AttachedManager {
	return &AttachedManager{
		endpoints: append([]string(nil), endpoints...),
		timeout:   2 * time.Second,
	}
}

func (m *AttachedManager) Start(ctx context.Context) ([]Backend, error) {
	if len(m.endpoints) == 0 {
		return nil, fmt.Errorf("attached mode requires at least one backend endpoint")
	}

	backends := make([]Backend, 0, len(m.endpoints))
	for i, rawEndpoint := range m.endpoints {
		parsed, err := url.Parse(rawEndpoint)
		if err != nil {
			return nil, fmt.Errorf("parse backend endpoint %q: %w", rawEndpoint, err)
		}
		if parsed.Host == "" {
			return nil, fmt.Errorf("backend endpoint %q is missing host", rawEndpoint)
		}

		if err := probeHostPort(ctx, hostPort(parsed)); err != nil {
			return nil, fmt.Errorf("probe backend endpoint %q: %w", rawEndpoint, err)
		}

		backends = append(backends, Backend{
			ID:       i,
			Endpoint: strings.TrimRight(parsed.String(), "/"),
		})
	}

	return backends, nil
}

func (m *AttachedManager) Close(ctx context.Context) error {
	_ = ctx
	return nil
}

func probeHostPort(ctx context.Context, hostport string) error {
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", hostport)
	if err != nil {
		return err
	}
	return conn.Close()
}

func hostPort(parsed *url.URL) string {
	if parsed.Port() != "" {
		return parsed.Host
	}
	switch parsed.Scheme {
	case "https":
		return net.JoinHostPort(parsed.Hostname(), "443")
	default:
		return net.JoinHostPort(parsed.Hostname(), "80")
	}
}
