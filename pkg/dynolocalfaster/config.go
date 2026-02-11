package dynolocalfaster

import (
	"fmt"
	"net/url"
	"strings"
)

type Mode string

const (
	ModeManaged  Mode = "managed"
	ModeAttached Mode = "attached"
)

const (
	DefaultListenAddr  = "127.0.0.1:8000"
	DefaultInstances   = 4
	DefaultMode        = ModeManaged
	DefaultDynamoImage = "amazon/dynamodb-local:latest"
	DefaultMetricsAddr = "127.0.0.1:9090"
)

type Config struct {
	ListenAddr       string
	Instances        int
	Mode             Mode
	BackendEndpoints []string
	DynamoImage      string
	StateDir         string
	MetricsAddr      string
}

func normalizeConfig(cfg Config) (Config, error) {
	if strings.TrimSpace(cfg.ListenAddr) == "" {
		cfg.ListenAddr = DefaultListenAddr
	}
	if cfg.Instances <= 0 {
		cfg.Instances = DefaultInstances
	}
	if cfg.Mode == "" {
		cfg.Mode = DefaultMode
	}
	if strings.TrimSpace(cfg.DynamoImage) == "" {
		cfg.DynamoImage = DefaultDynamoImage
	}
	if strings.TrimSpace(cfg.MetricsAddr) == "" {
		cfg.MetricsAddr = DefaultMetricsAddr
	}

	cfg.BackendEndpoints = normalizeEndpointList(cfg.BackendEndpoints)

	if err := validateConfig(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func normalizeEndpointList(endpoints []string) []string {
	out := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			continue
		}
		if !strings.Contains(endpoint, "://") {
			endpoint = "http://" + endpoint
		}
		out = append(out, strings.TrimRight(endpoint, "/"))
	}
	return out
}

func validateConfig(cfg Config) error {
	switch cfg.Mode {
	case ModeManaged:
		if cfg.Instances <= 0 {
			return fmt.Errorf("instances must be > 0 in managed mode")
		}
	case ModeAttached:
		if len(cfg.BackendEndpoints) == 0 {
			return fmt.Errorf("backend_endpoints is required in attached mode")
		}
	default:
		return fmt.Errorf("mode must be %q or %q", ModeManaged, ModeAttached)
	}

	for _, endpoint := range cfg.BackendEndpoints {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return fmt.Errorf("invalid backend endpoint %q: %w", endpoint, err)
		}
		if parsed.Host == "" {
			return fmt.Errorf("invalid backend endpoint %q: missing host", endpoint)
		}
	}
	return nil
}
