package config

import (
	"flag"
	"strings"

	"github.com/parsnips/dynamodb-local-faster/pkg/dynolocalfaster"
)

func ParseFlags(args []string) (dynolocalfaster.Config, error) {
	fs := flag.NewFlagSet("dynamodb-local-faster", flag.ContinueOnError)

	var cfg dynolocalfaster.Config
	var mode string
	var backendEndpoints string
	var backendRuntime string

	fs.StringVar(&cfg.ListenAddr, "listen-addr", dynolocalfaster.DefaultListenAddr, "address for the DynamoDB-compatible API")
	fs.IntVar(&cfg.Instances, "instances", dynolocalfaster.DefaultInstances, "number of DynamoDB Local instances in managed mode")
	fs.StringVar(&mode, "mode", string(dynolocalfaster.DefaultMode), "startup mode: managed|attached")
	fs.StringVar(&backendEndpoints, "backend-endpoints", "", "comma-separated backend endpoints (required for attached mode)")
	fs.StringVar(&cfg.DynamoImage, "image", dynolocalfaster.DefaultDynamoImage, "DynamoDB Local image for managed mode")
	fs.StringVar(
		&backendRuntime,
		"backend-runtime",
		string(dynolocalfaster.DefaultManagedRuntime),
		"managed backend runtime: host|container",
	)
	fs.StringVar(
		&cfg.DynamoLocalPath,
		"dynamodb-local-path",
		"",
		"path to DynamoDB Local distribution dir (contains DynamoDBLocal.jar and DynamoDBLocal_lib) for host runtime",
	)
	fs.StringVar(&cfg.StateDir, "state-dir", "", "state directory for managed mode (enables persistent backend volumes; default is in-memory)")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", dynolocalfaster.DefaultMetricsAddr, "metrics and health endpoint address")

	if err := fs.Parse(args); err != nil {
		return dynolocalfaster.Config{}, err
	}

	cfg.Mode = dynolocalfaster.Mode(mode)
	cfg.BackendEndpoints = splitCSV(backendEndpoints)
	cfg.BackendRuntime = dynolocalfaster.ManagedBackendRuntime(backendRuntime)

	return cfg, nil
}

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}
