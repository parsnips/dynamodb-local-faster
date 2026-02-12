package config

import (
	"testing"

	"github.com/parsnips/dynamodb-local-faster/pkg/dynolocalfaster"
)

func TestParseFlagsManagedRuntimeDefaults(t *testing.T) {
	cfg, err := ParseFlags(nil)
	if err != nil {
		t.Fatalf("ParseFlags() error = %v", err)
	}

	if cfg.BackendRuntime != dynolocalfaster.DefaultManagedRuntime {
		t.Fatalf(
			"BackendRuntime = %q, want %q",
			cfg.BackendRuntime,
			dynolocalfaster.DefaultManagedRuntime,
		)
	}
	if cfg.DynamoLocalPath != "" {
		t.Fatalf("DynamoLocalPath = %q, want empty", cfg.DynamoLocalPath)
	}
}

func TestParseFlagsManagedRuntimeOverrides(t *testing.T) {
	cfg, err := ParseFlags([]string{
		"--backend-runtime=container",
		"--dynamodb-local-path=/opt/dynamodb-local",
	})
	if err != nil {
		t.Fatalf("ParseFlags() error = %v", err)
	}

	if cfg.BackendRuntime != dynolocalfaster.ManagedBackendRuntimeContainer {
		t.Fatalf("BackendRuntime = %q, want %q", cfg.BackendRuntime, dynolocalfaster.ManagedBackendRuntimeContainer)
	}
	if cfg.DynamoLocalPath != "/opt/dynamodb-local" {
		t.Fatalf("DynamoLocalPath = %q, want %q", cfg.DynamoLocalPath, "/opt/dynamodb-local")
	}
}
