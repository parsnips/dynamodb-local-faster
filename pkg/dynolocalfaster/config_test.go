package dynolocalfaster

import "testing"

func TestNormalizeConfigDefaults(t *testing.T) {
	cfg, err := normalizeConfig(Config{})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}

	if cfg.ListenAddr != DefaultListenAddr {
		t.Fatalf("ListenAddr = %q, want %q", cfg.ListenAddr, DefaultListenAddr)
	}
	if cfg.Mode != DefaultMode {
		t.Fatalf("Mode = %q, want %q", cfg.Mode, DefaultMode)
	}
	if cfg.Instances != DefaultInstances {
		t.Fatalf("Instances = %d, want %d", cfg.Instances, DefaultInstances)
	}
	if cfg.BackendRuntime != DefaultManagedRuntime {
		t.Fatalf("BackendRuntime = %q, want %q", cfg.BackendRuntime, DefaultManagedRuntime)
	}
}

func TestNormalizeConfigAttachedRequiresEndpoints(t *testing.T) {
	_, err := normalizeConfig(Config{
		Mode: ModeAttached,
	})
	if err == nil {
		t.Fatal("expected error for attached mode without endpoints")
	}
}

func TestNormalizeConfigAddsBackendScheme(t *testing.T) {
	cfg, err := normalizeConfig(Config{
		Mode:             ModeAttached,
		BackendEndpoints: []string{"127.0.0.1:8001", "http://127.0.0.1:8002/"},
	})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}

	if cfg.BackendEndpoints[0] != "http://127.0.0.1:8001" {
		t.Fatalf("endpoint[0] = %q", cfg.BackendEndpoints[0])
	}
	if cfg.BackendEndpoints[1] != "http://127.0.0.1:8002" {
		t.Fatalf("endpoint[1] = %q", cfg.BackendEndpoints[1])
	}
}

func TestNormalizeConfigRejectsUnknownManagedBackendRuntime(t *testing.T) {
	_, err := normalizeConfig(Config{
		Mode:           ModeManaged,
		BackendRuntime: ManagedBackendRuntime("wat"),
	})
	if err == nil {
		t.Fatal("expected error for unknown managed backend runtime")
	}
}
