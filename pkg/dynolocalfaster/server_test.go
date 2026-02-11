package dynolocalfaster

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/parsnips/dynamodb-local-faster/internal/backends"
	"github.com/parsnips/dynamodb-local-faster/internal/metrics"
)

type fakeServerManager struct {
	startBackends []backends.Backend
	startErr      error
	closeErr      error
	startCalls    int
	closeCalls    int
}

func (m *fakeServerManager) Start(ctx context.Context) ([]backends.Backend, error) {
	_ = ctx
	m.startCalls++
	if m.startErr != nil {
		return nil, m.startErr
	}
	return append([]backends.Backend(nil), m.startBackends...), nil
}

func (m *fakeServerManager) Close(ctx context.Context) error {
	_ = ctx
	m.closeCalls++
	return m.closeErr
}

func TestServerStartClosesManagerOnRouterBuildFailure(t *testing.T) {
	manager := &fakeServerManager{
		startBackends: nil,
	}
	server := &Server{
		cfg: Config{
			ListenAddr:  "127.0.0.1:0",
			MetricsAddr: "",
		},
		manager: manager,
		state:   metrics.NewState(),
	}

	err := server.Start(context.Background())
	if err == nil {
		t.Fatal("expected Start() error")
	}
	if !strings.Contains(err.Error(), "at least one backend is required") {
		t.Fatalf("Start() error = %q, want to contain %q", err.Error(), "at least one backend is required")
	}
	if manager.startCalls != 1 {
		t.Fatalf("manager.startCalls = %d, want 1", manager.startCalls)
	}
	if manager.closeCalls != 1 {
		t.Fatalf("manager.closeCalls = %d, want 1", manager.closeCalls)
	}
	if server.started {
		t.Fatal("server.started = true, want false")
	}
}

func TestServerStartClosesManagerOnListenFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	defer listener.Close()

	manager := &fakeServerManager{
		startBackends: []backends.Backend{{ID: 0, Endpoint: "http://127.0.0.1:20001"}},
	}
	server := &Server{
		cfg: Config{
			ListenAddr:  listener.Addr().String(),
			MetricsAddr: "",
		},
		manager: manager,
		state:   metrics.NewState(),
	}

	err = server.Start(context.Background())
	if err == nil {
		t.Fatal("expected Start() error")
	}
	if !strings.Contains(err.Error(), "listen on") {
		t.Fatalf("Start() error = %q, want to contain %q", err.Error(), "listen on")
	}
	if manager.startCalls != 1 {
		t.Fatalf("manager.startCalls = %d, want 1", manager.startCalls)
	}
	if manager.closeCalls != 1 {
		t.Fatalf("manager.closeCalls = %d, want 1", manager.closeCalls)
	}
	if server.started {
		t.Fatal("server.started = true, want false")
	}
}
