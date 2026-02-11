package backends

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	testcontainers "github.com/testcontainers/testcontainers-go"
)

type fakeManagedContainer struct {
	endpoint       string
	endpointErr    error
	terminateErr   error
	terminateCalls int
}

func (c *fakeManagedContainer) Endpoint(ctx context.Context, proto string) (string, error) {
	_ = ctx
	_ = proto
	if c.endpointErr != nil {
		return "", c.endpointErr
	}
	return c.endpoint, nil
}

func (c *fakeManagedContainer) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	_ = ctx
	_ = opts
	c.terminateCalls++
	return c.terminateErr
}

func TestManagedManagerStartAndClose(t *testing.T) {
	stateDir := t.TempDir()
	containers := []*fakeManagedContainer{
		{endpoint: "http://127.0.0.1:20001/"},
		{endpoint: "http://127.0.0.1:20002/"},
	}

	var requests []testcontainers.GenericContainerRequest
	manager := NewManagedManager(2, "amazon/dynamodb-local:latest", stateDir)
	manager.probeHostPort = func(ctx context.Context, hostport string) error {
		_ = ctx
		if hostport == "" {
			return fmt.Errorf("hostport must not be empty")
		}
		return nil
	}
	manager.startContainer = func(ctx context.Context, req testcontainers.GenericContainerRequest) (managedContainer, error) {
		_ = ctx
		requests = append(requests, req)
		return containers[len(requests)-1], nil
	}

	backends, err := manager.Start(context.Background())
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if len(backends) != 2 {
		t.Fatalf("len(backends) = %d, want 2", len(backends))
	}
	if len(requests) != 2 {
		t.Fatalf("len(requests) = %d, want 2", len(requests))
	}

	for i := range backends {
		if backends[i].ID != i {
			t.Fatalf("backends[%d].ID = %d, want %d", i, backends[i].ID, i)
		}
		wantEndpoint := strings.TrimRight(containers[i].endpoint, "/")
		if backends[i].Endpoint != wantEndpoint {
			t.Fatalf("backends[%d].Endpoint = %q, want %q", i, backends[i].Endpoint, wantEndpoint)
		}

		request := requests[i]
		if !request.Started {
			t.Fatalf("requests[%d].Started = false, want true", i)
		}
		if request.ContainerRequest.Image != "amazon/dynamodb-local:latest" {
			t.Fatalf("requests[%d].Image = %q, want %q", i, request.ContainerRequest.Image, "amazon/dynamodb-local:latest")
		}
		if len(request.ContainerRequest.ExposedPorts) != 1 || request.ContainerRequest.ExposedPorts[0] != managedDynamoPort {
			t.Fatalf("requests[%d].ExposedPorts = %v, want [%q]", i, request.ContainerRequest.ExposedPorts, managedDynamoPort)
		}
		if len(request.ContainerRequest.Mounts) != 1 {
			t.Fatalf("requests[%d].Mounts = %d, want 1", i, len(request.ContainerRequest.Mounts))
		}

		mount := request.ContainerRequest.Mounts[0]
		if got, want := mount.Target.Target(), managedDynamoStateMountDir; got != want {
			t.Fatalf("requests[%d].Mount target = %q, want %q", i, got, want)
		}

		wantSource := filepath.Join(stateDir, fmt.Sprintf("instance-%d", i))
		if got := mount.Source.Source(); got != wantSource {
			t.Fatalf("requests[%d].Mount source = %q, want %q", i, got, wantSource)
		}
		if _, err := os.Stat(wantSource); err != nil {
			t.Fatalf("os.Stat(%q) error = %v", wantSource, err)
		}
	}

	if err := manager.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	for i, container := range containers {
		if container.terminateCalls != 1 {
			t.Fatalf("container[%d].terminateCalls = %d, want 1", i, container.terminateCalls)
		}
	}
}

func TestManagedManagerStartCleansUpOnStartupError(t *testing.T) {
	first := &fakeManagedContainer{endpoint: "http://127.0.0.1:20001"}

	manager := NewManagedManager(2, "amazon/dynamodb-local:latest", "")
	manager.probeHostPort = func(ctx context.Context, hostport string) error {
		_ = ctx
		_ = hostport
		return nil
	}

	var calls int
	manager.startContainer = func(ctx context.Context, req testcontainers.GenericContainerRequest) (managedContainer, error) {
		_ = ctx
		_ = req
		if calls == 0 {
			calls++
			return first, nil
		}
		return nil, errors.New("boom")
	}

	_, err := manager.Start(context.Background())
	if err == nil {
		t.Fatal("expected Start() error")
	}
	if !strings.Contains(err.Error(), "start managed backend 1") {
		t.Fatalf("Start() error = %q, want to contain %q", err.Error(), "start managed backend 1")
	}
	if first.terminateCalls != 1 {
		t.Fatalf("first.terminateCalls = %d, want 1", first.terminateCalls)
	}
}

func TestManagedManagerStartValidation(t *testing.T) {
	manager := NewManagedManager(0, "amazon/dynamodb-local:latest", "")
	if _, err := manager.Start(context.Background()); err == nil {
		t.Fatal("expected error for instances <= 0")
	}

	manager = NewManagedManager(1, "   ", "")
	if _, err := manager.Start(context.Background()); err == nil {
		t.Fatal("expected error for empty image")
	}
}
