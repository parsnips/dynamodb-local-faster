package backends

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestAttachedManagerStart(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	defer listener.Close()

	manager := NewAttachedManager([]string{"http://" + listener.Addr().String()})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	backends, err := manager.Start(ctx)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if len(backends) != 1 {
		t.Fatalf("len(backends) = %d, want 1", len(backends))
	}
}

func TestAttachedManagerRequiresEndpoints(t *testing.T) {
	manager := NewAttachedManager(nil)
	if _, err := manager.Start(context.Background()); err == nil {
		t.Fatal("expected error when starting with no endpoints")
	}
}
