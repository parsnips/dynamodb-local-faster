package router

import (
	"testing"

	"github.com/parsnips/dynamodb-local-faster/internal/backends"
)

func TestStaticRouterResolveItemDeterministic(t *testing.T) {
	r, err := NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: "http://127.0.0.1:8001"},
		{ID: 1, Endpoint: "http://127.0.0.1:8002"},
		{ID: 2, Endpoint: "http://127.0.0.1:8003"},
	})
	if err != nil {
		t.Fatalf("NewStaticRouter() error = %v", err)
	}

	first, err := r.ResolveItem("users", []byte("user#123"))
	if err != nil {
		t.Fatalf("ResolveItem(first) error = %v", err)
	}

	for i := 0; i < 10; i++ {
		next, err := r.ResolveItem("users", []byte("user#123"))
		if err != nil {
			t.Fatalf("ResolveItem(next) error = %v", err)
		}
		if next.ID != first.ID {
			t.Fatalf("backend changed: got %d want %d", next.ID, first.ID)
		}
	}
}

func TestStaticRouterResolveItemEmptyKey(t *testing.T) {
	r, err := NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: "http://127.0.0.1:8001"},
	})
	if err != nil {
		t.Fatalf("NewStaticRouter() error = %v", err)
	}

	if _, err := r.ResolveItem("users", nil); err == nil {
		t.Fatal("expected error for empty partition key")
	}
}
