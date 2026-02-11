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

func TestResolveStatementReadOnlyWithoutTableFansOut(t *testing.T) {
	r, err := NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: "http://127.0.0.1:8001"},
		{ID: 1, Endpoint: "http://127.0.0.1:8002"},
	})
	if err != nil {
		t.Fatalf("NewStaticRouter() error = %v", err)
	}

	backendsList, err := r.ResolveStatement(ParsedStatement{ReadOnly: true})
	if err != nil {
		t.Fatalf("ResolveStatement() error = %v", err)
	}
	if len(backendsList) != 2 {
		t.Fatalf("len(backendsList) = %d, want 2", len(backendsList))
	}
}

func TestResolveStatementWriteWithoutPartitionKeyRejected(t *testing.T) {
	r, err := NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: "http://127.0.0.1:8001"},
		{ID: 1, Endpoint: "http://127.0.0.1:8002"},
	})
	if err != nil {
		t.Fatalf("NewStaticRouter() error = %v", err)
	}

	_, err = r.ResolveStatement(ParsedStatement{
		TableName: "users",
		ReadOnly:  false,
	})
	if err == nil {
		t.Fatal("expected error for write statement without partition key")
	}
}

func TestPlanOperationCreateTableBroadcast(t *testing.T) {
	r, err := NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: "http://127.0.0.1:8001"},
		{ID: 1, Endpoint: "http://127.0.0.1:8002"},
	})
	if err != nil {
		t.Fatalf("NewStaticRouter() error = %v", err)
	}

	route, err := PlanOperation(r, OperationRequest{
		Operation: "CreateTable",
		TableName: "users",
	})
	if err != nil {
		t.Fatalf("PlanOperation() error = %v", err)
	}
	if route.Mode != RouteModeBroadcast {
		t.Fatalf("route.Mode = %q, want %q", route.Mode, RouteModeBroadcast)
	}
	if len(route.Backends) != 2 {
		t.Fatalf("len(route.Backends) = %d, want 2", len(route.Backends))
	}
}

func TestPlanOperationGetItemUsesSingleResolvedBackend(t *testing.T) {
	r, err := NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: "http://127.0.0.1:8001"},
		{ID: 1, Endpoint: "http://127.0.0.1:8002"},
		{ID: 2, Endpoint: "http://127.0.0.1:8003"},
	})
	if err != nil {
		t.Fatalf("NewStaticRouter() error = %v", err)
	}

	route, err := PlanOperation(r, OperationRequest{
		Operation:       "GetItem",
		TableName:       "users",
		PartitionKey:    []byte("pk-123"),
		HasPartitionKey: true,
	})
	if err != nil {
		t.Fatalf("PlanOperation() error = %v", err)
	}
	if route.Mode != RouteModeSingle {
		t.Fatalf("route.Mode = %q, want %q", route.Mode, RouteModeSingle)
	}
	if len(route.Backends) != 1 {
		t.Fatalf("len(route.Backends) = %d, want 1", len(route.Backends))
	}
}
