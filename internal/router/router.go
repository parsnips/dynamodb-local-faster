package router

import (
	"fmt"
	"hash/fnv"

	"github.com/parsnips/dynamodb-local-faster/internal/backends"
)

type ParsedStatement struct {
	TableName       string
	PartitionKey    []byte
	HasPartitionKey bool
	ReadOnly        bool
}

type BackendRouter interface {
	ResolveTable(table string) ([]backends.Backend, error)
	ResolveItem(table string, partitionKey []byte) (backends.Backend, error)
	ResolveStatement(statement ParsedStatement) ([]backends.Backend, error)
}

type StaticRouter struct {
	backends []backends.Backend
}

func NewStaticRouter(backendsList []backends.Backend) (*StaticRouter, error) {
	if len(backendsList) == 0 {
		return nil, fmt.Errorf("at least one backend is required")
	}
	return &StaticRouter{backends: append([]backends.Backend(nil), backendsList...)}, nil
}

func (r *StaticRouter) ResolveTable(table string) ([]backends.Backend, error) {
	if table == "" {
		return nil, fmt.Errorf("table name is required")
	}
	return append([]backends.Backend(nil), r.backends...), nil
}

func (r *StaticRouter) ResolveItem(table string, partitionKey []byte) (backends.Backend, error) {
	if table == "" {
		return backends.Backend{}, fmt.Errorf("table name is required")
	}
	if len(partitionKey) == 0 {
		return backends.Backend{}, fmt.Errorf("partition key is required")
	}

	index := hashPartitionKey(partitionKey) % uint64(len(r.backends))
	return r.backends[index], nil
}

func (r *StaticRouter) ResolveStatement(statement ParsedStatement) ([]backends.Backend, error) {
	if statement.TableName == "" {
		return nil, fmt.Errorf("statement table name is required")
	}
	if statement.HasPartitionKey {
		target, err := r.ResolveItem(statement.TableName, statement.PartitionKey)
		if err != nil {
			return nil, err
		}
		return []backends.Backend{target}, nil
	}
	return r.ResolveTable(statement.TableName)
}

func hashPartitionKey(partitionKey []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(partitionKey)
	return h.Sum64()
}
