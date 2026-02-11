package router

import (
	"fmt"
	"strings"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/parsnips/dynamodb-local-faster/internal/backends"
)

type ParsedStatement struct {
	TableName       string
	PartitionKey    []byte
	HasPartitionKey bool
	ReadOnly        bool
}

type RouteMode string

const (
	RouteModeSingle    RouteMode = "single"
	RouteModeBroadcast RouteMode = "broadcast"
	RouteModeFanout    RouteMode = "fanout"
)

type OperationRequest struct {
	Operation       string
	TableName       string
	PartitionKey    []byte
	HasPartitionKey bool
	Statement       *ParsedStatement
}

type OperationRoute struct {
	Mode     RouteMode
	Backends []backends.Backend
}

type BackendRouter interface {
	ResolveTable(table string) ([]backends.Backend, error)
	ResolveItem(table string, partitionKey []byte) (backends.Backend, error)
	ResolveStatement(statement ParsedStatement) ([]backends.Backend, error)
}

type TableSchemaRegistry interface {
	RememberPartitionKey(tableName string, partitionKeyAttribute string)
	ForgetTable(tableName string)
	PartitionKeyAttribute(tableName string) (string, bool)
}

type StaticRouter struct {
	backends []backends.Backend

	mu                  sync.RWMutex
	partitionKeyByTable map[string]string
}

func NewStaticRouter(backendsList []backends.Backend) (*StaticRouter, error) {
	if len(backendsList) == 0 {
		return nil, fmt.Errorf("at least one backend is required")
	}
	return &StaticRouter{
		backends:            append([]backends.Backend(nil), backendsList...),
		partitionKeyByTable: make(map[string]string),
	}, nil
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
	tableName := strings.TrimSpace(statement.TableName)
	if tableName == "" {
		if statement.ReadOnly {
			return append([]backends.Backend(nil), r.backends...), nil
		}
		return nil, fmt.Errorf("statement table name is required")
	}
	if statement.HasPartitionKey {
		target, err := r.ResolveItem(tableName, statement.PartitionKey)
		if err != nil {
			return nil, err
		}
		return []backends.Backend{target}, nil
	}
	if !statement.ReadOnly {
		return nil, fmt.Errorf("write statement requires a concrete partition key")
	}
	return r.ResolveTable(tableName)
}

func (r *StaticRouter) RememberPartitionKey(tableName string, partitionKeyAttribute string) {
	tableName = strings.TrimSpace(tableName)
	partitionKeyAttribute = strings.TrimSpace(partitionKeyAttribute)
	if tableName == "" || partitionKeyAttribute == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.partitionKeyByTable[tableName] = partitionKeyAttribute
}

func (r *StaticRouter) ForgetTable(tableName string) {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.partitionKeyByTable, tableName)
}

func (r *StaticRouter) PartitionKeyAttribute(tableName string) (string, bool) {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return "", false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.partitionKeyByTable[tableName]
	return value, ok
}

func PlanOperation(r BackendRouter, request OperationRequest) (OperationRoute, error) {
	op := strings.TrimSpace(request.Operation)
	if op == "" {
		return OperationRoute{}, fmt.Errorf("operation is required")
	}

	switch op {
	case "CreateTable", "UpdateTable", "DeleteTable":
		if request.TableName == "" {
			return OperationRoute{}, fmt.Errorf("table name is required")
		}
		targets, err := r.ResolveTable(request.TableName)
		if err != nil {
			return OperationRoute{}, err
		}
		return OperationRoute{Mode: RouteModeBroadcast, Backends: targets}, nil
	case "ListTables":
		targets, err := r.ResolveTable(op)
		if err != nil {
			return OperationRoute{}, err
		}
		return OperationRoute{Mode: RouteModeFanout, Backends: targets}, nil
	case "Scan":
		if request.TableName == "" {
			return OperationRoute{}, fmt.Errorf("table name is required")
		}
		targets, err := r.ResolveTable(request.TableName)
		if err != nil {
			return OperationRoute{}, err
		}
		return OperationRoute{Mode: RouteModeFanout, Backends: targets}, nil
	case "Query":
		if request.TableName == "" {
			return OperationRoute{}, fmt.Errorf("table name is required")
		}
		if request.HasPartitionKey {
			target, err := r.ResolveItem(request.TableName, request.PartitionKey)
			if err != nil {
				return OperationRoute{}, err
			}
			return OperationRoute{Mode: RouteModeSingle, Backends: []backends.Backend{target}}, nil
		}
		targets, err := r.ResolveTable(request.TableName)
		if err != nil {
			return OperationRoute{}, err
		}
		return OperationRoute{Mode: RouteModeFanout, Backends: targets}, nil
	case "DescribeTable", "GetItem", "PutItem", "DeleteItem", "UpdateItem":
		if request.TableName == "" {
			return OperationRoute{}, fmt.Errorf("table name is required")
		}
		if request.HasPartitionKey {
			target, err := r.ResolveItem(request.TableName, request.PartitionKey)
			if err != nil {
				return OperationRoute{}, err
			}
			return OperationRoute{Mode: RouteModeSingle, Backends: []backends.Backend{target}}, nil
		}
		targets, err := r.ResolveTable(request.TableName)
		if err != nil {
			return OperationRoute{}, err
		}
		return OperationRoute{Mode: RouteModeSingle, Backends: []backends.Backend{targets[0]}}, nil
	case "ExecuteStatement":
		if request.Statement == nil {
			return OperationRoute{}, fmt.Errorf("statement is required")
		}
		targets, err := r.ResolveStatement(*request.Statement)
		if err != nil {
			return OperationRoute{}, err
		}
		mode := RouteModeSingle
		if len(targets) > 1 {
			mode = RouteModeFanout
		}
		return OperationRoute{Mode: mode, Backends: targets}, nil
	default:
		table := strings.TrimSpace(request.TableName)
		if table == "" {
			targets, err := r.ResolveTable(op)
			if err != nil {
				return OperationRoute{}, err
			}
			return OperationRoute{Mode: RouteModeSingle, Backends: []backends.Backend{targets[0]}}, nil
		}
		if request.HasPartitionKey {
			target, err := r.ResolveItem(table, request.PartitionKey)
			if err != nil {
				return OperationRoute{}, err
			}
			return OperationRoute{Mode: RouteModeSingle, Backends: []backends.Backend{target}}, nil
		}
		targets, err := r.ResolveTable(table)
		if err != nil {
			return OperationRoute{}, err
		}
		return OperationRoute{Mode: RouteModeSingle, Backends: []backends.Backend{targets[0]}}, nil
	}
}

func hashPartitionKey(partitionKey []byte) uint64 {
	return xxhash.Sum64(partitionKey)
}
