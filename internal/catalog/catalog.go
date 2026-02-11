package catalog

import (
	"context"
	"encoding/json"
)

type TableRequest struct {
	TableName string
	Payload   json.RawMessage
}

type CatalogReplicator interface {
	CreateTable(ctx context.Context, request TableRequest) error
	UpdateTable(ctx context.Context, request TableRequest) error
	DeleteTable(ctx context.Context, request TableRequest) error
	EnsureConsistent(ctx context.Context, tableName string) error
}

type NoopReplicator struct{}

func NewNoopReplicator() *NoopReplicator {
	return &NoopReplicator{}
}

func (r *NoopReplicator) CreateTable(ctx context.Context, request TableRequest) error {
	_ = ctx
	_ = request
	return nil
}

func (r *NoopReplicator) UpdateTable(ctx context.Context, request TableRequest) error {
	_ = ctx
	_ = request
	return nil
}

func (r *NoopReplicator) DeleteTable(ctx context.Context, request TableRequest) error {
	_ = ctx
	_ = request
	return nil
}

func (r *NoopReplicator) EnsureConsistent(ctx context.Context, tableName string) error {
	_ = ctx
	_ = tableName
	return nil
}
