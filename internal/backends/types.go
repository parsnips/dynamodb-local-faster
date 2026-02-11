package backends

import "context"

type Backend struct {
	ID       int
	Endpoint string
}

type Manager interface {
	Start(ctx context.Context) ([]Backend, error)
	Close(ctx context.Context) error
}
