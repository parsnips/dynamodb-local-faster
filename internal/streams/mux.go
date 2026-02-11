package streams

import "context"

type StreamDescriptor struct {
	TableName string
	StreamARN string
	BackendID int
}

type DescribeStreamResult struct {
	StreamARN string
	Shards    []string
}

type GetRecordsResult struct {
	Records           []map[string]any
	NextShardIterator string
}

type StreamMux interface {
	RegisterTable(ctx context.Context, tableName string, streams []StreamDescriptor) error
	DescribeStream(ctx context.Context, streamARN string) (DescribeStreamResult, error)
	GetShardIterator(ctx context.Context, streamARN string, shardID string) (string, error)
	GetRecords(ctx context.Context, shardIterator string, limit int32) (GetRecordsResult, error)
}

type NoopMux struct{}

func NewNoopMux() *NoopMux {
	return &NoopMux{}
}

func (m *NoopMux) RegisterTable(ctx context.Context, tableName string, streams []StreamDescriptor) error {
	_ = ctx
	_ = tableName
	_ = streams
	return nil
}

func (m *NoopMux) DescribeStream(ctx context.Context, streamARN string) (DescribeStreamResult, error) {
	_ = ctx
	return DescribeStreamResult{StreamARN: streamARN}, nil
}

func (m *NoopMux) GetShardIterator(ctx context.Context, streamARN string, shardID string) (string, error) {
	_ = ctx
	_ = streamARN
	_ = shardID
	return "", nil
}

func (m *NoopMux) GetRecords(ctx context.Context, shardIterator string, limit int32) (GetRecordsResult, error) {
	_ = ctx
	_ = shardIterator
	_ = limit
	return GetRecordsResult{}, nil
}
