//go:build integration

package dynolocalfaster

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// BenchmarkWriteThroughput compares PutItem throughput between a single
// DynamoDB Local instance and the proxy with NumCPU instances.
//
// Run with:
//
//	go test -tags=integration -run=^$ -bench=BenchmarkWriteThroughput -benchtime=10s -count=1 ./pkg/dynolocalfaster
func BenchmarkWriteThroughput(b *testing.B) {
	cpus := runtime.NumCPU()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	b.Cleanup(cancel)

	// Start both servers once in the outer function so they survive
	// calibration rounds (Go re-invokes sub-benchmark bodies with
	// increasing b.N).
	singleServer := startBenchServer(b, ctx, 1)
	singleClient := newBenchDynamoClient(b, singleServer.Endpoint())
	singleTable := setupBenchTable(b, ctx, singleClient, "bench-direct")

	multiServer := startBenchServer(b, ctx, cpus)
	multiClient := newBenchDynamoClient(b, multiServer.Endpoint())
	multiTable := setupBenchTable(b, ctx, multiClient, "bench-proxy")

	var singleSeq atomic.Int64
	var multiSeq atomic.Int64

	b.Run("direct-single-instance", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				pk := "d-" + strconv.FormatInt(singleSeq.Add(1), 10)
				benchPutItem(b, ctx, singleClient, singleTable, pk)
			}
		})
	})

	b.Run(fmt.Sprintf("proxy-%d-instances", cpus), func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				pk := "p-" + strconv.FormatInt(multiSeq.Add(1), 10)
				benchPutItem(b, ctx, multiClient, multiTable, pk)
			}
		})
	})
}

const benchPutRetries = 5

// benchPutItem executes a PutItem with retries to tolerate transient connection
// resets under heavy concurrency (DynamoDB Local is single-threaded).
func benchPutItem(b *testing.B, ctx context.Context, client *dynamodb.Client, table, pk string) {
	b.Helper()
	var lastErr error
	for attempt := range benchPutRetries {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: pk},
				"payload": &types.AttributeValueMemberS{Value: "bench-payload"},
			},
		})
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}
	b.Fatalf("PutItem failed after %d attempts: %v", benchPutRetries, lastErr)
}

func startBenchServer(b *testing.B, ctx context.Context, instances int) *Server {
	b.Helper()

	server, err := New(ctx, Config{
		ListenAddr:  "127.0.0.1:0",
		MetricsAddr: "127.0.0.1:0",
		Mode:        ModeManaged,
		Instances:   instances,
		DynamoImage: DefaultDynamoImage,
		StateDir:    b.TempDir(),
	})
	if err != nil {
		b.Fatalf("New(instances=%d) error = %v", instances, err)
	}
	if err := server.Start(ctx); err != nil {
		b.Fatalf("Start(instances=%d) error = %v", instances, err)
	}
	b.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer closeCancel()
		server.Close(closeCtx)
	})
	return server
}

func newBenchDynamoClient(b *testing.B, endpoint string) *dynamodb.Client {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion("us-west-2"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
	)
	if err != nil {
		b.Fatalf("LoadDefaultConfig() error = %v", err)
	}

	return dynamodb.NewFromConfig(cfg, func(options *dynamodb.Options) {
		options.BaseEndpoint = aws.String(endpoint)
	})
}

func setupBenchTable(b *testing.B, ctx context.Context, client *dynamodb.Client, prefix string) string {
	b.Helper()

	tableName := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		b.Fatalf("CreateTable error = %v", err)
	}

	waiter := dynamodb.NewTableExistsWaiter(client)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 2*time.Minute); err != nil {
		b.Fatalf("wait for table error = %v", err)
	}
	if err := waitForTableReadyOnAllBackends(ctx, client, tableName); err != nil {
		b.Fatalf("waitForTableReadyOnAllBackends error = %v", err)
	}

	b.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		client.DeleteTable(cleanupCtx, &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	})

	return tableName
}
