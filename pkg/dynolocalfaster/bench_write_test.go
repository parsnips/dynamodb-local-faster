//go:build integration

package dynolocalfaster

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	backendspkg "github.com/parsnips/dynamodb-local-faster/internal/backends"
	"github.com/parsnips/dynamodb-local-faster/internal/httpx"
)

const (
	benchPutRetries     = 5
	benchClusterTimeout = 20 * time.Minute
)

// BenchmarkWriteThroughput reports four baselines at multiple concurrency levels:
// 1) direct-single-backend: direct client -> one backend
// 2) proxy-1-instance: client -> proxy -> one backend
// 3) direct-sharded-N-backends: client-side hash routing -> N backends
// 4) proxy-N-instances: client -> proxy -> N backends
//
// Configure worker levels with env var DLF_BENCH_WORKERS (or
// DLF_BENCH_PARALLELISM for backwards compatibility), e.g.:
//
//	DLF_BENCH_WORKERS=1,10,40 go test -tags=integration -run='^$' -bench=BenchmarkWriteThroughput -benchtime=30s -count=1 -v ./pkg/dynolocalfaster
//
// Configure latency sampling with DLF_BENCH_LATENCY_SAMPLE_RATE, where 1
// records every request and 10 records every tenth request.
func BenchmarkWriteThroughput(b *testing.B) {
	cpus := runtime.NumCPU()
	shardInstances := min(10, cpus)
	if shardInstances < 2 {
		shardInstances = 2
	}

	workerLevels := benchWorkerSweep(cpus)
	latencySampleRate := benchLatencySampleRate()
	b.Logf(
		"worker levels: %v (shard instances=%d latency sample rate=%d)",
		workerLevels,
		shardInstances,
		latencySampleRate,
	)

	for _, workers := range workerLevels {
		workers := workers
		b.Run(fmt.Sprintf("workers-%d", workers), func(b *testing.B) {
			runSingleBackendComparisons(b, workers, latencySampleRate)
			runShardedBackendComparisons(b, workers, shardInstances, latencySampleRate)
		})
	}
}

func runSingleBackendComparisons(b *testing.B, workers int, latencySampleRate int) {
	ctx, cancel := context.WithTimeout(context.Background(), benchClusterTimeout)
	b.Cleanup(cancel)

	backendsList := startBenchManagedBackends(b, ctx, 1)
	directClient := newBenchDynamoClient(b, backendsList[0].Endpoint)
	proxyServer := startBenchAttachedServer(b, ctx, backendsList)
	proxyClient := newBenchDynamoClientWithMaxAttempts(b, proxyServer.Endpoint(), 1)

	directTable := setupBenchTableOnClient(b, ctx, directClient, "bench-direct-single")
	proxyTable := setupBenchTableViaProxy(b, ctx, proxyClient, "bench-proxy-single")

	var directStats benchStats
	var proxyStats benchStats

	b.Run("direct-single-backend", func(b *testing.B) {
		runBenchPutParallel(
			b,
			ctx,
			workers,
			latencySampleRate,
			"ds-",
			directTable,
			&directStats,
			func(_ string) *dynamodb.Client { return directClient },
		)
		directStats.report(b, "direct-single-backend")
	})

	b.Run("proxy-1-instance", func(b *testing.B) {
		runBenchPutParallel(
			b,
			ctx,
			workers,
			latencySampleRate,
			"p1-",
			proxyTable,
			&proxyStats,
			func(_ string) *dynamodb.Client { return proxyClient },
		)
		proxyStats.report(b, "proxy-1-instance")
	})
}

func runShardedBackendComparisons(b *testing.B, workers int, instances int, latencySampleRate int) {
	ctx, cancel := context.WithTimeout(context.Background(), benchClusterTimeout)
	b.Cleanup(cancel)

	backendsList := startBenchManagedBackends(b, ctx, instances)
	directClients := make([]*dynamodb.Client, len(backendsList))
	for i, backend := range backendsList {
		directClients[i] = newBenchDynamoClient(b, backend.Endpoint)
	}

	proxyServer := startBenchAttachedServer(b, ctx, backendsList)
	proxyClient := newBenchDynamoClientWithMaxAttempts(b, proxyServer.Endpoint(), 1)

	directTable := setupBenchTableOnAllClients(b, ctx, directClients, "bench-direct-sharded")
	proxyTable := setupBenchTableViaProxy(b, ctx, proxyClient, "bench-proxy-sharded")

	var directStats benchStats
	var proxyStats benchStats

	directName := fmt.Sprintf("direct-sharded-%d-backends", instances)
	b.Run(directName, func(b *testing.B) {
		runBenchPutParallel(
			b,
			ctx,
			workers,
			latencySampleRate,
			"dn-",
			directTable,
			&directStats,
			func(pk string) *dynamodb.Client {
				bucket := routeBucketForStringPK(pk, uint64(len(directClients)))
				return directClients[int(bucket)]
			},
		)
		directStats.report(b, directName)
	})

	proxyName := fmt.Sprintf("proxy-%d-instances", instances)
	b.Run(proxyName, func(b *testing.B) {
		runBenchPutParallel(
			b,
			ctx,
			workers,
			latencySampleRate,
			"pn-",
			proxyTable,
			&proxyStats,
			func(_ string) *dynamodb.Client { return proxyClient },
		)
		proxyStats.report(b, proxyName)
	})
}

type benchLatencySummary struct {
	sampleCount int
	p50NS       int64
	p95NS       int64
	p99NS       int64
}

func runBenchPutParallel(
	b *testing.B,
	ctx context.Context,
	workers int,
	latencySampleRate int,
	keyPrefix string,
	tableName string,
	stats *benchStats,
	pickClient func(pk string) *dynamodb.Client,
) {
	b.Helper()

	stats.reset()
	sampleEvery := max(latencySampleRate, 1)
	collector := newLatencyCollector(sampleEvery, max(b.N/sampleEvery, 1))

	workerCount := max(workers, 1)
	var opSeq atomic.Int64
	startWorkers := make(chan struct{})
	var wg sync.WaitGroup

	b.ResetTimer()
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()

			localSamples := make([]int64, 0, 256)
			localCount := 0
			<-startWorkers
			for {
				opIndex := int(opSeq.Add(1))
				if opIndex > b.N {
					break
				}

				pk := keyPrefix + strconv.FormatInt(stats.seq.Add(1), 10)
				startedAt := time.Now()
				benchPutItem(ctx, pickClient(pk), tableName, pk, stats)
				localCount++
				if localCount%sampleEvery == 0 {
					localSamples = append(localSamples, time.Since(startedAt).Nanoseconds())
				}
			}
			collector.addBatch(localSamples)
		}()
	}
	close(startWorkers)
	wg.Wait()
	b.StopTimer()

	elapsed := b.Elapsed()
	summary := collector.summary()

	total := stats.success.Load() + stats.failures.Load()
	reqPerSecond := 0.0
	if elapsed > 0 {
		reqPerSecond = float64(total) / elapsed.Seconds()
	}

	b.ReportMetric(reqPerSecond, "req/s")
	b.ReportMetric(float64(workerCount), "workers")
	b.ReportMetric(float64(stats.failures.Load()), "errors")
	b.ReportMetric(float64(stats.retries.Load()), "retries")
	if summary.sampleCount > 0 {
		b.ReportMetric(float64(summary.p50NS)/1e6, "p50_ms")
		b.ReportMetric(float64(summary.p95NS)/1e6, "p95_ms")
		b.ReportMetric(float64(summary.p99NS)/1e6, "p99_ms")
		b.ReportMetric(float64(summary.sampleCount), "latency_samples")
	}
}

type latencyCollector struct {
	mu      sync.Mutex
	samples []int64
}

func newLatencyCollector(sampleEvery int, expectedSamples int) *latencyCollector {
	if sampleEvery <= 0 {
		sampleEvery = 1
	}
	if expectedSamples < 0 {
		expectedSamples = 0
	}
	return &latencyCollector{
		samples: make([]int64, 0, expectedSamples),
	}
}

func (c *latencyCollector) addBatch(batch []int64) {
	if len(batch) == 0 {
		return
	}

	c.mu.Lock()
	c.samples = append(c.samples, batch...)
	c.mu.Unlock()
}

func (c *latencyCollector) summary() benchLatencySummary {
	c.mu.Lock()
	values := append([]int64(nil), c.samples...)
	c.mu.Unlock()

	if len(values) == 0 {
		return benchLatencySummary{}
	}

	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return benchLatencySummary{
		sampleCount: len(values),
		p50NS:       percentileNearestRank(values, 50),
		p95NS:       percentileNearestRank(values, 95),
		p99NS:       percentileNearestRank(values, 99),
	}
}

func percentileNearestRank(sortedValues []int64, percentile int) int64 {
	if len(sortedValues) == 0 {
		return 0
	}
	rank := int(math.Ceil((float64(percentile) / 100.0) * float64(len(sortedValues))))
	rank = min(max(rank, 1), len(sortedValues))
	return sortedValues[rank-1]
}

type benchStats struct {
	seq      atomic.Int64
	success  atomic.Int64
	retries  atomic.Int64
	failures atomic.Int64
}

func (s *benchStats) reset() {
	s.seq.Store(0)
	s.success.Store(0)
	s.retries.Store(0)
	s.failures.Store(0)
}

func (s *benchStats) report(b *testing.B, label string) {
	b.Helper()
	ok := s.success.Load()
	retries := s.retries.Load()
	failures := s.failures.Load()
	total := ok + failures
	b.Logf(
		"%s: %d total, %d ok, %d retries, %d failures (%.1f%% error rate)",
		label,
		total,
		ok,
		retries,
		failures,
		float64(failures)/float64(max(total, 1))*100,
	)
}

// benchPutItem executes a PutItem with retries to tolerate transient connection
// resets under heavy concurrency (DynamoDB Local is single-threaded per process).
func benchPutItem(ctx context.Context, client *dynamodb.Client, table, pk string, stats *benchStats) {
	for attempt := range benchPutRetries {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: pk},
				"payload": &types.AttributeValueMemberS{Value: "bench-payload"},
			},
		})
		if err == nil {
			if attempt > 0 {
				stats.retries.Add(int64(attempt))
			}
			stats.success.Add(1)
			return
		}
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}
	stats.retries.Add(benchPutRetries)
	stats.failures.Add(1)
}

func startBenchManagedBackends(b *testing.B, ctx context.Context, instances int) []backendspkg.Backend {
	b.Helper()

	manager := backendspkg.NewManagedManager(instances, DefaultDynamoImage, b.TempDir())
	backendsList, err := manager.Start(ctx)
	if err != nil {
		b.Fatalf("managed backends Start(instances=%d) error = %v", instances, err)
	}
	b.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer closeCancel()
		if closeErr := manager.Close(closeCtx); closeErr != nil {
			b.Logf("managed backends Close(instances=%d) error: %v", instances, closeErr)
		}
	})

	return backendsList
}

func startBenchAttachedServer(b *testing.B, ctx context.Context, backendsList []backendspkg.Backend) *Server {
	b.Helper()

	endpoints := make([]string, 0, len(backendsList))
	for _, backend := range backendsList {
		endpoints = append(endpoints, backend.Endpoint)
	}

	server, err := New(ctx, Config{
		ListenAddr:       "127.0.0.1:0",
		MetricsAddr:      "127.0.0.1:0",
		Mode:             ModeAttached,
		BackendEndpoints: endpoints,
	})
	if err != nil {
		b.Fatalf("New(attached) error = %v", err)
	}
	if err := server.Start(ctx); err != nil {
		b.Fatalf("Start(attached) error = %v", err)
	}
	b.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer closeCancel()
		if closeErr := server.Close(closeCtx); closeErr != nil {
			b.Logf("Close(attached) error: %v", closeErr)
		}
	})
	return server
}

func newBenchDynamoClient(b *testing.B, endpoint string) *dynamodb.Client {
	return newBenchDynamoClientWithMaxAttempts(b, endpoint, 0)
}

func newBenchDynamoClientWithMaxAttempts(b *testing.B, endpoint string, maxAttempts int) *dynamodb.Client {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	loadOptions := []func(*awscfg.LoadOptions) error{
		awscfg.WithRegion("us-west-2"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
		awscfg.WithHTTPClient(httpx.NewPooledClient(30 * time.Second)),
	}
	if maxAttempts > 0 {
		loadOptions = append(loadOptions, awscfg.WithRetryMaxAttempts(maxAttempts))
	}

	cfg, err := awscfg.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		b.Fatalf("LoadDefaultConfig() error = %v", err)
	}

	return dynamodb.NewFromConfig(cfg, func(options *dynamodb.Options) {
		options.BaseEndpoint = aws.String(endpoint)
	})
}

func setupBenchTableViaProxy(b *testing.B, ctx context.Context, client *dynamodb.Client, prefix string) string {
	b.Helper()

	tableName := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	if err := createBenchTable(ctx, client, tableName); err != nil {
		b.Fatalf("CreateTable via proxy error = %v", err)
	}
	if err := waitForTableReadyOnAllBackends(ctx, client, tableName); err != nil {
		b.Fatalf("waitForTableReadyOnAllBackends(%q) error = %v", tableName, err)
	}

	b.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		_, _ = client.DeleteTable(cleanupCtx, &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})

	return tableName
}

func setupBenchTableOnClient(b *testing.B, ctx context.Context, client *dynamodb.Client, prefix string) string {
	b.Helper()

	tableName := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	if err := createBenchTable(ctx, client, tableName); err != nil {
		b.Fatalf("CreateTable direct error = %v", err)
	}

	b.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		_, _ = client.DeleteTable(cleanupCtx, &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})

	return tableName
}

func setupBenchTableOnAllClients(b *testing.B, ctx context.Context, clients []*dynamodb.Client, prefix string) string {
	b.Helper()

	tableName := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	for i, client := range clients {
		if err := createBenchTable(ctx, client, tableName); err != nil {
			b.Fatalf("CreateTable direct-sharded client[%d] error = %v", i, err)
		}
	}

	b.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		for _, client := range clients {
			_, _ = client.DeleteTable(cleanupCtx, &dynamodb.DeleteTableInput{
				TableName: aws.String(tableName),
			})
		}
	})

	return tableName
}

func createBenchTable(ctx context.Context, client *dynamodb.Client, tableName string) error {
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
		return err
	}

	waiter := dynamodb.NewTableExistsWaiter(client)
	return waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 2*time.Minute)
}

func benchWorkerSweep(cpus int) []int {
	if cpus <= 0 {
		cpus = 1
	}

	raw := strings.TrimSpace(os.Getenv("DLF_BENCH_WORKERS"))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("DLF_BENCH_PARALLELISM"))
	}
	if raw != "" {
		parts := strings.Split(raw, ",")
		levels := make([]int, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			value, err := strconv.Atoi(part)
			if err != nil {
				continue
			}
			if value > 0 {
				levels = append(levels, value)
			}
		}
		levels = uniqueSortedPositive(levels)
		if len(levels) > 0 {
			return levels
		}
	}

	levels := []int{1, cpus, cpus * 2}
	return uniqueSortedPositive(levels)
}

func benchLatencySampleRate() int {
	raw := strings.TrimSpace(os.Getenv("DLF_BENCH_LATENCY_SAMPLE_RATE"))
	if raw == "" {
		return 1
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 1
	}
	return value
}

func uniqueSortedPositive(input []int) []int {
	seen := make(map[int]struct{}, len(input))
	output := make([]int, 0, len(input))
	for _, value := range input {
		if value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		output = append(output, value)
	}
	sort.Ints(output)
	return output
}
