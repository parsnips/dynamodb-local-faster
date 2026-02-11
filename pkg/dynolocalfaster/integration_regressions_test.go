//go:build integration

package dynolocalfaster

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	dynamodbstreamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/cespare/xxhash/v2"
)

func TestManagedRegressionBatchGetItemCrossShardReturnsAllItems(t *testing.T) {
	ctx, client := startManagedRegressionHarness(t, 2)
	tableName := createRegressionTable(t, ctx, client, "it-reg-batch")

	keys := findStringPartitionKeysByBucket(t, 2)
	keyBucket0 := keys[0]
	keyBucket1 := keys[1]

	for _, key := range []string{keyBucket0, keyBucket1} {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: key},
				"payload": &types.AttributeValueMemberS{Value: "payload-" + key},
			},
		})
		if err != nil {
			t.Fatalf("PutItem(%q) error = %v", key, err)
		}
	}

	output, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			tableName: {
				Keys: []map[string]types.AttributeValue{
					{"pk": &types.AttributeValueMemberS{Value: keyBucket0}},
					{"pk": &types.AttributeValueMemberS{Value: keyBucket1}},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("BatchGetItem() error = %v", err)
	}

	items := output.Responses[tableName]
	if got, want := len(items), 2; got != want {
		t.Fatalf("BatchGetItem() returned %d items, want %d; got PKs=%v", got, want, collectPKs(items))
	}
}

func TestManagedRegressionScanLimitRespectsGlobalLimit(t *testing.T) {
	ctx, client := startManagedRegressionHarness(t, 2)
	tableName := createRegressionTable(t, ctx, client, "it-reg-scan-limit")

	keys := findStringPartitionKeysByBucket(t, 2)
	keyBucket0 := keys[0]
	keyBucket1 := keys[1]

	for _, key := range []string{keyBucket0, keyBucket1} {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: key},
				"payload": &types.AttributeValueMemberS{Value: "payload-" + key},
			},
		})
		if err != nil {
			t.Fatalf("PutItem(%q) error = %v", key, err)
		}
	}

	output, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
		Limit:     aws.Int32(1),
	})
	if err != nil {
		t.Fatalf("Scan(Limit=1) error = %v", err)
	}

	if got := len(output.Items); got != 1 {
		t.Fatalf("Scan(Limit=1) returned len(Items)=%d, want 1", got)
	}
	if got := output.Count; got != 1 {
		t.Fatalf("Scan(Limit=1) returned Count=%d, want 1", got)
	}
}

func TestManagedRegressionExecuteStatementInsertWorks(t *testing.T) {
	ctx, client := startManagedRegressionHarness(t, 1)
	tableName := createRegressionTable(t, ctx, client, "it-reg-exec-insert")

	statement := fmt.Sprintf(
		`INSERT INTO "%s" VALUE {'pk':'%s','payload':'%s'}`,
		tableName,
		"stmt-1",
		"value-1",
	)
	_, err := client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
		Statement: aws.String(statement),
	})
	if err != nil {
		t.Fatalf("ExecuteStatement(INSERT) error = %v", err)
	}

	getOutput, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "stmt-1"},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("GetItem(stmt-1) error = %v", err)
	}
	if got := stringAttribute(getOutput.Item, "payload"); got != "value-1" {
		t.Fatalf("GetItem(stmt-1).payload = %q, want %q", got, "value-1")
	}
}

func TestManagedRegressionExecuteStatementInsertWithParametersWorks(t *testing.T) {
	ctx, client := startManagedRegressionHarness(t, 2)
	tableName := createRegressionTable(t, ctx, client, "it-reg-exec-insert-params")

	_, err := client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
		Statement: aws.String(fmt.Sprintf(`INSERT INTO "%s" VALUE {'pk': ?, 'payload': ?}`, tableName)),
		Parameters: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "stmt-param-1"},
			&types.AttributeValueMemberS{Value: "value-param-1"},
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStatement(INSERT params) error = %v", err)
	}

	getOutput, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "stmt-param-1"},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("GetItem(stmt-param-1) error = %v", err)
	}
	if got := stringAttribute(getOutput.Item, "payload"); got != "value-param-1" {
		t.Fatalf("GetItem(stmt-param-1).payload = %q, want %q", got, "value-param-1")
	}
}

func TestManagedRegressionGSIQueryReturnsItemsFromAllBackends(t *testing.T) {
	ctx, client := startManagedRegressionHarness(t, 2)

	tableName := fmt.Sprintf("it-reg-gsi-xbackend-%d", time.Now().UnixNano())
	gsiName := "gsi-by-group"

	// Create table with pk as partition key, plus gsi_pk attribute for the GSI.
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi_pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(gsiName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("gsi_pk"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable error = %v", err)
	}

	waiter := dynamodb.NewTableExistsWaiter(client)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 2*time.Minute); err != nil {
		t.Fatalf("wait for table error = %v", err)
	}
	if err := waitForTableReadyOnAllBackends(ctx, client, tableName); err != nil {
		t.Fatalf("waitForTableReadyOnAllBackends error = %v", err)
	}
	if err := waitForGSIReadyOnAllBackends(ctx, client, tableName, gsiName); err != nil {
		t.Fatalf("waitForGSIReadyOnAllBackends error = %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		client.DeleteTable(cleanupCtx, &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	})

	// Find two table PKs that hash to different backends (backend 0 and backend 1).
	keys := findStringPartitionKeysByBucket(t, 2)
	keyOnBackend0 := keys[0]
	keyOnBackend1 := keys[1]

	// Write both items with the SAME gsi_pk so they'd appear together in a GSI query,
	// but they live on different backends due to different table partition keys.
	sharedGSIKey := "same-group"

	for _, pk := range []string{keyOnBackend0, keyOnBackend1} {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: pk},
				"gsi_pk":  &types.AttributeValueMemberS{Value: sharedGSIKey},
				"payload": &types.AttributeValueMemberS{Value: "payload-" + pk},
			},
		})
		if err != nil {
			t.Fatalf("PutItem(%q) error = %v", pk, err)
		}
	}

	// Query the GSI — this must fan out to BOTH backends to find both items.
	queryOutput, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String(gsiName),
		KeyConditionExpression: aws.String("gsi_pk = :gsi"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":gsi": &types.AttributeValueMemberS{Value: sharedGSIKey},
		},
	})
	if err != nil {
		t.Fatalf("Query(GSI gsi_pk=%q) error = %v", sharedGSIKey, err)
	}

	gotPKs := collectPKs(queryOutput.Items)
	wantPKs := []string{keyOnBackend0, keyOnBackend1}
	sort.Strings(wantPKs)

	if len(gotPKs) != 2 {
		t.Fatalf("Query(GSI) returned %d items (pks=%v), want 2 items (pks=%v)", len(gotPKs), gotPKs, wantPKs)
	}
	if gotPKs[0] != wantPKs[0] || gotPKs[1] != wantPKs[1] {
		t.Fatalf("Query(GSI) returned pks=%v, want %v", gotPKs, wantPKs)
	}
}

func TestManagedRegressionStreamsOperationsWork(t *testing.T) {
	ctx, client := startManagedRegressionHarness(t, 2)
	endpoint := aws.ToString(client.Options().BaseEndpoint)
	if endpoint == "" {
		t.Fatal("dynamodb client endpoint is empty")
	}
	streamClient := newIntegrationStreamsClient(t, endpoint)
	tableName := createRegressionStreamTable(t, ctx, client, "it-reg-streams")

	keys := findStringPartitionKeysByBucket(t, 2)
	keyOnBackend0 := keys[0]
	keyOnBackend1 := keys[1]

	for _, pk := range []string{keyOnBackend0, keyOnBackend1} {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: pk},
				"payload": &types.AttributeValueMemberS{Value: "payload-" + pk},
			},
		})
		if err != nil {
			t.Fatalf("PutItem(%q) error = %v", pk, err)
		}
	}

	var (
		observedStreamARN string
		observedShards    int
		observedRecords   int
	)
	if err := waitForSuccess(ctx, 300*time.Millisecond, func(runCtx context.Context) error {
		listOutput, err := streamClient.ListStreams(runCtx, &dynamodbstreams.ListStreamsInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			return fmt.Errorf("ListStreams(%q): %w", tableName, err)
		}
		if len(listOutput.Streams) == 0 {
			return fmt.Errorf("ListStreams(%q) returned no streams", tableName)
		}

		streamARN := ""
		for _, stream := range listOutput.Streams {
			if aws.ToString(stream.TableName) == tableName {
				streamARN = aws.ToString(stream.StreamArn)
				break
			}
		}
		if streamARN == "" {
			return fmt.Errorf("ListStreams(%q) returned no stream ARN for table", tableName)
		}

		describeOutput, err := streamClient.DescribeStream(runCtx, &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String(streamARN),
		})
		if err != nil {
			return fmt.Errorf("DescribeStream(%q): %w", streamARN, err)
		}
		if describeOutput.StreamDescription == nil {
			return fmt.Errorf("DescribeStream(%q) returned nil StreamDescription", streamARN)
		}
		if len(describeOutput.StreamDescription.Shards) == 0 {
			return fmt.Errorf("DescribeStream(%q) returned no shards", streamARN)
		}

		recordsFound := 0
		for _, shard := range describeOutput.StreamDescription.Shards {
			shardID := aws.ToString(shard.ShardId)
			if shardID == "" {
				return fmt.Errorf("DescribeStream(%q) returned shard with empty ShardId", streamARN)
			}
			if !strings.HasPrefix(shardID, "dlfb") {
				return fmt.Errorf("DescribeStream(%q) returned non-virtual shard ID %q", streamARN, shardID)
			}

			iteratorOutput, err := streamClient.GetShardIterator(runCtx, &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         aws.String(streamARN),
				ShardId:           aws.String(shardID),
				ShardIteratorType: dynamodbstreamstypes.ShardIteratorTypeTrimHorizon,
			})
			if err != nil {
				return fmt.Errorf("GetShardIterator(%q, %q): %w", streamARN, shardID, err)
			}

			shardIterator := aws.ToString(iteratorOutput.ShardIterator)
			if shardIterator == "" {
				return fmt.Errorf("GetShardIterator(%q, %q) returned empty iterator", streamARN, shardID)
			}
			if !strings.HasPrefix(shardIterator, "dlfb") {
				return fmt.Errorf("GetShardIterator(%q, %q) returned non-virtual iterator %q", streamARN, shardID, shardIterator)
			}

			recordsOutput, err := streamClient.GetRecords(runCtx, &dynamodbstreams.GetRecordsInput{
				ShardIterator: iteratorOutput.ShardIterator,
				Limit:         aws.Int32(100),
			})
			if err != nil {
				return fmt.Errorf("GetRecords(%q): %w", shardIterator, err)
			}
			recordsFound += len(recordsOutput.Records)

			nextIterator := aws.ToString(recordsOutput.NextShardIterator)
			if nextIterator != "" && !strings.HasPrefix(nextIterator, "dlfb") {
				return fmt.Errorf("GetRecords(%q) returned non-virtual next iterator %q", shardIterator, nextIterator)
			}
		}
		if recordsFound == 0 {
			return fmt.Errorf("GetRecords found no records yet for %q", streamARN)
		}

		observedStreamARN = streamARN
		observedShards = len(describeOutput.StreamDescription.Shards)
		observedRecords = recordsFound
		return nil
	}); err != nil {
		t.Fatalf("stream operations did not stabilize: %v", err)
	}

	if observedStreamARN == "" {
		t.Fatal("observedStreamARN is empty after successful stream operations")
	}
	if observedShards == 0 {
		t.Fatal("observedShards is zero after successful stream operations")
	}
	if observedRecords == 0 {
		t.Fatal("observedRecords is zero after successful stream operations")
	}
}

func startManagedRegressionHarness(t *testing.T, instances int) (context.Context, *dynamodb.Client) {
	t.Helper()

	serverCtx, serverCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(serverCancel)

	server, err := New(serverCtx, Config{
		ListenAddr:  "127.0.0.1:0",
		MetricsAddr: "127.0.0.1:0",
		Mode:        ModeManaged,
		Instances:   instances,
		DynamoImage: DefaultDynamoImage,
		StateDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := server.Start(serverCtx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer closeCancel()
		if closeErr := server.Close(closeCtx); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})

	return serverCtx, newIntegrationDynamoClient(t, server.Endpoint())
}

func createRegressionTable(t *testing.T, ctx context.Context, client *dynamodb.Client, prefix string) string {
	t.Helper()

	tableName := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	if err := createTestTable(ctx, client, tableName); err != nil {
		t.Fatalf("createTestTable(%q) error = %v", tableName, err)
	}
	if err := waitForTableReadyOnAllBackends(ctx, client, tableName); err != nil {
		t.Fatalf("waitForTableReadyOnAllBackends(%q) error = %v", tableName, err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()

		_, deleteErr := client.DeleteTable(cleanupCtx, &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
		if deleteErr != nil {
			t.Logf("DeleteTable(%q) cleanup error = %v", tableName, deleteErr)
			return
		}
		waiter := dynamodb.NewTableNotExistsWaiter(client)
		if waitErr := waiter.Wait(
			cleanupCtx,
			&dynamodb.DescribeTableInput{TableName: aws.String(tableName)},
			time.Minute,
		); waitErr != nil {
			t.Logf("wait for DeleteTable(%q) cleanup error = %v", tableName, waitErr)
		}
	})

	return tableName
}

func createRegressionStreamTable(t *testing.T, ctx context.Context, client *dynamodb.Client, prefix string) string {
	t.Helper()

	tableName := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("pk"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("pk"),
				KeyType:       types.KeyTypeHash,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewImage,
		},
	})
	if err != nil {
		t.Fatalf("CreateTable(%q) with stream error = %v", tableName, err)
	}

	waiter := dynamodb.NewTableExistsWaiter(client)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 2*time.Minute); err != nil {
		t.Fatalf("wait for CreateTable(%q) error = %v", tableName, err)
	}
	if err := waitForTableReadyOnAllBackends(ctx, client, tableName); err != nil {
		t.Fatalf("waitForTableReadyOnAllBackends(%q) error = %v", tableName, err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()

		_, deleteErr := client.DeleteTable(cleanupCtx, &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
		if deleteErr != nil {
			t.Logf("DeleteTable(%q) cleanup error = %v", tableName, deleteErr)
			return
		}
		waiter := dynamodb.NewTableNotExistsWaiter(client)
		if waitErr := waiter.Wait(
			cleanupCtx,
			&dynamodb.DescribeTableInput{TableName: aws.String(tableName)},
			time.Minute,
		); waitErr != nil {
			t.Logf("wait for DeleteTable(%q) cleanup error = %v", tableName, waitErr)
		}
	})

	return tableName
}

func findStringPartitionKeysByBucket(t *testing.T, instances int) map[uint64]string {
	t.Helper()

	if instances <= 0 {
		t.Fatalf("instances must be > 0")
	}

	found := make(map[uint64]string, instances)
	targetBuckets := make(map[uint64]struct{}, instances)
	for i := 0; i < instances; i++ {
		targetBuckets[uint64(i)] = struct{}{}
	}

	for i := 0; i < 1_000_000; i++ {
		candidate := fmt.Sprintf("pk-%d", i)
		bucket := routeBucketForStringPK(candidate, uint64(instances))
		if _, wanted := targetBuckets[bucket]; !wanted {
			continue
		}
		if _, exists := found[bucket]; exists {
			continue
		}
		found[bucket] = candidate
		if len(found) == instances {
			return found
		}
	}

	t.Fatalf("unable to find partition keys for %d buckets", instances)
	return nil
}

func routeBucketForStringPK(pk string, instances uint64) uint64 {
	raw, _ := json.Marshal(map[string]string{"S": pk})
	return xxhash.Sum64(raw) % instances
}

func collectPKs(items []map[string]types.AttributeValue) []string {
	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, stringAttribute(item, "pk"))
	}
	sort.Strings(keys)
	return keys
}
