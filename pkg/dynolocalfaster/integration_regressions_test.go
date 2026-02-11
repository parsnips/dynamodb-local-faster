//go:build integration

package dynolocalfaster

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

func startManagedRegressionHarness(t *testing.T, instances int) (context.Context, *dynamodb.Client) {
	t.Helper()

	serverCtx, serverCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(serverCancel)

	server, err := New(serverCtx, Config{
		ListenAddr:  "127.0.0.1:0",
		MetricsAddr: "",
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
