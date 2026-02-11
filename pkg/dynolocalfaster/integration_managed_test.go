//go:build integration

package dynolocalfaster

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
)

func TestManagedIntegrationAWSv2Modes(t *testing.T) {
	testCases := []struct {
		name      string
		instances int
	}{
		{name: "single-instance", instances: 1},
		{name: "two-instances", instances: 2},
		{name: "four-instances", instances: 4},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runManagedIntegrationScenario(t, tc.instances)
		})
	}
}

func runManagedIntegrationScenario(t *testing.T, instances int) {
	t.Helper()

	serverCtx, serverCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer serverCancel()

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
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if closeErr := server.Close(closeCtx); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})

	client := newIntegrationDynamoClient(t, server.Endpoint())

	testID := time.Now().UnixNano()
	tableName := fmt.Sprintf("it-table-%d-%d", instances, testID)
	gsiName := fmt.Sprintf("it-gsi-%d-%d", instances, testID)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	if err := createTestTable(ctx, client, tableName); err != nil {
		t.Fatalf("createTestTable() error = %v", err)
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

	if err := waitForTableReadyOnAllBackends(ctx, client, tableName); err != nil {
		t.Fatalf("waitForTableReadyOnAllBackends() error = %v", err)
	}

	if err := createTestGSI(ctx, client, tableName, gsiName); err != nil {
		t.Fatalf("createTestGSI() error = %v", err)
	}
	if err := waitForGSIReadyOnAllBackends(ctx, client, tableName, gsiName); err != nil {
		t.Fatalf("waitForGSIReadyOnAllBackends() error = %v", err)
	}

	if err := assertTableVisible(ctx, client, tableName); err != nil {
		t.Fatalf("assertTableVisible() error = %v", err)
	}

	items := []map[string]types.AttributeValue{
		{
			"pk":      &types.AttributeValueMemberS{Value: "item-1"},
			"gsi_pk":  &types.AttributeValueMemberS{Value: "group-a"},
			"payload": &types.AttributeValueMemberS{Value: "payload-1"},
		},
		{
			"pk":      &types.AttributeValueMemberS{Value: "item-2"},
			"gsi_pk":  &types.AttributeValueMemberS{Value: "group-a"},
			"payload": &types.AttributeValueMemberS{Value: "payload-2"},
		},
		{
			"pk":      &types.AttributeValueMemberS{Value: "item-3"},
			"gsi_pk":  &types.AttributeValueMemberS{Value: "group-b"},
			"payload": &types.AttributeValueMemberS{Value: "payload-3"},
		},
		{
			"pk":      &types.AttributeValueMemberS{Value: "item-4"},
			"gsi_pk":  &types.AttributeValueMemberS{Value: "group-c"},
			"payload": &types.AttributeValueMemberS{Value: "payload-4"},
		},
	}

	for i, item := range items {
		if _, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		}); err != nil {
			t.Fatalf("PutItem(%d) error = %v", i, err)
		}
	}

	getOutput, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item-1"},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("GetItem(item-1) error = %v", err)
	}
	if got := stringAttribute(getOutput.Item, "payload"); got != "payload-1" {
		t.Fatalf("GetItem(item-1).payload = %q, want %q", got, "payload-1")
	}

	updateOutput, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item-1"},
		},
		UpdateExpression: aws.String("SET payload = :value"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":value": &types.AttributeValueMemberS{Value: "payload-1-updated"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		t.Fatalf("UpdateItem(item-1) error = %v", err)
	}
	if got := stringAttribute(updateOutput.Attributes, "payload"); got != "payload-1-updated" {
		t.Fatalf("UpdateItem(item-1).payload = %q, want %q", got, "payload-1-updated")
	}

	queryByPKOutput, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "item-1"},
		},
	})
	if err != nil {
		t.Fatalf("Query(pk=item-1) error = %v", err)
	}
	if got := len(queryByPKOutput.Items); got != 1 {
		t.Fatalf("Query(pk=item-1).len(Items) = %d, want 1", got)
	}

	queryByGSIOutput, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String(gsiName),
		KeyConditionExpression: aws.String("gsi_pk = :gsi"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":gsi": &types.AttributeValueMemberS{Value: "group-a"},
		},
	})
	if err != nil {
		t.Fatalf("Query(gsi_pk=group-a) error = %v", err)
	}
	if got := len(queryByGSIOutput.Items); got != 2 {
		t.Fatalf("Query(gsi_pk=group-a).len(Items) = %d, want 2", got)
	}

	scanOutput, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if got := len(scanOutput.Items); got != 4 {
		t.Fatalf("Scan().len(Items) = %d, want 4", got)
	}

	if _, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item-4"},
		},
	}); err != nil {
		t.Fatalf("DeleteItem(item-4) error = %v", err)
	}

	postDeleteGet, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item-4"},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("GetItem(item-4 after delete) error = %v", err)
	}
	if len(postDeleteGet.Item) != 0 {
		t.Fatalf("GetItem(item-4 after delete).Item = %#v, want empty", postDeleteGet.Item)
	}

	postDeleteScan, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		t.Fatalf("Scan() after delete error = %v", err)
	}
	if got := len(postDeleteScan.Items); got != 3 {
		t.Fatalf("Scan() after delete len(Items) = %d, want 3", got)
	}
}

func newIntegrationDynamoClient(t *testing.T, endpoint string) *dynamodb.Client {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion("us-west-2"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig() error = %v", err)
	}

	return dynamodb.NewFromConfig(cfg, func(options *dynamodb.Options) {
		options.BaseEndpoint = aws.String(endpoint)
	})
}

func newIntegrationStreamsClient(t *testing.T, endpoint string) *dynamodbstreams.Client {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion("us-west-2"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig() error = %v", err)
	}

	return dynamodbstreams.NewFromConfig(cfg, func(options *dynamodbstreams.Options) {
		options.BaseEndpoint = aws.String(endpoint)
	})
}

func createTestTable(ctx context.Context, client *dynamodb.Client, tableName string) error {
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
	})
	if err != nil {
		return err
	}

	waiter := dynamodb.NewTableExistsWaiter(client)
	return waiter.Wait(
		ctx,
		&dynamodb.DescribeTableInput{TableName: aws.String(tableName)},
		2*time.Minute,
	)
}

func createTestGSI(ctx context.Context, client *dynamodb.Client, tableName string, gsiName string) error {
	_, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("gsi_pk"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{
				Create: &types.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String(gsiName),
					KeySchema: []types.KeySchemaElement{
						{
							AttributeName: aws.String("gsi_pk"),
							KeyType:       types.KeyTypeHash,
						},
					},
					Projection: &types.Projection{
						ProjectionType: types.ProjectionTypeAll,
					},
				},
			},
		},
	})
	return err
}

func assertTableVisible(ctx context.Context, client *dynamodb.Client, tableName string) error {
	output, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		return err
	}
	if !slices.Contains(output.TableNames, tableName) {
		return fmt.Errorf("ListTables() missing table %q in %v", tableName, output.TableNames)
	}
	return nil
}

func waitForTableReadyOnAllBackends(ctx context.Context, client *dynamodb.Client, tableName string) error {
	return waitForSuccess(ctx, 300*time.Millisecond, func(runCtx context.Context) error {
		_, err := client.Scan(runCtx, &dynamodb.ScanInput{
			TableName: aws.String(tableName),
			Limit:     aws.Int32(1),
		})
		return err
	})
}

func waitForGSIReadyOnAllBackends(ctx context.Context, client *dynamodb.Client, tableName string, gsiName string) error {
	return waitForSuccess(ctx, 300*time.Millisecond, func(runCtx context.Context) error {
		_, err := client.Query(runCtx, &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			IndexName:              aws.String(gsiName),
			KeyConditionExpression: aws.String("gsi_pk = :value"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":value": &types.AttributeValueMemberS{Value: "bootstrap"},
			},
			Limit: aws.Int32(1),
		})
		return err
	})
}

func waitForSuccess(ctx context.Context, interval time.Duration, fn func(context.Context) error) error {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}

	var lastErr error
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if lastErr == nil {
				return ctx.Err()
			}
			return fmt.Errorf("timed out waiting for readiness: %w (last error: %v)", ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
}

func stringAttribute(item map[string]types.AttributeValue, key string) string {
	if item == nil {
		return ""
	}
	attribute, ok := item[key]
	if !ok {
		return ""
	}
	value, ok := attribute.(*types.AttributeValueMemberS)
	if !ok {
		return ""
	}
	return value.Value
}
