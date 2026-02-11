package streams

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/parsnips/dynamodb-local-faster/internal/backends"
)

func makeTestBackends() []backends.Backend {
	return []backends.Backend{
		{ID: 0, Endpoint: "http://backend0:8000"},
		{ID: 1, Endpoint: "http://backend1:8000"},
	}
}

func TestMuxListStreamsDeduplicates(t *testing.T) {
	bs := makeTestBackends()

	proxy := func(ctx context.Context, backend backends.Backend, target string, body []byte) (int, []byte, error) {
		switch backend.ID {
		case 0:
			return 200, []byte(`{"Streams":[
				{"StreamArn":"arn:aws:dynamodb:ddblocal:000000000000:table/users/stream/2024-01-01","TableName":"users","StreamLabel":"2024-01-01"},
				{"StreamArn":"arn:aws:dynamodb:ddblocal:000000000000:table/orders/stream/2024-01-01","TableName":"orders","StreamLabel":"2024-01-01"}
			]}`), nil
		case 1:
			return 200, []byte(`{"Streams":[
				{"StreamArn":"arn:aws:dynamodb:ddblocal:000000000001:table/users/stream/2024-01-01","TableName":"users","StreamLabel":"2024-01-01"},
				{"StreamArn":"arn:aws:dynamodb:ddblocal:000000000001:table/orders/stream/2024-01-01","TableName":"orders","StreamLabel":"2024-01-01"}
			]}`), nil
		default:
			return 0, nil, fmt.Errorf("unexpected backend %d", backend.ID)
		}
	}

	mux := NewMux(bs, proxy)
	sc, body, err := mux.ListStreams(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("ListStreams error = %v", err)
	}
	if sc != 200 {
		t.Fatalf("status = %d, want 200", sc)
	}

	var resp struct {
		Streams []struct {
			StreamArn string `json:"StreamArn"`
			TableName string `json:"TableName"`
		} `json:"Streams"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal error = %v", err)
	}

	if len(resp.Streams) != 2 {
		t.Fatalf("got %d streams, want 2", len(resp.Streams))
	}

	// Should use first backend's ARN as canonical.
	for _, s := range resp.Streams {
		if !strings.Contains(s.StreamArn, "000000000000") {
			t.Errorf("stream ARN %q should use backend 0's ARN", s.StreamArn)
		}
	}
}

func TestMuxDescribeStreamMergesShards(t *testing.T) {
	bs := makeTestBackends()
	canonicalARN := "arn:aws:dynamodb:ddblocal:000000000000:table/users/stream/2024-01-01"

	proxy := func(ctx context.Context, backend backends.Backend, target string, body []byte) (int, []byte, error) {
		if strings.Contains(target, "ListStreams") {
			switch backend.ID {
			case 0:
				return 200, []byte(fmt.Sprintf(`{"Streams":[{"StreamArn":"%s","TableName":"users"}]}`, canonicalARN)), nil
			case 1:
				return 200, []byte(`{"Streams":[{"StreamArn":"arn:aws:dynamodb:ddblocal:000000000001:table/users/stream/2024-01-01","TableName":"users"}]}`), nil
			}
		}
		if strings.Contains(target, "DescribeStream") {
			switch backend.ID {
			case 0:
				return 200, []byte(fmt.Sprintf(`{"StreamDescription":{
					"StreamArn":"%s",
					"StreamStatus":"ENABLED",
					"StreamViewType":"NEW_AND_OLD_IMAGES",
					"TableName":"users",
					"Shards":[
						{"ShardId":"shardId-000000000001","SequenceNumberRange":{"StartingSequenceNumber":"100"}},
						{"ShardId":"shardId-000000000002","ParentShardId":"shardId-000000000001","SequenceNumberRange":{"StartingSequenceNumber":"200"}}
					]
				}}`, canonicalARN)), nil
			case 1:
				return 200, []byte(`{"StreamDescription":{
					"StreamArn":"arn:aws:dynamodb:ddblocal:000000000001:table/users/stream/2024-01-01",
					"StreamStatus":"ENABLED",
					"StreamViewType":"NEW_AND_OLD_IMAGES",
					"TableName":"users",
					"Shards":[
						{"ShardId":"shardId-000000000001","SequenceNumberRange":{"StartingSequenceNumber":"300"}}
					]
				}}`), nil
			}
		}
		return 0, nil, fmt.Errorf("unexpected call: backend=%d target=%s", backend.ID, target)
	}

	mux := NewMux(bs, proxy)

	// Populate cache.
	_, _, err := mux.ListStreams(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("ListStreams error = %v", err)
	}

	sc, body, err := mux.DescribeStream(context.Background(), []byte(fmt.Sprintf(`{"StreamArn":"%s"}`, canonicalARN)))
	if err != nil {
		t.Fatalf("DescribeStream error = %v", err)
	}
	if sc != 200 {
		t.Fatalf("status = %d, want 200", sc)
	}

	var resp struct {
		StreamDescription struct {
			StreamArn string `json:"StreamArn"`
			TableName string `json:"TableName"`
			Shards    []struct {
				ShardId       string `json:"ShardId"`
				ParentShardId string `json:"ParentShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal error = %v", err)
	}

	if resp.StreamDescription.StreamArn != canonicalARN {
		t.Errorf("StreamArn = %q, want %q", resp.StreamDescription.StreamArn, canonicalARN)
	}

	// Should have 3 shards total (2 from backend 0, 1 from backend 1).
	if len(resp.StreamDescription.Shards) != 3 {
		t.Fatalf("got %d shards, want 3", len(resp.StreamDescription.Shards))
	}

	// Verify all shard IDs are virtual.
	for _, shard := range resp.StreamDescription.Shards {
		if !strings.HasPrefix(shard.ShardId, "dlfb") {
			t.Errorf("shard ID %q should have virtual prefix", shard.ShardId)
		}
		if shard.ParentShardId != "" && !strings.HasPrefix(shard.ParentShardId, "dlfb") {
			t.Errorf("parent shard ID %q should have virtual prefix", shard.ParentShardId)
		}
	}

	// Check that backend 0's shards have dlfb0: prefix and backend 1's have dlfb1:
	b0Count, b1Count := 0, 0
	for _, shard := range resp.StreamDescription.Shards {
		if strings.HasPrefix(shard.ShardId, "dlfb0:") {
			b0Count++
		} else if strings.HasPrefix(shard.ShardId, "dlfb1:") {
			b1Count++
		}
	}
	if b0Count != 2 {
		t.Errorf("backend 0 shards = %d, want 2", b0Count)
	}
	if b1Count != 1 {
		t.Errorf("backend 1 shards = %d, want 1", b1Count)
	}
}

func TestMuxGetShardIteratorRoutesToCorrectBackend(t *testing.T) {
	bs := makeTestBackends()
	canonicalARN := "arn:aws:dynamodb:ddblocal:000000000000:table/users/stream/2024-01-01"

	var proxiedBackendID int

	proxy := func(ctx context.Context, backend backends.Backend, target string, body []byte) (int, []byte, error) {
		if strings.Contains(target, "ListStreams") {
			switch backend.ID {
			case 0:
				return 200, []byte(fmt.Sprintf(`{"Streams":[{"StreamArn":"%s","TableName":"users"}]}`, canonicalARN)), nil
			case 1:
				return 200, []byte(`{"Streams":[{"StreamArn":"arn:aws:dynamodb:ddblocal:000000000001:table/users/stream/2024-01-01","TableName":"users"}]}`), nil
			}
		}
		if strings.Contains(target, "GetShardIterator") {
			proxiedBackendID = backend.ID
			// Verify the real shard ID was sent.
			var req struct {
				ShardId string `json:"ShardId"`
			}
			json.Unmarshal(body, &req)
			if req.ShardId != "shardId-000000000001" {
				t.Errorf("backend received ShardId = %q, want %q", req.ShardId, "shardId-000000000001")
			}
			return 200, []byte(`{"ShardIterator":"real-iterator-token-abc"}`), nil
		}
		return 0, nil, fmt.Errorf("unexpected: backend=%d target=%s", backend.ID, target)
	}

	mux := NewMux(bs, proxy)

	// Populate cache.
	_, _, _ = mux.ListStreams(context.Background(), []byte(`{}`))

	// Request with virtual shard ID pointing to backend 1.
	virtualShardID := EncodeVirtualToken(1, "shardId-000000000001")
	reqBody, _ := json.Marshal(map[string]string{
		"StreamArn":         canonicalARN,
		"ShardId":           virtualShardID,
		"ShardIteratorType": "TRIM_HORIZON",
	})

	sc, body, err := mux.GetShardIterator(context.Background(), reqBody)
	if err != nil {
		t.Fatalf("GetShardIterator error = %v", err)
	}
	if sc != 200 {
		t.Fatalf("status = %d, want 200", sc)
	}

	if proxiedBackendID != 1 {
		t.Errorf("proxied to backend %d, want 1", proxiedBackendID)
	}

	// Verify the returned iterator is virtual.
	var resp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal error = %v", err)
	}

	decodedID, decodedToken, err := DecodeVirtualToken(resp.ShardIterator)
	if err != nil {
		t.Fatalf("DecodeVirtualToken error = %v", err)
	}
	if decodedID != 1 {
		t.Errorf("iterator backend ID = %d, want 1", decodedID)
	}
	if decodedToken != "real-iterator-token-abc" {
		t.Errorf("iterator real token = %q, want %q", decodedToken, "real-iterator-token-abc")
	}
}

func TestMuxGetRecordsRoutesAndEncodesNextIterator(t *testing.T) {
	bs := makeTestBackends()

	proxy := func(ctx context.Context, backend backends.Backend, target string, body []byte) (int, []byte, error) {
		if backend.ID != 0 {
			return 0, nil, fmt.Errorf("should only proxy to backend 0")
		}
		var req struct {
			ShardIterator string `json:"ShardIterator"`
		}
		json.Unmarshal(body, &req)
		if req.ShardIterator != "real-iterator-abc" {
			t.Errorf("backend received ShardIterator = %q, want %q", req.ShardIterator, "real-iterator-abc")
		}
		return 200, []byte(`{"Records":[{"eventID":"e1"}],"NextShardIterator":"real-next-iterator-def"}`), nil
	}

	mux := NewMux(bs, proxy)

	virtualIterator := EncodeVirtualToken(0, "real-iterator-abc")
	reqBody, _ := json.Marshal(map[string]string{
		"ShardIterator": virtualIterator,
	})

	sc, body, err := mux.GetRecords(context.Background(), reqBody)
	if err != nil {
		t.Fatalf("GetRecords error = %v", err)
	}
	if sc != 200 {
		t.Fatalf("status = %d, want 200", sc)
	}

	var resp struct {
		Records           []map[string]any `json:"Records"`
		NextShardIterator string           `json:"NextShardIterator"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal error = %v", err)
	}

	if len(resp.Records) != 1 {
		t.Errorf("got %d records, want 1", len(resp.Records))
	}

	decodedID, decodedToken, err := DecodeVirtualToken(resp.NextShardIterator)
	if err != nil {
		t.Fatalf("DecodeVirtualToken error = %v", err)
	}
	if decodedID != 0 {
		t.Errorf("next iterator backend ID = %d, want 0", decodedID)
	}
	if decodedToken != "real-next-iterator-def" {
		t.Errorf("next iterator real token = %q, want %q", decodedToken, "real-next-iterator-def")
	}
}

func TestMuxGetRecordsWithNullNextShardIterator(t *testing.T) {
	bs := makeTestBackends()

	proxy := func(ctx context.Context, backend backends.Backend, target string, body []byte) (int, []byte, error) {
		return 200, []byte(`{"Records":[{"eventID":"e1"}],"NextShardIterator":null}`), nil
	}

	mux := NewMux(bs, proxy)

	virtualIterator := EncodeVirtualToken(0, "real-iterator-abc")
	reqBody, _ := json.Marshal(map[string]string{
		"ShardIterator": virtualIterator,
	})

	sc, body, err := mux.GetRecords(context.Background(), reqBody)
	if err != nil {
		t.Fatalf("GetRecords error = %v", err)
	}
	if sc != 200 {
		t.Fatalf("status = %d, want 200", sc)
	}

	var resp struct {
		NextShardIterator *string `json:"NextShardIterator"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal error = %v", err)
	}

	if resp.NextShardIterator != nil {
		t.Errorf("NextShardIterator = %q, want nil", *resp.NextShardIterator)
	}
}

func TestMuxCacheMissTriggersLazyRefresh(t *testing.T) {
	bs := makeTestBackends()
	canonicalARN := "arn:aws:dynamodb:ddblocal:000000000000:table/users/stream/2024-01-01"
	var listCallCount atomic.Int32

	proxy := func(ctx context.Context, backend backends.Backend, target string, body []byte) (int, []byte, error) {
		if strings.Contains(target, "ListStreams") {
			listCallCount.Add(1)
			switch backend.ID {
			case 0:
				return 200, []byte(fmt.Sprintf(`{"Streams":[{"StreamArn":"%s","TableName":"users"}]}`, canonicalARN)), nil
			case 1:
				return 200, []byte(`{"Streams":[{"StreamArn":"arn:aws:dynamodb:ddblocal:000000000001:table/users/stream/2024-01-01","TableName":"users"}]}`), nil
			}
		}
		if strings.Contains(target, "DescribeStream") {
			return 200, []byte(`{"StreamDescription":{
				"StreamArn":"whatever",
				"StreamStatus":"ENABLED",
				"Shards":[]
			}}`), nil
		}
		return 0, nil, fmt.Errorf("unexpected: backend=%d target=%s", backend.ID, target)
	}

	mux := NewMux(bs, proxy)

	// DescribeStream without prior ListStreams should trigger a lazy refresh.
	sc, _, err := mux.DescribeStream(context.Background(), []byte(fmt.Sprintf(`{"StreamArn":"%s"}`, canonicalARN)))
	if err != nil {
		t.Fatalf("DescribeStream error = %v", err)
	}
	if sc != 200 {
		t.Fatalf("status = %d, want 200", sc)
	}

	// ListStreams should have been called (2 backends = 2 proxy calls for ListStreams).
	if got := listCallCount.Load(); got < 2 {
		t.Errorf("ListStreams proxy calls = %d, want >= 2", got)
	}
}

func TestMuxListStreamsBackendPagination(t *testing.T) {
	bs := makeTestBackends()
	var backend0Calls atomic.Int32

	proxy := func(ctx context.Context, backend backends.Backend, target string, body []byte) (int, []byte, error) {
		if !strings.Contains(target, "ListStreams") {
			return 0, nil, fmt.Errorf("unexpected target %s", target)
		}

		var req map[string]any
		if err := json.Unmarshal(body, &req); err != nil {
			return 0, nil, fmt.Errorf("unmarshal request: %w", err)
		}

		switch backend.ID {
		case 0:
			call := backend0Calls.Add(1)
			if call == 1 {
				if _, ok := req["ExclusiveStartStreamArn"]; ok {
					t.Fatalf("first backend0 page unexpectedly had ExclusiveStartStreamArn")
				}
				return 200, []byte(`{"Streams":[{"StreamArn":"arn:aws:dynamodb:ddblocal:000000000000:table/users/stream/2024-01-01","TableName":"users"}],"LastEvaluatedStreamArn":"cursor-1"}`), nil
			}
			if call == 2 {
				if got, _ := req["ExclusiveStartStreamArn"].(string); got != "cursor-1" {
					t.Fatalf("second backend0 page ExclusiveStartStreamArn = %q, want %q", got, "cursor-1")
				}
				return 200, []byte(`{"Streams":[{"StreamArn":"arn:aws:dynamodb:ddblocal:000000000000:table/orders/stream/2024-01-01","TableName":"orders"}]}`), nil
			}
			return 0, nil, fmt.Errorf("backend0 called %d times, want 2", call)
		case 1:
			return 200, []byte(`{"Streams":[{"StreamArn":"arn:aws:dynamodb:ddblocal:000000000001:table/inventory/stream/2024-01-01","TableName":"inventory"}]}`), nil
		default:
			return 0, nil, fmt.Errorf("unexpected backend %d", backend.ID)
		}
	}

	mux := NewMux(bs, proxy)
	sc, body, err := mux.ListStreams(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("ListStreams error = %v", err)
	}
	if sc != 200 {
		t.Fatalf("status = %d, want 200", sc)
	}
	if got := backend0Calls.Load(); got != 2 {
		t.Fatalf("backend0 calls = %d, want 2", got)
	}

	var resp struct {
		Streams []struct {
			TableName string `json:"TableName"`
		} `json:"Streams"`
		LastEvaluatedStreamArn string `json:"LastEvaluatedStreamArn"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if len(resp.Streams) != 3 {
		t.Fatalf("got %d streams, want 3", len(resp.Streams))
	}
	if resp.LastEvaluatedStreamArn != "" {
		t.Fatalf("LastEvaluatedStreamArn = %q, want empty", resp.LastEvaluatedStreamArn)
	}
}

func TestMuxDescribeStreamBackendPagination(t *testing.T) {
	bs := makeTestBackends()
	canonicalARN := "arn:aws:dynamodb:ddblocal:000000000000:table/users/stream/2024-01-01"
	var backend0DescribeCalls atomic.Int32

	proxy := func(ctx context.Context, backend backends.Backend, target string, body []byte) (int, []byte, error) {
		if strings.Contains(target, "ListStreams") {
			switch backend.ID {
			case 0:
				return 200, []byte(fmt.Sprintf(`{"Streams":[{"StreamArn":"%s","TableName":"users"}]}`, canonicalARN)), nil
			case 1:
				return 200, []byte(`{"Streams":[{"StreamArn":"arn:aws:dynamodb:ddblocal:000000000001:table/users/stream/2024-01-01","TableName":"users"}]}`), nil
			}
		}

		if strings.Contains(target, "DescribeStream") {
			var req map[string]any
			if err := json.Unmarshal(body, &req); err != nil {
				return 0, nil, fmt.Errorf("unmarshal request: %w", err)
			}

			switch backend.ID {
			case 0:
				call := backend0DescribeCalls.Add(1)
				if call == 1 {
					if _, ok := req["ExclusiveStartShardId"]; ok {
						t.Fatalf("first backend0 page unexpectedly had ExclusiveStartShardId")
					}
					return 200, []byte(fmt.Sprintf(`{"StreamDescription":{"StreamArn":"%s","StreamStatus":"ENABLED","TableName":"users","Shards":[{"ShardId":"shardId-000000000001","SequenceNumberRange":{"StartingSequenceNumber":"100"}}],"LastEvaluatedShardId":"cursor-shard-1"}}`, canonicalARN)), nil
				}
				if call == 2 {
					if got, _ := req["ExclusiveStartShardId"].(string); got != "cursor-shard-1" {
						t.Fatalf("second backend0 page ExclusiveStartShardId = %q, want %q", got, "cursor-shard-1")
					}
					return 200, []byte(fmt.Sprintf(`{"StreamDescription":{"StreamArn":"%s","StreamStatus":"ENABLED","TableName":"users","Shards":[{"ShardId":"shardId-000000000002","SequenceNumberRange":{"StartingSequenceNumber":"200"}}]}}`, canonicalARN)), nil
				}
				return 0, nil, fmt.Errorf("backend0 called %d times, want 2", call)
			case 1:
				if _, ok := req["ExclusiveStartShardId"]; ok {
					t.Fatalf("backend1 should have single page")
				}
				return 200, []byte(`{"StreamDescription":{"StreamArn":"arn:aws:dynamodb:ddblocal:000000000001:table/users/stream/2024-01-01","StreamStatus":"ENABLED","TableName":"users","Shards":[{"ShardId":"shardId-000000000003","SequenceNumberRange":{"StartingSequenceNumber":"300"}}]}}`), nil
			}
		}
		return 0, nil, fmt.Errorf("unexpected: backend=%d target=%s", backend.ID, target)
	}

	mux := NewMux(bs, proxy)
	_, _, _ = mux.ListStreams(context.Background(), []byte(`{}`))

	sc, body, err := mux.DescribeStream(context.Background(), []byte(fmt.Sprintf(`{"StreamArn":"%s"}`, canonicalARN)))
	if err != nil {
		t.Fatalf("DescribeStream error = %v", err)
	}
	if sc != 200 {
		t.Fatalf("status = %d, want 200", sc)
	}
	if got := backend0DescribeCalls.Load(); got != 2 {
		t.Fatalf("backend0 describe calls = %d, want 2", got)
	}

	var resp struct {
		StreamDescription struct {
			Shards               []map[string]any `json:"Shards"`
			LastEvaluatedShardId string           `json:"LastEvaluatedShardId"`
		} `json:"StreamDescription"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if len(resp.StreamDescription.Shards) != 3 {
		t.Fatalf("got %d shards, want 3", len(resp.StreamDescription.Shards))
	}
	if resp.StreamDescription.LastEvaluatedShardId != "" {
		t.Fatalf("LastEvaluatedShardId = %q, want empty", resp.StreamDescription.LastEvaluatedShardId)
	}
}

func TestMuxListStreamsInvalidRequestPayload(t *testing.T) {
	mux := NewMux(makeTestBackends(), func(ctx context.Context, backend backends.Backend, target string, body []byte) (int, []byte, error) {
		return 200, []byte(`{"Streams":[]}`), nil
	})

	_, _, err := mux.ListStreams(context.Background(), []byte(`{"Limit":"bad"}`))
	if err == nil {
		t.Fatal("ListStreams expected error for invalid payload")
	}
	streamErr, ok := err.(*StreamError)
	if !ok {
		t.Fatalf("error type = %T, want *StreamError", err)
	}
	if streamErr.StatusCode != 400 {
		t.Fatalf("StatusCode = %d, want 400", streamErr.StatusCode)
	}
	if streamErr.ErrorType != "ValidationException" {
		t.Fatalf("ErrorType = %q, want %q", streamErr.ErrorType, "ValidationException")
	}
}

func TestMuxDescribeStreamPagination(t *testing.T) {
	bs := makeTestBackends()
	canonicalARN := "arn:aws:dynamodb:ddblocal:000000000000:table/users/stream/2024-01-01"

	proxy := func(ctx context.Context, backend backends.Backend, target string, body []byte) (int, []byte, error) {
		if strings.Contains(target, "ListStreams") {
			switch backend.ID {
			case 0:
				return 200, []byte(fmt.Sprintf(`{"Streams":[{"StreamArn":"%s","TableName":"users"}]}`, canonicalARN)), nil
			case 1:
				return 200, []byte(`{"Streams":[{"StreamArn":"arn:aws:dynamodb:ddblocal:000000000001:table/users/stream/2024-01-01","TableName":"users"}]}`), nil
			}
		}
		if strings.Contains(target, "DescribeStream") {
			// Verify backend requests don't contain Limit or ExclusiveStartShardId.
			var req map[string]any
			json.Unmarshal(body, &req)
			if _, ok := req["Limit"]; ok {
				t.Errorf("backend request should not contain Limit")
			}
			if _, ok := req["ExclusiveStartShardId"]; ok {
				t.Errorf("backend request should not contain ExclusiveStartShardId")
			}

			switch backend.ID {
			case 0:
				return 200, []byte(fmt.Sprintf(`{"StreamDescription":{
					"StreamArn":"%s",
					"StreamStatus":"ENABLED",
					"TableName":"users",
					"Shards":[
						{"ShardId":"shardId-000000000001","SequenceNumberRange":{"StartingSequenceNumber":"100"}},
						{"ShardId":"shardId-000000000002","SequenceNumberRange":{"StartingSequenceNumber":"200"}}
					]
				}}`, canonicalARN)), nil
			case 1:
				return 200, []byte(`{"StreamDescription":{
					"StreamArn":"arn:aws:dynamodb:ddblocal:000000000001:table/users/stream/2024-01-01",
					"StreamStatus":"ENABLED",
					"TableName":"users",
					"Shards":[
						{"ShardId":"shardId-000000000001","SequenceNumberRange":{"StartingSequenceNumber":"300"}}
					]
				}}`), nil
			}
		}
		return 0, nil, fmt.Errorf("unexpected: backend=%d target=%s", backend.ID, target)
	}

	mux := NewMux(bs, proxy)
	_, _, _ = mux.ListStreams(context.Background(), []byte(`{}`))

	type shardInfo struct {
		ShardId string `json:"ShardId"`
	}
	type descResp struct {
		StreamDescription struct {
			Shards               []shardInfo `json:"Shards"`
			LastEvaluatedShardId string      `json:"LastEvaluatedShardId"`
		} `json:"StreamDescription"`
	}

	// Test Limit=2 on 3 total shards: should return 2 with LastEvaluatedShardId set.
	reqBody, _ := json.Marshal(map[string]any{
		"StreamArn": canonicalARN,
		"Limit":     2,
	})
	sc, body, err := mux.DescribeStream(context.Background(), reqBody)
	if err != nil {
		t.Fatalf("DescribeStream(Limit=2) error = %v", err)
	}
	if sc != 200 {
		t.Fatalf("status = %d, want 200", sc)
	}
	var resp1 descResp
	json.Unmarshal(body, &resp1)
	if len(resp1.StreamDescription.Shards) != 2 {
		t.Fatalf("Limit=2: got %d shards, want 2", len(resp1.StreamDescription.Shards))
	}
	if resp1.StreamDescription.LastEvaluatedShardId == "" {
		t.Fatal("Limit=2: LastEvaluatedShardId should be set")
	}
	lastShardID := resp1.StreamDescription.LastEvaluatedShardId

	// Test ExclusiveStartShardId=lastShardID: should return the remaining shard(s).
	reqBody, _ = json.Marshal(map[string]any{
		"StreamArn":             canonicalARN,
		"ExclusiveStartShardId": lastShardID,
	})
	sc, body, err = mux.DescribeStream(context.Background(), reqBody)
	if err != nil {
		t.Fatalf("DescribeStream(ExclusiveStartShardId) error = %v", err)
	}
	if sc != 200 {
		t.Fatalf("status = %d, want 200", sc)
	}
	var resp2 descResp
	json.Unmarshal(body, &resp2)
	if len(resp2.StreamDescription.Shards) != 1 {
		t.Fatalf("after ExclusiveStartShardId: got %d shards, want 1", len(resp2.StreamDescription.Shards))
	}
	if resp2.StreamDescription.LastEvaluatedShardId != "" {
		t.Errorf("after ExclusiveStartShardId: LastEvaluatedShardId should be empty, got %q", resp2.StreamDescription.LastEvaluatedShardId)
	}

	// Verify we got all 3 unique shards across both pages.
	allShardIDs := make(map[string]bool)
	for _, s := range resp1.StreamDescription.Shards {
		allShardIDs[s.ShardId] = true
	}
	for _, s := range resp2.StreamDescription.Shards {
		allShardIDs[s.ShardId] = true
	}
	if len(allShardIDs) != 3 {
		t.Errorf("total unique shards across pages = %d, want 3", len(allShardIDs))
	}
}

func TestNoopMux(t *testing.T) {
	mux := NewNoopMux()

	sc, body, err := mux.ListStreams(context.Background(), nil)
	if err != nil || sc != 200 || !strings.Contains(string(body), "Streams") {
		t.Errorf("NoopMux.ListStreams unexpected result")
	}

	sc, body, err = mux.DescribeStream(context.Background(), nil)
	if err != nil || sc != 200 {
		t.Errorf("NoopMux.DescribeStream unexpected result")
	}

	sc, body, err = mux.GetShardIterator(context.Background(), nil)
	if err != nil || sc != 200 {
		t.Errorf("NoopMux.GetShardIterator unexpected result")
	}

	sc, body, err = mux.GetRecords(context.Background(), nil)
	if err != nil || sc != 200 {
		t.Errorf("NoopMux.GetRecords unexpected result")
	}
	_ = body
}

func TestMakeProxyFuncAddsAuthHeaders(t *testing.T) {
	var (
		gotAuth    string
		gotAmzDate string
		gotTarget  string
		gotBody    string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotAmzDate = r.Header.Get("X-Amz-Date")
		gotTarget = r.Header.Get("X-Amz-Target")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("io.ReadAll() error = %v", err)
		}
		gotBody = string(body)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	proxy := MakeProxyFunc(http.DefaultClient)
	statusCode, responseBody, err := proxy(
		context.Background(),
		backends.Backend{ID: 7, Endpoint: server.URL},
		"DynamoDBStreams_20120810.ListStreams",
		[]byte(`{"TableName":"users"}`),
	)
	if err != nil {
		t.Fatalf("proxy error = %v", err)
	}
	if statusCode != http.StatusOK {
		t.Fatalf("statusCode = %d, want %d", statusCode, http.StatusOK)
	}
	if string(responseBody) != `{"ok":true}` {
		t.Fatalf("responseBody = %q, want %q", string(responseBody), `{"ok":true}`)
	}

	if gotTarget != "DynamoDBStreams_20120810.ListStreams" {
		t.Fatalf("X-Amz-Target = %q, want %q", gotTarget, "DynamoDBStreams_20120810.ListStreams")
	}
	if gotAuth == "" {
		t.Fatal("Authorization header was empty")
	}
	if gotAmzDate == "" {
		t.Fatal("X-Amz-Date header was empty")
	}
	if gotBody != `{"TableName":"users"}` {
		t.Fatalf("request body = %q, want %q", gotBody, `{"TableName":"users"}`)
	}
}
