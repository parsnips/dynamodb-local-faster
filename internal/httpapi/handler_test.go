package httpapi

import (
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/parsnips/dynamodb-local-faster/internal/backends"
	"github.com/parsnips/dynamodb-local-faster/internal/catalog"
	"github.com/parsnips/dynamodb-local-faster/internal/partiql"
	"github.com/parsnips/dynamodb-local-faster/internal/router"
	"github.com/parsnips/dynamodb-local-faster/internal/streams"
)

func TestHandlerProxiesSingleOperation(t *testing.T) {
	var calls atomic.Int32

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)

		if got := r.Header.Get("X-Amz-Target"); got != "DynamoDB_20120810.GetItem" {
			t.Fatalf("X-Amz-Target = %q, want %q", got, "DynamoDB_20120810.GetItem")
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("io.ReadAll() error = %v", err)
		}
		if got, want := strings.TrimSpace(string(body)), `{"TableName":"users","Key":{"id":{"S":"u-1"}}}`; got != want {
			t.Fatalf("request body = %q, want %q", got, want)
		}

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"Item":{"id":{"S":"u-1"}}}`))
	}))
	defer backend.Close()

	r, err := router.NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: backend.URL},
	})
	if err != nil {
		t.Fatalf("NewStaticRouter() error = %v", err)
	}

	handler := NewHandler(
		r,
		catalog.NewNoopReplicator(),
		streams.NewNoopMux(),
		partiql.NewNoopParser(),
	)

	req := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader(`{"TableName":"users","Key":{"id":{"S":"u-1"}}}`),
	)
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if status := recorder.Code; status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}
	if got, want := strings.TrimSpace(recorder.Body.String()), `{"Item":{"id":{"S":"u-1"}}}`; got != want {
		t.Fatalf("response body = %q, want %q", got, want)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestHandlerBroadcastsCreateTable(t *testing.T) {
	var firstCalls atomic.Int32
	var secondCalls atomic.Int32

	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstCalls.Add(1)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"TableDescription":{"TableName":"users"}}`))
	}))
	defer first.Close()

	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"TableDescription":{"TableName":"users"}}`))
	}))
	defer second.Close()

	r, err := router.NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: first.URL},
		{ID: 1, Endpoint: second.URL},
	})
	if err != nil {
		t.Fatalf("NewStaticRouter() error = %v", err)
	}

	handler := NewHandler(
		r,
		catalog.NewNoopReplicator(),
		streams.NewNoopMux(),
		partiql.NewNoopParser(),
	)

	req := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader(`{"TableName":"users","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}`),
	)
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if status := recorder.Code; status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}
	if got := firstCalls.Load(); got != 1 {
		t.Fatalf("firstCalls = %d, want 1", got)
	}
	if got := secondCalls.Load(); got != 1 {
		t.Fatalf("secondCalls = %d, want 1", got)
	}
}

func TestHandlerMergesListTablesFanout(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"TableNames":["users","orders"]}`))
	}))
	defer first.Close()

	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"TableNames":["orders","inventory"]}`))
	}))
	defer second.Close()

	r, err := router.NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: first.URL},
		{ID: 1, Endpoint: second.URL},
	})
	if err != nil {
		t.Fatalf("NewStaticRouter() error = %v", err)
	}

	handler := NewHandler(
		r,
		catalog.NewNoopReplicator(),
		streams.NewNoopMux(),
		partiql.NewNoopParser(),
	)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if status := recorder.Code; status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}
	if got, want := strings.TrimSpace(recorder.Body.String()), `{"TableNames":["inventory","orders","users"]}`; got != want {
		t.Fatalf("response body = %q, want %q", got, want)
	}
	wantCRC := strconv.FormatUint(uint64(crc32.ChecksumIEEE(recorder.Body.Bytes())), 10)
	if got := recorder.Header().Get("X-Amz-Crc32"); got != wantCRC {
		t.Fatalf("X-Amz-Crc32 = %q, want %q", got, wantCRC)
	}
}

func TestHandlerRoutesQueryByExpressionAttributesToSingleBackend(t *testing.T) {
	var firstCalls atomic.Int32
	var secondCalls atomic.Int32

	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstCalls.Add(1)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"Items":[{"pk":{"S":"u-1"}}],"Count":1,"ScannedCount":1}`))
	}))
	defer first.Close()

	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"Items":[],"Count":0,"ScannedCount":0}`))
	}))
	defer second.Close()

	r, err := router.NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: first.URL},
		{ID: 1, Endpoint: second.URL},
	})
	if err != nil {
		t.Fatalf("NewStaticRouter() error = %v", err)
	}
	r.RememberPartitionKey("users", "pk")

	target, err := r.ResolveItem("users", []byte(`{"S":"u-1"}`))
	if err != nil {
		t.Fatalf("ResolveItem() error = %v", err)
	}

	handler := NewHandler(
		r,
		catalog.NewNoopReplicator(),
		streams.NewNoopMux(),
		partiql.NewNoopParser(),
	)

	req := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader(`{"TableName":"users","KeyConditionExpression":"#pk = :pk","ExpressionAttributeNames":{"#pk":"pk"},"ExpressionAttributeValues":{":pk":{"S":"u-1"}}}`),
	)
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Query")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if status := recorder.Code; status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}

	totalCalls := firstCalls.Load() + secondCalls.Load()
	if totalCalls != 1 {
		t.Fatalf("total backend calls = %d, want 1", totalCalls)
	}
	if target.ID == 0 {
		if got := firstCalls.Load(); got != 1 {
			t.Fatalf("firstCalls = %d, want 1", got)
		}
		if got := secondCalls.Load(); got != 0 {
			t.Fatalf("secondCalls = %d, want 0", got)
		}
	} else {
		if got := firstCalls.Load(); got != 0 {
			t.Fatalf("firstCalls = %d, want 0", got)
		}
		if got := secondCalls.Load(); got != 1 {
			t.Fatalf("secondCalls = %d, want 1", got)
		}
	}
}

func TestHandlerRoutesExecuteStatementWithParametersToSingleBackend(t *testing.T) {
	var firstCalls atomic.Int32
	var secondCalls atomic.Int32

	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstCalls.Add(1)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer first.Close()

	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer second.Close()

	r, err := router.NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: first.URL},
		{ID: 1, Endpoint: second.URL},
	})
	if err != nil {
		t.Fatalf("NewStaticRouter() error = %v", err)
	}
	r.RememberPartitionKey("users", "pk")

	target, err := r.ResolveItem("users", []byte(`{"S":"u-7"}`))
	if err != nil {
		t.Fatalf("ResolveItem() error = %v", err)
	}

	handler := NewHandler(
		r,
		catalog.NewNoopReplicator(),
		streams.NewNoopMux(),
		partiql.NewNoopParser(),
	)

	req := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader(`{"Statement":"INSERT INTO \"users\" VALUE {'pk': ?, 'payload': ?}","Parameters":[{"S":"u-7"},{"S":"payload"}]}`),
	)
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ExecuteStatement")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if status := recorder.Code; status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}

	totalCalls := firstCalls.Load() + secondCalls.Load()
	if totalCalls != 1 {
		t.Fatalf("total backend calls = %d, want 1", totalCalls)
	}
	if target.ID == 0 {
		if got := firstCalls.Load(); got != 1 {
			t.Fatalf("firstCalls = %d, want 1", got)
		}
		if got := secondCalls.Load(); got != 0 {
			t.Fatalf("secondCalls = %d, want 0", got)
		}
	} else {
		if got := firstCalls.Load(); got != 0 {
			t.Fatalf("firstCalls = %d, want 0", got)
		}
		if got := secondCalls.Load(); got != 1 {
			t.Fatalf("secondCalls = %d, want 1", got)
		}
	}
}

func TestHandlerReturnsValidationForInvalidListStreamsPayload(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("backend should not be called for invalid ListStreams payload")
	}))
	defer backend.Close()

	backendsList := []backends.Backend{
		{ID: 0, Endpoint: backend.URL},
	}
	r, err := router.NewStaticRouter(backendsList)
	if err != nil {
		t.Fatalf("NewStaticRouter() error = %v", err)
	}

	handler := NewHandler(
		r,
		catalog.NewNoopReplicator(),
		streams.NewMux(backendsList, streams.MakeProxyFunc(http.DefaultClient)),
		partiql.NewNoopParser(),
	)

	req := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader(`{"Limit":"bad"}`),
	)
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListStreams")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if status := recorder.Code; status != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", status, http.StatusBadRequest)
	}
	if got := recorder.Body.String(); !strings.Contains(got, `"__type":"ValidationException"`) {
		t.Fatalf("response body = %q, want ValidationException", got)
	}
}
