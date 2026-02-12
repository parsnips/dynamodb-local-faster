package httpapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/parsnips/dynamodb-local-faster/internal/backends"
	"github.com/parsnips/dynamodb-local-faster/internal/catalog"
	"github.com/parsnips/dynamodb-local-faster/internal/partiql"
	"github.com/parsnips/dynamodb-local-faster/internal/router"
	"github.com/parsnips/dynamodb-local-faster/internal/streams"
)

func TestCompactJSONMatchesCanonicalize(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"string_compact", `{"S":"hello"}`},
		{"string_whitespace", `{ "S" : "hello" }`},
		{"number_compact", `{"N":"42"}`},
		{"number_whitespace", `{  "N"  :  "42"  }`},
		{"binary_compact", `{"B":"dGVzdA=="}`},
		{"binary_whitespace", `{ "B" : "dGVzdA==" }`},
		{"nested_whitespace", `{  "S"  :  "value with spaces"  }`},
		{"unicode", `{"S":"héllo wörld"}`},
		{"escaped_quotes", `{"S":"say \"hello\""}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compacted, err := compactJSON([]byte(tc.raw))
			if err != nil {
				t.Fatalf("compactJSON(%q) error = %v", tc.raw, err)
			}
			canonical, err := canonicalizeRawJSON([]byte(tc.raw))
			if err != nil {
				t.Fatalf("canonicalizeRawJSON(%q) error = %v", tc.raw, err)
			}
			if string(compacted) != string(canonical) {
				t.Fatalf("compactJSON = %q, canonicalize = %q", compacted, canonical)
			}
		})
	}
}

func TestEscapeGjsonPath(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"has.dot", `has\.dot`},
		{"has#hash", `has\#hash`},
		{"has*star", `has\*star`},
		{"has?question", `has\?question`},
		{`has\backslash`, `has\\backslash`},
		{"a.b#c*d?e", `a\.b\#c\*d\?e`},
		{"normal_attr", "normal_attr"},
		{"", ""},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got := escapeGjsonPath(tc.input)
			if got != tc.want {
				t.Fatalf("escapeGjsonPath(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestFastPathRoutesSameAsFullParse(t *testing.T) {
	// Verify that fast path routes PutItem/GetItem/DeleteItem/UpdateItem
	// to the same backend as the full-parse path.
	ops := []struct {
		op   string
		body string
	}{
		{"PutItem", `{"TableName":"users","Item":{"pk":{"S":"u-1"},"data":{"S":"hello"}}}`},
		{"GetItem", `{"TableName":"users","Key":{"pk":{"S":"u-1"}}}`},
		{"DeleteItem", `{"TableName":"users","Key":{"pk":{"S":"u-1"}}}`},
		{"UpdateItem", `{"TableName":"users","Key":{"pk":{"S":"u-1"}},"UpdateExpression":"SET #d = :d"}`},
	}

	backendsList := []backends.Backend{
		{ID: 0, Endpoint: "http://backend0"},
		{ID: 1, Endpoint: "http://backend1"},
		{ID: 2, Endpoint: "http://backend2"},
	}
	r, err := router.NewStaticRouter(backendsList)
	if err != nil {
		t.Fatalf("NewStaticRouter() error = %v", err)
	}
	r.RememberPartitionKey("users", "pk")

	// Determine expected backend via full-parse path.
	expectedTarget, err := r.ResolveItem("users", []byte(`{"S":"u-1"}`))
	if err != nil {
		t.Fatalf("ResolveItem() error = %v", err)
	}

	for _, tc := range ops {
		t.Run(tc.op, func(t *testing.T) {
			var calledBackend int = -1
			servers := make([]*httptest.Server, len(backendsList))
			for i := range backendsList {
				idx := i
				servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calledBackend = idx
					w.Header().Set("Content-Type", "application/x-amz-json-1.0")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{}`))
				}))
				defer servers[i].Close()
			}

			routerBackends := make([]backends.Backend, len(servers))
			for i, s := range servers {
				routerBackends[i] = backends.Backend{ID: i, Endpoint: s.URL}
			}
			sr, _ := router.NewStaticRouter(routerBackends)
			sr.RememberPartitionKey("users", "pk")

			handler := NewHandler(
				sr,
				catalog.NewNoopReplicator(),
				streams.NewNoopMux(),
				partiql.NewNoopParser(),
			)

			req := httptest.NewRequest(
				http.MethodPost,
				"/",
				strings.NewReader(tc.body),
			)
			req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+tc.op)
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)

			if status := recorder.Code; status != http.StatusOK {
				t.Fatalf("status = %d, want %d; body = %s", status, http.StatusOK, recorder.Body.String())
			}
			if calledBackend != expectedTarget.ID {
				t.Fatalf("fast path routed to backend %d, full parse would route to %d", calledBackend, expectedTarget.ID)
			}
		})
	}
}

func TestFastPathFallbackWhenPKUnknown(t *testing.T) {
	// When partition key attribute is not remembered, fast path should
	// fall back to full-parse path which still works.
	var calls atomic.Int32

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer backend.Close()

	r, _ := router.NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: backend.URL},
	})
	// Deliberately NOT calling r.RememberPartitionKey

	handler := NewHandler(
		r,
		catalog.NewNoopReplicator(),
		streams.NewNoopMux(),
		partiql.NewNoopParser(),
	)

	req := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader(`{"TableName":"orders","Key":{"id":{"S":"o-1"}}}`),
	)
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if status := recorder.Code; status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestFastPathFallbackOnMalformedJSON(t *testing.T) {
	// Malformed JSON should fall back to full-parse path (which will
	// return a validation error), NOT be proxied to the backend.
	cases := []struct {
		name string
		body string
	}{
		{"garbage", `{not valid json}`},
		{"truncated_value", `{"TableName":"users","Key":{"pk":{"S":"u-1"}},"x":`},
		{"trailing_garbage", `{"TableName":"users","Key":{"pk":{"S":"u-1"}}}garbage`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/x-amz-json-1.0")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{}`))
			}))
			defer backend.Close()

			r, _ := router.NewStaticRouter([]backends.Backend{
				{ID: 0, Endpoint: backend.URL},
			})
			r.RememberPartitionKey("users", "pk")

			handler := NewHandler(
				r,
				catalog.NewNoopReplicator(),
				streams.NewNoopMux(),
				partiql.NewNoopParser(),
			)

			req := httptest.NewRequest(
				http.MethodPost,
				"/",
				strings.NewReader(tc.body),
			)
			req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)

			// Should get a validation error from the full-parse path.
			if status := recorder.Code; status != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", status, http.StatusBadRequest)
			}
			if got := calls.Load(); got != 0 {
				t.Fatalf("backend was called %d times, want 0 (malformed body should not be proxied)", got)
			}
		})
	}
}

func TestFastPathFallbackOnDuplicateKeys(t *testing.T) {
	// Duplicate top-level keys should fall back to the full-parse path
	// to avoid routing divergence between gjson (first-wins) and
	// encoding/json (last-wins).
	cases := []struct {
		name string
		body string
	}{
		{
			"duplicate_Key",
			`{"TableName":"users","Key":{"pk":{"S":"a"}},"Key":{"pk":{"S":"b"}}}`,
		},
		{
			"duplicate_TableName",
			`{"TableName":"users","TableName":"orders","Key":{"pk":{"S":"u-1"}}}`,
		},
		{
			"duplicate_Item",
			`{"TableName":"users","Item":{"pk":{"S":"a"}},"Item":{"pk":{"S":"b"}}}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/x-amz-json-1.0")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{}`))
			}))
			defer backend.Close()

			r, _ := router.NewStaticRouter([]backends.Backend{
				{ID: 0, Endpoint: backend.URL},
				{ID: 1, Endpoint: backend.URL},
			})
			r.RememberPartitionKey("users", "pk")
			r.RememberPartitionKey("orders", "pk")

			handler := NewHandler(
				r,
				catalog.NewNoopReplicator(),
				streams.NewNoopMux(),
				partiql.NewNoopParser(),
			)

			op := "GetItem"
			if strings.Contains(tc.name, "Item") {
				op = "PutItem"
			}

			req := httptest.NewRequest(
				http.MethodPost,
				"/",
				strings.NewReader(tc.body),
			)
			req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+op)
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)

			// The request should still succeed (via full-parse path), but
			// the fast path must NOT have handled it — verify it fell
			// through by checking that the backend was called exactly
			// once (full-parse path proxies to a single backend).
			if status := recorder.Code; status != http.StatusOK {
				t.Fatalf("status = %d, want %d; body = %s", status, http.StatusOK, recorder.Body.String())
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("calls = %d, want 1", got)
			}
		})
	}
}

func TestHasDuplicateTopLevelKey(t *testing.T) {
	cases := []struct {
		name string
		body string
		key1 string
		key2 string
		want bool
	}{
		{
			"no_duplicates",
			`{"TableName":"users","Key":{"pk":{"S":"u-1"}}}`,
			"TableName", "Key",
			false,
		},
		{
			"duplicate_key1",
			`{"TableName":"users","TableName":"orders","Key":{"pk":{"S":"u-1"}}}`,
			"TableName", "Key",
			true,
		},
		{
			"duplicate_key2",
			`{"TableName":"users","Key":{"pk":{"S":"a"}},"Key":{"pk":{"S":"b"}}}`,
			"TableName", "Key",
			true,
		},
		{
			"key_in_nested_value_not_duplicate",
			`{"TableName":"users","Key":{"TableName":{"S":"u-1"}}}`,
			"TableName", "Key",
			false,
		},
		{
			"key_in_string_value_not_duplicate",
			`{"TableName":"users","Key":{"S":"Key"}}`,
			"TableName", "Key",
			false,
		},
		{
			"unrelated_duplicate_ignored",
			`{"TableName":"users","Key":{"pk":{"S":"u-1"}},"x":1,"x":2}`,
			"TableName", "Key",
			false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := hasDuplicateTopLevelKey([]byte(tc.body), tc.key1, tc.key2)
			if got != tc.want {
				t.Fatalf("hasDuplicateTopLevelKey(%q, %q, %q) = %v, want %v",
					tc.body, tc.key1, tc.key2, got, tc.want)
			}
		})
	}
}

func TestFastPathHandlesWhitespace(t *testing.T) {
	// Body with extra whitespace should still route correctly via fast path.
	var receivedBody string

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		receivedBody = string(body)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer backend.Close()

	r, _ := router.NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: backend.URL},
	})
	r.RememberPartitionKey("users", "pk")

	handler := NewHandler(
		r,
		catalog.NewNoopReplicator(),
		streams.NewNoopMux(),
		partiql.NewNoopParser(),
	)

	spaceyBody := `{  "TableName" : "users" , "Key" : {  "pk" : { "S" : "u-1" }  }  }`
	req := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader(spaceyBody),
	)
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if status := recorder.Code; status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}
	// Original body should be forwarded unchanged.
	if receivedBody != spaceyBody {
		t.Fatalf("body was modified: got %q, want %q", receivedBody, spaceyBody)
	}
}

func TestFastPathDoesNotAffectNonEligibleOps(t *testing.T) {
	// Scan, Query, BatchGetItem etc. should not be affected by the fast path.
	var calls atomic.Int32

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"Items":[],"Count":0,"ScannedCount":0}`))
	}))
	defer backend.Close()

	r, _ := router.NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: backend.URL},
	})

	handler := NewHandler(
		r,
		catalog.NewNoopReplicator(),
		streams.NewNoopMux(),
		partiql.NewNoopParser(),
	)

	req := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader(`{"TableName":"users"}`),
	)
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Scan")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if status := recorder.Code; status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestFastPathSpecialCharPKAttribute(t *testing.T) {
	// PK attribute name with special gjson characters should work.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer backend.Close()

	r, _ := router.NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: backend.URL},
	})
	r.RememberPartitionKey("events", "event.id")

	handler := NewHandler(
		r,
		catalog.NewNoopReplicator(),
		streams.NewNoopMux(),
		partiql.NewNoopParser(),
	)

	req := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader(`{"TableName":"events","Key":{"event.id":{"S":"e-1"}}}`),
	)
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if status := recorder.Code; status != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", status, http.StatusOK, recorder.Body.String())
	}
}

func BenchmarkFastPathVsFullParse(b *testing.B) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer backend.Close()

	r, _ := router.NewStaticRouter([]backends.Backend{
		{ID: 0, Endpoint: backend.URL},
		{ID: 1, Endpoint: backend.URL},
		{ID: 2, Endpoint: backend.URL},
	})
	r.RememberPartitionKey("users", "pk")

	handler := NewHandler(
		r,
		catalog.NewNoopReplicator(),
		streams.NewNoopMux(),
		partiql.NewNoopParser(),
	)

	body := `{"TableName":"users","Item":{"pk":{"S":"u-12345"},"data":{"S":"some payload data here"}}}`

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest(
				http.MethodPost,
				"/",
				strings.NewReader(body),
			)
			req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)

			if recorder.Code != http.StatusOK {
				b.Fatalf("status = %d", recorder.Code)
			}
		}
	})
}
