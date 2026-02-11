package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/parsnips/dynamodb-local-faster/internal/catalog"
	"github.com/parsnips/dynamodb-local-faster/internal/partiql"
	"github.com/parsnips/dynamodb-local-faster/internal/router"
	"github.com/parsnips/dynamodb-local-faster/internal/streams"
)

var supportedOperations = map[string]struct{}{
	"BatchGetItem":     {},
	"BatchWriteItem":   {},
	"CreateTable":      {},
	"DeleteItem":       {},
	"DeleteTable":      {},
	"DescribeStream":   {},
	"DescribeTable":    {},
	"ExecuteStatement": {},
	"GetItem":          {},
	"GetRecords":       {},
	"GetShardIterator": {},
	"ListStreams":      {},
	"ListTables":       {},
	"PutItem":          {},
	"Query":            {},
	"Scan":             {},
	"UpdateItem":       {},
	"UpdateTable":      {},
}

type Handler struct {
	router  router.BackendRouter
	catalog catalog.CatalogReplicator
	streams streams.StreamMux
	parser  partiql.Parser
}

func NewHandler(
	r router.BackendRouter,
	c catalog.CatalogReplicator,
	s streams.StreamMux,
	p partiql.Parser,
) *Handler {
	return &Handler{
		router:  r,
		catalog: c,
		streams: s,
		parser:  p,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeDynamoError(w, http.StatusMethodNotAllowed, "ValidationException", "only POST is supported")
		return
	}

	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		writeDynamoError(w, http.StatusBadRequest, "ValidationException", "missing X-Amz-Target header")
		return
	}

	op := operationFromTarget(target)
	if _, ok := supportedOperations[op]; !ok {
		writeDynamoError(w, http.StatusBadRequest, "ValidationException", "unsupported operation "+op)
		return
	}

	if op == "ExecuteStatement" {
		var payload struct {
			Statement string `json:"Statement"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeDynamoError(w, http.StatusBadRequest, "ValidationException", "invalid ExecuteStatement payload")
			return
		}
		if _, err := h.parser.Parse(payload.Statement); err != nil {
			writeDynamoError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
	}

	// Route-wiring scaffold:
	_ = h.router
	_ = h.catalog
	_ = h.streams

	writeDynamoError(
		w,
		http.StatusNotImplemented,
		"NotImplementedException",
		"operation "+op+" is scaffolded but not implemented yet",
	)
}

func operationFromTarget(target string) string {
	idx := strings.LastIndex(target, ".")
	if idx < 0 {
		return target
	}
	return target[idx+1:]
}

func writeDynamoError(w http.ResponseWriter, statusCode int, errorType string, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"__type":  errorType,
		"message": message,
	})
}
