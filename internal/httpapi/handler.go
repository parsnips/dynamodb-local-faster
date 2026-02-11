package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/parsnips/dynamodb-local-faster/internal/backends"
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

const defaultProxyTimeout = 30 * time.Second

type Handler struct {
	router  router.BackendRouter
	catalog catalog.CatalogReplicator
	streams streams.StreamMux
	parser  partiql.Parser
	client  *http.Client
}

type proxiedResponse struct {
	statusCode int
	header     http.Header
	body       []byte
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
		client:  &http.Client{Timeout: defaultProxyTimeout},
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

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, "ValidationException", "unable to read request body")
		return
	}

	payload, err := decodePayloadObject(body)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, "ValidationException", "invalid request payload")
		return
	}

	routeRequest, err := h.buildOperationRequest(op, payload)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}

	if op == "DescribeTable" && routeRequest.TableName != "" {
		if err := h.catalog.EnsureConsistent(r.Context(), routeRequest.TableName); err != nil {
			writeDynamoError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
			return
		}
	}

	route, err := router.PlanOperation(h.router, routeRequest)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if len(route.Backends) == 0 {
		writeDynamoError(w, http.StatusInternalServerError, "InternalServerError", "operation has no routed backend")
		return
	}

	switch route.Mode {
	case router.RouteModeSingle:
		response, err := h.proxyToBackend(r.Context(), r, route.Backends[0], body)
		if err != nil {
			writeDynamoError(w, http.StatusBadGateway, "InternalServerError", err.Error())
			return
		}
		writeProxiedResponse(w, response)
	case router.RouteModeBroadcast:
		responses, err := h.proxyToAll(r.Context(), r, route.Backends, body)
		if err != nil {
			writeDynamoError(w, http.StatusBadGateway, "InternalServerError", err.Error())
			return
		}
		if failed := firstFailureResponse(responses); failed != nil {
			writeProxiedResponse(w, *failed)
			return
		}
		if err := h.applyControlPlaneSideEffects(r.Context(), op, routeRequest.TableName, payload, body); err != nil {
			writeDynamoError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
			return
		}
		writeProxiedResponse(w, responses[0])
	case router.RouteModeFanout:
		responses, err := h.proxyToAll(r.Context(), r, route.Backends, body)
		if err != nil {
			writeDynamoError(w, http.StatusBadGateway, "InternalServerError", err.Error())
			return
		}
		merged, err := mergeFanoutResponses(op, responses)
		if err != nil {
			writeDynamoError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
			return
		}
		writeProxiedResponse(w, merged)
	default:
		writeDynamoError(w, http.StatusInternalServerError, "InternalServerError", "unsupported route mode")
	}
}

func (h *Handler) buildOperationRequest(op string, payload map[string]json.RawMessage) (router.OperationRequest, error) {
	tableName := extractStringField(payload, "TableName")
	request := router.OperationRequest{
		Operation: op,
		TableName: tableName,
	}

	switch op {
	case "GetItem", "DeleteItem", "UpdateItem":
		key, ok := payload["Key"]
		if !ok {
			return router.OperationRequest{}, fmt.Errorf("Key is required")
		}
		partitionKey, hasPartitionKey, err := h.partitionKeyFromAttributeMap(tableName, key)
		if err != nil {
			return router.OperationRequest{}, err
		}
		request.PartitionKey = partitionKey
		request.HasPartitionKey = hasPartitionKey
	case "PutItem":
		item, ok := payload["Item"]
		if !ok {
			return router.OperationRequest{}, fmt.Errorf("Item is required")
		}
		partitionKey, hasPartitionKey, err := h.partitionKeyFromAttributeMap(tableName, item)
		if err != nil {
			return router.OperationRequest{}, err
		}
		request.PartitionKey = partitionKey
		request.HasPartitionKey = hasPartitionKey
	case "ExecuteStatement":
		statement, ok := payload["Statement"]
		if !ok {
			return router.OperationRequest{}, fmt.Errorf("Statement is required")
		}
		var statementText string
		if err := json.Unmarshal(statement, &statementText); err != nil {
			return router.OperationRequest{}, fmt.Errorf("Statement must be a string")
		}

		parsedStatement, err := h.parser.Parse(statementText)
		if err != nil {
			return router.OperationRequest{}, err
		}
		statementRoute := &router.ParsedStatement{
			TableName:       parsedStatement.TableName,
			PartitionKey:    parsedStatement.PartitionKey,
			HasPartitionKey: parsedStatement.HasPartitionKey,
			ReadOnly:        parsedStatement.ReadOnly,
		}
		request.Statement = statementRoute
	}

	return request, nil
}

func (h *Handler) applyControlPlaneSideEffects(
	ctx context.Context,
	op string,
	tableName string,
	payload map[string]json.RawMessage,
	body []byte,
) error {
	request := catalog.TableRequest{
		TableName: tableName,
		Payload:   append([]byte(nil), body...),
	}

	switch op {
	case "CreateTable":
		if err := h.catalog.CreateTable(ctx, request); err != nil {
			return err
		}
		h.rememberTablePartitionKey(payload)
	case "UpdateTable":
		if err := h.catalog.UpdateTable(ctx, request); err != nil {
			return err
		}
	case "DeleteTable":
		if err := h.catalog.DeleteTable(ctx, request); err != nil {
			return err
		}
		h.forgetTable(tableName)
	}
	return nil
}

func (h *Handler) rememberTablePartitionKey(payload map[string]json.RawMessage) {
	registry, ok := h.router.(router.TableSchemaRegistry)
	if !ok {
		return
	}

	tableName := extractStringField(payload, "TableName")
	if tableName == "" {
		return
	}

	keySchemaRaw, ok := payload["KeySchema"]
	if !ok {
		return
	}

	var keySchema []struct {
		AttributeName string `json:"AttributeName"`
		KeyType       string `json:"KeyType"`
	}
	if err := json.Unmarshal(keySchemaRaw, &keySchema); err != nil {
		return
	}

	for _, element := range keySchema {
		if strings.EqualFold(strings.TrimSpace(element.KeyType), "HASH") {
			partitionKey := strings.TrimSpace(element.AttributeName)
			if partitionKey != "" {
				registry.RememberPartitionKey(tableName, partitionKey)
			}
			return
		}
	}
}

func (h *Handler) forgetTable(tableName string) {
	registry, ok := h.router.(router.TableSchemaRegistry)
	if !ok {
		return
	}
	registry.ForgetTable(tableName)
}

func (h *Handler) partitionKeyFromAttributeMap(
	tableName string,
	raw json.RawMessage,
) ([]byte, bool, error) {
	var attributes map[string]json.RawMessage
	if err := json.Unmarshal(raw, &attributes); err != nil {
		return nil, false, fmt.Errorf("invalid attribute map")
	}
	if len(attributes) == 0 {
		return nil, false, nil
	}

	partitionKeyAttr, hasKnownAttr := h.partitionKeyAttribute(tableName)
	if hasKnownAttr {
		value, ok := attributes[partitionKeyAttr]
		if !ok {
			return nil, false, nil
		}
		partitionKey, err := canonicalizeRawJSON(value)
		if err != nil {
			return nil, false, fmt.Errorf("invalid partition key value")
		}
		return partitionKey, true, nil
	}

	if len(attributes) == 1 {
		for _, value := range attributes {
			partitionKey, err := canonicalizeRawJSON(value)
			if err != nil {
				return nil, false, fmt.Errorf("invalid partition key value")
			}
			return partitionKey, true, nil
		}
	}

	return nil, false, nil
}

func (h *Handler) partitionKeyAttribute(tableName string) (string, bool) {
	registry, ok := h.router.(router.TableSchemaRegistry)
	if !ok {
		return "", false
	}
	return registry.PartitionKeyAttribute(tableName)
}

func (h *Handler) proxyToAll(
	ctx context.Context,
	original *http.Request,
	targets []backends.Backend,
	body []byte,
) ([]proxiedResponse, error) {
	responses := make([]proxiedResponse, 0, len(targets))
	for _, target := range targets {
		response, err := h.proxyToBackend(ctx, original, target, body)
		if err != nil {
			return nil, err
		}
		responses = append(responses, response)
	}
	return responses, nil
}

func (h *Handler) proxyToBackend(
	ctx context.Context,
	original *http.Request,
	target backends.Backend,
	body []byte,
) (proxiedResponse, error) {
	targetURL := strings.TrimRight(target.Endpoint, "/") + original.URL.Path
	if original.URL.RawQuery != "" {
		targetURL += "?" + original.URL.RawQuery
	}

	request, err := http.NewRequestWithContext(ctx, original.Method, targetURL, bytes.NewReader(body))
	if err != nil {
		return proxiedResponse{}, fmt.Errorf("build proxy request: %w", err)
	}
	copyHeader(request.Header, original.Header)

	response, err := h.client.Do(request)
	if err != nil {
		return proxiedResponse{}, fmt.Errorf("proxy %s to backend %d at %s: %w", operationFromTarget(original.Header.Get("X-Amz-Target")), target.ID, target.Endpoint, err)
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return proxiedResponse{}, fmt.Errorf("read backend response: %w", err)
	}

	return proxiedResponse{
		statusCode: response.StatusCode,
		header:     cloneHeader(response.Header),
		body:       responseBody,
	}, nil
}

func mergeFanoutResponses(op string, responses []proxiedResponse) (proxiedResponse, error) {
	if len(responses) == 0 {
		return proxiedResponse{}, fmt.Errorf("no responses to merge")
	}

	switch op {
	case "DescribeStream", "GetShardIterator", "GetRecords":
		for _, response := range responses {
			if isHTTPSuccess(response.statusCode) {
				return response, nil
			}
		}
		return responses[0], nil
	case "ListTables":
		return mergeListTablesResponses(responses)
	case "ListStreams":
		return mergeListStreamsResponses(responses)
	case "Scan", "Query", "ExecuteStatement":
		if failed := firstFailureResponse(responses); failed != nil {
			return *failed, nil
		}
		return mergeItemsResponse(responses)
	default:
		if failed := firstFailureResponse(responses); failed != nil {
			return *failed, nil
		}
		return responses[0], nil
	}
}

func mergeListTablesResponses(responses []proxiedResponse) (proxiedResponse, error) {
	if failed := firstFailureResponse(responses); failed != nil {
		return *failed, nil
	}

	seen := make(map[string]struct{})
	names := make([]string, 0)

	for _, response := range responses {
		payload, err := decodeJSONMap(response.body)
		if err != nil {
			return proxiedResponse{}, err
		}
		rawNames, _ := payload["TableNames"].([]any)
		for _, rawName := range rawNames {
			name, ok := rawName.(string)
			if !ok || name == "" {
				continue
			}
			if _, exists := seen[name]; exists {
				continue
			}
			seen[name] = struct{}{}
			names = append(names, name)
		}
	}

	sort.Strings(names)
	body, err := json.Marshal(map[string]any{
		"TableNames": names,
	})
	if err != nil {
		return proxiedResponse{}, err
	}

	return proxiedResponse{
		statusCode: http.StatusOK,
		header:     defaultDynamoHeader(body),
		body:       body,
	}, nil
}

func mergeListStreamsResponses(responses []proxiedResponse) (proxiedResponse, error) {
	if failed := firstFailureResponse(responses); failed != nil {
		return *failed, nil
	}

	streamsList := make([]any, 0)
	for _, response := range responses {
		payload, err := decodeJSONMap(response.body)
		if err != nil {
			return proxiedResponse{}, err
		}
		rawStreams, _ := payload["Streams"].([]any)
		streamsList = append(streamsList, rawStreams...)
	}

	body, err := json.Marshal(map[string]any{
		"Streams": streamsList,
	})
	if err != nil {
		return proxiedResponse{}, err
	}

	return proxiedResponse{
		statusCode: http.StatusOK,
		header:     defaultDynamoHeader(body),
		body:       body,
	}, nil
}

func mergeItemsResponse(responses []proxiedResponse) (proxiedResponse, error) {
	merged := make(map[string]any)
	mergedItems := make([]any, 0)

	totalCount := int64(0)
	sawCount := false

	totalScannedCount := int64(0)
	sawScannedCount := false

	for i, response := range responses {
		payload, err := decodeJSONMap(response.body)
		if err != nil {
			return proxiedResponse{}, err
		}
		if i == 0 {
			for key, value := range payload {
				if key == "Items" || key == "Count" || key == "ScannedCount" {
					continue
				}
				merged[key] = value
			}
		}

		rawItems, _ := payload["Items"].([]any)
		mergedItems = append(mergedItems, rawItems...)

		if rawCount, exists := payload["Count"]; exists {
			count, ok := numberToInt64(rawCount)
			if ok {
				totalCount += count
				sawCount = true
			}
		}
		if rawScannedCount, exists := payload["ScannedCount"]; exists {
			scannedCount, ok := numberToInt64(rawScannedCount)
			if ok {
				totalScannedCount += scannedCount
				sawScannedCount = true
			}
		}
	}

	merged["Items"] = mergedItems
	if sawCount {
		merged["Count"] = totalCount
	}
	if sawScannedCount {
		merged["ScannedCount"] = totalScannedCount
	}

	body, err := json.Marshal(merged)
	if err != nil {
		return proxiedResponse{}, err
	}

	return proxiedResponse{
		statusCode: http.StatusOK,
		header:     defaultDynamoHeader(body),
		body:       body,
	}, nil
}

func decodePayloadObject(body []byte) (map[string]json.RawMessage, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return map[string]json.RawMessage{}, nil
	}

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func canonicalizeRawJSON(raw json.RawMessage) ([]byte, error) {
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	return json.Marshal(decoded)
}

func firstFailureResponse(responses []proxiedResponse) *proxiedResponse {
	for i := range responses {
		if !isHTTPSuccess(responses[i].statusCode) {
			return &responses[i]
		}
	}
	return nil
}

func isHTTPSuccess(statusCode int) bool {
	return statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices
}

func decodeJSONMap(body []byte) (map[string]any, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return map[string]any{}, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()

	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func numberToInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		return parsed, true
	case float64:
		return int64(typed), true
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	default:
		return 0, false
	}
}

func writeProxiedResponse(w http.ResponseWriter, response proxiedResponse) {
	for key, values := range response.header {
		if shouldSkipResponseHeader(key) {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	w.WriteHeader(response.statusCode)
	_, _ = w.Write(response.body)
}

func shouldSkipResponseHeader(header string) bool {
	switch http.CanonicalHeaderKey(header) {
	case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade", "Content-Length":
		return true
	default:
		return false
	}
}

func cloneHeader(header http.Header) http.Header {
	cloned := make(http.Header, len(header))
	for key, values := range header {
		copied := append([]string(nil), values...)
		cloned[key] = copied
	}
	return cloned
}

func copyHeader(dst http.Header, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func defaultDynamoHeader(body []byte) http.Header {
	return http.Header{
		"Content-Type": []string{"application/x-amz-json-1.0"},
		"X-Amz-Crc32":  []string{strconv.FormatUint(uint64(crc32.ChecksumIEEE(body)), 10)},
	}
}

func extractStringField(payload map[string]json.RawMessage, field string) string {
	raw, ok := payload[field]
	if !ok {
		return ""
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return ""
	}
	return strings.TrimSpace(value)
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
