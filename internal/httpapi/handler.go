package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
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

const (
	defaultProxyTimeout    = 30 * time.Second
	fanoutCursorBackendKey = "__dlf_fanout_backend"
	fanoutCursorTokenKey   = "__dlf_fanout_cursor"
	fanoutNextTokenPrefix  = "dlfv1:"
)

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

	switch op {
	case "BatchGetItem":
		response, err := h.handleBatchGetItem(r.Context(), r, payload)
		if err != nil {
			writeDynamoError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
		writeProxiedResponse(w, response)
		return
	case "BatchWriteItem":
		response, err := h.handleBatchWriteItem(r.Context(), r, payload)
		if err != nil {
			writeDynamoError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
		writeProxiedResponse(w, response)
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
		if op == "Scan" || op == "Query" || op == "ExecuteStatement" {
			response, err := h.proxySequentialFanout(r.Context(), r, op, payload, route.Backends)
			if err != nil {
				writeDynamoError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
				return
			}
			writeProxiedResponse(w, response)
			return
		}

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
	case "Query":
		keyCondition := extractStringField(payload, "KeyConditionExpression")
		if keyCondition != "" {
			partitionKey, hasPartitionKey, err := h.partitionKeyFromQueryCondition(
				tableName,
				keyCondition,
				payload["ExpressionAttributeValues"],
				payload["ExpressionAttributeNames"],
			)
			if err != nil {
				return router.OperationRequest{}, err
			}
			request.PartitionKey = partitionKey
			request.HasPartitionKey = hasPartitionKey
		}
	case "ExecuteStatement":
		statement, ok := payload["Statement"]
		if !ok {
			return router.OperationRequest{}, fmt.Errorf("Statement is required")
		}
		var statementText string
		if err := json.Unmarshal(statement, &statementText); err != nil {
			return router.OperationRequest{}, fmt.Errorf("Statement must be a string")
		}

		routingStatement := statementText
		if parametersRaw, ok := payload["Parameters"]; ok {
			var parameters []json.RawMessage
			if err := json.Unmarshal(parametersRaw, &parameters); err != nil {
				return router.OperationRequest{}, fmt.Errorf("Parameters must be an array")
			}
			substitutedStatement, substituteErr := substitutePartiqlParameters(statementText, parameters)
			if substituteErr != nil {
				return router.OperationRequest{}, substituteErr
			}
			routingStatement = substitutedStatement
		}

		parsedStatement, err := h.parser.Parse(routingStatement)
		if err != nil {
			return router.OperationRequest{}, err
		}
		statementRoute := &router.ParsedStatement{
			TableName: parsedStatement.TableName,
			ReadOnly:  parsedStatement.ReadOnly,
		}
		if parsedStatement.HasPartitionKey {
			keepPartitionKey := true
			if parsedStatement.TableName != "" && parsedStatement.PartitionKeyAttribute != "" {
				if knownPartitionKey, known := h.partitionKeyAttribute(parsedStatement.TableName); known &&
					!strings.EqualFold(knownPartitionKey, parsedStatement.PartitionKeyAttribute) {
					keepPartitionKey = false
				}
			}
			if keepPartitionKey {
				statementRoute.PartitionKey = parsedStatement.PartitionKey
				statementRoute.HasPartitionKey = true
			}
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

func (h *Handler) partitionKeyFromQueryCondition(
	tableName string,
	keyConditionExpression string,
	rawExpressionAttributeValues json.RawMessage,
	rawExpressionAttributeNames json.RawMessage,
) ([]byte, bool, error) {
	partitionKeyAttribute, known := h.partitionKeyAttribute(tableName)
	if !known || strings.TrimSpace(partitionKeyAttribute) == "" {
		return nil, false, nil
	}

	attrNameToken, attrValueToken, ok := parseQueryKeyConditionEquality(keyConditionExpression)
	if !ok {
		return nil, false, nil
	}

	nameAliases := make(map[string]string)
	if len(bytes.TrimSpace(rawExpressionAttributeNames)) > 0 {
		if err := json.Unmarshal(rawExpressionAttributeNames, &nameAliases); err != nil {
			return nil, false, fmt.Errorf("ExpressionAttributeNames must be an object")
		}
	}
	attrNameToken = resolveExpressionAttributeName(attrNameToken, nameAliases)
	if !strings.EqualFold(strings.TrimSpace(attrNameToken), strings.TrimSpace(partitionKeyAttribute)) {
		return nil, false, nil
	}

	if !strings.HasPrefix(attrValueToken, ":") {
		return nil, false, nil
	}
	expressionValues := make(map[string]json.RawMessage)
	if len(bytes.TrimSpace(rawExpressionAttributeValues)) == 0 {
		return nil, false, fmt.Errorf("ExpressionAttributeValues is required for %s", attrValueToken)
	}
	if err := json.Unmarshal(rawExpressionAttributeValues, &expressionValues); err != nil {
		return nil, false, fmt.Errorf("ExpressionAttributeValues must be an object")
	}
	attributeValueRaw, ok := expressionValues[attrValueToken]
	if !ok {
		return nil, false, fmt.Errorf("ExpressionAttributeValues is missing %s", attrValueToken)
	}

	partitionKey, err := canonicalizeRawJSON(attributeValueRaw)
	if err != nil {
		return nil, false, fmt.Errorf("invalid partition key value in ExpressionAttributeValues")
	}
	return partitionKey, true, nil
}

func (h *Handler) handleBatchGetItem(
	ctx context.Context,
	original *http.Request,
	payload map[string]json.RawMessage,
) (proxiedResponse, error) {
	requestItemsRaw, ok := payload["RequestItems"]
	if !ok {
		return proxiedResponse{}, fmt.Errorf("RequestItems is required")
	}

	var requestItems map[string]json.RawMessage
	if err := json.Unmarshal(requestItemsRaw, &requestItems); err != nil {
		return proxiedResponse{}, fmt.Errorf("RequestItems must be an object")
	}
	if len(requestItems) == 0 {
		return proxiedResponse{}, fmt.Errorf("RequestItems is required")
	}

	type tableRequest struct {
		fields map[string]json.RawMessage
		keys   []json.RawMessage
	}

	backendByID := make(map[int]backends.Backend)
	perBackend := make(map[int]map[string]*tableRequest)

	for tableName, tableRequestRaw := range requestItems {
		tablePayload, err := decodePayloadObject(tableRequestRaw)
		if err != nil {
			return proxiedResponse{}, fmt.Errorf("RequestItems[%s] must be an object", tableName)
		}

		keysRaw, ok := tablePayload["Keys"]
		if !ok {
			return proxiedResponse{}, fmt.Errorf("RequestItems[%s].Keys is required", tableName)
		}

		var keys []json.RawMessage
		if err := json.Unmarshal(keysRaw, &keys); err != nil {
			return proxiedResponse{}, fmt.Errorf("RequestItems[%s].Keys must be an array", tableName)
		}
		if len(keys) == 0 {
			continue
		}

		tableFields := cloneRawPayload(tablePayload)
		delete(tableFields, "Keys")

		for _, keyRaw := range keys {
			targets, err := h.resolveBatchTargets(tableName, keyRaw)
			if err != nil {
				return proxiedResponse{}, fmt.Errorf("resolve %s key target: %w", tableName, err)
			}
			if len(targets) == 0 {
				return proxiedResponse{}, fmt.Errorf("operation has no routed backend for table %s", tableName)
			}

			for _, target := range targets {
				backendByID[target.ID] = target

				tableByName, ok := perBackend[target.ID]
				if !ok {
					tableByName = make(map[string]*tableRequest)
					perBackend[target.ID] = tableByName
				}
				entry, ok := tableByName[tableName]
				if !ok {
					entry = &tableRequest{
						fields: cloneRawPayload(tableFields),
					}
					tableByName[tableName] = entry
				}
				entry.keys = append(entry.keys, append(json.RawMessage(nil), keyRaw...))
			}
		}
	}

	backendIDs := make([]int, 0, len(perBackend))
	for backendID := range perBackend {
		backendIDs = append(backendIDs, backendID)
	}
	sort.Ints(backendIDs)

	responses := make([]proxiedResponse, 0, len(backendIDs))
	for _, backendID := range backendIDs {
		requestPayload := cloneRawPayload(payload)
		requestItems := make(map[string]json.RawMessage, len(perBackend[backendID]))

		for tableName, entry := range perBackend[backendID] {
			tablePayload := cloneRawPayload(entry.fields)
			keysRaw, marshalErr := json.Marshal(entry.keys)
			if marshalErr != nil {
				return proxiedResponse{}, marshalErr
			}
			tablePayload["Keys"] = keysRaw

			tableRequestRaw, marshalErr := json.Marshal(tablePayload)
			if marshalErr != nil {
				return proxiedResponse{}, marshalErr
			}
			requestItems[tableName] = tableRequestRaw
		}

		requestItemsRaw, marshalErr := json.Marshal(requestItems)
		if marshalErr != nil {
			return proxiedResponse{}, marshalErr
		}
		requestPayload["RequestItems"] = requestItemsRaw

		requestBody, marshalErr := json.Marshal(requestPayload)
		if marshalErr != nil {
			return proxiedResponse{}, marshalErr
		}

		target, ok := backendByID[backendID]
		if !ok {
			return proxiedResponse{}, fmt.Errorf("missing backend metadata for backend %d", backendID)
		}
		response, proxyErr := h.proxyToBackend(ctx, original, target, requestBody)
		if proxyErr != nil {
			return proxiedResponse{}, proxyErr
		}
		responses = append(responses, response)
	}

	return mergeBatchGetItemResponses(responses)
}

func (h *Handler) handleBatchWriteItem(
	ctx context.Context,
	original *http.Request,
	payload map[string]json.RawMessage,
) (proxiedResponse, error) {
	requestItemsRaw, ok := payload["RequestItems"]
	if !ok {
		return proxiedResponse{}, fmt.Errorf("RequestItems is required")
	}

	var requestItems map[string]json.RawMessage
	if err := json.Unmarshal(requestItemsRaw, &requestItems); err != nil {
		return proxiedResponse{}, fmt.Errorf("RequestItems must be an object")
	}
	if len(requestItems) == 0 {
		return proxiedResponse{}, fmt.Errorf("RequestItems is required")
	}

	type tableRequest struct {
		requests []json.RawMessage
	}

	backendByID := make(map[int]backends.Backend)
	perBackend := make(map[int]map[string]*tableRequest)

	for tableName, tableRequestRaw := range requestItems {
		var writeRequests []json.RawMessage
		if err := json.Unmarshal(tableRequestRaw, &writeRequests); err != nil {
			return proxiedResponse{}, fmt.Errorf("RequestItems[%s] must be an array", tableName)
		}

		for _, writeRequestRaw := range writeRequests {
			targets, err := h.resolveBatchWriteTargets(tableName, writeRequestRaw)
			if err != nil {
				return proxiedResponse{}, fmt.Errorf("resolve %s write target: %w", tableName, err)
			}
			if len(targets) == 0 {
				return proxiedResponse{}, fmt.Errorf("operation has no routed backend for table %s", tableName)
			}

			for _, target := range targets {
				backendByID[target.ID] = target

				tableByName, ok := perBackend[target.ID]
				if !ok {
					tableByName = make(map[string]*tableRequest)
					perBackend[target.ID] = tableByName
				}
				entry, ok := tableByName[tableName]
				if !ok {
					entry = &tableRequest{}
					tableByName[tableName] = entry
				}
				entry.requests = append(entry.requests, append(json.RawMessage(nil), writeRequestRaw...))
			}
		}
	}

	backendIDs := make([]int, 0, len(perBackend))
	for backendID := range perBackend {
		backendIDs = append(backendIDs, backendID)
	}
	sort.Ints(backendIDs)

	responses := make([]proxiedResponse, 0, len(backendIDs))
	for _, backendID := range backendIDs {
		requestPayload := cloneRawPayload(payload)
		requestItems := make(map[string]json.RawMessage, len(perBackend[backendID]))

		for tableName, entry := range perBackend[backendID] {
			tableRequestRaw, marshalErr := json.Marshal(entry.requests)
			if marshalErr != nil {
				return proxiedResponse{}, marshalErr
			}
			requestItems[tableName] = tableRequestRaw
		}

		requestItemsRaw, marshalErr := json.Marshal(requestItems)
		if marshalErr != nil {
			return proxiedResponse{}, marshalErr
		}
		requestPayload["RequestItems"] = requestItemsRaw

		requestBody, marshalErr := json.Marshal(requestPayload)
		if marshalErr != nil {
			return proxiedResponse{}, marshalErr
		}

		target, ok := backendByID[backendID]
		if !ok {
			return proxiedResponse{}, fmt.Errorf("missing backend metadata for backend %d", backendID)
		}
		response, proxyErr := h.proxyToBackend(ctx, original, target, requestBody)
		if proxyErr != nil {
			return proxiedResponse{}, proxyErr
		}
		responses = append(responses, response)
	}

	return mergeBatchWriteItemResponses(responses)
}

func (h *Handler) resolveBatchTargets(tableName string, keyRaw json.RawMessage) ([]backends.Backend, error) {
	partitionKey, hasPartitionKey, err := h.partitionKeyFromAttributeMap(tableName, keyRaw)
	if err != nil {
		return nil, err
	}
	if hasPartitionKey {
		target, err := h.router.ResolveItem(tableName, partitionKey)
		if err != nil {
			return nil, err
		}
		return []backends.Backend{target}, nil
	}
	return h.router.ResolveTable(tableName)
}

func (h *Handler) resolveBatchWriteTargets(tableName string, writeRequestRaw json.RawMessage) ([]backends.Backend, error) {
	writePayload, err := decodePayloadObject(writeRequestRaw)
	if err != nil {
		return nil, fmt.Errorf("write request must be an object")
	}

	if putRequestRaw, ok := writePayload["PutRequest"]; ok {
		putPayload, err := decodePayloadObject(putRequestRaw)
		if err != nil {
			return nil, fmt.Errorf("PutRequest must be an object")
		}
		itemRaw, ok := putPayload["Item"]
		if !ok {
			return nil, fmt.Errorf("PutRequest.Item is required")
		}
		return h.resolveBatchTargets(tableName, itemRaw)
	}

	if deleteRequestRaw, ok := writePayload["DeleteRequest"]; ok {
		deletePayload, err := decodePayloadObject(deleteRequestRaw)
		if err != nil {
			return nil, fmt.Errorf("DeleteRequest must be an object")
		}
		keyRaw, ok := deletePayload["Key"]
		if !ok {
			return nil, fmt.Errorf("DeleteRequest.Key is required")
		}
		return h.resolveBatchTargets(tableName, keyRaw)
	}

	return nil, fmt.Errorf("write request must include PutRequest or DeleteRequest")
}

func (h *Handler) proxySequentialFanout(
	ctx context.Context,
	original *http.Request,
	op string,
	payload map[string]json.RawMessage,
	targets []backends.Backend,
) (proxiedResponse, error) {
	if len(targets) == 0 {
		return proxiedResponse{}, fmt.Errorf("operation has no routed backend")
	}

	remainingLimit, hasLimit, err := extractPositiveLimit(payload)
	if err != nil {
		return proxiedResponse{}, err
	}

	backendIndex := 0
	var backendCursor json.RawMessage
	var backendNextToken string
	switch op {
	case "Scan", "Query":
		backendIndex, backendCursor, err = decodeFanoutExclusiveStartKey(payload["ExclusiveStartKey"], len(targets))
		if err != nil {
			return proxiedResponse{}, err
		}
	case "ExecuteStatement":
		backendIndex, backendNextToken, err = decodeFanoutNextToken(extractStringField(payload, "NextToken"), len(targets))
		if err != nil {
			return proxiedResponse{}, err
		}
	default:
		return proxiedResponse{}, fmt.Errorf("sequential fanout is unsupported for operation %s", op)
	}

	visitedResponses := make([]proxiedResponse, 0, len(targets)-backendIndex)
	nextBackendIndex := -1
	var nextBackendCursor json.RawMessage
	nextBackendToken := ""

	for idx := backendIndex; idx < len(targets); idx++ {
		if hasLimit && remainingLimit <= 0 {
			nextBackendIndex = idx
			break
		}

		requestPayload := cloneRawPayload(payload)
		switch op {
		case "Scan", "Query":
			if idx == backendIndex {
				if len(bytes.TrimSpace(backendCursor)) == 0 {
					delete(requestPayload, "ExclusiveStartKey")
				} else {
					requestPayload["ExclusiveStartKey"] = append(json.RawMessage(nil), backendCursor...)
				}
			} else {
				delete(requestPayload, "ExclusiveStartKey")
			}
		case "ExecuteStatement":
			if idx == backendIndex {
				if strings.TrimSpace(backendNextToken) == "" {
					delete(requestPayload, "NextToken")
				} else {
					tokenRaw, err := json.Marshal(backendNextToken)
					if err != nil {
						return proxiedResponse{}, err
					}
					requestPayload["NextToken"] = tokenRaw
				}
			} else {
				delete(requestPayload, "NextToken")
			}
		}

		if hasLimit {
			requestPayload["Limit"] = json.RawMessage(strconv.FormatInt(remainingLimit, 10))
		}

		requestBody, err := json.Marshal(requestPayload)
		if err != nil {
			return proxiedResponse{}, err
		}

		response, err := h.proxyToBackend(ctx, original, targets[idx], requestBody)
		if err != nil {
			return proxiedResponse{}, err
		}
		if !isHTTPSuccess(response.statusCode) {
			return response, nil
		}
		visitedResponses = append(visitedResponses, response)

		responsePayload, err := decodePayloadObject(response.body)
		if err != nil {
			return proxiedResponse{}, err
		}

		if hasLimit {
			itemCount, countErr := itemCountFromResponse(response.body)
			if countErr != nil {
				return proxiedResponse{}, countErr
			}
			remainingLimit -= int64(itemCount)
			if remainingLimit < 0 {
				remainingLimit = 0
			}
		}

		switch op {
		case "Scan", "Query":
			lastEvaluatedKeyRaw := responsePayload["LastEvaluatedKey"]
			hasMore, moreErr := hasNonEmptyObject(lastEvaluatedKeyRaw)
			if moreErr != nil {
				return proxiedResponse{}, moreErr
			}
			if hasMore {
				nextBackendIndex = idx
				nextBackendCursor = append(json.RawMessage(nil), lastEvaluatedKeyRaw...)
				idx = len(targets)
				continue
			}
		case "ExecuteStatement":
			token := extractStringField(responsePayload, "NextToken")
			if token != "" {
				nextBackendIndex = idx
				nextBackendToken = token
				idx = len(targets)
				continue
			}
		}

		if hasLimit && remainingLimit <= 0 {
			if idx+1 < len(targets) {
				nextBackendIndex = idx + 1
			}
			break
		}

		if idx+1 == len(targets) {
			break
		}
	}

	if len(visitedResponses) == 0 {
		return proxiedResponse{}, fmt.Errorf("fanout request produced no backend responses")
	}

	merged, err := mergeItemsResponse(visitedResponses)
	if err != nil {
		return proxiedResponse{}, err
	}

	mergedPayload, err := decodePayloadObject(merged.body)
	if err != nil {
		return proxiedResponse{}, err
	}

	switch op {
	case "Scan", "Query":
		if nextBackendIndex >= 0 {
			nextKey, encodeErr := encodeFanoutExclusiveStartKey(nextBackendIndex, nextBackendCursor)
			if encodeErr != nil {
				return proxiedResponse{}, encodeErr
			}
			mergedPayload["LastEvaluatedKey"] = nextKey
		} else {
			delete(mergedPayload, "LastEvaluatedKey")
		}
	case "ExecuteStatement":
		if nextBackendIndex >= 0 {
			nextToken, encodeErr := encodeFanoutNextToken(nextBackendIndex, nextBackendToken)
			if encodeErr != nil {
				return proxiedResponse{}, encodeErr
			}
			nextTokenRaw, marshalErr := json.Marshal(nextToken)
			if marshalErr != nil {
				return proxiedResponse{}, marshalErr
			}
			mergedPayload["NextToken"] = nextTokenRaw
		} else {
			delete(mergedPayload, "NextToken")
		}
	}

	mergedBody, err := json.Marshal(mergedPayload)
	if err != nil {
		return proxiedResponse{}, err
	}

	merged.body = mergedBody
	merged.header = defaultDynamoHeader(mergedBody)
	return merged, nil
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

func mergeBatchGetItemResponses(responses []proxiedResponse) (proxiedResponse, error) {
	if len(responses) == 0 {
		return proxiedResponse{}, fmt.Errorf("no responses to merge")
	}
	if failed := firstFailureResponse(responses); failed != nil {
		return *failed, nil
	}

	mergedResponses := make(map[string][]any)
	mergedUnprocessedKeys := make(map[string]map[string]any)
	consumedCapacity := make([]any, 0)

	for _, response := range responses {
		payload, err := decodeJSONMap(response.body)
		if err != nil {
			return proxiedResponse{}, err
		}

		if rawResponses, ok := payload["Responses"].(map[string]any); ok {
			for tableName, tableItemsRaw := range rawResponses {
				tableItems, ok := tableItemsRaw.([]any)
				if !ok {
					continue
				}
				mergedResponses[tableName] = append(mergedResponses[tableName], tableItems...)
			}
		}

		if rawUnprocessedKeys, ok := payload["UnprocessedKeys"].(map[string]any); ok {
			for tableName, tableRequestRaw := range rawUnprocessedKeys {
				tableRequest, ok := tableRequestRaw.(map[string]any)
				if !ok {
					continue
				}
				current, exists := mergedUnprocessedKeys[tableName]
				if !exists {
					current = cloneAnyMap(tableRequest)
					mergedUnprocessedKeys[tableName] = current
					continue
				}
				if newKeys, ok := tableRequest["Keys"].([]any); ok {
					existingKeys, _ := current["Keys"].([]any)
					current["Keys"] = append(existingKeys, newKeys...)
				}
				for key, value := range tableRequest {
					if key == "Keys" {
						continue
					}
					if _, present := current[key]; !present {
						current[key] = value
					}
				}
			}
		}

		if rawConsumedCapacity, ok := payload["ConsumedCapacity"].([]any); ok {
			consumedCapacity = append(consumedCapacity, rawConsumedCapacity...)
		}
	}

	bodyPayload := make(map[string]any)
	responsesPayload := make(map[string]any, len(mergedResponses))
	for tableName, tableItems := range mergedResponses {
		responsesPayload[tableName] = tableItems
	}
	bodyPayload["Responses"] = responsesPayload

	if len(mergedUnprocessedKeys) > 0 {
		unprocessedPayload := make(map[string]any, len(mergedUnprocessedKeys))
		for tableName, tableRequest := range mergedUnprocessedKeys {
			unprocessedPayload[tableName] = tableRequest
		}
		bodyPayload["UnprocessedKeys"] = unprocessedPayload
	}

	if len(consumedCapacity) > 0 {
		bodyPayload["ConsumedCapacity"] = consumedCapacity
	}

	body, err := json.Marshal(bodyPayload)
	if err != nil {
		return proxiedResponse{}, err
	}

	return proxiedResponse{
		statusCode: http.StatusOK,
		header:     defaultDynamoHeader(body),
		body:       body,
	}, nil
}

func mergeBatchWriteItemResponses(responses []proxiedResponse) (proxiedResponse, error) {
	if len(responses) == 0 {
		return proxiedResponse{}, fmt.Errorf("no responses to merge")
	}
	if failed := firstFailureResponse(responses); failed != nil {
		return *failed, nil
	}

	mergedUnprocessedItems := make(map[string][]any)
	consumedCapacity := make([]any, 0)
	mergedItemCollectionMetrics := make(map[string][]any)

	for _, response := range responses {
		payload, err := decodeJSONMap(response.body)
		if err != nil {
			return proxiedResponse{}, err
		}

		if rawUnprocessedItems, ok := payload["UnprocessedItems"].(map[string]any); ok {
			for tableName, writeRequestsRaw := range rawUnprocessedItems {
				writeRequests, ok := writeRequestsRaw.([]any)
				if !ok {
					continue
				}
				mergedUnprocessedItems[tableName] = append(mergedUnprocessedItems[tableName], writeRequests...)
			}
		}

		if rawConsumedCapacity, ok := payload["ConsumedCapacity"].([]any); ok {
			consumedCapacity = append(consumedCapacity, rawConsumedCapacity...)
		}

		if rawCollectionMetrics, ok := payload["ItemCollectionMetrics"].(map[string]any); ok {
			for tableName, metricsRaw := range rawCollectionMetrics {
				metrics, ok := metricsRaw.([]any)
				if !ok {
					continue
				}
				mergedItemCollectionMetrics[tableName] = append(mergedItemCollectionMetrics[tableName], metrics...)
			}
		}
	}

	bodyPayload := make(map[string]any)
	if len(mergedUnprocessedItems) > 0 {
		unprocessedPayload := make(map[string]any, len(mergedUnprocessedItems))
		for tableName, writeRequests := range mergedUnprocessedItems {
			unprocessedPayload[tableName] = writeRequests
		}
		bodyPayload["UnprocessedItems"] = unprocessedPayload
	}

	if len(consumedCapacity) > 0 {
		bodyPayload["ConsumedCapacity"] = consumedCapacity
	}

	if len(mergedItemCollectionMetrics) > 0 {
		metricsPayload := make(map[string]any, len(mergedItemCollectionMetrics))
		for tableName, metrics := range mergedItemCollectionMetrics {
			metricsPayload[tableName] = metrics
		}
		bodyPayload["ItemCollectionMetrics"] = metricsPayload
	}

	body, err := json.Marshal(bodyPayload)
	if err != nil {
		return proxiedResponse{}, err
	}

	return proxiedResponse{
		statusCode: http.StatusOK,
		header:     defaultDynamoHeader(body),
		body:       body,
	}, nil
}

func extractPositiveLimit(payload map[string]json.RawMessage) (int64, bool, error) {
	rawLimit, ok := payload["Limit"]
	if !ok {
		return 0, false, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(rawLimit))
	decoder.UseNumber()

	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return 0, false, fmt.Errorf("Limit must be a number")
	}
	limit, ok := numberToInt64(decoded)
	if !ok || limit < 0 {
		return 0, false, fmt.Errorf("Limit must be a non-negative integer")
	}
	return limit, true, nil
}

func itemCountFromResponse(body []byte) (int, error) {
	payload, err := decodeJSONMap(body)
	if err != nil {
		return 0, err
	}

	if rawItems, ok := payload["Items"].([]any); ok {
		return len(rawItems), nil
	}
	if rawCount, ok := payload["Count"]; ok {
		count, ok := numberToInt64(rawCount)
		if ok && count >= 0 {
			return int(count), nil
		}
	}
	return 0, nil
}

func hasNonEmptyObject(raw json.RawMessage) (bool, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return false, nil
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return false, err
	}
	return len(obj) > 0, nil
}

func decodeFanoutExclusiveStartKey(raw json.RawMessage, backendCount int) (int, json.RawMessage, error) {
	if backendCount <= 0 {
		return 0, nil, fmt.Errorf("backend count must be > 0")
	}

	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return 0, nil, nil
	}

	var keyPayload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &keyPayload); err != nil {
		return 0, nil, fmt.Errorf("ExclusiveStartKey must be an object")
	}

	backendRaw, hasBackend := keyPayload[fanoutCursorBackendKey]
	tokenRaw, hasToken := keyPayload[fanoutCursorTokenKey]
	if hasBackend || hasToken {
		if !hasBackend {
			return 0, nil, fmt.Errorf("fanout cursor is missing backend index")
		}
		backendIndex, err := decodeCursorBackendAttribute(backendRaw)
		if err != nil {
			return 0, nil, err
		}
		if backendIndex < 0 || backendIndex >= backendCount {
			return 0, nil, fmt.Errorf("fanout cursor backend %d is out of range", backendIndex)
		}

		cursorToken := ""
		if hasToken {
			cursorToken, err = decodeCursorTokenAttribute(tokenRaw)
			if err != nil {
				return 0, nil, err
			}
		}
		if cursorToken == "" {
			return backendIndex, nil, nil
		}

		decoded, err := base64.RawURLEncoding.DecodeString(cursorToken)
		if err != nil {
			return 0, nil, fmt.Errorf("invalid fanout cursor token: %w", err)
		}
		if _, err := hasNonEmptyObject(decoded); err != nil {
			return 0, nil, fmt.Errorf("fanout cursor token must encode a JSON object")
		}
		return backendIndex, decoded, nil
	}

	return 0, raw, nil
}

func decodeCursorBackendAttribute(raw json.RawMessage) (int, error) {
	var attribute struct {
		Number string `json:"N"`
	}
	if err := json.Unmarshal(raw, &attribute); err != nil {
		return 0, fmt.Errorf("fanout backend cursor must be a DynamoDB N attribute")
	}
	attribute.Number = strings.TrimSpace(attribute.Number)
	if attribute.Number == "" {
		return 0, fmt.Errorf("fanout backend cursor is empty")
	}

	value, err := strconv.Atoi(attribute.Number)
	if err != nil {
		return 0, fmt.Errorf("fanout backend cursor is invalid: %w", err)
	}
	return value, nil
}

func decodeCursorTokenAttribute(raw json.RawMessage) (string, error) {
	var attribute struct {
		String string `json:"S"`
	}
	if err := json.Unmarshal(raw, &attribute); err != nil {
		return "", fmt.Errorf("fanout token cursor must be a DynamoDB S attribute")
	}
	return attribute.String, nil
}

func encodeFanoutExclusiveStartKey(backendIndex int, backendCursor json.RawMessage) (json.RawMessage, error) {
	cursorToken := ""
	backendCursor = bytes.TrimSpace(backendCursor)
	if len(backendCursor) > 0 && !bytes.Equal(backendCursor, []byte("null")) {
		cursorToken = base64.RawURLEncoding.EncodeToString(backendCursor)
	}

	return json.Marshal(map[string]map[string]string{
		fanoutCursorBackendKey: {"N": strconv.Itoa(backendIndex)},
		fanoutCursorTokenKey:   {"S": cursorToken},
	})
}

func decodeFanoutNextToken(nextToken string, backendCount int) (int, string, error) {
	if backendCount <= 0 {
		return 0, "", fmt.Errorf("backend count must be > 0")
	}

	nextToken = strings.TrimSpace(nextToken)
	if nextToken == "" {
		return 0, "", nil
	}

	if !strings.HasPrefix(nextToken, fanoutNextTokenPrefix) {
		return 0, nextToken, nil
	}

	encoded := strings.TrimPrefix(nextToken, fanoutNextTokenPrefix)
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return 0, "", fmt.Errorf("invalid fanout next token: %w", err)
	}

	var decoded struct {
		Backend int    `json:"backend"`
		Token   string `json:"token"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return 0, "", fmt.Errorf("invalid fanout next token payload: %w", err)
	}
	if decoded.Backend < 0 || decoded.Backend >= backendCount {
		return 0, "", fmt.Errorf("fanout next token backend %d is out of range", decoded.Backend)
	}
	return decoded.Backend, decoded.Token, nil
}

func encodeFanoutNextToken(backendIndex int, backendToken string) (string, error) {
	raw, err := json.Marshal(map[string]any{
		"backend": backendIndex,
		"token":   backendToken,
	})
	if err != nil {
		return "", err
	}
	return fanoutNextTokenPrefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

func cloneRawPayload(payload map[string]json.RawMessage) map[string]json.RawMessage {
	cloned := make(map[string]json.RawMessage, len(payload))
	for key, value := range payload {
		cloned[key] = append(json.RawMessage(nil), value...)
	}
	return cloned
}

func cloneAnyMap(input map[string]any) map[string]any {
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func parseQueryKeyConditionEquality(expression string) (string, string, bool) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return "", "", false
	}

	andIdx := findKeywordOutsideQuotesForQuery(expression, "AND")
	if andIdx >= 0 {
		expression = strings.TrimSpace(expression[:andIdx])
	}

	eqIdx := findEqualityOutsideQuotesForQuery(expression)
	if eqIdx <= 0 {
		return "", "", false
	}

	left := strings.TrimSpace(expression[:eqIdx])
	right := strings.TrimSpace(expression[eqIdx+1:])
	if left == "" || right == "" {
		return "", "", false
	}

	if strings.HasPrefix(left, ":") && !strings.HasPrefix(right, ":") {
		return right, left, true
	}
	if !strings.HasPrefix(left, ":") && strings.HasPrefix(right, ":") {
		return left, right, true
	}
	return left, right, false
}

func resolveExpressionAttributeName(nameToken string, aliases map[string]string) string {
	nameToken = strings.TrimSpace(nameToken)
	if nameToken == "" {
		return ""
	}
	if strings.HasPrefix(nameToken, "#") {
		if resolved, ok := aliases[nameToken]; ok {
			return resolved
		}
	}
	if strings.Contains(nameToken, ".") {
		parts := strings.Split(nameToken, ".")
		last := strings.TrimSpace(parts[len(parts)-1])
		if strings.HasPrefix(last, "#") {
			if resolved, ok := aliases[last]; ok {
				return resolved
			}
		}
		return last
	}
	return strings.Trim(nameToken, "\"")
}

func findKeywordOutsideQuotesForQuery(input string, keyword string) int {
	keyword = strings.ToUpper(keyword)
	if keyword == "" {
		return -1
	}

	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i <= len(input)-len(keyword); i++ {
		ch := input[i]
		if inSingleQuote {
			if ch == '\'' {
				if i+1 < len(input) && input[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		}
		if inDoubleQuote {
			if ch == '"' {
				if i+1 < len(input) && input[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		}

		if ch == '\'' {
			inSingleQuote = true
			continue
		}
		if ch == '"' {
			inDoubleQuote = true
			continue
		}

		if !strings.EqualFold(input[i:i+len(keyword)], keyword) {
			continue
		}
		beforeOK := i == 0 || !isIdentifierLikeRune(rune(input[i-1]))
		afterPos := i + len(keyword)
		afterOK := afterPos >= len(input) || !isIdentifierLikeRune(rune(input[afterPos]))
		if beforeOK && afterOK {
			return i
		}
	}

	return -1
}

func findEqualityOutsideQuotesForQuery(input string) int {
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(input); i++ {
		ch := input[i]
		if inSingleQuote {
			if ch == '\'' {
				if i+1 < len(input) && input[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		}
		if inDoubleQuote {
			if ch == '"' {
				if i+1 < len(input) && input[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingleQuote = true
		case '"':
			inDoubleQuote = true
		case '=':
			return i
		}
	}

	return -1
}

func isIdentifierLikeRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '#'
}

func substitutePartiqlParameters(statement string, parameters []json.RawMessage) (string, error) {
	if len(parameters) == 0 {
		return statement, nil
	}

	var builder strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	paramIndex := 0

	for i := 0; i < len(statement); i++ {
		ch := statement[i]
		if inSingleQuote {
			builder.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(statement) && statement[i+1] == '\'' {
					builder.WriteByte(statement[i+1])
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		}
		if inDoubleQuote {
			builder.WriteByte(ch)
			if ch == '"' {
				if i+1 < len(statement) && statement[i+1] == '"' {
					builder.WriteByte(statement[i+1])
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingleQuote = true
			builder.WriteByte(ch)
		case '"':
			inDoubleQuote = true
			builder.WriteByte(ch)
		case '?':
			if paramIndex >= len(parameters) {
				return "", fmt.Errorf("missing parameter for PartiQL placeholder")
			}
			literal, err := partiqlParameterToLiteral(parameters[paramIndex])
			if err != nil {
				return "", err
			}
			builder.WriteString(literal)
			paramIndex++
		default:
			builder.WriteByte(ch)
		}
	}

	return builder.String(), nil
}

func partiqlParameterToLiteral(raw json.RawMessage) (string, error) {
	attributeValue, err := decodePayloadObject(raw)
	if err != nil {
		return "", fmt.Errorf("invalid ExecuteStatement parameter")
	}

	if rawString, ok := attributeValue["S"]; ok {
		var value string
		if err := json.Unmarshal(rawString, &value); err != nil {
			return "", fmt.Errorf("invalid string ExecuteStatement parameter")
		}
		return "'" + escapePartiqlSingleQuotedString(value) + "'", nil
	}
	if rawNumber, ok := attributeValue["N"]; ok {
		var value string
		if err := json.Unmarshal(rawNumber, &value); err != nil {
			return "", fmt.Errorf("invalid number ExecuteStatement parameter")
		}
		value = strings.TrimSpace(value)
		if value == "" {
			return "", fmt.Errorf("invalid number ExecuteStatement parameter")
		}
		return value, nil
	}
	if rawBool, ok := attributeValue["BOOL"]; ok {
		var value bool
		if err := json.Unmarshal(rawBool, &value); err != nil {
			return "", fmt.Errorf("invalid BOOL ExecuteStatement parameter")
		}
		if value {
			return "TRUE", nil
		}
		return "FALSE", nil
	}
	if rawNull, ok := attributeValue["NULL"]; ok {
		var value bool
		if err := json.Unmarshal(rawNull, &value); err != nil {
			return "", fmt.Errorf("invalid NULL ExecuteStatement parameter")
		}
		if value {
			return "NULL", nil
		}
		return "", fmt.Errorf("invalid NULL ExecuteStatement parameter")
	}

	return "", fmt.Errorf("unsupported ExecuteStatement parameter type")
}

func escapePartiqlSingleQuotedString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
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
