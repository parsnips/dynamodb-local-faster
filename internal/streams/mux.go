package streams

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/parsnips/dynamodb-local-faster/internal/backends"
)

const (
	defaultProxyAuthorizationHeader = "AWS4-HMAC-SHA256 Credential=local/19700101/us-west-2/dynamodb/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000"
	defaultProxyAmzDateHeader       = "19700101T000000Z"
)

// StreamError is a client-facing error with an HTTP status code and DynamoDB error type.
type StreamError struct {
	StatusCode int
	ErrorType  string
	Message    string
}

func (e *StreamError) Error() string {
	return e.Message
}

// ProxyFunc sends a request to a backend and returns the response.
type ProxyFunc func(ctx context.Context, backend backends.Backend, target string, body []byte) (statusCode int, responseBody []byte, err error)

// StreamMux routes DynamoDB Streams operations across multiple backends,
// presenting one virtual stream per table.
type StreamMux interface {
	ListStreams(ctx context.Context, reqBody []byte) (statusCode int, respBody []byte, err error)
	DescribeStream(ctx context.Context, reqBody []byte) (statusCode int, respBody []byte, err error)
	GetShardIterator(ctx context.Context, reqBody []byte) (statusCode int, respBody []byte, err error)
	GetRecords(ctx context.Context, reqBody []byte) (statusCode int, respBody []byte, err error)
}

type backendStream struct {
	backendID int
	streamARN string
}

type streamEntry struct {
	StreamArn   string `json:"StreamArn"`
	StreamLabel string `json:"StreamLabel,omitempty"`
	TableName   string `json:"TableName"`
}

// Mux implements StreamMux by encoding backend identity into shard IDs and
// iterator tokens for stateless routing.
type Mux struct {
	backends    []backends.Backend
	backendByID map[int]backends.Backend
	proxy       ProxyFunc

	mu           sync.RWMutex
	arnToTable   map[string]string          // streamARN → tableName
	tableStreams map[string][]backendStream // tableName → per-backend stream info
}

// NewMux creates a new stream mux.
func NewMux(backendsList []backends.Backend, proxy ProxyFunc) *Mux {
	byID := make(map[int]backends.Backend, len(backendsList))
	for _, b := range backendsList {
		byID[b.ID] = b
	}
	return &Mux{
		backends:     append([]backends.Backend(nil), backendsList...),
		backendByID:  byID,
		proxy:        proxy,
		arnToTable:   make(map[string]string),
		tableStreams: make(map[string][]backendStream),
	}
}

func (m *Mux) ListStreams(ctx context.Context, reqBody []byte) (int, []byte, error) {
	// Parse the client request to extract pagination params.
	var clientReq struct {
		TableName               string `json:"TableName,omitempty"`
		Limit                   *int   `json:"Limit,omitempty"`
		ExclusiveStartStreamArn string `json:"ExclusiveStartStreamArn,omitempty"`
	}
	if len(bytes.TrimSpace(reqBody)) > 0 {
		if err := json.Unmarshal(reqBody, &clientReq); err != nil {
			return 0, nil, &StreamError{
				StatusCode: http.StatusBadRequest,
				ErrorType:  "ValidationException",
				Message:    "invalid request payload",
			}
		}
	}

	type result struct {
		backendID  int
		statusCode int
		body       []byte
		streams    []streamEntry
		err        error
	}

	results := make([]result, len(m.backends))
	var wg sync.WaitGroup
	for i, b := range m.backends {
		wg.Add(1)
		go func(idx int, backend backends.Backend) {
			defer wg.Done()
			streamsList, sc, body, err := m.fetchAllStreamsFromBackend(ctx, backend, clientReq.TableName)
			results[idx] = result{
				backendID:  backend.ID,
				statusCode: sc,
				body:       body,
				streams:    streamsList,
				err:        err,
			}
		}(i, b)
	}
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return 0, nil, r.err
		}
		if r.statusCode < 200 || r.statusCode >= 300 {
			return r.statusCode, r.body, nil
		}
	}

	seen := make(map[string]*streamEntry)
	var orderedTables []string

	m.mu.Lock()
	for _, r := range results {
		for _, s := range r.streams {
			tableName := s.TableName
			if tableName == "" {
				var err error
				tableName, err = ParseTableNameFromStreamARN(s.StreamArn)
				if err != nil {
					continue
				}
			}

			m.arnToTable[s.StreamArn] = tableName
			found := false
			for i, bs := range m.tableStreams[tableName] {
				if bs.backendID == r.backendID {
					// Update ARN if it changed (fix #3: stale ARN refresh).
					m.tableStreams[tableName][i].streamARN = s.StreamArn
					found = true
					break
				}
			}
			if !found {
				m.tableStreams[tableName] = append(m.tableStreams[tableName], backendStream{
					backendID: r.backendID,
					streamARN: s.StreamArn,
				})
			}

			if _, exists := seen[tableName]; !exists {
				entry := s
				entry.TableName = tableName
				seen[tableName] = &entry
				orderedTables = append(orderedTables, tableName)
			}
		}
	}
	m.mu.Unlock()

	// Build deduplicated list.
	allStreams := make([]streamEntry, 0, len(orderedTables))
	for _, t := range orderedTables {
		allStreams = append(allStreams, *seen[t])
	}

	// Apply ExclusiveStartStreamArn: skip past it.
	if clientReq.ExclusiveStartStreamArn != "" {
		startIdx := -1
		for i, s := range allStreams {
			if s.StreamArn == clientReq.ExclusiveStartStreamArn {
				startIdx = i
				break
			}
		}
		if startIdx >= 0 && startIdx+1 < len(allStreams) {
			allStreams = allStreams[startIdx+1:]
		} else if startIdx >= 0 {
			allStreams = nil
		}
	}

	// Apply Limit and set LastEvaluatedStreamArn if truncated.
	resp := map[string]any{}
	if clientReq.Limit != nil && *clientReq.Limit > 0 && len(allStreams) > *clientReq.Limit {
		resp["LastEvaluatedStreamArn"] = allStreams[*clientReq.Limit-1].StreamArn
		allStreams = allStreams[:*clientReq.Limit]
	}
	resp["Streams"] = allStreams

	respBody, err := json.Marshal(resp)
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, respBody, nil
}

func (m *Mux) DescribeStream(ctx context.Context, reqBody []byte) (int, []byte, error) {
	var clientReq struct {
		StreamArn             string `json:"StreamArn"`
		Limit                 *int   `json:"Limit,omitempty"`
		ExclusiveStartShardId string `json:"ExclusiveStartShardId,omitempty"`
	}
	if err := json.Unmarshal(reqBody, &clientReq); err != nil {
		return 0, nil, &StreamError{
			StatusCode: http.StatusBadRequest,
			ErrorType:  "ValidationException",
			Message:    "invalid request payload",
		}
	}
	if strings.TrimSpace(clientReq.StreamArn) == "" {
		return 0, nil, &StreamError{
			StatusCode: http.StatusBadRequest,
			ErrorType:  "ValidationException",
			Message:    "StreamArn is required",
		}
	}

	streams, err := m.resolveStreamsForARN(ctx, clientReq.StreamArn)
	if err != nil {
		return 0, nil, err
	}

	type shardEntry struct {
		ShardId             string `json:"ShardId"`
		ParentShardId       string `json:"ParentShardId,omitempty"`
		SequenceNumberRange any    `json:"SequenceNumberRange,omitempty"`
	}

	type streamDescription struct {
		StreamArn               string       `json:"StreamArn"`
		StreamLabel             string       `json:"StreamLabel,omitempty"`
		StreamStatus            string       `json:"StreamStatus,omitempty"`
		StreamViewType          string       `json:"StreamViewType,omitempty"`
		TableName               string       `json:"TableName,omitempty"`
		KeySchema               any          `json:"KeySchema,omitempty"`
		Shards                  []shardEntry `json:"Shards"`
		CreationRequestDateTime any          `json:"CreationRequestDateTime,omitempty"`
		LastEvaluatedShardId    string       `json:"LastEvaluatedShardId,omitempty"`
	}

	var merged *streamDescription
	var allShards []shardEntry

	for _, bs := range streams {
		backend, ok := m.backendByID[bs.backendID]
		if !ok {
			continue
		}

		backendFailed := false
		backendCursor := ""
		seenBackendCursors := make(map[string]struct{})
		backendShards := make([]shardEntry, 0)
		var backendDescription *streamDescription

		for {
			// Strip client pagination and fetch each backend completely.
			backendReq := map[string]any{
				"StreamArn": bs.streamARN,
			}
			if backendCursor != "" {
				backendReq["ExclusiveStartShardId"] = backendCursor
			}
			backendReqBody, err := json.Marshal(backendReq)
			if err != nil {
				return 0, nil, err
			}

			sc, body, err := m.proxy(ctx, backend, "DynamoDBStreams_20120810.DescribeStream", backendReqBody)
			if err != nil {
				return 0, nil, err
			}
			if sc < 200 || sc >= 300 {
				backendFailed = true
				break
			}

			var resp struct {
				StreamDescription streamDescription `json:"StreamDescription"`
			}
			if err := json.Unmarshal(body, &resp); err != nil {
				return 0, nil, fmt.Errorf("unmarshal DescribeStream response from backend %d: %w", bs.backendID, err)
			}

			if backendDescription == nil {
				desc := resp.StreamDescription
				desc.Shards = nil
				desc.LastEvaluatedShardId = ""
				backendDescription = &desc
			}

			for _, shard := range resp.StreamDescription.Shards {
				virtualShard := shardEntry{
					ShardId:             EncodeVirtualToken(bs.backendID, shard.ShardId),
					SequenceNumberRange: shard.SequenceNumberRange,
				}
				if shard.ParentShardId != "" {
					virtualShard.ParentShardId = EncodeVirtualToken(bs.backendID, shard.ParentShardId)
				}
				backendShards = append(backendShards, virtualShard)
			}

			nextCursor := strings.TrimSpace(resp.StreamDescription.LastEvaluatedShardId)
			if nextCursor == "" {
				break
			}
			if _, exists := seenBackendCursors[nextCursor]; exists {
				return 0, nil, fmt.Errorf("backend %d returned repeated LastEvaluatedShardId", bs.backendID)
			}
			seenBackendCursors[nextCursor] = struct{}{}
			backendCursor = nextCursor
		}

		if backendFailed || backendDescription == nil {
			continue
		}

		if merged == nil {
			desc := *backendDescription
			merged = &desc
		}
		allShards = append(allShards, backendShards...)
	}

	if merged == nil {
		return http.StatusBadRequest, []byte(`{"__type":"ResourceNotFoundException","message":"Requested resource not found"}`), nil
	}

	// Use the requested ARN (which is the canonical one).
	merged.StreamArn = clientReq.StreamArn

	// Apply ExclusiveStartShardId: skip shards up to and including it.
	if clientReq.ExclusiveStartShardId != "" {
		startIdx := -1
		for i, s := range allShards {
			if s.ShardId == clientReq.ExclusiveStartShardId {
				startIdx = i
				break
			}
		}
		if startIdx >= 0 && startIdx+1 < len(allShards) {
			allShards = allShards[startIdx+1:]
		} else if startIdx >= 0 {
			allShards = nil
		}
	}

	// Apply Limit and set LastEvaluatedShardId if truncated.
	merged.LastEvaluatedShardId = ""
	if clientReq.Limit != nil && *clientReq.Limit > 0 && len(allShards) > *clientReq.Limit {
		merged.LastEvaluatedShardId = allShards[*clientReq.Limit-1].ShardId
		allShards = allShards[:*clientReq.Limit]
	}

	merged.Shards = allShards

	respBody, err := json.Marshal(map[string]any{
		"StreamDescription": merged,
	})
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, respBody, nil
}

func (m *Mux) GetShardIterator(ctx context.Context, reqBody []byte) (int, []byte, error) {
	var req struct {
		StreamArn         string `json:"StreamArn"`
		ShardId           string `json:"ShardId"`
		ShardIteratorType string `json:"ShardIteratorType"`
		SequenceNumber    string `json:"SequenceNumber,omitempty"`
	}
	if err := json.Unmarshal(reqBody, &req); err != nil {
		return 0, nil, &StreamError{
			StatusCode: http.StatusBadRequest,
			ErrorType:  "ValidationException",
			Message:    "invalid request payload",
		}
	}

	backendID, realShardID, err := DecodeVirtualToken(req.ShardId)
	if err != nil {
		return 0, nil, &StreamError{StatusCode: http.StatusBadRequest, ErrorType: "ValidationException", Message: "Invalid ShardId"}
	}

	backend, ok := m.backendByID[backendID]
	if !ok {
		return 0, nil, &StreamError{StatusCode: http.StatusBadRequest, ErrorType: "ValidationException", Message: "Invalid ShardId"}
	}

	realStreamARN, err := m.resolveRealStreamARN(ctx, req.StreamArn, backendID)
	if err != nil {
		return 0, nil, &StreamError{StatusCode: http.StatusBadRequest, ErrorType: "ResourceNotFoundException", Message: "Requested resource not found"}
	}

	backendReq := map[string]string{
		"StreamArn":         realStreamARN,
		"ShardId":           realShardID,
		"ShardIteratorType": req.ShardIteratorType,
	}
	if req.SequenceNumber != "" {
		backendReq["SequenceNumber"] = req.SequenceNumber
	}
	backendReqBody, err := json.Marshal(backendReq)
	if err != nil {
		return 0, nil, err
	}

	sc, body, err := m.proxy(ctx, backend, "DynamoDBStreams_20120810.GetShardIterator", backendReqBody)
	if err != nil {
		return 0, nil, err
	}
	if sc < 200 || sc >= 300 {
		return sc, body, nil
	}

	var resp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, nil, fmt.Errorf("unmarshal GetShardIterator response: %w", err)
	}

	virtualIterator := EncodeVirtualToken(backendID, resp.ShardIterator)
	respBody, err := json.Marshal(map[string]string{
		"ShardIterator": virtualIterator,
	})
	if err != nil {
		return 0, nil, err
	}
	return sc, respBody, nil
}

func (m *Mux) GetRecords(ctx context.Context, reqBody []byte) (int, []byte, error) {
	var req struct {
		ShardIterator string `json:"ShardIterator"`
		Limit         *int   `json:"Limit,omitempty"`
	}
	if err := json.Unmarshal(reqBody, &req); err != nil {
		return 0, nil, &StreamError{
			StatusCode: http.StatusBadRequest,
			ErrorType:  "ValidationException",
			Message:    "invalid request payload",
		}
	}

	backendID, realIterator, err := DecodeVirtualToken(req.ShardIterator)
	if err != nil {
		return 0, nil, &StreamError{StatusCode: http.StatusBadRequest, ErrorType: "ValidationException", Message: "Invalid ShardIterator"}
	}

	backend, ok := m.backendByID[backendID]
	if !ok {
		return 0, nil, &StreamError{StatusCode: http.StatusBadRequest, ErrorType: "ValidationException", Message: "Invalid ShardIterator"}
	}

	backendReq := map[string]any{
		"ShardIterator": realIterator,
	}
	if req.Limit != nil {
		backendReq["Limit"] = *req.Limit
	}
	backendReqBody, err := json.Marshal(backendReq)
	if err != nil {
		return 0, nil, err
	}

	sc, body, err := m.proxy(ctx, backend, "DynamoDBStreams_20120810.GetRecords", backendReqBody)
	if err != nil {
		return 0, nil, err
	}
	if sc < 200 || sc >= 300 {
		return sc, body, nil
	}

	// Parse response and encode NextShardIterator if present.
	var resp map[string]json.RawMessage
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, nil, fmt.Errorf("unmarshal GetRecords response: %w", err)
	}

	if nextRaw, ok := resp["NextShardIterator"]; ok {
		var nextIterator *string
		if err := json.Unmarshal(nextRaw, &nextIterator); err != nil {
			return 0, nil, fmt.Errorf("unmarshal NextShardIterator: %w", err)
		}
		if nextIterator != nil && *nextIterator != "" {
			virtualNext := EncodeVirtualToken(backendID, *nextIterator)
			encoded, _ := json.Marshal(virtualNext)
			resp["NextShardIterator"] = encoded
		}
	}

	respBody, err := json.Marshal(resp)
	if err != nil {
		return 0, nil, err
	}
	return sc, respBody, nil
}

// resolveStreamsForARN returns the backend streams for a given canonical stream ARN.
// If the cache doesn't have the mapping, it does a lazy ListStreams refresh.
func (m *Mux) resolveStreamsForARN(ctx context.Context, streamARN string) ([]backendStream, error) {
	m.mu.RLock()
	tableName, ok := m.arnToTable[streamARN]
	if ok {
		streams := append([]backendStream(nil), m.tableStreams[tableName]...)
		m.mu.RUnlock()
		if len(streams) > 0 {
			return streams, nil
		}
	} else {
		m.mu.RUnlock()
	}

	// Cache miss — do a ListStreams to populate.
	if _, _, err := m.ListStreams(ctx, []byte(`{}`)); err != nil {
		return nil, fmt.Errorf("lazy ListStreams refresh: %w", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	tableName, ok = m.arnToTable[streamARN]
	if !ok {
		return nil, nil
	}
	return append([]backendStream(nil), m.tableStreams[tableName]...), nil
}

// resolveRealStreamARN finds the real stream ARN for a specific backend from the cache.
func (m *Mux) resolveRealStreamARN(ctx context.Context, canonicalARN string, backendID int) (string, error) {
	m.mu.RLock()
	tableName, ok := m.arnToTable[canonicalARN]
	if ok {
		for _, bs := range m.tableStreams[tableName] {
			if bs.backendID == backendID {
				m.mu.RUnlock()
				return bs.streamARN, nil
			}
		}
	}
	m.mu.RUnlock()

	// Cache miss — refresh.
	if _, _, err := m.ListStreams(ctx, []byte(`{}`)); err != nil {
		return "", fmt.Errorf("lazy ListStreams refresh: %w", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	tableName, ok = m.arnToTable[canonicalARN]
	if !ok {
		return "", &StreamError{StatusCode: http.StatusBadRequest, ErrorType: "ResourceNotFoundException", Message: "Requested resource not found"}
	}
	for _, bs := range m.tableStreams[tableName] {
		if bs.backendID == backendID {
			return bs.streamARN, nil
		}
	}
	return "", &StreamError{StatusCode: http.StatusBadRequest, ErrorType: "ResourceNotFoundException", Message: "Requested resource not found"}
}

func (m *Mux) fetchAllStreamsFromBackend(
	ctx context.Context,
	backend backends.Backend,
	tableName string,
) ([]streamEntry, int, []byte, error) {
	cursor := ""
	seenCursors := make(map[string]struct{})
	allStreams := make([]streamEntry, 0)

	for {
		reqPayload := make(map[string]any)
		if strings.TrimSpace(tableName) != "" {
			reqPayload["TableName"] = tableName
		}
		if cursor != "" {
			reqPayload["ExclusiveStartStreamArn"] = cursor
		}
		reqBody, err := json.Marshal(reqPayload)
		if err != nil {
			return nil, 0, nil, err
		}

		sc, body, err := m.proxy(ctx, backend, "DynamoDBStreams_20120810.ListStreams", reqBody)
		if err != nil {
			return nil, 0, nil, err
		}
		if sc < 200 || sc >= 300 {
			return nil, sc, body, nil
		}

		var resp struct {
			Streams                []streamEntry `json:"Streams"`
			LastEvaluatedStreamArn string        `json:"LastEvaluatedStreamArn,omitempty"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, 0, nil, fmt.Errorf("unmarshal ListStreams response from backend %d: %w", backend.ID, err)
		}
		allStreams = append(allStreams, resp.Streams...)

		nextCursor := strings.TrimSpace(resp.LastEvaluatedStreamArn)
		if nextCursor == "" {
			break
		}
		if _, exists := seenCursors[nextCursor]; exists {
			return nil, 0, nil, fmt.Errorf("backend %d returned repeated LastEvaluatedStreamArn", backend.ID)
		}
		seenCursors[nextCursor] = struct{}{}
		cursor = nextCursor
	}

	return allStreams, http.StatusOK, nil, nil
}

// MakeProxyFunc constructs a ProxyFunc from an HTTP client.
func MakeProxyFunc(client *http.Client) ProxyFunc {
	return func(ctx context.Context, backend backends.Backend, target string, body []byte) (int, []byte, error) {
		url := backend.Endpoint
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", target)
		// DynamoDB Local requires a SigV4-style Authorization header, but it does not
		// verify the signature value. Stream fanout requests are internal and unsigned,
		// so attach minimal synthetic signing headers for backend acceptance.
		req.Header.Set("Authorization", defaultProxyAuthorizationHeader)
		req.Header.Set("X-Amz-Date", defaultProxyAmzDateHeader)

		resp, err := client.Do(req)
		if err != nil {
			return 0, nil, fmt.Errorf("proxy to backend %d: %w", backend.ID, err)
		}
		defer resp.Body.Close()

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return 0, nil, fmt.Errorf("read response from backend %d: %w", backend.ID, err)
		}
		return resp.StatusCode, respBody, nil
	}
}

// NoopMux is a no-op implementation of StreamMux for tests.
type NoopMux struct{}

func NewNoopMux() *NoopMux {
	return &NoopMux{}
}

func (m *NoopMux) ListStreams(ctx context.Context, reqBody []byte) (int, []byte, error) {
	return http.StatusOK, []byte(`{"Streams":[]}`), nil
}

func (m *NoopMux) DescribeStream(ctx context.Context, reqBody []byte) (int, []byte, error) {
	return http.StatusOK, []byte(`{"StreamDescription":{}}`), nil
}

func (m *NoopMux) GetShardIterator(ctx context.Context, reqBody []byte) (int, []byte, error) {
	return http.StatusOK, []byte(`{"ShardIterator":""}`), nil
}

func (m *NoopMux) GetRecords(ctx context.Context, reqBody []byte) (int, []byte, error) {
	return http.StatusOK, []byte(`{"Records":[]}`), nil
}
