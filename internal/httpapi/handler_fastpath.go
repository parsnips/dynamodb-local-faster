package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/parsnips/dynamodb-local-faster/internal/backends"
	"github.com/tidwall/gjson"
)

// fastPathContainerField maps single-item operations to the JSON field
// containing the partition key attribute (Item for PutItem, Key for others).
var fastPathContainerField = map[string]string{
	"PutItem":    "Item",
	"GetItem":    "Key",
	"DeleteItem": "Key",
	"UpdateItem": "Key",
}

// maxPoolBufferSize is the maximum buffer capacity retained by pools.
// Buffers that grew beyond this size are discarded to avoid pool bloat.
const maxPoolBufferSize = 256 * 1024

var compactPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// bodyPool holds reusable buffers for reading request bodies in ServeHTTP.
var bodyPool = sync.Pool{
	New: func() any { return bytes.NewBuffer(make([]byte, 0, 4096)) },
}

// respBodyPool holds reusable buffers for reading backend response bodies
// in proxyToBackendDirect.
var respBodyPool = sync.Pool{
	New: func() any { return bytes.NewBuffer(make([]byte, 0, 4096)) },
}

// putPoolBuffer returns a buffer to its pool if it hasn't grown too large.
func putPoolBuffer(pool *sync.Pool, buf *bytes.Buffer) {
	if buf.Cap() <= maxPoolBufferSize {
		pool.Put(buf)
	}
}

// readBody reads the full request body into a pooled buffer.
// The caller must call putPoolBuffer(&bodyPool, buf) when done.
func readBody(r io.Reader) (*bytes.Buffer, error) {
	buf := bodyPool.Get().(*bytes.Buffer)
	buf.Reset()
	if _, err := buf.ReadFrom(r); err != nil {
		putPoolBuffer(&bodyPool, buf)
		return nil, err
	}
	return buf, nil
}

// tryFastPathSingleItem attempts to route a single-item operation using gjson
// field extraction instead of full JSON unmarshaling. Returns true if the
// request was handled (success or error written to w), false to fall through
// to the standard full-parse path.
func (h *Handler) tryFastPathSingleItem(w http.ResponseWriter, r *http.Request, op string, body []byte) bool {
	containerField, ok := fastPathContainerField[op]
	if !ok {
		return false
	}

	// Reject malformed JSON so we don't proxy bodies the full-parse path
	// would reject with a 400 ValidationException.
	if !json.Valid(body) {
		return false
	}

	// Extract table name.
	tableResult := gjson.GetBytes(body, "TableName")
	if !tableResult.Exists() || tableResult.Type != gjson.String {
		return false
	}
	tableName := tableResult.Str
	if tableName == "" {
		return false
	}

	// Look up known partition key attribute for this table.
	pkAttr, known := h.partitionKeyAttribute(tableName)
	if !known || pkAttr == "" {
		return false
	}

	// Build gjson path: containerField.pkAttr (with special chars escaped).
	gjsonPath := containerField + "." + escapeGjsonPath(pkAttr)
	pkResult := gjson.GetBytes(body, gjsonPath)
	if !pkResult.Exists() {
		return false
	}

	// Guard against duplicate top-level keys. gjson returns the first
	// occurrence while encoding/json keeps the last, which could route
	// to the wrong backend. Fall through to the full-parse path.
	if hasDuplicateTopLevelKey(body, "TableName", containerField) {
		return false
	}

	// Extract the raw bytes for the PK value from the original body.
	pkRaw := rawBytesFromResult(body, pkResult)
	if pkRaw == nil {
		return false
	}

	// Canonicalize via json.Compact (strips whitespace, no reflection).
	pkBytes, err := compactJSON(pkRaw)
	if err != nil {
		return false
	}

	// Route to backend.
	target, err := h.router.ResolveItem(tableName, pkBytes)
	if err != nil {
		return false
	}

	// Proxy the original body unchanged via h.client (inherits timeout).
	if err := h.proxyToBackendDirect(w, r, target, body); err != nil {
		writeDynamoError(w, http.StatusBadGateway, "InternalServerError", err.Error())
	}
	return true
}

// proxyToBackendDirect sends body to the target backend and writes the
// response directly to w. Buffers the response body so w.Write sends the
// complete payload in one call, letting net/http compute Content-Length and
// write the response in a single TCP frame. Uses h.client.Do which inherits
// the configured timeout.
func (h *Handler) proxyToBackendDirect(w http.ResponseWriter, original *http.Request, target backends.Backend, body []byte) error {
	targetURL := strings.TrimRight(target.Endpoint, "/") + original.URL.Path
	if original.URL.RawQuery != "" {
		targetURL += "?" + original.URL.RawQuery
	}

	req, err := http.NewRequestWithContext(original.Context(), original.Method, targetURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build proxy request: %w", err)
	}
	copyHeader(req.Header, original.Header)

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("proxy %s to backend %d at %s: %w",
			operationFromTarget(original.Header.Get("X-Amz-Target")), target.ID, target.Endpoint, err)
	}
	defer resp.Body.Close()

	// Read response body into a pooled buffer.
	respBuf := respBodyPool.Get().(*bytes.Buffer)
	respBuf.Reset()
	defer putPoolBuffer(&respBodyPool, respBuf)

	if _, err := respBuf.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("read backend response: %w", err)
	}

	// Write response headers directly to w (no intermediate clone).
	wh := w.Header()
	for key, values := range resp.Header {
		if shouldSkipResponseHeader(key) {
			continue
		}
		for _, value := range values {
			wh.Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	w.Write(respBuf.Bytes())
	return nil
}

// hasDuplicateTopLevelKey reports whether body (which must be valid JSON)
// contains more than one occurrence of key1 or key2 at the top level of
// the JSON object. Uses zero-allocation byte scanning; the Go compiler
// optimizes string([]byte) == "literal" comparisons to avoid allocating.
func hasDuplicateTopLevelKey(body []byte, key1, key2 string) bool {
	n := len(body)
	i := skipJSONWhitespace(body, 0)
	if i >= n || body[i] != '{' {
		return false
	}
	i++ // skip '{'

	saw1, saw2 := false, false

	for {
		i = skipJSONWhitespace(body, i)
		if i >= n || body[i] == '}' {
			return false
		}

		// Expect a quoted key.
		if body[i] != '"' {
			return false
		}
		keyStart := i + 1
		keyEnd := scanToClosingQuote(body, keyStart)
		if keyEnd < 0 {
			return false
		}

		// Compare without allocation (Go optimizes string([]byte) == string).
		// If the key contains escape sequences (e.g. \u004E for 'N'), raw
		// byte comparison is unreliable. Fall through to full-parse path.
		keyBytes := body[keyStart:keyEnd]
		if bytes.IndexByte(keyBytes, '\\') >= 0 {
			return true
		}
		if string(keyBytes) == key1 {
			if saw1 {
				return true
			}
			saw1 = true
		} else if string(keyBytes) == key2 {
			if saw2 {
				return true
			}
			saw2 = true
		}

		i = keyEnd + 1 // past closing quote

		// Skip colon.
		i = skipJSONWhitespace(body, i)
		if i >= n || body[i] != ':' {
			return false
		}
		i++

		// Skip value.
		i = skipJSONWhitespace(body, i)
		i = scanPastJSONValue(body, i)
		if i < 0 {
			return false
		}

		// Skip comma if present.
		i = skipJSONWhitespace(body, i)
		if i < n && body[i] == ',' {
			i++
		}
	}
}

// skipJSONWhitespace advances past JSON whitespace characters.
func skipJSONWhitespace(b []byte, i int) int {
	for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	return i
}

// scanToClosingQuote finds the closing '"' for a JSON string.
// i must point at the first character after the opening '"'.
// Returns the index of the closing '"', or -1 if not found.
func scanToClosingQuote(b []byte, i int) int {
	for i < len(b) {
		if b[i] == '\\' {
			i += 2
			continue
		}
		if b[i] == '"' {
			return i
		}
		i++
	}
	return -1
}

// scanPastJSONValue scans past a complete JSON value starting at i.
// Returns the index after the value, or -1 on error.
func scanPastJSONValue(b []byte, i int) int {
	if i >= len(b) {
		return -1
	}
	switch b[i] {
	case '"':
		end := scanToClosingQuote(b, i+1)
		if end < 0 {
			return -1
		}
		return end + 1
	case '{', '[':
		return scanPastJSONContainer(b, i)
	default:
		// number, true, false, null
		for i < len(b) {
			switch b[i] {
			case ' ', '\t', '\n', '\r', ',', '}', ']':
				return i
			}
			i++
		}
		return i
	}
}

// scanPastJSONContainer scans past a JSON object or array.
// i must point at '{' or '['. Correctly handles nested strings.
func scanPastJSONContainer(b []byte, i int) int {
	if i >= len(b) {
		return -1
	}
	depth := 0
	for i < len(b) {
		switch b[i] {
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				return i + 1
			}
		case '"':
			end := scanToClosingQuote(b, i+1)
			if end < 0 {
				return -1
			}
			i = end + 1
			continue
		}
		i++
	}
	return -1
}

// escapeGjsonPath escapes characters that have special meaning in gjson paths.
// gjson treats . as path separator and uses # * ? \ as special characters.
func escapeGjsonPath(s string) string {
	if !strings.ContainsAny(s, ".#*?\\") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.', '#', '*', '?', '\\':
			b.WriteByte('\\')
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// compactJSON strips insignificant whitespace from raw JSON using json.Compact.
// For DynamoDB PK attribute values (single-key objects like {"S":"..."}),
// this produces output identical to canonicalizeRawJSON but without
// reflection-based unmarshal/remarshal.
func compactJSON(raw []byte) ([]byte, error) {
	buf := compactPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer putPoolBuffer(&compactPool, buf)

	if err := json.Compact(buf, raw); err != nil {
		return nil, err
	}
	// Return a copy since the buffer will be reused.
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

// rawBytesFromResult extracts the raw JSON bytes from body corresponding to
// the gjson result. gjson provides Index for unescaped values or Raw otherwise.
func rawBytesFromResult(body []byte, result gjson.Result) []byte {
	if result.Index > 0 {
		start := result.Index
		end := start + len(result.Raw)
		if end <= len(body) {
			return body[start:end]
		}
	}
	if result.Raw != "" {
		return []byte(result.Raw)
	}
	return nil
}
