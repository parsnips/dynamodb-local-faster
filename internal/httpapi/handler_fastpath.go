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

	// Proxy the original body unchanged, writing directly to w.
	if err := h.proxyToBackendDirect(w, r, target, body); err != nil {
		writeDynamoError(w, http.StatusBadGateway, "InternalServerError", err.Error())
	}
	return true
}

// hasDuplicateTopLevelKey reports whether body (which must be valid JSON)
// contains more than one occurrence of key1 or key2 at the top level of
// the JSON object. Uses json.Decoder token scanning to correctly
// distinguish keys from string values.
func hasDuplicateTopLevelKey(body []byte, key1, key2 string) bool {
	dec := json.NewDecoder(bytes.NewReader(body))
	// Read opening '{'.
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return false
	}

	saw1, saw2 := false, false

	for dec.More() {
		// Read key.
		tok, err = dec.Token()
		if err != nil {
			return false
		}
		key, ok := tok.(string)
		if !ok {
			return false
		}

		if key == key1 {
			if saw1 {
				return true
			}
			saw1 = true
		} else if key == key2 {
			if saw2 {
				return true
			}
			saw2 = true
		}

		// Skip value: read one token; if it opens an object or array,
		// consume tokens until the matching close.
		tok, err = dec.Token()
		if err != nil {
			return false
		}
		if delim, ok := tok.(json.Delim); ok && (delim == '{' || delim == '[') {
			depth := 1
			for depth > 0 {
				tok, err = dec.Token()
				if err != nil {
					return false
				}
				if d, ok := tok.(json.Delim); ok {
					switch d {
					case '{', '[':
						depth++
					case '}', ']':
						depth--
					}
				}
			}
		}
	}

	return false
}

// proxyToBackendDirect sends body to the target backend and writes the
// response directly to w, avoiding intermediate proxiedResponse allocation
// and header cloning. Used only by the fast path.
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
