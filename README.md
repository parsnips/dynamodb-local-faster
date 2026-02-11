# DynamoDB Local Faster

A DynamoDB-compatible routing layer that shards requests across multiple DynamoDB Local backends to overcome the single-threaded bottleneck.

## Build

### Prerequisites

- Go `1.25.1` (or newer `1.25.x`)
- Docker (required for `managed` mode and integration tests)

### Build the CLI

```bash
go build ./cmd/dynamodb-local-faster
```

### Build a named binary

```bash
mkdir -p bin
go build -o bin/dynamodb-local-faster ./cmd/dynamodb-local-faster
```

## Testing

### Run default test suite

```bash
go test ./...
```

### Run integration tests (managed mode)

```bash
go test -tags=integration ./pkg/dynolocalfaster -run TestManagedIntegrationAWSv2Modes -count=1
```

## Plan

### Goals

- Provide one DynamoDB-compatible endpoint for local development and CI.
- Increase throughput by routing partition-key traffic across `N` DynamoDB Local instances.
- Preserve core DynamoDB API behavior, including Streams.
- Support both:
  - `binary mode` (standalone process)
  - `library mode` (embeddable in tests/apps)

### Non-goals (v1)

- Full DynamoDB API parity.
- Cross-backend ACID transactions.
- Production durability/HA guarantees.

## Library-first design

The project will be built as a reusable Go library, with the CLI as a thin wrapper.

### Why

- Direct in-process usage avoids extra process startup overhead.
- Tests can boot and tear down faster.
- Consumers can choose startup mode per environment.

### Public API (v1)

```go
package dynolocalfaster

type Mode string

const (
    ModeManaged  Mode = "managed"  // start local backends with testcontainers
    ModeAttached Mode = "attached" // connect to already-running backends
)

type Config struct {
    ListenAddr      string
    Instances       int
    Mode            Mode
    BackendEndpoints []string // required for ModeAttached
    DynamoImage     string
    StateDir        string
    MetricsAddr     string
}

type Server struct {
    // internal fields
}

func New(ctx context.Context, cfg Config) (*Server, error)
func (s *Server) Start(ctx context.Context) error
func (s *Server) Endpoint() string
func (s *Server) Close(ctx context.Context) error
```

### Startup behavior

- `ModeAttached` is the immediate startup path:
  - no container lifecycle management
  - server becomes ready as soon as endpoint probes pass
- `ModeManaged` starts `N` containers via `testcontainers-go`.

## Routing + API behavior

Every request hits a single HTTP endpoint. The handler inspects the `X-Amz-Target` header to determine the DynamoDB operation, extracts routing keys from the JSON body, and dispatches to one or more of the `N` backends.

Partition-key routing uses `xxhash64(jsonSerializedPK) % N` for deterministic placement.

### Route modes

| Mode | Meaning |
|------|---------|
| **single** | Route to exactly one backend based on partition key hash. |
| **broadcast** | Send to all `N` backends; all must succeed. |
| **fanout** | Send to all `N` backends in parallel; merge responses. |

### Control plane operations

| Operation | Route | Notes |
|-----------|-------|-------|
| `CreateTable` | broadcast | Partition key attribute is remembered for future routing. |
| `UpdateTable` | broadcast | |
| `DeleteTable` | broadcast | Partition key attribute is forgotten on success. |
| `DescribeTable` | single | Sent to one backend (any); schema is identical across all. |
| `ListTables` | fanout | Results deduplicated. |

### Single-item operations

| Operation | Route | Notes |
|-----------|-------|-------|
| `GetItem` | single | Routed by partition key hash. |
| `PutItem` | single | Routed by partition key hash. |
| `DeleteItem` | single | Routed by partition key hash. |
| `UpdateItem` | single | Routed by partition key hash. |

### Batch operations

| Operation | Route | Notes |
|-----------|-------|-------|
| `BatchGetItem` | split | Each key is hashed to its backend. Per-backend sub-requests are sent in parallel, responses merged. `UnprocessedKeys` are combined. |
| `BatchWriteItem` | split | Same split-by-key approach. `UnprocessedItems` are combined. |

### Query and Scan

| Operation | Route | Notes |
|-----------|-------|-------|
| `Query` (table, with partition key) | single | Partition key extracted from `KeyConditionExpression`; routed by hash. |
| `Query` (GSI) | sequential fanout | GSI partition key doesn't match the table partition key, so all backends are queried. |
| `Query` (no extractable key) | sequential fanout | |
| `Scan` | sequential fanout | All backends are visited in order. |

### PartiQL (`ExecuteStatement`)

| Statement type | Route | Notes |
|----------------|-------|-------|
| `INSERT` with literal/parameter PK | single | Table + partition key extracted from the parsed statement. |
| `SELECT` with PK in `WHERE` | single | |
| `SELECT` without PK | sequential fanout | Read-only fan out to all backends. |
| Write without concrete PK | error | Rejected — would require unsafe multi-backend writes. |

### Sequential fanout pagination

When a `Scan`, `Query`, or `ExecuteStatement` fans out across multiple backends, the handler walks backends sequentially (backend 0, then 1, ..., N-1) and merges `Items` arrays from each response into a single result. This avoids the complexity of parallel pagination while preserving DynamoDB-compatible `Limit` and cursor semantics.

**How it works:**

1. The handler starts at backend 0 (or wherever the incoming cursor points) and proxies the request.
2. If the backend returns a `LastEvaluatedKey` (or `NextToken` for `ExecuteStatement`), the handler stops — that backend still has more data.
3. If the backend is exhausted (no cursor) and a `Limit` hasn't been reached, the handler moves to the next backend and continues.
4. Items from all visited backends are concatenated into a single `Items` array. `Count` and `ScannedCount` are summed.

**Cursor encoding:**

The handler wraps the real backend cursor inside a synthetic `LastEvaluatedKey` that encodes which backend to resume from:

```json
{
  "__dlf_fanout_backend": {"N": "2"},
  "__dlf_fanout_cursor":  {"S": "<base64-encoded real LastEvaluatedKey>"}
}
```

The two synthetic attributes use DynamoDB attribute-value format (`{"N": ...}`, `{"S": ...}`) so the cursor is a valid DynamoDB key map. When the client sends this back as `ExclusiveStartKey`, the handler decodes it, routes to backend 2, and passes the real cursor through.

For `ExecuteStatement`, the same scheme uses `NextToken` instead — a `dlfv1:`-prefixed base64 string encoding `{"backend": N, "token": "..."}`.

If the incoming `ExclusiveStartKey` doesn't contain the synthetic attributes, it's treated as a plain cursor for backend 0 — this allows a natural first request with no special encoding.

**Limit handling:**

The client's `Limit` is passed through to each backend request. After each backend responds, the handler subtracts the number of returned items from the remaining limit. When the limit is exhausted, the handler stops and returns a cursor pointing to the next unvisited backend (or the current backend if it still has data).

### Streams

Stream operations are handled by a dedicated `StreamMux` that presents one virtual stream per table, regardless of how many backends exist. Backend identity is encoded into shard IDs and iterator tokens using a `dlfb{backendID}:{realToken}` prefix, making routing stateless after initial discovery.

| Operation | Route | Notes |
|-----------|-------|-------|
| `ListStreams` | fanout | All backends queried (auto-paginating through each). Results deduplicated by table name; first backend's ARN is canonical. Supports `Limit` and `ExclusiveStartStreamArn` pagination applied after merge. |
| `DescribeStream` | fanout | All backends for the table are queried (auto-paginating through each). Shards from all backends are merged into one list with virtual shard IDs. Supports `Limit` and `ExclusiveStartShardId` pagination applied after merge. |
| `GetShardIterator` | single | Virtual shard ID is decoded to extract backend ID + real shard ID. Request is proxied to that single backend. Returned iterator is re-encoded as virtual. |
| `GetRecords` | single | Virtual iterator is decoded to extract backend ID + real iterator. Request is proxied to that single backend. `NextShardIterator` is re-encoded if present. |

Per-shard ordering is preserved. There is no global ordering guarantee across shards from different backends.

## Package layout

```text
cmd/dynamodb-local-faster/       # CLI wrapper over library
pkg/dynolocalfaster/             # public embeddable API
internal/config/                 # config parsing + validation
internal/backends/               # container/endpoint management
internal/router/                 # operation -> backend resolution
internal/catalog/                # schema replication + drift checks
internal/streams/                # stream mux + iterator token codec
internal/httpapi/                # DynamoDB JSON protocol handling
internal/partiql/                # statement parsing/routing
internal/metrics/                # Prometheus metrics + health endpoints
```

## Milestones

1. **Core framework**
   - Config, logging, lifecycle, health/readiness.
   - Library API + CLI wiring.
2. **Backend manager**
   - Managed/attached modes.
   - Container startup and endpoint health probing.
3. **API dispatch + routing**
   - Target-based operation dispatch.
   - PK hashing, fanout, batch partitioning.
4. **Schema replication**
   - Broadcast table/index changes.
   - Degraded state and reconciliation.
5. **Streams multiplexing**
   - Virtual shard IDs and iterator token mapping.
6. **PartiQL routing**
   - Parse and route `ExecuteStatement`.
7. **Hardening**
   - Compatibility tests, benchmarks, docs.

## Testing plan

### Unit tests

- Routing determinism and hash stability.
- Partition-key extraction by operation.
- Batch split/merge correctness.
- Stream iterator encode/decode.
- Error mapping to DynamoDB-style responses.

### Integration tests (`aws-sdk-go-v2`)

- End-to-end compatibility for supported operations.
- Schema replication across all backends.
- Correct single-backend routing for key-bound ops.
- Fanout merge correctness for scans/unconstrained reads.
- Unified stream consumption for writes across multiple backends.
- Degraded behavior on partial control-plane failure.

### Benchmark targets

- Compare against single DynamoDB Local baseline.
- Target: `>=2x` throughput at `N=4` on write-heavy, high-cardinality partition-key workloads.

## CLI behavior

`cmd/dynamodb-local-faster` will call into `pkg/dynolocalfaster`:

- `--listen-addr` (default `127.0.0.1:8000`)
- `--instances` (default `4`)
- `--mode` (`managed|attached`)
- `--backend-endpoints` (comma-separated, required in `attached`)
- `--image`
- `--state-dir`
- `--metrics-addr`

## Assumptions and defaults

- Primary use case is local dev/test, not production.
- `N` is fixed at startup in v1.
- Strong schema consistency is prioritized over partial availability.
- Unsupported operations fail explicitly with clear DynamoDB-style errors.
