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

### Control plane (table/index metadata)

- `CreateTable`, `UpdateTable`, `DeleteTable`, and GSI CRUD are broadcast to all backends.
- Success requires all backends (`N/N`) to avoid schema drift.
- On partial failure, table is marked `degraded` until reconciliation succeeds.

### Data plane (item operations)

- Partition-key hash decides backend: `xxhash64(pkBytes) % N`.
- Single-item ops route to one backend.
- Batch ops are split per backend and merged into DynamoDB-shaped responses.
- `Query` with a concrete partition key routes to one backend.
- `Scan` and unconstrained requests fan out to all backends and merge.

### PartiQL

- Parse statement and extract table + partition key when possible.
- Route to one backend when key can be resolved.
- Fan out for read-only statements without a concrete key.
- Reject ambiguous write statements rather than multi-write blindly.

### Streams

- Maintain virtual stream identifiers mapped to backend stream ARNs.
- Expose merged stream APIs:
  - `ListStreams`
  - `DescribeStream`
  - `GetShardIterator`
  - `GetRecords`
- Preserve per-shard ordering; no global ordering guarantee across backends.

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
