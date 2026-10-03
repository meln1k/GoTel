# GoTel

Let your clanker see.

GoTel is a local OpenTelemetry collector and debugging server written in Go. It accepts OTLP/HTTP traces and logs, stores them in embedded SQLite, and exposes JSON HTTP, CLI, and MCP interfaces for coding agents.

GoTel has no TUI or browser UI, ask you coding agent to use GoTel instead.

## Install

GoTel supports macOS and Linux on amd64 and arm64.

### Linux binaries

Prebuilt archives are available from [GitHub releases](https://github.com/meln1k/GoTel/releases). Check the release notes: older published binaries predate this SQLite-only implementation. To use the code in this checkout, build from source below. SQLite is embedded; no database server or separately installed SQLite library is needed.

### From source

For macOS or a source install on Linux, install Go and a C compiler. The embedded SQLite driver requires CGO.

```bash
go install ./cmd/...
```

Run this in the repository checkout. It installs `gotel` and `gotel-mcp` into `GOBIN` or `GOPATH/bin`.

## Start

```bash
gotel start
```

The default server is `http://127.0.0.1:27686`:

```text
POST /v1/traces
POST /v1/logs
GET  /api/health
GET  /openapi.json
```

Configure an OTLP/HTTP exporter:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:27686
export OTEL_SERVICE_NAME=my-service
```

Both OTLP JSON and protobuf payloads are accepted. Metrics are not collected.

Ingestion is acknowledged after telemetry is accepted into bounded process memory, **not after a database commit**. The `X-GoTel-Acknowledgment: accepted-not-persisted` response header makes this explicit. JSON responses retain `insertedSpans`/`insertedLogs`; protobuf responses are valid OTLP Export responses. Every 500 ms, a single writer drains admission-ordered spans and logs in separate bounded transactions. Queries can lag by the interval, backlog, transaction time, and retries. An abrupt process or machine failure can lose acknowledged telemetry; there is no disk-backed spool.

## Commands

```text
gotel start
gotel status
gotel stop
gotel restart
gotel server
gotel services
gotel traces [service] [limit]
gotel trace <trace-id>
gotel search-traces [service] [operation] [attr.key=value ...]
gotel search-spans [service] [operation] [parent=<operation>] [attr.key=value ...]
gotel search-logs [service] [body] [attr.key=value ...]
gotel mcp
```

Run `gotel help` for the complete command list. Bare `gotel`, `gotel tui`, and `gotel ui` also print help.

## MCP

Use either executable as a stdio MCP server:

```json
{
  "mcpServers": {
    "gotel": {
      "command": "gotel-mcp"
    }
  }
}
```

The server exposes read-only `gotel_*` tools for traces, spans, logs, facets, AI calls, statistics, and bundled documentation. It discovers the best running server from the current working directory. Set `GOTEL_URL` to select one explicitly.

## Debug skill

The agent skill is under [`skills/gotel-debug`](skills/gotel-debug). Install it with a skill manager that supports GitHub repositories, or copy that directory into your agent's skills directory.

The skill uses temporary OpenTelemetry instrumentation to test explicit debugging hypotheses. After the user approves cleanup:

```bash
gotel clear-debug /path/to/project
```

## Configuration

| Variable | Default |
| --- | --- |
| `GOTEL_OTEL_BASE_URL` | `http://127.0.0.1:27686` |
| `GOTEL_OTEL_QUERY_URL` | base URL |
| `GOTEL_OTEL_COLLECTOR_URL` | base URL fallback |
| `GOTEL_OTEL_HOST` | base URL host |
| `GOTEL_OTEL_PORT` | `27686` |
| `GOTEL_OTEL_EXPORTER_URL` | `<base URL>/v1/traces` |
| `GOTEL_OTEL_LOGS_EXPORTER_URL` | `<base URL>/v1/logs` |
| `GOTEL_OTEL_DB_PATH` | `$XDG_STATE_HOME/gotel/telemetry.sqlite` |
| `GOTEL_OTEL_SERVICE_NAME` | `gotel-otel-tui` |
| `GOTEL_OTEL_ENABLED` | `false` |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | `GOTEL_OTEL_EXPORTER_URL` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset; `/v1/traces` is appended when set |
| `GOTEL_RUNTIME_DIR` | `$XDG_STATE_HOME/gotel` |
| `GOTEL_OTEL_TRACE_LOOKBACK_MINUTES` | `1440` |
| `GOTEL_OTEL_TRACE_LIMIT` | `100` |
| `GOTEL_OTEL_LOG_LIMIT` | `80` |
| `GOTEL_OTEL_RETENTION_HOURS` | `168` |
| `GOTEL_OTEL_MAX_DB_SIZE_MB` | `1024` |

`GOTEL_OTEL_RETENTION_TRACE_BATCH`, `GOTEL_OTEL_RETENTION_LOG_BATCH`, and `GOTEL_OTEL_RETENTION_INTERVAL_SECONDS` tune maintenance work. `gotel endpoints` reports the resolved local endpoints and database path.

For lifecycle and query commands, GoTel normalizes the managed endpoint from `GOTEL_OTEL_BASE_URL` or `GOTEL_OTEL_QUERY_URL`, plus optional host/port overrides. `GOTEL_OTEL_COLLECTOR_URL` remains an exporter configuration fallback and does not move the local managed daemon. MCP clients can set `GOTEL_URL` to select a specific running instance.

Set `GOTEL_OTEL_ENABLED=true` to emit GoTel's HTTP server spans over OTLP/HTTP protobuf. The trace-specific standard endpoint takes precedence over the standard base endpoint and `GOTEL_OTEL_EXPORTER_URL`. For example, one instance can observe another:

```bash
GOTEL_OTEL_ENABLED=true \
GOTEL_OTEL_SERVICE_NAME=gotel1 \
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:27687/v1/traces \
gotel server
```

Trace context and baggage are extracted from incoming requests. If the resolved exporter points back to the same listener, ingestion endpoints are excluded from instrumentation to prevent an export feedback loop.

## SQLite storage

SQLite is the only storage backend. Stop an older server before upgrading. The default database is `telemetry.sqlite`; existing SQLite telemetry remains readable. Older database formats are **not opened or automatically migrated**, and their files are left untouched. A custom `GOTEL_OTEL_DB_PATH` must be a plain SQLite filesystem path, not a connection string. Health/status report `databaseBackend: sqlite`; managed commands reject a running server with another or missing backend identity, even when file paths match. The driver is `github.com/mattn/go-sqlite3`.

A single ingestion writer drains a bounded FIFO into separate transactions limited to 256 records, 2 MiB estimated bytes, and 8,192 work units by default. Span upserts, attribute replacement, logs, and touched summaries commit atomically. Maintenance serializes with ingestion through the same writer lock. SQL statements remain row-by-row for primary records, with one attribute INSERT per record; records share a batch transaction rather than committing individually.

SQLite supports at most 8,191 merged attribute/resource rows per span and 10,922 per log, preserving the driver's 32,766-parameter statement bound even when budgets are raised. Records exceeding that bound receive 413 before admission. Overlapping resource/record keys count once; record attributes still override resource attributes.

SQLite connections use WAL, `synchronous=FULL`, `BEGIN IMMEDIATE` for writes, a one-second busy timeout, approximately 8 MiB page cache per connection, and disabled mmap. The shared pool has at most eight connections, including the writer. FULL avoids trading away commit durability for benchmark throughput; actual power-loss behavior still depends on the filesystem. None of these settings is a process RSS cap. Readers can proceed during a write, but long read snapshots can delay WAL checkpoints and grow the WAL. Size retention counts live pages, excludes reusable free pages, and uses passive checkpoints; it does not promise to shrink the physical file or bound WAL growth. Database cancellation remains best-effort; shutdown callers still have their existing deadline.

The query/identity tests cover duplicate updates, late roots, synthetic parents, SQL search/statistics against Go reference implementations, attributes, AI analytics, retention, real constraint rollback, and cancellation. Unicode case conversion and query helpers are registered on every pooled connection. SQLite numeric analytics reject malformed numeric prefixes rather than using its permissive CAST. NaN numeric attributes return a query error rather than silently becoming SQL NULL. Full-trace hydration and whole-trace summary rebuilding remain proportional to trace size.

## Ingestion budgets and persistence failures

All the following settings are positive, strictly parsed integers. Invalid values fail startup; batch record/byte budgets may not exceed outstanding budgets. Bytes are base-10 integer byte counts, not values such as `2MB`.

| Variable | Default | Bounds |
| --- | ---: | --- |
| `GOTEL_OTEL_MAX_REQUEST_BYTES` | `4194304` (4 MiB) | HTTP body, including chunked requests |
| `GOTEL_OTEL_MAX_CONCURRENT_INGEST` | `2` | Shared trace/log body-reading, decoding, and admission slots; no waiting queue |
| `GOTEL_OTEL_MAX_OUTSTANDING_RECORDS` | `10000` | Pending + in-flight + retry spans/logs |
| `GOTEL_OTEL_MAX_OUTSTANDING_BYTES` | `67108864` (64 MiB) | Estimated outstanding telemetry bytes |
| `GOTEL_OTEL_MAX_BATCH_RECORDS` | `256` | Records per committed transaction |
| `GOTEL_OTEL_MAX_BATCH_BYTES` | `2097152` (2 MiB) | Estimated input bytes per transaction |
| `GOTEL_OTEL_MAX_BATCH_WORK` | `8192` | Record, attribute, resource, and event work units per transaction |
| `GOTEL_OTEL_WRITE_TIMEOUT_SECONDS` | `5` | Context deadline per database attempt, including writer-lock wait |
| `GOTEL_OTEL_SHUTDOWN_TIMEOUT_SECONDS` | `20` | Store drain and database-close wait |

Accounting reserves capacity before cloning records and releases it only after commit or a reported terminal discard. It uses fixed overhead, eight times variable string lengths, and map/event overhead; work counts attribute/event entries even when they do not create separate SQL rows. **This is not an RSS cap.** Request decoding, Go allocation/GC, SQLite native memory, summary rebuilding, and query results consume additional memory. Body size plus decoder concurrency bounds simultaneous decoding, but wire-to-object expansion varies with payload shape. A small attribute-heavy record can reach the work limit; an ordinary payload can reach the accounting limit before the wire-body limit. Lower budgets increase producer retries; larger budgets increase memory and drain latency.

Admission is all-or-nothing for the parsed request. Temporary capacity exhaustion returns HTTP **503**, OTLP `RESOURCE_EXHAUSTED`, and `Retry-After: 1`. Oversized bodies, records exceeding a single transaction's byte/work limit, and requests that cannot fit even an empty outstanding budget return **413**. Invalid payloads return **400**. JSON/protobuf errors use `google.rpc.Status`; rejected requests do not receive a success acknowledgment.

Retries keep the original bounded queue prefix; new arrivals never enlarge it. Backoff starts at 100 ms and caps at two seconds, interruptible by shutdown cancellation. Driver error types distinguish memory failures, permanent input/constraint/conversion failures, and interruption. SQLite busy/locked errors remain transient. SQLite allocation failures are identified explicitly; no SQLite engine memory limit is configured. Memory/permanent failures reduce the batch immediately; other failures reduce it after five attempts. Unknown/transient single-record failures remain queued with capped retries and overload protection. A permanently failing single record is **discarded and reported**; a single record still failing with a database memory error after five attempts is also discarded. Discards increment record/byte counters, produce rate-limited error logs without telemetry values, keep readiness degraded, and make subsequent `Flush` and shutdown return errors for this store's lifetime. This explicit loss policy lets unrelated records make progress; restart resets process-local diagnostics, not lost telemetry.

`Store.Flush(ctx)` snapshots admission order at entry: success means every record admitted before that barrier committed. Later admissions do not extend the barrier. Persistence failure, prior terminal loss, context expiry, or writer termination returns an error; an error does not cancel the background retry. It does not promise crash durability.

Shutdown first stops admission, cancels HTTP request contexts, and allows up to five seconds for HTTP shutdown. After self-telemetry shutdown (up to five seconds), the store attempts draining within its configured deadline. Database statements receive cancellable contexts; the close caller returns by its deadline even if a native driver operation or checkpoint fails to stop. In that case driver cleanup continues behind the writer until process exit. Incomplete drains report remaining record/estimated-byte counts and the last sanitized persistence error. Health advertises the running server's shutdown budget; managed `gotel stop` allows the larger of that budget and the local CLI budget plus 15 seconds (35 seconds by default). Forced termination returns an error. Configure an external service manager's stop timeout above this allowance. Inspect `daemon.log` for persistence/drain errors; process termination alone does not establish that every acknowledged record persisted.

`GET /api/health` retains HTTP 200 and `ok: true` for **liveness/identity**, separately reporting `ready`, `persistence`, and `ingestion`. `gotel status` includes persistence readiness. Diagnostics include pending/in-flight/outstanding counts and estimated bytes, oldest outstanding age, high-water marks, transaction sizes/durations, commits, failures/retries/discards, admission rejections, last error/failure/commit, and summary timing/row counts. HTTP rejected-request counts include body/decoder failures; store rejection counts concern queue admission and must not be added to HTTP counts. Readiness is false while stopping, at capacity, on persistence failure or terminal loss, or when the oldest backlog exceeds twice the write timeout. An idle process is not penalized for an old last commit. Repeated failure/discard logs are limited to one per class of log every five seconds.

**Large traces remain a limitation:** summary rebuilding still scans and sorts all existing spans of each touched trace. Input batch limits do not bound that scan; the statement deadline can cancel it but is not a memory cap. Full-trace queries also load and hydrate the entire trace, and simultaneous readers add to total process memory. Timings and `summaryRowsScanned` expose this work. Attribute-row insertion was costly in burst measurements, so each record's attribute rows use one SQL statement; span/log record writes remain row-by-row. Duplicate span updates, attribute replacement, late roots/spans, and summaries retain their existing transaction/query semantics.

## Development

```bash
go test -race ./... -count=1
go test -race -tags stress ./internal/server -count=1
go vet ./...
go build ./cmd/...
```

The server is intentionally local and unauthenticated. Do not expose it to an untrusted network or record secrets in telemetry.
