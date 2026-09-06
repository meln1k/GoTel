# GoTel

Let your clanker see.

GoTel is a local OpenTelemetry collector and debugging server written in Go. It accepts OTLP/HTTP traces and logs, stores them in embedded DuckDB, and exposes JSON HTTP, CLI, and MCP interfaces for coding agents.

GoTel has no TUI or browser UI, ask you coding agent to use GoTel instead.

## Install

GoTel supports macOS and Linux on amd64 and arm64. The embedded DuckDB driver requires CGO and a C/C++ compiler.

```bash
go install github.com/meln1k/gotel/cmd/...@latest
```

This installs `gotel` and `gotel-mcp` into `GOBIN` or `GOPATH/bin`.

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

Ingestion is acknowledged after telemetry is accepted into process memory. Every 500 ms, a single writer commits queued spans and logs together, so queries can lag by one interval plus transaction time. Graceful shutdown flushes the queue; an abrupt process or machine failure can lose the acknowledged in-memory window.

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
| `GOTEL_OTEL_DB_PATH` | `$XDG_STATE_HOME/gotel/telemetry.duckdb` |
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

## Development

```bash
go test ./...
go vet ./...
go build ./cmd/...
```

The server is intentionally local and unauthenticated. Do not expose it to an untrusted network or record secrets in telemetry.
