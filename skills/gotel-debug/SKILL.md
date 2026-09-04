---
name: gotel-debug
description: Debug applications with Gotel's local OpenTelemetry ingest and query server. Use for runtime-evidence debugging with traces or logs, temporary removable instrumentation, or configuring a repository to export OTLP/HTTP telemetry.
---

# Gotel Debug

Enter debug mode and use runtime evidence instead of guessing from source code alone. Gotel collects local OpenTelemetry traces and logs and makes them searchable during an investigation.

Default server details:

- Base URL: `http://127.0.0.1:27686`
- Trace ingest: `POST /v1/traces`
- Log ingest: `POST /v1/logs`
- Query API: `GET /api/*`
- OpenAPI: `GET /openapi.json`
- OTLP content type: `application/json` or `application/x-protobuf`
- Authentication: none

Use a URL supplied by the user instead of the default.

## Workflow

### 1. Verify the server

Request `GET /api/health`. If it fails or returns a non-200 response, start the background server without opening an interactive interface:

```bash
gotel start
```

Starting is idempotent. Runtime files live under `${XDG_STATE_HOME:-~/.local/state}/gotel/`. Check health again after startup. If the server still fails, inspect `${XDG_STATE_HOME:-~/.local/state}/gotel/daemon.log` and report the error.

Useful lifecycle commands:

```bash
gotel status
gotel stop
```

Use `GET /api/services` to discover which services are reporting.

### 2. State hypotheses before editing

Write three to five falsifiable hypotheses. Each hypothesis must identify a component, a possible failure, and evidence that would confirm or reject it. Prefer a concrete claim such as “the cache key omits the account ID” over “caching is broken.”

### 3. Add tagged instrumentation

Instrument all hypotheses in parallel with the tracing, structured logging, or annotation mechanism already used by the target repository. Do not issue raw HTTP requests from production code solely for debugging.

Every temporary block must:

- sit between exact `#region gotel debug` and `#endregion gotel debug` markers;
- include a `debug.hypothesis` attribute tied to one stated hypothesis;
- include the reusable structured keys below.

| Key | Purpose |
| --- | --- |
| `debug.session` | One identifier shared by this investigation |
| `debug.hypothesis` | Stable hypothesis ID, such as `H1` or `cache-miss` |
| `debug.step` | Position in the flow, such as `entry` or `after-read` |
| `debug.label` | Short human-readable description of the observation |

Choose points that discriminate between hypotheses: function boundaries, critical before/after values, branch selection, state transitions, and failures before retry or translation.

Use at least one and no more than ten instrumentation points. Two to six is typical. If more than ten seem necessary, narrow the hypotheses first.

### 4. Reproduce

- Run an existing failing test directly when possible.
- For a straightforward CLI, HTTP, or script reproduction, execute it yourself.
- Otherwise, give the user numbered reproduction steps and ask them to run those steps.
- Reuse the same reproduction path in later iterations.

### 5. Analyze the telemetry

Query by session and hypothesis. After exporter flushing, allow one 500 ms database write interval and retry if the batch may still be committing. For every hypothesis, report exactly one classification—**CONFIRMED**, **REJECTED**, or **INCONCLUSIVE**—and cite the specific span, log, or attribute values supporting it. Missing telemetry is inconclusive until execution, exporter flushing, and the database write interval are verified.

### 6. Fix from evidence

Implement only the smallest change supported by observed evidence. Keep all instrumentation in place and follow the target repository's established architecture.

### 7. Verify with instrumentation still present

Run the same reproduction and compare before/after evidence. Cite the observations proving the behavior changed. If the fix fails, revert changes based on rejected hypotheses, form hypotheses in different subsystems, adjust instrumentation, and repeat. Do not accumulate speculative fixes.

### 8. Clean up only with confirmation

After the fix is verified and the user confirms there are no remaining issues, remove instrumentation:

```bash
gotel clear-debug <project-root>
```

Then search for remaining markers and inspect `git diff` to ensure only the intended fix remains.

## Instrumentation rules

Use these exact markers, adapting only the language's comment syntax:

```ts
// #region gotel debug
// temporary debug instrumentation
// #endregion gotel debug
```

For example, Python may use `# #region gotel debug`. The cleanup command handles JS and TS extensions; remove marked blocks manually in other languages.

Never:

- record secrets, credentials, authorization headers, passwords, or raw personal data;
- remove instrumentation before post-fix verification;
- use sleeps, timers, or artificial delays as a fix;
- leave code changes from rejected hypotheses in place.

## Query patterns

Attribute filters support exact matches with `attr.<key>` and case-insensitive substring matches with `attrContains.<key>`.

```bash
curl http://127.0.0.1:27686/api/health
curl http://127.0.0.1:27686/api/services

# Trace search
curl "http://127.0.0.1:27686/api/traces/search?service=<service>&operation=<text>&attr.debug.session=<session>"

# Span search and trace scoping
curl "http://127.0.0.1:27686/api/spans/search?service=<service>&traceId=<trace-id>&attr.debug.hypothesis=<id>"
curl "http://127.0.0.1:27686/api/spans/search?service=<service>&attrContains.ai.prompt.messages=<phrase>"

# Log search
curl "http://127.0.0.1:27686/api/logs/search?service=<service>&severity=ERROR&body=<text>"
curl "http://127.0.0.1:27686/api/logs/search?service=<service>&attrContains.debug.label=<substring>"

# AI call summaries, details, and stats
curl "http://127.0.0.1:27686/api/ai/calls?model=<model>&sessionId=<session>"
curl "http://127.0.0.1:27686/api/ai/calls?text=<phrase>&status=error"
curl "http://127.0.0.1:27686/api/ai/calls/<span-id>"
curl "http://127.0.0.1:27686/api/ai/stats?groupBy=model&agg=total_input_tokens"

curl http://127.0.0.1:27686/openapi.json
```

List and search responses contain pagination metadata. Use `meta.nextCursor` for a subsequent trace or log page. Navigate correlation with `GET /api/traces/<trace-id>/spans`, `GET /api/traces/<trace-id>/logs`, and `GET /api/spans/<span-id>/logs`.
