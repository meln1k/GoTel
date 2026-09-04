# Gotel runtime debugging

Use Gotel when a failure needs runtime evidence rather than more static inspection.

1. Confirm `GET /api/health` succeeds. Run `gotel start` if necessary.
2. Write three to five concrete hypotheses.
3. Add focused OpenTelemetry spans or logs carrying `debug.session`, `debug.hypothesis`, `debug.step`, and `debug.label` attributes.
4. Wrap every temporary instrumentation block with `#region gotel debug` and `#endregion gotel debug` markers in comments.
5. Reproduce the failure and query Gotel by session and hypothesis attributes.
6. Classify each hypothesis from observed evidence before changing production logic.
7. Verify the fix while instrumentation remains.
8. Ask before cleanup, then run `gotel clear-debug [path]`.

Prefer one session identifier throughout a debugging run. Keep instrumentation narrow, avoid secrets and large payloads, and do not treat missing telemetry as proof that a hypothesis is false.
