package store

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/model"
)

func TestSQLQueriesMatchReferenceImplementations(t *testing.T) {
	telemetryStore, since := differentialStore(t)
	ctx := context.Background()

	spanCases := []struct {
		name   string
		filter SpanFilter
		limit  int
	}{
		{"unfiltered", SpanFilter{SinceMs: since}, 12},
		{"service-operation-status", SpanFilter{Service: "service-1", Operation: "worker step", Status: "ok", SinceMs: since}, 20},
		{"trace", SpanFilter{TraceID: "trace-08", SinceMs: since}, 20},
		{"exact-and-contains", SpanFilter{Attributes: map[string]string{"span.group": "group-2"}, AttributeContains: map[string]string{"span.group": "ROUP"}, SinceMs: since}, 20},
		{"parent", SpanFilter{Operation: "worker", ParentOperation: "root batch", SinceMs: since}, 20},
		{"parent-and-contains", SpanFilter{Operation: "worker", ParentOperation: "root batch", AttributeContains: map[string]string{"search.text": "NEEDLE"}, SinceMs: since}, 20},
		{"synthetic-parent", SpanFilter{Operation: "worker", ParentOperation: "missing parent", SinceMs: since}, 20},
	}
	for _, test := range spanCases {
		t.Run("spans/"+test.name, func(t *testing.T) {
			want, err := referenceSearchSpans(ctx, telemetryStore, test.filter, test.limit)
			if err != nil {
				t.Fatal(err)
			}
			got, err := telemetryStore.SearchSpans(ctx, test.filter, test.limit)
			assertSame(t, got, err, want)
		})
	}

	traceStatCases := []struct {
		name, group, aggregate string
		filter                 TraceFilter
	}{
		{"count-by-service", "service", "count", TraceFilter{SinceMs: since}},
		{"average-by-operation", "operation", "avg_duration", TraceFilter{Service: "service-1", SinceMs: since}},
		{"p95-by-status", "status", "p95_duration", TraceFilter{Operation: "worker", SinceMs: since}},
		{"error-rate-by-attribute", "attr.span.group", "error_rate", TraceFilter{SinceMs: since}},
		{"multiple-values-for-attribute", "attr.multi.group", "count", TraceFilter{SinceMs: since}},
		{"count-with-attribute-filter", "service", "count", TraceFilter{Attributes: map[string]string{"span.group": "group-2"}, SinceMs: since}},
		{"average-with-duration-filter", "status", "avg_duration", TraceFilter{MinDurationMs: floatPointer(500), SinceMs: since}},
	}
	for _, test := range traceStatCases {
		t.Run("trace-stats/"+test.name, func(t *testing.T) {
			want, err := referenceTraceStats(ctx, telemetryStore, test.group, test.aggregate, test.filter, 20)
			if err != nil {
				t.Fatal(err)
			}
			got, err := telemetryStore.TraceStats(ctx, test.group, test.aggregate, test.filter, 20)
			assertSame(t, got, err, want)
		})
	}

	logStatCases := []struct {
		name, group string
		filter      LogFilter
	}{
		{"service", "service", LogFilter{SinceMs: since}},
		{"severity", "severity", LogFilter{Service: "service-1", SinceMs: since}},
		{"scope", "scope", LogFilter{Body: "event body", SinceMs: since}},
		{"attribute", "attr.log.group", LogFilter{SinceMs: since}},
		{"attribute-filter", "service", LogFilter{Attributes: map[string]string{"log.group": "logs-2"}, SinceMs: since}},
		{"unknown-group", "unsupported", LogFilter{SinceMs: since}},
	}
	for _, test := range logStatCases {
		t.Run("log-stats/"+test.name, func(t *testing.T) {
			want, err := referenceLogStats(ctx, telemetryStore, test.group, test.filter, 20)
			if err != nil {
				t.Fatal(err)
			}
			got, err := telemetryStore.LogStats(ctx, test.group, test.filter, 20)
			assertSame(t, got, err, want)
		})
	}

	aiFilters := []struct {
		name   string
		filter AIFilter
	}{
		{"unfiltered", AIFilter{SinceMs: since}},
		{"service", AIFilter{Service: "service-0", SinceMs: since}},
		{"trace", AIFilter{TraceID: "trace-08", SinceMs: since}},
		{"attributes", AIFilter{SessionID: "session-2", FunctionID: "function-0", Provider: "provider-0", Model: "model-2", SinceMs: since}},
		{"operation-status-text-duration", AIFilter{Operation: "streamText", Status: "ok", Text: "response needle", MinDurationMs: floatPointer(100), SinceMs: since}},
	}
	for _, test := range aiFilters {
		t.Run("ai-search/"+test.name, func(t *testing.T) {
			want, err := referenceSearchAICalls(ctx, telemetryStore, test.filter, 30)
			if err != nil {
				t.Fatal(err)
			}
			got, err := telemetryStore.SearchAICalls(ctx, test.filter, 30)
			assertSame(t, got, err, want)
		})
	}

	aiStatCases := []struct {
		name, group, aggregate string
		filter                 AIFilter
	}{
		{"count-provider", "provider", "count", AIFilter{SinceMs: since}},
		{"average-model", "model", "avg_duration", AIFilter{SinceMs: since}},
		{"nearest-rank-p95-model", "model", "p95_duration", AIFilter{SinceMs: since}},
		{"p95-function", "functionId", "p95_duration", AIFilter{Provider: "provider-0", SinceMs: since}},
		{"input-session", "sessionId", "total_input_tokens", AIFilter{SinceMs: since}},
		{"output-provider", "provider", "total_output_tokens", AIFilter{SinceMs: since}},
		{"count-status", "status", "count", AIFilter{SinceMs: since}},
		{"unsupported-status-aggregate", "status", "p95_duration", AIFilter{SinceMs: since}},
	}
	for _, test := range aiStatCases {
		t.Run("ai-stats/"+test.name, func(t *testing.T) {
			want, err := referenceAIStats(ctx, telemetryStore, test.group, test.aggregate, test.filter, 20)
			if err != nil {
				t.Fatal(err)
			}
			got, err := telemetryStore.AIStats(ctx, test.group, test.aggregate, test.filter, 20)
			assertSame(t, got, err, want)
		})
	}
}

func TestSQLQueriesPreserveCandidateAndStatsCaps(t *testing.T) {
	ctx := context.Background()
	t.Run("parent-candidate-window", func(t *testing.T) {
		telemetryStore := emptyDifferentialStore(t)
		base := time.Now().Add(-time.Hour).UnixMilli()
		spans := make([]model.SpanRecord, 0, 402)
		for i := 0; i <= 200; i++ {
			traceID, rootID := fmt.Sprintf("window-trace-%03d", i), fmt.Sprintf("window-root-%03d", i)
			operation := "wrong parent"
			if i == 0 {
				operation = "target parent"
			}
			start := base + int64(i*10)
			spans = append(spans,
				spanRecord(traceID, rootID, nil, "window", operation, start, 5, "ok", nil),
				spanRecord(traceID, fmt.Sprintf("window-child-%03d", i), &rootID, "window", "candidate child", start+1, 2, "ok", nil),
			)
		}
		if _, err := telemetryStore.IngestSpans(ctx, spans); err != nil {
			t.Fatal(err)
		}
		mustFlush(t, telemetryStore)
		filter := SpanFilter{Operation: "candidate child", ParentOperation: "target parent", SinceMs: base - 1}
		want, err := referenceSearchSpans(ctx, telemetryStore, filter, 20)
		if err != nil {
			t.Fatal(err)
		}
		if len(want) != 0 {
			t.Fatalf("reference candidate window changed: got %d results", len(want))
		}
		got, err := telemetryStore.SearchSpans(ctx, filter, 20)
		assertSame(t, got, err, want)
	})

	t.Run("five-thousand-row-stats-window", func(t *testing.T) {
		telemetryStore := emptyDifferentialStore(t)
		base := time.Now().Add(-time.Hour).UnixMilli()
		fixtureRows := `(WITH RECURSIVE r(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM r WHERE i<5000) SELECT i FROM r) AS r`
		_, err := telemetryStore.db.ExecContext(ctx, `INSERT INTO trace_summaries
			SELECT 'cap-trace-' || printf('%05d', i), CASE WHEN i=0 THEN 'old' ELSE 'new' END,
				'cap operation', ? + i, ? + i + 1, 0, 1, 1, 0 FROM `+fixtureRows, base, base)
		if err != nil {
			t.Fatal(err)
		}
		_, err = telemetryStore.db.ExecContext(ctx, `INSERT INTO spans
			SELECT 'cap-trace-' || printf('%05d', i), 'cap-span-' || printf('%05d', i), NULL,
				CASE WHEN i=0 THEN 'old' ELSE 'new' END, NULL, 'cap operation', NULL,
				? + i, ? + i + 1, 1, 'ok', '{}', '{}', '[]' FROM `+fixtureRows, base, base)
		if err != nil {
			t.Fatal(err)
		}
		filter := TraceFilter{Operation: "cap operation", SinceMs: base - 1}
		want, err := referenceTraceStats(ctx, telemetryStore, "service", "count", filter, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(want) != 1 || want[0].Group != "new" || want[0].Count != 5000 {
			t.Fatalf("reference trace cap changed: %#v", want)
		}
		got, err := telemetryStore.TraceStats(ctx, "service", "count", filter, 10)
		assertSame(t, got, err, want)

		_, err = telemetryStore.db.ExecContext(ctx, `INSERT INTO logs
			(trace_id, span_id, service_name, scope_name, severity_text, timestamp_ms, body, attributes_json, resource_json)
			SELECT NULL, NULL, CASE WHEN i=0 THEN 'old' ELSE 'new' END, NULL, 'INFO', ? + i,
				'cap log', '{"cap":"yes"}', '{}' FROM `+fixtureRows, base)
		if err != nil {
			t.Fatal(err)
		}
		_, err = telemetryStore.db.ExecContext(ctx, `INSERT INTO log_attributes SELECT id, 'cap', 'yes' FROM logs`)
		if err != nil {
			t.Fatal(err)
		}
		logFilter := LogFilter{Attributes: map[string]string{"cap": "yes"}, SinceMs: base - 1}
		wantLogs, err := referenceLogStats(ctx, telemetryStore, "service", logFilter, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(wantLogs) != 1 || wantLogs[0].Group != "new" || wantLogs[0].Count != 5000 {
			t.Fatalf("reference log cap changed: %#v", wantLogs)
		}
		gotLogs, err := telemetryStore.LogStats(ctx, "service", logFilter, 10)
		assertSame(t, gotLogs, err, wantLogs)
	})
}

func differentialStore(t *testing.T) (*Store, int64) {
	t.Helper()
	telemetryStore := emptyDifferentialStore(t)
	base := time.Now().Add(-time.Hour).UnixMilli()
	spans := make([]model.SpanRecord, 0)
	logs := make([]model.LogRecord, 0)
	for i := 0; i < 32; i++ {
		traceID, rootID := fmt.Sprintf("trace-%02d", i), fmt.Sprintf("root-%02d", i)
		service := fmt.Sprintf("service-%d", i%3)
		status := "ok"
		if i%7 == 0 {
			status = "error"
		}
		start := base + int64(i*1_000)
		spans = append(spans, spanRecord(traceID, rootID, nil, service, fmt.Sprintf("root.batch.%d", i%4), start, int64(400+i*10), status, map[string]string{
			"root.group": fmt.Sprintf("root-%d", i%2), "multi.group": fmt.Sprintf("root-value-%d", i%2),
		}))
		parentID := rootID
		if i%9 == 0 {
			parentID = fmt.Sprintf("missing-%02d", i)
		}
		spans = append(spans, spanRecord(traceID, fmt.Sprintf("child-%02d", i), &parentID, service, fmt.Sprintf("worker.step.%d", i%3), start+50, int64(100+i), "ok", map[string]string{
			"span.group": fmt.Sprintf("group-%d", i%4), "multi.group": fmt.Sprintf("child-value-%d", i%3), "search.text": fmt.Sprintf("needle value %d", i),
		}))
		if i%4 == 0 {
			aiID := fmt.Sprintf("ai-%02d", i)
			aiStatus := "ok"
			if i%8 == 0 {
				aiStatus = "error"
			}
			inputTokens := fmt.Sprintf("%d", 10+i)
			outputTokens := fmt.Sprintf("%d", 5+i)
			if i == 4 {
				inputTokens = " 14 "
			}
			if i == 12 {
				outputTokens = "invalid"
			}
			spans = append(spans, spanRecord(traceID, aiID, &rootID, service, "ai.streamText", start+100, int64(200+i), aiStatus, map[string]string{
				"ai.telemetry.functionId":         fmt.Sprintf("function-%d", i%2),
				"ai.model.provider":               fmt.Sprintf("provider-%d", i%2),
				"ai.model.id":                     fmt.Sprintf("model-%d", i%3),
				"ai.telemetry.metadata.sessionId": fmt.Sprintf("session-%d", i%3),
				"ai.prompt.messages":              fmt.Sprintf(`[{"role":"user","content":"prompt needle %d"}]`, i),
				"ai.response.text":                fmt.Sprintf("response needle %d", i),
				"ai.response.finishReason":        "stop",
				"ai.usage.inputTokens":            inputTokens,
				"ai.usage.outputTokens":           outputTokens,
			}))
			spans = append(spans, spanRecord(traceID, fmt.Sprintf("tool-%02d", i), &aiID, service, "ai.toolCall.lookup", start+120, 20, "ok", map[string]string{"ai.toolCall.name": "lookup"}))
		}
		scope := "scope-" + fmt.Sprint(i%2)
		traceCopy, childCopy := traceID, fmt.Sprintf("child-%02d", i)
		logs = append(logs,
			model.LogRecord{TraceID: &traceCopy, SpanID: &childCopy, ScopeName: &scope, ServiceName: service, SeverityText: []string{"INFO", "WARN", "ERROR"}[i%3], Body: fmt.Sprintf("event body needle %d", i), TimestampMs: start + 75, Attributes: map[string]string{"log.group": fmt.Sprintf("logs-%d", i%4)}, Resource: map[string]string{"region": fmt.Sprintf("region-%d", i%2)}},
			model.LogRecord{ServiceName: service, SeverityText: "DEBUG", Body: fmt.Sprintf("uncorrelated body %d", i), TimestampMs: start + 80, Attributes: map[string]string{}, Resource: map[string]string{}},
		)
	}
	if _, err := telemetryStore.IngestSpans(context.Background(), spans); err != nil {
		t.Fatal(err)
	}
	if _, err := telemetryStore.IngestLogs(context.Background(), logs); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, telemetryStore)
	return telemetryStore, base - 1
}

func emptyDifferentialStore(t *testing.T) *Store {
	t.Helper()
	cfg := config.Load()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "differential.sqlite")
	telemetryStore, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = telemetryStore.Close() })
	return telemetryStore
}

func spanRecord(traceID, spanID string, parent *string, service, operation string, start, duration int64, status string, attributes map[string]string) model.SpanRecord {
	return model.SpanRecord{
		TraceID: traceID, SpanID: spanID, ParentSpanID: parent, ServiceName: service, OperationName: operation,
		StartTimeMs: start, EndTimeMs: start + duration, DurationMs: float64(duration), Status: status,
		Attributes: attributes, Resource: map[string]string{"deployment": "differential"}, Events: []model.EventRecord{},
	}
}

func referenceSearchSpans(ctx context.Context, telemetryStore *Store, filter SpanFilter, requestedLimit int) ([]model.SpanWithContext, error) {
	conditions := []string{"s.start_time_ms>=?"}
	args := []any{filter.SinceMs}
	if filter.Service != "" {
		conditions, args = append(conditions, "s.service_name=?"), append(args, filter.Service)
	}
	if filter.TraceID != "" {
		conditions, args = append(conditions, "s.trace_id=?"), append(args, filter.TraceID)
	}
	if filter.Operation != "" {
		match, matchArgs := textMatch("s.operation_name", filter.Operation)
		conditions, args = append(conditions, match), append(args, matchArgs...)
	}
	if filter.Status == "ok" || filter.Status == "error" {
		conditions, args = append(conditions, "s.status=?"), append(args, filter.Status)
	}
	if len(filter.Attributes) > 0 {
		expression, attrArgs := spanAttributeMatch("s.trace_id", "s.span_id", filter.Attributes, nil)
		conditions, args = append(conditions, expression), append(args, attrArgs...)
	}
	if len(filter.AttributeContains) > 0 {
		expression, attrArgs := spanAttributeMatch("s.trace_id", "s.span_id", nil, filter.AttributeContains)
		conditions, args = append(conditions, expression), append(args, attrArgs...)
	}
	candidateLimit := requestedLimit
	if filter.ParentOperation != "" {
		candidateLimit = max(requestedLimit*10, 200)
		if len(filter.AttributeContains) > 0 {
			candidateLimit = max(requestedLimit*20, 500)
		}
	}
	args = append(args, candidateLimit)
	rows, err := telemetryStore.db.QueryContext(ctx, `SELECT `+spanColumns+` FROM spans s WHERE `+strings.Join(conditions, " AND ")+`
		ORDER BY s.start_time_ms DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	candidates := make([]dbSpan, 0)
	for rows.Next() {
		span, err := scanSpan(rows)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, span)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	traces := make(map[string]*model.Trace)
	result := make([]model.SpanWithContext, 0, min(len(candidates), requestedLimit))
	for _, candidate := range candidates {
		trace := traces[candidate.traceID]
		if trace == nil {
			trace, err = telemetryStore.GetTrace(ctx, candidate.traceID)
			if err != nil {
				return nil, err
			}
			traces[candidate.traceID] = trace
		}
		if trace == nil {
			continue
		}
		for _, span := range trace.Spans {
			if span.SpanID != candidate.spanID || strings.HasPrefix(span.OperationName, "[missing parent ") {
				continue
			}
			item := spanContext(trace, span)
			if filter.ParentOperation != "" && (item.ParentOperationName == nil || !strings.Contains(strings.ToLower(*item.ParentOperationName), strings.ToLower(filter.ParentOperation))) {
				continue
			}
			result = append(result, *item)
			break
		}
		if len(result) >= requestedLimit {
			break
		}
	}
	return result, nil
}

func referenceTraceStats(ctx context.Context, telemetryStore *Store, groupBy, aggregate string, filter TraceFilter, limit int) ([]model.Stat, error) {
	queryLimit := unrestrictedStatsRows
	if strings.HasPrefix(groupBy, "attr.") || filter.Operation != "" || len(filter.Attributes) > 0 {
		queryLimit = 5000
	}
	items, err := telemetryStore.ListTraceSummaries(ctx, filter, queryLimit)
	if err != nil {
		return nil, err
	}
	type bucket struct {
		durations []float64
		errors    int
	}
	buckets := make(map[string]*bucket)
	for _, item := range items {
		group := "unknown"
		switch {
		case groupBy == "service":
			group = item.ServiceName
		case groupBy == "operation":
			group = item.RootOperationName
		case groupBy == "status":
			if item.ErrorCount > 0 {
				group = "error"
			} else {
				group = "ok"
			}
		case strings.HasPrefix(groupBy, "attr."):
			var value string
			err := telemetryStore.db.QueryRowContext(ctx, `SELECT value FROM span_attributes WHERE trace_id=? AND key=? LIMIT 1`, item.TraceID, strings.TrimPrefix(groupBy, "attr.")).Scan(&value)
			if err == nil {
				group = value
			} else if err != sql.ErrNoRows {
				return nil, err
			}
		}
		entry := buckets[group]
		if entry == nil {
			entry = &bucket{}
			buckets[group] = entry
		}
		entry.durations = append(entry.durations, item.DurationMs)
		if item.ErrorCount > 0 {
			entry.errors++
		}
	}
	result := make([]model.Stat, 0, len(buckets))
	for group, bucket := range buckets {
		value := float64(len(bucket.durations))
		switch aggregate {
		case "avg_duration":
			value = average(bucket.durations)
		case "p95_duration":
			value = percentile95(bucket.durations)
		case "error_rate":
			value = float64(bucket.errors) / float64(len(bucket.durations))
		}
		result = append(result, model.Stat{Group: group, Value: value, Count: len(bucket.durations)})
	}
	sortStats(result)
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func referenceLogStats(ctx context.Context, telemetryStore *Store, groupBy string, filter LogFilter, limit int) ([]model.Stat, error) {
	queryLimit := unrestrictedStatsRows
	if strings.HasPrefix(groupBy, "attr.") || len(filter.Attributes) > 0 {
		queryLimit = 5000
	}
	items, err := telemetryStore.SearchLogs(ctx, filter, queryLimit)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int)
	for _, item := range items {
		group := "unknown"
		switch {
		case groupBy == "service":
			group = item.ServiceName
		case groupBy == "severity":
			group = strings.ToUpper(item.SeverityText)
		case groupBy == "scope" && item.ScopeName != nil:
			group = *item.ScopeName
		case strings.HasPrefix(groupBy, "attr."):
			if value, ok := item.Attributes[strings.TrimPrefix(groupBy, "attr.")]; ok {
				group = value
			}
		}
		counts[group]++
	}
	result := make([]model.Stat, 0, len(counts))
	for group, count := range counts {
		result = append(result, model.Stat{Group: group, Value: float64(count), Count: count})
	}
	sortStats(result)
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func referenceSearchAICalls(ctx context.Context, telemetryStore *Store, filter AIFilter, limit int) ([]model.AICallSummary, error) {
	conditions := []string{"s.operation_name LIKE 'ai.%'", "s.operation_name NOT LIKE 'ai.%.do%'", "s.start_time_ms>=?"}
	args := []any{filter.SinceMs}
	for _, pair := range []struct{ column, value string }{{"s.service_name", filter.Service}, {"s.trace_id", filter.TraceID}, {"s.status", filter.Status}} {
		if pair.value != "" {
			conditions, args = append(conditions, pair.column+"=?"), append(args, pair.value)
		}
	}
	if filter.Operation != "" {
		conditions, args = append(conditions, "s.operation_name LIKE ?"), append(args, "ai."+filter.Operation+"%")
	}
	if filter.MinDurationMs != nil {
		conditions, args = append(conditions, "s.duration_ms>=?"), append(args, *filter.MinDurationMs)
	}
	for _, pair := range []struct{ key, value string }{
		{aiAttributeNames["sessionId"], filter.SessionID}, {aiAttributeNames["functionId"], filter.FunctionID},
		{aiAttributeNames["provider"], filter.Provider}, {aiAttributeNames["model"], filter.Model},
	} {
		if pair.value != "" {
			conditions = append(conditions, `EXISTS (SELECT 1 FROM span_attributes a WHERE a.trace_id=s.trace_id AND a.span_id=s.span_id AND a.key=? AND a.value=?)`)
			args = append(args, pair.key, pair.value)
		}
	}
	if filter.Text != "" {
		match, matchArgs := textMatch("a.value", filter.Text)
		conditions = append(conditions, `EXISTS (SELECT 1 FROM span_attributes a WHERE a.trace_id=s.trace_id AND a.span_id=s.span_id
			AND a.key IN (`+quotedKeys(aiContentKeys)+`) AND `+match+")")
		args = append(args, matchArgs...)
	}
	args = append(args, limit)
	rows, err := telemetryStore.db.QueryContext(ctx, `SELECT `+spanColumns+` FROM spans s WHERE `+strings.Join(conditions, " AND ")+` ORDER BY s.start_time_ms DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.AICallSummary, 0)
	for rows.Next() {
		span, err := scanSpan(rows)
		if err != nil {
			return nil, err
		}
		tags := merged(decodeObject(span.resourceJSON), decodeObject(span.attributesJSON))
		var toolCallCount int
		if err := telemetryStore.db.QueryRowContext(ctx, `SELECT count(*) FROM spans WHERE trace_id=? AND parent_span_id=? AND operation_name LIKE 'ai.toolCall%'`, span.traceID, span.spanID).Scan(&toolCallCount); err != nil {
			return nil, err
		}
		prompt := attribute(tags, aiAttributeNames["promptMessages"])
		if prompt == nil {
			prompt = attribute(tags, aiAttributeNames["prompt"])
		}
		result = append(result, model.AICallSummary{
			TraceID: span.traceID, SpanID: span.spanID, Operation: aiOperation(span.operationName), Service: span.serviceName,
			FunctionID: attribute(tags, aiAttributeNames["functionId"]), Provider: attribute(tags, aiAttributeNames["provider"]), Model: attribute(tags, aiAttributeNames["model"]),
			Status: span.status, StartedAt: model.ISOTime(span.startTimeMs), DurationMs: span.durationMs,
			SessionID: attribute(tags, aiAttributeNames["sessionId"]), UserID: attribute(tags, aiAttributeNames["userId"]),
			PromptPreview: preview(prompt), ResponsePreview: preview(attribute(tags, aiAttributeNames["responseText"])),
			FinishReason: attribute(tags, aiAttributeNames["finishReason"]), ToolCallCount: toolCallCount, Usage: usage(tags),
		})
	}
	return result, rows.Err()
}

func referenceAIStats(ctx context.Context, telemetryStore *Store, groupBy, aggregate string, filter AIFilter, limit int) ([]model.Stat, error) {
	if groupBy == "status" && aggregate != "count" && aggregate != "avg_duration" {
		return []model.Stat{}, nil
	}
	items, err := referenceSearchAICalls(ctx, telemetryStore, filter, unrestrictedStatsRows)
	if err != nil {
		return nil, err
	}
	type bucket struct{ durations, inputs, outputs []float64 }
	buckets := make(map[string]*bucket)
	for _, item := range items {
		group := "unknown"
		var value *string
		switch groupBy {
		case "provider":
			value = item.Provider
		case "model":
			value = item.Model
		case "functionId":
			value = item.FunctionID
		case "sessionId":
			value = item.SessionID
		case "status":
			group = item.Status
		}
		if value != nil {
			group = *value
		}
		entry := buckets[group]
		if entry == nil {
			entry = &bucket{}
			buckets[group] = entry
		}
		entry.durations = append(entry.durations, item.DurationMs)
		entry.inputs = append(entry.inputs, numberOrZero(item.Usage.InputTokens))
		entry.outputs = append(entry.outputs, numberOrZero(item.Usage.OutputTokens))
	}
	result := make([]model.Stat, 0, len(buckets))
	for group, entry := range buckets {
		value := float64(len(entry.durations))
		switch aggregate {
		case "avg_duration":
			value = average(entry.durations)
		case "p95_duration":
			value = percentile95(entry.durations)
		case "total_input_tokens":
			value = sum(entry.inputs)
		case "total_output_tokens":
			value = sum(entry.outputs)
		}
		result = append(result, model.Stat{Group: group, Value: value, Count: len(entry.durations)})
	}
	sortStats(result)
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func assertSame[T any](t *testing.T, got []T, err error, want []T) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SQL result differs from reference\ngot:  %#v\nwant: %#v", got, want)
	}
}

func floatPointer(value float64) *float64 { return &value }

func percentile95(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	copyValues := append([]float64(nil), values...)
	sort.Float64s(copyValues)
	index := int(math.Ceil(float64(len(copyValues))*0.95)) - 1
	index = max(0, min(index, len(copyValues)-1))
	return copyValues[index]
}

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return sum(values) / float64(len(values))
}

func sum(values []float64) float64 {
	result := 0.0
	for _, value := range values {
		result += value
	}
	return result
}

func numberOrZero(value *float64) float64 {
	if value == nil {
		return 0
	}
	return *value
}

func sortStats(values []model.Stat) {
	sort.Slice(values, func(i, j int) bool {
		if values[i].Value == values[j].Value {
			return values[i].Group < values[j].Group
		}
		return values[i].Value > values[j].Value
	})
}
