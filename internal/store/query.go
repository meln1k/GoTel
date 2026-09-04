package store

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/meln1k/gotel/internal/model"
)

type TraceCursor struct {
	StartedAt int64  `json:"startedAt"`
	ID        string `json:"id"`
}

type LogCursor struct {
	Timestamp int64  `json:"timestamp"`
	ID        string `json:"id"`
}

type TraceFilter struct {
	Service, Operation, Status, AIText string
	MinDurationMs                      *float64
	Attributes                         map[string]string
	SinceMs                            int64
	Cursor                             *TraceCursor
}

type SpanFilter struct {
	Service, TraceID, Operation, ParentOperation, Status string
	Attributes, AttributeContains                        map[string]string
	SinceMs                                              int64
}

type LogFilter struct {
	Service, Severity, TraceID, SpanID, Body string
	Attributes, AttributeContains            map[string]string
	SinceMs                                  int64
	Cursor                                   *LogCursor
}

func (s *Store) ListServices(ctx context.Context) ([]string, error) {
	cutoff := time.Now().Add(-time.Duration(s.config.TraceLookbackMinutes) * time.Minute).UnixMilli()
	rows, err := s.db.QueryContext(ctx, `SELECT service_name FROM (
		SELECT DISTINCT service_name FROM spans WHERE start_time_ms>=?
		UNION SELECT DISTINCT service_name FROM logs WHERE timestamp_ms>=?
	) services ORDER BY service_name ASC`, cutoff, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]string, 0)
	for rows.Next() {
		var service string
		if err := rows.Scan(&service); err != nil {
			return nil, err
		}
		result = append(result, service)
	}
	return result, rows.Err()
}

func (s *Store) ListTraceSummaries(ctx context.Context, filter TraceFilter, limit int) ([]model.TraceSummary, error) {
	where, args := traceSummaryFilter(filter)
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `SELECT t.trace_id, t.service_name, t.root_operation_name,
		t.started_at_ms, t.ended_at_ms, t.active_span_count, t.span_count, t.error_count
		FROM trace_summaries t WHERE `+where+`
		ORDER BY t.started_at_ms DESC, t.trace_id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.TraceSummary, 0)
	now := time.Now().UnixMilli()
	for rows.Next() {
		var traceID string
		var facts traceFacts
		if err := rows.Scan(&traceID, &facts.rootService, &facts.rootOperation, &facts.started,
			&facts.ended, &facts.activeSpanCount, &facts.spanCount, &facts.errorCount); err != nil {
			return nil, err
		}
		result = append(result, facts.summary(traceID, now))
	}
	return result, rows.Err()
}

func traceSummaryFilter(filter TraceFilter) (string, []any) {
	conditions := []string{"t.started_at_ms>=?"}
	args := []any{filter.SinceMs}
	if filter.Service != "" {
		conditions = append(conditions, "t.service_name=?")
		args = append(args, filter.Service)
	}
	if filter.Status == "error" {
		conditions = append(conditions, "t.error_count>0")
	} else if filter.Status == "ok" {
		conditions = append(conditions, "t.error_count=0")
	}
	if filter.MinDurationMs != nil {
		conditions = append(conditions, "t.duration_ms>=?")
		args = append(args, *filter.MinDurationMs)
	}
	if filter.Operation != "" {
		match, matchArgs := textMatch("s.operation_name", filter.Operation)
		conditions = append(conditions, "EXISTS (SELECT 1 FROM spans s WHERE s.trace_id=t.trace_id AND "+match+")")
		args = append(args, matchArgs...)
	}
	if len(filter.Attributes) > 0 {
		expression, attrArgs := spanAttributeMatch("t.trace_id", "", filter.Attributes, nil)
		conditions = append(conditions, expression)
		args = append(args, attrArgs...)
	}
	if filter.AIText != "" {
		match, matchArgs := textMatch("a.value", filter.AIText)
		conditions = append(conditions, `EXISTS (SELECT 1 FROM span_attributes a WHERE a.trace_id=t.trace_id
			AND a.key IN (`+quotedKeys(aiContentKeys)+`) AND `+match+")")
		args = append(args, matchArgs...)
	}
	if filter.Cursor != nil {
		conditions = append(conditions, "(t.started_at_ms<? OR (t.started_at_ms=? AND t.trace_id<?))")
		args = append(args, filter.Cursor.StartedAt, filter.Cursor.StartedAt, filter.Cursor.ID)
	}
	return strings.Join(conditions, " AND "), args
}

func (s *Store) GetSpan(ctx context.Context, spanID string) (*model.SpanWithContext, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+spanColumns+` FROM spans WHERE span_id=? LIMIT 1`, spanID)
	span, err := scanSpan(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	trace, err := s.GetTrace(ctx, span.traceID)
	if err != nil || trace == nil {
		return nil, err
	}
	for _, item := range trace.Spans {
		if item.SpanID == spanID && !strings.HasPrefix(item.OperationName, "[missing parent ") {
			return spanContext(trace, item), nil
		}
	}
	return nil, nil
}

func (s *Store) ListTraceSpans(ctx context.Context, traceID string) ([]model.SpanWithContext, error) {
	trace, err := s.GetTrace(ctx, traceID)
	if err != nil || trace == nil {
		return []model.SpanWithContext{}, err
	}
	result := make([]model.SpanWithContext, 0, len(trace.Spans))
	for _, span := range trace.Spans {
		result = append(result, *spanContext(trace, span))
	}
	return result, nil
}

func (s *Store) SearchSpans(ctx context.Context, filter SpanFilter, requestedLimit int) ([]model.SpanWithContext, error) {
	conditions := []string{"s.start_time_ms>=?"}
	args := []any{filter.SinceMs}
	if filter.Service != "" {
		conditions = append(conditions, "s.service_name=?")
		args = append(args, filter.Service)
	}
	if filter.TraceID != "" {
		conditions = append(conditions, "s.trace_id=?")
		args = append(args, filter.TraceID)
	}
	if filter.Operation != "" {
		match, matchArgs := textMatch("s.operation_name", filter.Operation)
		conditions = append(conditions, match)
		args = append(args, matchArgs...)
	}
	if filter.Status == "ok" || filter.Status == "error" {
		conditions = append(conditions, "s.status=?")
		args = append(args, filter.Status)
	}
	if len(filter.Attributes) > 0 {
		expression, attrArgs := spanAttributeMatch("s.trace_id", "s.span_id", filter.Attributes, nil)
		conditions = append(conditions, expression)
		args = append(args, attrArgs...)
	}
	if len(filter.AttributeContains) > 0 {
		expression, attrArgs := spanAttributeMatch("s.trace_id", "s.span_id", nil, filter.AttributeContains)
		conditions = append(conditions, expression)
		args = append(args, attrArgs...)
	}
	candidateLimit := requestedLimit
	if filter.ParentOperation != "" {
		candidateLimit = max(requestedLimit*10, 200)
		if len(filter.AttributeContains) > 0 {
			candidateLimit = max(requestedLimit*20, 500)
		}
	}
	args = append(args, candidateLimit)
	parentJoin := ""
	parentCondition := ""
	if filter.ParentOperation != "" {
		parentJoin = `LEFT JOIN spans parent ON parent.trace_id=c.trace_id AND parent.span_id=c.parent_span_id`
		parentCondition = `WHERE c.parent_span_id IS NOT NULL AND c.parent_span_id<>'' AND contains(lower(
			CASE WHEN parent.span_id IS NULL THEN '[missing parent ' || left(c.parent_span_id, 8) || ']' ELSE parent.operation_name END
		), lower(?))`
		args = append(args, filter.ParentOperation)
	}
	args = append(args, requestedLimit)
	rows, err := s.db.QueryContext(ctx, `WITH candidates AS (
		SELECT s.trace_id, s.span_id, s.parent_span_id, s.start_time_ms FROM spans s
		WHERE `+strings.Join(conditions, " AND ")+` ORDER BY s.start_time_ms DESC LIMIT ?
	)
	SELECT c.trace_id, c.span_id FROM candidates c `+parentJoin+` `+parentCondition+`
	ORDER BY c.start_time_ms DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	type spanKey struct{ traceID, spanID string }
	candidates := make([]spanKey, 0)
	traceIDs := make([]string, 0)
	seenTrace := make(map[string]bool)
	for rows.Next() {
		var candidate spanKey
		if err := rows.Scan(&candidate.traceID, &candidate.spanID); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, candidate)
		if !seenTrace[candidate.traceID] {
			seenTrace[candidate.traceID] = true
			traceIDs = append(traceIDs, candidate.traceID)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	traces, err := s.loadTraces(ctx, traceIDs)
	if err != nil {
		return nil, err
	}
	result := make([]model.SpanWithContext, 0, min(len(candidates), requestedLimit))
	for _, candidate := range candidates {
		trace := traces[candidate.traceID]
		if trace == nil {
			continue
		}
		for _, span := range trace.Spans {
			if span.SpanID != candidate.spanID || strings.HasPrefix(span.OperationName, "[missing parent ") {
				continue
			}
			item := spanContext(trace, span)
			result = append(result, *item)
			break
		}
		if len(result) >= requestedLimit {
			break
		}
	}
	return result, nil
}

func spanContext(trace *model.Trace, span model.TraceSpan) *model.SpanWithContext {
	var parentOperation *string
	if span.ParentSpanID != nil {
		for _, candidate := range trace.Spans {
			if candidate.SpanID == *span.ParentSpanID {
				value := candidate.OperationName
				parentOperation = &value
				break
			}
		}
	}
	return &model.SpanWithContext{TraceID: trace.TraceID, RootOperationName: trace.RootOperationName, ParentOperationName: parentOperation, Span: span}
}

func (s *Store) SearchLogs(ctx context.Context, filter LogFilter, limit int) ([]model.Log, error) {
	where, args := logFilterSQL(filter)
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `SELECT l.id, l.trace_id, l.span_id, l.service_name, l.scope_name,
		l.severity_text, l.timestamp_ms, l.body, l.attributes_json, l.resource_json
		FROM logs l WHERE `+where+` ORDER BY l.timestamp_ms DESC, l.id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.Log, 0)
	for rows.Next() {
		var id int64
		var traceID, spanID, scope sql.NullString
		var item model.Log
		var timestamp int64
		var attributesJSON, resourceJSON string
		if err := rows.Scan(&id, &traceID, &spanID, &item.ServiceName, &scope, &item.SeverityText,
			&timestamp, &item.Body, &attributesJSON, &resourceJSON); err != nil {
			return nil, err
		}
		item.ID = strconv.FormatInt(id, 10)
		item.Timestamp = model.ISOTime(timestamp)
		item.TraceID, item.SpanID, item.ScopeName = nullString(traceID), nullString(spanID), nullString(scope)
		item.Attributes = merged(decodeObject(resourceJSON), decodeObject(attributesJSON))
		result = append(result, item)
	}
	return result, rows.Err()
}

func logFilterSQL(filter LogFilter) (string, []any) {
	conditions := []string{"l.timestamp_ms>=?"}
	args := []any{filter.SinceMs}
	if filter.Service != "" {
		conditions = append(conditions, "l.service_name=?")
		args = append(args, filter.Service)
	}
	if filter.Severity != "" {
		conditions = append(conditions, "lower(l.severity_text)=lower(?)")
		args = append(args, filter.Severity)
	}
	if filter.TraceID != "" {
		conditions = append(conditions, "l.trace_id=?")
		args = append(args, filter.TraceID)
	}
	if filter.SpanID != "" {
		conditions = append(conditions, "l.span_id=?")
		args = append(args, filter.SpanID)
	}
	if filter.Body != "" {
		match, matchArgs := textMatch("l.body", filter.Body)
		conditions = append(conditions, match)
		args = append(args, matchArgs...)
	}
	if len(filter.Attributes) > 0 {
		expression, attrArgs := logAttributeMatch(filter.Attributes, nil)
		conditions = append(conditions, expression)
		args = append(args, attrArgs...)
	}
	if len(filter.AttributeContains) > 0 {
		expression, attrArgs := logAttributeMatch(nil, filter.AttributeContains)
		conditions = append(conditions, expression)
		args = append(args, attrArgs...)
	}
	if filter.Cursor != nil {
		id, _ := strconv.ParseInt(filter.Cursor.ID, 10, 64)
		conditions = append(conditions, "(l.timestamp_ms<? OR (l.timestamp_ms=? AND l.id<?))")
		args = append(args, filter.Cursor.Timestamp, filter.Cursor.Timestamp, id)
	}
	return strings.Join(conditions, " AND "), args
}

var wordPattern = regexp.MustCompile(`[A-Za-z0-9_]+`)

func textMatch(column, query string) (string, []any) {
	words := wordPattern.FindAllString(query, -1)
	conditions := make([]string, 0, len(words))
	args := make([]any, 0, len(words))
	for _, word := range words {
		if len(word) <= 1 {
			continue
		}
		conditions = append(conditions, "regexp_matches(lower("+column+"), ?)")
		args = append(args, `(^|[^a-z0-9_])`+strings.ToLower(word))
	}
	if len(conditions) == 0 {
		return "contains(lower(" + column + "), lower(?))", []any{query}
	}
	return "(" + strings.Join(conditions, " AND ") + ")", args
}

func spanAttributeMatch(traceColumn, spanColumn string, exact, contains map[string]string) (string, []any) {
	predicates := make([]string, 0, len(exact)+len(contains))
	args := make([]any, 0, 2*(len(exact)+len(contains))+1)
	for key, value := range exact {
		predicates = append(predicates, "(a.key=? AND a.value=?)")
		args = append(args, key, value)
	}
	for key, value := range contains {
		predicates = append(predicates, "(a.key=? AND contains(lower(a.value), lower(?)))")
		args = append(args, key, value)
	}
	link := "a.trace_id=" + traceColumn
	if spanColumn != "" {
		link += " AND a.span_id=" + spanColumn
	}
	args = append(args, len(predicates))
	return `EXISTS (SELECT 1 FROM span_attributes a WHERE ` + link + ` AND (` + strings.Join(predicates, " OR ") + `)
		GROUP BY a.trace_id, a.span_id HAVING count(DISTINCT a.key)=?)`, args
}

func logAttributeMatch(exact, contains map[string]string) (string, []any) {
	predicates := make([]string, 0, len(exact)+len(contains))
	args := make([]any, 0, 2*(len(exact)+len(contains))+1)
	for key, value := range exact {
		predicates = append(predicates, "(a.key=? AND a.value=?)")
		args = append(args, key, value)
	}
	for key, value := range contains {
		predicates = append(predicates, "(a.key=? AND contains(lower(a.value), lower(?)))")
		args = append(args, key, value)
	}
	args = append(args, len(predicates))
	return `EXISTS (SELECT 1 FROM log_attributes a WHERE a.log_id=l.id AND (` + strings.Join(predicates, " OR ") + `)
		GROUP BY a.log_id HAVING count(DISTINCT a.key)=?)`, args
}

var aiContentKeys = []string{
	"ai.prompt", "ai.prompt.messages", "ai.response.text", "ai.response.toolCalls",
	"gen_ai.prompt", "gen_ai.completion", "gen_ai.input.messages", "gen_ai.output.messages",
	"input.value", "output.value",
}

func quotedKeys(keys []string) string {
	quoted := make([]string, len(keys))
	for i, key := range keys {
		quoted[i] = "'" + strings.ReplaceAll(key, "'", "''") + "'"
	}
	return strings.Join(quoted, ",")
}

func parseMillis(value string) int64 {
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed.UnixMilli()
}

func traceCursorFor(summary model.TraceSummary) TraceCursor {
	return TraceCursor{StartedAt: parseMillis(summary.StartedAt), ID: summary.TraceID}
}

func logCursorFor(item model.Log) LogCursor {
	return LogCursor{Timestamp: parseMillis(item.Timestamp), ID: item.ID}
}

func DebugCursor(kind string, value any) string { return fmt.Sprintf("%s:%v", kind, value) }
