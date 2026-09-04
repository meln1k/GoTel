package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/meln1k/gotel/internal/model"
)

const unrestrictedStatsRows = int(^uint(0) >> 1)

func (s *Store) TraceStats(ctx context.Context, groupBy, aggregate string, filter TraceFilter, limit int) ([]model.Stat, error) {
	queryLimit := unrestrictedStatsRows
	if strings.HasPrefix(groupBy, "attr.") || filter.Operation != "" || len(filter.Attributes) > 0 {
		queryLimit = 5000
	}
	where, filterArgs := traceSummaryFilter(filter)
	groupExpression := `'unknown'`
	groupArgs := make([]any, 0, 1)
	switch {
	case groupBy == "service":
		groupExpression = "f.service_name"
	case groupBy == "operation":
		groupExpression = "f.root_operation_name"
	case groupBy == "status":
		groupExpression = "CASE WHEN f.error_count>0 THEN 'error' ELSE 'ok' END"
	case strings.HasPrefix(groupBy, "attr."):
		groupExpression = `coalesce((SELECT a.value FROM span_attributes a
			WHERE a.trace_id=f.trace_id AND a.key=? LIMIT 1), 'unknown')`
		groupArgs = append(groupArgs, strings.TrimPrefix(groupBy, "attr."))
	}
	args := append([]any{time.Now().UnixMilli()}, filterArgs...)
	args = append(args, queryLimit)
	args = append(args, groupArgs...)
	cte := `WITH filtered AS (
		SELECT t.trace_id, t.service_name, t.root_operation_name,
			CASE WHEN t.active_span_count>0 THEN greatest(0, ?-t.started_at_ms)::DOUBLE ELSE t.duration_ms END AS duration_ms,
			t.error_count
		FROM trace_summaries t WHERE ` + where + `
		ORDER BY t.started_at_ms DESC, t.trace_id DESC LIMIT ?
	), bucketed AS (
		SELECT ` + groupExpression + ` AS group_name, f.duration_ms, f.error_count FROM filtered f
	)`
	return s.aggregateStats(ctx, cte, aggregate, limit, args)
}

func (s *Store) LogStats(ctx context.Context, groupBy string, filter LogFilter, limit int) ([]model.Stat, error) {
	queryLimit := unrestrictedStatsRows
	if strings.HasPrefix(groupBy, "attr.") || len(filter.Attributes) > 0 {
		queryLimit = 5000
	}
	where, filterArgs := logFilterSQL(filter)
	groupExpression := `'unknown'`
	groupArgs := make([]any, 0, 1)
	switch {
	case groupBy == "service":
		groupExpression = "f.service_name"
	case groupBy == "severity":
		groupExpression = "upper(f.severity_text)"
	case groupBy == "scope":
		groupExpression = "coalesce(f.scope_name, 'unknown')"
	case strings.HasPrefix(groupBy, "attr."):
		groupExpression = `coalesce((SELECT a.value FROM log_attributes a WHERE a.log_id=f.id AND a.key=? LIMIT 1), 'unknown')`
		groupArgs = append(groupArgs, strings.TrimPrefix(groupBy, "attr."))
	}
	args := append(filterArgs, queryLimit)
	args = append(args, groupArgs...)
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `WITH filtered AS (
		SELECT l.id, l.service_name, l.scope_name, l.severity_text FROM logs l WHERE `+where+`
		ORDER BY l.timestamp_ms DESC, l.id DESC LIMIT ?
	), bucketed AS (
		SELECT `+groupExpression+` AS group_name FROM filtered f
	)
	SELECT group_name, count(*)::DOUBLE AS value, count(*) AS item_count FROM bucketed
	GROUP BY group_name ORDER BY value DESC, group_name ASC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	return scanStats(rows)
}

func (s *Store) aggregateStats(ctx context.Context, cte, aggregate string, limit int, args []any) ([]model.Stat, error) {
	if aggregate == "p95_duration" {
		args = append(args, limit)
		rows, err := s.db.QueryContext(ctx, cte+`, ranked AS (
			SELECT group_name, duration_ms,
				row_number() OVER (PARTITION BY group_name ORDER BY duration_ms ASC) AS duration_rank,
				count(*) OVER (PARTITION BY group_name) AS item_count
			FROM bucketed
		)
		SELECT group_name,
			max(CASE WHEN duration_rank=ceil(item_count*0.95)::BIGINT THEN duration_ms END) AS value,
			max(item_count) AS item_count
		FROM ranked GROUP BY group_name ORDER BY value DESC, group_name ASC LIMIT ?`, args...)
		if err != nil {
			return nil, err
		}
		return scanStats(rows)
	}
	expression := "count(*)::DOUBLE"
	switch aggregate {
	case "avg_duration":
		expression = "avg(duration_ms)"
	case "error_rate":
		expression = "sum(CASE WHEN error_count>0 THEN 1 ELSE 0 END)::DOUBLE/count(*)"
	case "total_input_tokens":
		expression = "sum(input_tokens)"
	case "total_output_tokens":
		expression = "sum(output_tokens)"
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, cte+`
		SELECT group_name, `+expression+` AS value, count(*) AS item_count FROM bucketed
		GROUP BY group_name ORDER BY value DESC, group_name ASC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	return scanStats(rows)
}

func scanStats(rows *sql.Rows) ([]model.Stat, error) {
	defer rows.Close()
	result := make([]model.Stat, 0)
	for rows.Next() {
		var item model.Stat
		if err := rows.Scan(&item.Group, &item.Value, &item.Count); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) ListFacets(ctx context.Context, telemetryType, field, key, service string, sinceMs int64, limit int) ([]model.Facet, error) {
	var query string
	args := make([]any, 0)
	switch telemetryType + ":" + field {
	case "logs:service":
		query = `SELECT service_name, count(*) FROM logs WHERE timestamp_ms>=? GROUP BY service_name ORDER BY count(*) DESC, service_name ASC LIMIT ?`
		args = append(args, sinceMs, limit)
	case "logs:severity":
		query = `SELECT upper(severity_text), count(*) FROM logs WHERE timestamp_ms>=?`
		args = append(args, sinceMs)
		if service != "" {
			query += ` AND service_name=?`
			args = append(args, service)
		}
		query += ` GROUP BY upper(severity_text) ORDER BY count(*) DESC, upper(severity_text) ASC LIMIT ?`
		args = append(args, limit)
	case "logs:scope":
		query = `SELECT coalesce(scope_name, 'unknown'), count(*) FROM logs WHERE timestamp_ms>=?`
		args = append(args, sinceMs)
		if service != "" {
			query += ` AND service_name=?`
			args = append(args, service)
		}
		query += ` GROUP BY coalesce(scope_name, 'unknown') ORDER BY count(*) DESC, coalesce(scope_name, 'unknown') ASC LIMIT ?`
		args = append(args, limit)
	case "traces:service":
		query = `SELECT service_name, count(*) FROM trace_summaries WHERE started_at_ms>=? GROUP BY service_name ORDER BY count(*) DESC, service_name ASC LIMIT ?`
		args = append(args, sinceMs, limit)
	case "traces:operation":
		query = `SELECT root_operation_name, count(*) FROM trace_summaries WHERE started_at_ms>=?`
		args = append(args, sinceMs)
		if service != "" {
			query += ` AND service_name=?`
			args = append(args, service)
		}
		query += ` GROUP BY root_operation_name ORDER BY count(*) DESC, root_operation_name ASC LIMIT ?`
		args = append(args, limit)
	case "traces:status":
		query = `SELECT CASE WHEN error_count>0 THEN 'error' ELSE 'ok' END, count(*) FROM trace_summaries WHERE started_at_ms>=?`
		args = append(args, sinceMs)
		if service != "" {
			query += ` AND service_name=?`
			args = append(args, service)
		}
		query += ` GROUP BY CASE WHEN error_count>0 THEN 'error' ELSE 'ok' END ORDER BY count(*) DESC LIMIT ?`
		args = append(args, limit)
	case "traces:attribute_keys":
		query = `SELECT a.key, count(DISTINCT a.trace_id) AS trace_count
			FROM span_attributes a JOIN trace_summaries t ON t.trace_id=a.trace_id
			WHERE t.started_at_ms>=? AND length(a.value)<512`
		args = append(args, sinceMs)
		if service != "" {
			query += ` AND t.service_name=?`
			args = append(args, service)
		}
		query += ` GROUP BY a.key ORDER BY (count(DISTINCT a.value)>1) DESC, count(DISTINCT a.value) DESC, trace_count DESC, a.key ASC LIMIT ?`
		args = append(args, limit)
	case "traces:attribute_values":
		if key == "" {
			return []model.Facet{}, nil
		}
		query = `SELECT a.value, count(DISTINCT a.trace_id) FROM span_attributes a
			JOIN spans s ON s.trace_id=a.trace_id AND s.span_id=a.span_id
			WHERE a.key=? AND length(a.value)<512 AND s.start_time_ms>=?`
		args = append(args, key, sinceMs)
		if service != "" {
			query += ` AND s.service_name=?`
			args = append(args, service)
		}
		query += ` GROUP BY a.value ORDER BY count(DISTINCT a.trace_id) DESC, a.value ASC LIMIT ?`
		args = append(args, limit)
	default:
		return []model.Facet{}, nil
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.Facet, 0)
	for rows.Next() {
		var item model.Facet
		if err := rows.Scan(&item.Value, &item.Count); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

type AIFilter struct {
	Service, TraceID, SessionID, FunctionID, Provider, Model, Operation, Status, Text string
	MinDurationMs                                                                     *float64
	SinceMs                                                                           int64
}

var aiAttributeNames = map[string]string{
	"functionId": "ai.telemetry.functionId", "provider": "ai.model.provider", "model": "ai.model.id",
	"sessionId": "ai.telemetry.metadata.sessionId", "userId": "ai.telemetry.metadata.userId",
	"finishReason": "ai.response.finishReason", "inputTokens": "ai.usage.inputTokens",
	"outputTokens": "ai.usage.outputTokens", "totalTokens": "ai.usage.totalTokens",
	"cachedInputTokens": "ai.usage.cachedInputTokens", "reasoningTokens": "ai.usage.reasoningTokens",
	"promptMessages": "ai.prompt.messages", "prompt": "ai.prompt", "responseText": "ai.response.text",
	"tools": "ai.prompt.tools", "providerMetadata": "ai.response.providerMetadata",
}

func (s *Store) SearchAICalls(ctx context.Context, filter AIFilter, limit int) ([]model.AICallSummary, error) {
	where, args := aiFilterSQL(filter)
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `WITH filtered AS (
		SELECT `+spanColumns+` FROM spans s WHERE `+where+` ORDER BY s.start_time_ms DESC LIMIT ?
	), tool_counts AS (
		SELECT tools.trace_id, tools.parent_span_id, count(*) AS tool_call_count FROM spans tools
		JOIN filtered f ON f.trace_id=tools.trace_id AND f.span_id=tools.parent_span_id
		WHERE tools.operation_name LIKE 'ai.toolCall%' GROUP BY tools.trace_id, tools.parent_span_id
	)
	SELECT f.*, coalesce(tool_counts.tool_call_count, 0) FROM filtered f
	LEFT JOIN tool_counts ON tool_counts.trace_id=f.trace_id AND tool_counts.parent_span_id=f.span_id
	ORDER BY f.start_time_ms DESC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.AICallSummary, 0)
	for rows.Next() {
		var span dbSpan
		var toolCallCount int
		targets := append(span.scanTargets(), &toolCallCount)
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		tags := merged(decodeObject(span.resourceJSON), decodeObject(span.attributesJSON))
		prompt := attribute(tags, aiAttributeNames["promptMessages"])
		if prompt == nil {
			prompt = attribute(tags, aiAttributeNames["prompt"])
		}
		result = append(result, model.AICallSummary{
			TraceID: span.traceID, SpanID: span.spanID, Operation: aiOperation(span.operationName), Service: span.serviceName,
			FunctionID: attribute(tags, aiAttributeNames["functionId"]), Provider: attribute(tags, aiAttributeNames["provider"]),
			Model: attribute(tags, aiAttributeNames["model"]), Status: span.status, StartedAt: model.ISOTime(span.startTimeMs),
			DurationMs: span.durationMs, SessionID: attribute(tags, aiAttributeNames["sessionId"]), UserID: attribute(tags, aiAttributeNames["userId"]),
			PromptPreview: preview(prompt), ResponsePreview: preview(attribute(tags, aiAttributeNames["responseText"])),
			FinishReason: attribute(tags, aiAttributeNames["finishReason"]), ToolCallCount: toolCallCount, Usage: usage(tags),
		})
	}
	return result, rows.Err()
}

func aiFilterSQL(filter AIFilter) (string, []any) {
	conditions := []string{"s.operation_name LIKE 'ai.%'", "s.operation_name NOT LIKE 'ai.%.do%'", "s.start_time_ms>=?"}
	args := []any{filter.SinceMs}
	for _, pair := range []struct{ column, value string }{
		{"s.service_name", filter.Service}, {"s.trace_id", filter.TraceID}, {"s.status", filter.Status},
	} {
		if pair.value != "" {
			conditions = append(conditions, pair.column+"=?")
			args = append(args, pair.value)
		}
	}
	if filter.Operation != "" {
		conditions = append(conditions, "s.operation_name LIKE ?")
		args = append(args, "ai."+filter.Operation+"%")
	}
	if filter.MinDurationMs != nil {
		conditions = append(conditions, "s.duration_ms>=?")
		args = append(args, *filter.MinDurationMs)
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
	return strings.Join(conditions, " AND "), args
}

func (s *Store) GetAICall(ctx context.Context, spanID string) (*model.AICallDetail, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+spanColumns+` FROM spans WHERE span_id=? AND operation_name LIKE 'ai.%' LIMIT 1`, spanID)
	span, err := scanSpan(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	tags := merged(decodeObject(span.resourceJSON), decodeObject(span.attributesJSON))
	toolRows, err := s.db.QueryContext(ctx, `SELECT `+spanColumns+` FROM spans WHERE trace_id=? AND parent_span_id=? AND operation_name LIKE 'ai.toolCall%' ORDER BY start_time_ms ASC`, span.traceID, span.spanID)
	if err != nil {
		return nil, err
	}
	tools := make([]model.AIToolCall, 0)
	for toolRows.Next() {
		tool, err := scanSpan(toolRows)
		if err != nil {
			toolRows.Close()
			return nil, err
		}
		toolTags := decodeObject(tool.attributesJSON)
		name := tool.operationName
		if value := attribute(toolTags, "ai.toolCall.name"); value != nil {
			name = *value
		}
		toolID := tool.spanID
		duration := tool.durationMs
		tools = append(tools, model.AIToolCall{Name: name, SpanID: &toolID, Status: tool.status, DurationMs: &duration})
	}
	toolRows.Close()
	logs, err := s.SearchLogs(ctx, LogFilter{SpanID: span.spanID, SinceMs: 0}, 100000)
	if err != nil {
		return nil, err
	}
	for left, right := 0, len(logs)-1; left < right; left, right = left+1, right-1 {
		logs[left], logs[right] = logs[right], logs[left]
	}
	return &model.AICallDetail{
		TraceID: span.traceID, SpanID: span.spanID, Operation: aiOperation(span.operationName), Service: span.serviceName,
		FunctionID: attribute(tags, aiAttributeNames["functionId"]), Provider: attribute(tags, aiAttributeNames["provider"]),
		Model: attribute(tags, aiAttributeNames["model"]), Status: span.status, StartedAt: model.ISOTime(span.startTimeMs), DurationMs: span.durationMs,
		SessionID: attribute(tags, aiAttributeNames["sessionId"]), UserID: attribute(tags, aiAttributeNames["userId"]),
		FinishReason: attribute(tags, aiAttributeNames["finishReason"]), PromptMessages: parsedFirstAttribute(tags, aiAttributeNames["promptMessages"], aiAttributeNames["prompt"]),
		ResponseText: attribute(tags, aiAttributeNames["responseText"]), ToolCalls: tools,
		ToolsAvailable: parsedAttribute(tags, aiAttributeNames["tools"]), ProviderMetadata: parsedAttribute(tags, aiAttributeNames["providerMetadata"]),
		Usage: usage(tags), Timing: model.AITiming{
			MsToFirstChunk: numberAttribute(tags, "ai.response.msToFirstChunk"), MsToFinish: numberAttribute(tags, "ai.response.msToFinish"),
			AvgOutputTokensPerSecond: numberAttribute(tags, "ai.response.avgOutputTokensPerSecond"),
		}, Logs: logs,
	}, nil
}

func (s *Store) AIStats(ctx context.Context, groupBy, aggregate string, filter AIFilter, limit int) ([]model.Stat, error) {
	if groupBy == "status" && aggregate != "count" && aggregate != "avg_duration" {
		return []model.Stat{}, nil
	}
	where, args := aiFilterSQL(filter)
	groupExpression := `'unknown'`
	switch groupBy {
	case "provider", "model", "functionId", "sessionId":
		key := aiAttributeNames[groupBy]
		groupExpression = `coalesce((SELECT a.value FROM span_attributes a
			WHERE a.trace_id=f.trace_id AND a.span_id=f.span_id AND a.key='` + strings.ReplaceAll(key, "'", "''") + `'), 'unknown')`
	case "status":
		groupExpression = "f.status"
	}
	inputKey := strings.ReplaceAll(aiAttributeNames["inputTokens"], "'", "''")
	outputKey := strings.ReplaceAll(aiAttributeNames["outputTokens"], "'", "''")
	cte := `WITH filtered AS (
		SELECT s.trace_id, s.span_id, s.status, s.duration_ms FROM spans s WHERE ` + where + `
	), bucketed AS (
		SELECT ` + groupExpression + ` AS group_name, f.duration_ms, 0 AS error_count,
			coalesce(CASE WHEN input.value=trim(input.value) THEN try_cast(input.value AS DOUBLE) END, 0) AS input_tokens,
			coalesce(CASE WHEN output.value=trim(output.value) THEN try_cast(output.value AS DOUBLE) END, 0) AS output_tokens
		FROM filtered f
		LEFT JOIN span_attributes input ON input.trace_id=f.trace_id AND input.span_id=f.span_id AND input.key='` + inputKey + `'
		LEFT JOIN span_attributes output ON output.trace_id=f.trace_id AND output.span_id=f.span_id AND output.key='` + outputKey + `'
	)`
	return s.aggregateStats(ctx, cte, aggregate, limit, args)
}

func attribute(tags map[string]string, key string) *string {
	value, ok := tags[key]
	if !ok {
		return nil
	}
	return &value
}

func numberAttribute(tags map[string]string, key string) *float64 {
	value, ok := tags[key]
	if !ok {
		return nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil
	}
	return &parsed
}

func usage(tags map[string]string) *model.AIUsage {
	return &model.AIUsage{
		InputTokens: numberAttribute(tags, aiAttributeNames["inputTokens"]), OutputTokens: numberAttribute(tags, aiAttributeNames["outputTokens"]),
		TotalTokens: numberAttribute(tags, aiAttributeNames["totalTokens"]), CachedInputTokens: numberAttribute(tags, aiAttributeNames["cachedInputTokens"]),
		ReasoningTokens: numberAttribute(tags, aiAttributeNames["reasoningTokens"]),
	}
}

func preview(value *string) *string {
	if value == nil {
		return nil
	}
	runes := []rune(*value)
	if len(runes) <= 200 {
		return value
	}
	result := string(runes[:200]) + "..."
	return &result
}

func parsedAttribute(tags map[string]string, key string) any {
	value, ok := tags[key]
	if !ok || value == "" {
		return nil
	}
	var decoded any
	if json.Unmarshal([]byte(value), &decoded) == nil {
		return decoded
	}
	return value
}

func parsedFirstAttribute(tags map[string]string, keys ...string) any {
	for _, key := range keys {
		if value, ok := tags[key]; ok && value != "" {
			return parsedAttribute(tags, key)
		}
	}
	return nil
}

func aiOperation(value string) string {
	value = strings.TrimPrefix(value, "ai.")
	if before, _, ok := strings.Cut(value, "."); ok {
		return before
	}
	return value
}
