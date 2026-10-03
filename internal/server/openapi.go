package server

import (
	"strings"

	"github.com/meln1k/gotel/internal/api"
)

func openAPISpec() map[string]any {
	paths := map[string]any{}
	for _, endpoint := range api.Endpoints() {
		var success map[string]any
		if endpoint.TextResponse {
			success = textResponse("Success")
		} else {
			schema := map[string]any{}
			if endpoint.ResponseComponent != "" {
				schema = ref(endpoint.ResponseComponent)
			}
			success = jsonResponse("Success", schema)
		}
		responses := map[string]any{"200": success}
		if endpoint.HasError {
			responses["500"] = jsonResponse("Error", ref("ErrorResponse"))
		}
		if endpoint.Operation == api.IngestTraces || endpoint.Operation == api.IngestLogs {
			success["description"] = "Accepted into bounded process memory, not persisted; X-GoTel-Acknowledgment: accepted-not-persisted"
			success["content"].(map[string]any)["application/x-protobuf"] = map[string]any{"schema": map[string]any{"type": "string", "format": "binary"}}
			for code, description := range map[string]string{"400": "Invalid telemetry", "413": "Request or record exceeds configured limits", "503": "Retryable overload or stopping; Retry-After: 1", "500": "Admission failure"} {
				response := jsonResponse(description, ref("OTLPStatus"))
				response["content"].(map[string]any)["application/x-protobuf"] = map[string]any{"schema": map[string]any{"type": "string", "format": "binary"}}
				responses[code] = response
			}
		}
		operation := map[string]any{
			"operationId": string(endpoint.Operation),
			"summary":     endpoint.Summary,
			"tags":        []string{"telemetry"},
			"responses":   responses,
		}
		parameters := make([]map[string]any, 0, len(endpoint.Parameters))
		for _, parameter := range endpoint.Parameters {
			parameters = append(parameters, openAPIParameter(parameter))
		}
		if len(parameters) != 0 {
			operation["parameters"] = parameters
		}
		if endpoint.JSONRequest {
			operation["requestBody"] = arbitraryJSONBody()
			if endpoint.Operation == api.IngestTraces || endpoint.Operation == api.IngestLogs {
				operation["requestBody"].(map[string]any)["content"].(map[string]any)["application/x-protobuf"] = map[string]any{"schema": map[string]any{"type": "string", "format": "binary"}}
			}
		}
		entry, _ := paths[endpoint.Path].(map[string]any)
		if entry == nil {
			entry = map[string]any{}
			paths[endpoint.Path] = entry
		}
		entry[strings.ToLower(endpoint.Method)] = operation
	}

	return map[string]any{
		"openapi": "3.1.0",
		"info": map[string]any{
			"title":       "Gotel Telemetry API",
			"version":     "1.0.0",
			"description": "Local OpenTelemetry ingest, query, and debugging API.",
		},
		"tags":       []map[string]any{{"name": "telemetry"}},
		"paths":      paths,
		"components": map[string]any{"schemas": componentSchemas()},
	}
}

func openAPIParameter(parameter api.Parameter) map[string]any {
	parameterSchema := stringSchema()
	if parameter.Type == api.Number {
		parameterSchema = numberSchema()
	} else if len(parameter.Enum) != 0 {
		parameterSchema = enumSchema(parameter.Enum...)
	}
	result := map[string]any{
		"name": parameter.Name, "in": string(parameter.Location), "required": parameter.Required, "schema": parameterSchema,
	}
	if parameter.Description != "" {
		result["description"] = parameter.Description
	}
	if parameter.QueryPrefix != "" {
		result["x-query-prefix"] = parameter.QueryPrefix
	}
	return result
}

func jsonResponse(description string, schema map[string]any) map[string]any {
	return map[string]any{"description": description, "content": map[string]any{"application/json": map[string]any{"schema": schema}}}
}

func textResponse(description string) map[string]any {
	return map[string]any{"description": description, "content": map[string]any{"text/plain": map[string]any{"schema": stringSchema()}}}
}

func arbitraryJSONBody() map[string]any {
	return map[string]any{"required": true, "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{}}}}
}

func ref(name string) map[string]any { return map[string]any{"$ref": "#/components/schemas/" + name} }
func stringSchema() map[string]any   { return map[string]any{"type": "string"} }
func numberSchema() map[string]any   { return map[string]any{"type": "number"} }
func booleanSchema() map[string]any  { return map[string]any{"type": "boolean"} }
func nullableString() map[string]any { return map[string]any{"type": []string{"string", "null"}} }
func nullableNumber() map[string]any { return map[string]any{"type": []string{"number", "null"}} }
func enumSchema(values ...string) map[string]any {
	return map[string]any{"type": "string", "enum": values}
}
func dateTimeSchema() map[string]any { return map[string]any{"type": "string", "format": "date-time"} }
func arraySchema(items map[string]any) map[string]any {
	return map[string]any{"type": "array", "items": items}
}
func stringMapSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": stringSchema()}
}
func objectSchema(properties map[string]any, required ...string) map[string]any {
	return map[string]any{"type": "object", "properties": properties, "required": required}
}

func componentSchemas() map[string]any {
	status := enumSchema("ok", "error")
	traceSpanEvent := objectSchema(map[string]any{
		"name": stringSchema(), "timestamp": dateTimeSchema(), "attributes": stringMapSchema(),
	}, "name", "timestamp", "attributes")
	traceSpan := objectSchema(map[string]any{
		"spanId": stringSchema(), "parentSpanId": nullableString(), "serviceName": stringSchema(), "scopeName": nullableString(),
		"kind": nullableString(), "operationName": stringSchema(), "startTime": dateTimeSchema(), "isRunning": booleanSchema(),
		"durationMs": numberSchema(), "status": status, "depth": numberSchema(), "tags": stringMapSchema(),
		"warnings": arraySchema(stringSchema()), "events": arraySchema(ref("TraceSpanEvent")),
	}, "spanId", "parentSpanId", "serviceName", "scopeName", "kind", "operationName", "startTime", "isRunning", "durationMs", "status", "depth", "tags", "warnings", "events")
	traceSummaryProperties := map[string]any{
		"traceId": stringSchema(), "serviceName": stringSchema(), "rootOperationName": stringSchema(), "startedAt": dateTimeSchema(),
		"isRunning": booleanSchema(), "durationMs": numberSchema(), "spanCount": numberSchema(), "errorCount": numberSchema(),
		"warnings": arraySchema(stringSchema()),
	}
	traceSummaryRequired := []string{"traceId", "serviceName", "rootOperationName", "startedAt", "isRunning", "durationMs", "spanCount", "errorCount", "warnings"}
	traceProperties := copyProperties(traceSummaryProperties)
	traceProperties["spans"] = arraySchema(ref("TraceSpan"))
	spanWithContext := objectSchema(map[string]any{
		"traceId": stringSchema(), "rootOperationName": stringSchema(), "parentOperationName": nullableString(), "span": ref("TraceSpan"),
	}, "traceId", "rootOperationName", "parentOperationName", "span")
	logSchema := objectSchema(map[string]any{
		"id": stringSchema(), "timestamp": dateTimeSchema(), "serviceName": stringSchema(), "severityText": stringSchema(),
		"body": stringSchema(), "traceId": nullableString(), "spanId": nullableString(), "scopeName": nullableString(), "attributes": stringMapSchema(),
	}, "id", "timestamp", "serviceName", "severityText", "body", "traceId", "spanId", "scopeName", "attributes")
	listMeta := objectSchema(map[string]any{
		"limit": numberSchema(), "lookback": stringSchema(), "returned": numberSchema(), "truncated": booleanSchema(), "nextCursor": nullableString(),
	}, "limit", "lookback", "returned", "truncated", "nextCursor")
	aiUsage := objectSchema(map[string]any{
		"inputTokens": nullableNumber(), "outputTokens": nullableNumber(), "totalTokens": nullableNumber(),
		"cachedInputTokens": nullableNumber(), "reasoningTokens": nullableNumber(),
	}, "inputTokens", "outputTokens", "totalTokens", "cachedInputTokens", "reasoningTokens")
	aiSummary := objectSchema(map[string]any{
		"traceId": stringSchema(), "spanId": stringSchema(), "operation": stringSchema(), "service": stringSchema(),
		"functionId": nullableString(), "provider": nullableString(), "model": nullableString(), "status": status,
		"startedAt": dateTimeSchema(), "durationMs": numberSchema(), "sessionId": nullableString(), "userId": nullableString(),
		"promptPreview": nullableString(), "responsePreview": nullableString(), "finishReason": nullableString(),
		"toolCallCount": numberSchema(), "usage": map[string]any{"anyOf": []any{ref("AiUsage"), map[string]any{"type": "null"}}},
	}, "traceId", "spanId", "operation", "service", "functionId", "provider", "model", "status", "startedAt", "durationMs", "sessionId", "userId", "promptPreview", "responsePreview", "finishReason", "toolCallCount", "usage")
	aiToolCall := objectSchema(map[string]any{
		"name": stringSchema(), "spanId": nullableString(), "status": status, "durationMs": nullableNumber(),
	}, "name", "spanId", "status", "durationMs")
	aiTiming := objectSchema(map[string]any{
		"msToFirstChunk": nullableNumber(), "msToFinish": nullableNumber(), "avgOutputTokensPerSecond": nullableNumber(),
	}, "msToFirstChunk", "msToFinish", "avgOutputTokensPerSecond")
	aiDetailProperties := copyProperties(aiSummary["properties"].(map[string]any))
	delete(aiDetailProperties, "promptPreview")
	delete(aiDetailProperties, "responsePreview")
	delete(aiDetailProperties, "toolCallCount")
	aiDetailProperties["promptMessages"] = map[string]any{}
	aiDetailProperties["responseText"] = nullableString()
	aiDetailProperties["toolCalls"] = arraySchema(ref("AiToolCall"))
	aiDetailProperties["toolsAvailable"] = map[string]any{}
	aiDetailProperties["providerMetadata"] = map[string]any{}
	aiDetailProperties["timing"] = ref("AiTiming")
	aiDetailProperties["logs"] = arraySchema(map[string]any{})
	aiDetailRequired := []string{"traceId", "spanId", "operation", "service", "functionId", "provider", "model", "status", "startedAt", "durationMs", "sessionId", "userId", "finishReason", "promptMessages", "responseText", "toolCalls", "toolsAvailable", "providerMetadata", "usage", "timing", "logs"}

	return map[string]any{
		"ErrorResponse": objectSchema(map[string]any{"error": stringSchema()}, "error"),
		"OTLPStatus":    objectSchema(map[string]any{"code": numberSchema(), "message": stringSchema(), "details": arraySchema(map[string]any{})}, "code", "message"),
		"Health": objectSchema(map[string]any{
			"ok": booleanSchema(), "service": stringSchema(), "databasePath": stringSchema(), "pid": numberSchema(),
			"databaseBackend": stringSchema(),
			"ready":           booleanSchema(), "persistence": map[string]any{"type": "object"}, "ingestion": map[string]any{"type": "object"},
			"shutdownTimeoutSeconds": numberSchema(),
			"url":                    stringSchema(), "workdir": stringSchema(), "startedAt": dateTimeSchema(), "version": stringSchema(), "instanceId": stringSchema(),
		}, "ok", "service", "databasePath", "pid", "url", "workdir", "startedAt", "version"),
		"IngestTraceResponse": objectSchema(map[string]any{"insertedSpans": numberSchema()}, "insertedSpans"),
		"IngestLogResponse":   objectSchema(map[string]any{"insertedLogs": numberSchema()}, "insertedLogs"),
		"ServiceList":         objectSchema(map[string]any{"data": arraySchema(stringSchema())}, "data"),
		"ListMeta":            listMeta,
		"TraceSpanEvent":      traceSpanEvent,
		"TraceSpan":           traceSpan,
		"TraceSummary":        objectSchema(traceSummaryProperties, traceSummaryRequired...),
		"Trace":               objectSchema(traceProperties, append(traceSummaryRequired, "spans")...),
		"SpanWithContext":     spanWithContext,
		"Log":                 logSchema,
		"Facet": objectSchema(map[string]any{
			"value": stringSchema(), "count": numberSchema(),
		}, "value", "count"),
		"Stat": objectSchema(map[string]any{
			"group": stringSchema(), "value": numberSchema(), "count": numberSchema(),
		}, "group", "value", "count"),
		"AiUsage":       aiUsage,
		"AiCallSummary": aiSummary,
		"AiToolCall":    aiToolCall,
		"AiTiming":      aiTiming,
		"AiCallDetail":  objectSchema(aiDetailProperties, aiDetailRequired...),
		"TraceSummaryList": objectSchema(map[string]any{
			"data": arraySchema(ref("TraceSummary")), "meta": ref("ListMeta"),
		}, "data", "meta"),
		"TraceResponse": objectSchema(map[string]any{"data": ref("Trace")}, "data"),
		"SpanResponse":  objectSchema(map[string]any{"data": ref("SpanWithContext")}, "data"),
		"SpanList":      objectSchema(map[string]any{"data": arraySchema(ref("SpanWithContext"))}, "data"),
		"PaginatedSpanList": objectSchema(map[string]any{
			"data": arraySchema(ref("SpanWithContext")), "meta": ref("ListMeta"),
		}, "data", "meta"),
		"LogList": objectSchema(map[string]any{
			"data": arraySchema(ref("Log")), "meta": ref("ListMeta"),
		}, "data", "meta"),
		"FacetList": objectSchema(map[string]any{"data": arraySchema(ref("Facet"))}, "data"),
		"StatList":  objectSchema(map[string]any{"data": arraySchema(ref("Stat"))}, "data"),
		"DocIndex": objectSchema(map[string]any{"docs": arraySchema(objectSchema(map[string]any{
			"name": stringSchema(), "title": stringSchema(), "path": stringSchema(),
		}, "name", "title", "path"))}, "docs"),
		"AiCallList": objectSchema(map[string]any{
			"data": arraySchema(ref("AiCallSummary")), "meta": ref("ListMeta"),
		}, "data", "meta"),
		"AiCallDetailResponse": objectSchema(map[string]any{"data": ref("AiCallDetail")}, "data"),
	}
}

func copyProperties(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
