package otlp

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"

	"github.com/meln1k/gotel/internal/model"
)

type anyValue struct {
	StringValue *string      `json:"stringValue"`
	BoolValue   *bool        `json:"boolValue"`
	IntValue    any          `json:"intValue"`
	DoubleValue *float64     `json:"doubleValue"`
	BytesValue  *string      `json:"bytesValue"`
	ArrayValue  *arrayValue  `json:"arrayValue"`
	KVListValue *kvListValue `json:"kvlistValue"`
}

type arrayValue struct {
	Values []anyValue `json:"values"`
}

type kvListValue struct {
	Values []keyValue `json:"values"`
}

type keyValue struct {
	Key   string    `json:"key"`
	Value *anyValue `json:"value"`
}

type jsonTraceRequest struct {
	ResourceSpans []struct {
		Resource *struct {
			Attributes []keyValue `json:"attributes"`
		} `json:"resource"`
		ScopeSpans []struct {
			Scope *struct {
				Name *string `json:"name"`
			} `json:"scope"`
			Spans []struct {
				TraceID           string     `json:"traceId"`
				SpanID            string     `json:"spanId"`
				ParentSpanID      string     `json:"parentSpanId"`
				Name              *string    `json:"name"`
				Kind              int        `json:"kind"`
				StartTimeUnixNano any        `json:"startTimeUnixNano"`
				EndTimeUnixNano   any        `json:"endTimeUnixNano"`
				Attributes        []keyValue `json:"attributes"`
				Status            *struct {
					Code int `json:"code"`
				} `json:"status"`
				Events []struct {
					TimeUnixNano any        `json:"timeUnixNano"`
					Name         *string    `json:"name"`
					Attributes   []keyValue `json:"attributes"`
				} `json:"events"`
			} `json:"spans"`
		} `json:"scopeSpans"`
	} `json:"resourceSpans"`
}

type jsonLogRequest struct {
	ResourceLogs []struct {
		Resource *struct {
			Attributes []keyValue `json:"attributes"`
		} `json:"resource"`
		ScopeLogs []struct {
			Scope *struct {
				Name *string `json:"name"`
			} `json:"scope"`
			LogRecords []struct {
				TimeUnixNano         any        `json:"timeUnixNano"`
				ObservedTimeUnixNano any        `json:"observedTimeUnixNano"`
				SeverityText         *string    `json:"severityText"`
				Body                 *anyValue  `json:"body"`
				Attributes           []keyValue `json:"attributes"`
				TraceID              string     `json:"traceId"`
				SpanID               string     `json:"spanId"`
			} `json:"logRecords"`
		} `json:"scopeLogs"`
	} `json:"resourceLogs"`
}

func ParseTraces(body []byte, protobuf bool) ([]model.SpanRecord, error) {
	if protobuf {
		var request collectortracev1.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &request); err != nil {
			return nil, err
		}
		return protobufTraces(&request), nil
	}
	var request jsonTraceRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&request); err != nil {
		return nil, err
	}
	return jsonTraces(&request), nil
}

func ParseLogs(body []byte, protobuf bool) ([]model.LogRecord, error) {
	if protobuf {
		var request collectorlogsv1.ExportLogsServiceRequest
		if err := proto.Unmarshal(body, &request); err != nil {
			return nil, err
		}
		return protobufLogs(&request), nil
	}
	var request jsonLogRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&request); err != nil {
		return nil, err
	}
	return jsonLogs(&request), nil
}

func jsonTraces(request *jsonTraceRequest) []model.SpanRecord {
	result := make([]model.SpanRecord, 0)
	for _, resourceSpans := range request.ResourceSpans {
		resource := mapFromJSON(resourceSpans.Resource)
		service := serviceName(resource)
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			var scope *string
			if scopeSpans.Scope != nil && scopeSpans.Scope.Name != nil {
				scope = stringPointer(*scopeSpans.Scope.Name)
			}
			for _, span := range scopeSpans.Spans {
				traceID := NormalizeID(span.TraceID, 16)
				spanID := NormalizeID(span.SpanID, 8)
				if traceID == nil || spanID == nil {
					continue
				}
				start := nanosToMillis(span.StartTimeUnixNano)
				end := nanosToMillis(span.EndTimeUnixNano)
				status := "ok"
				if span.Status != nil && span.Status.Code == 2 {
					status = "error"
				}
				events := make([]model.EventRecord, 0, len(span.Events))
				for _, event := range span.Events {
					name := "event"
					if event.Name != nil {
						name = *event.Name
					}
					events = append(events, model.EventRecord{Name: name, Timestamp: nanosToMillis(event.TimeUnixNano), Attributes: attributesJSON(event.Attributes)})
				}
				operation := "unknown"
				if span.Name != nil {
					operation = *span.Name
				}
				result = append(result, model.SpanRecord{
					TraceID: traceIDValue(traceID), SpanID: traceIDValue(spanID), ParentSpanID: NormalizeID(span.ParentSpanID, 8),
					ServiceName: service, ScopeName: scope, OperationName: operation, Kind: spanKind(span.Kind),
					StartTimeMs: start, EndTimeMs: end, DurationMs: float64(max(0, end-start)), Status: status,
					Attributes: attributesJSON(span.Attributes), Resource: cloneMap(resource), Events: events,
				})
			}
		}
	}
	return result
}

func jsonLogs(request *jsonLogRequest) []model.LogRecord {
	result := make([]model.LogRecord, 0)
	for _, resourceLogs := range request.ResourceLogs {
		resource := mapFromJSON(resourceLogs.Resource)
		service := serviceName(resource)
		for _, scopeLogs := range resourceLogs.ScopeLogs {
			var scope *string
			if scopeLogs.Scope != nil && scopeLogs.Scope.Name != nil {
				scope = stringPointer(*scopeLogs.Scope.Name)
			}
			for _, record := range scopeLogs.LogRecords {
				attributes := attributesJSON(record.Attributes)
				traceID := NormalizeID(record.TraceID, 16)
				spanID := NormalizeID(record.SpanID, 8)
				if value, ok := firstAttribute(attributes, "traceId", "trace_id"); ok {
					traceID = NormalizeID(value, 16)
				}
				if value, ok := firstAttribute(attributes, "spanId", "span_id"); ok {
					spanID = NormalizeID(value, 8)
				}
				severity := "INFO"
				if record.SeverityText != nil {
					severity = *record.SeverityText
				}
				timestamp := nanosToMillis(record.TimeUnixNano)
				if record.TimeUnixNano == nil {
					timestamp = nanosToMillis(record.ObservedTimeUnixNano)
				}
				result = append(result, model.LogRecord{
					TraceID: traceID, SpanID: spanID, ScopeName: scope, ServiceName: service, SeverityText: severity,
					Body: stringifyJSONValue(record.Body), TimestampMs: timestamp, Attributes: attributes, Resource: cloneMap(resource),
				})
			}
		}
	}
	return result
}

func protobufTraces(request *collectortracev1.ExportTraceServiceRequest) []model.SpanRecord {
	result := make([]model.SpanRecord, 0)
	for _, resourceSpans := range request.ResourceSpans {
		resource := attributesProto(resourceSpans.GetResource().GetAttributes())
		service := serviceName(resource)
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			scope := optionalString(scopeSpans.GetScope().GetName())
			for _, span := range scopeSpans.Spans {
				traceID := normalizeBytes(span.TraceId, 16)
				spanID := normalizeBytes(span.SpanId, 8)
				if traceID == nil || spanID == nil {
					continue
				}
				start, end := int64(span.StartTimeUnixNano/1_000_000), int64(span.EndTimeUnixNano/1_000_000)
				status := "ok"
				if span.GetStatus().GetCode() == 2 {
					status = "error"
				}
				events := make([]model.EventRecord, 0, len(span.Events))
				for _, event := range span.Events {
					name := event.Name
					if name == "" {
						name = "event"
					}
					events = append(events, model.EventRecord{Name: name, Timestamp: int64(event.TimeUnixNano / 1_000_000), Attributes: attributesProto(event.Attributes)})
				}
				operation := span.Name
				if operation == "" {
					operation = "unknown"
				}
				result = append(result, model.SpanRecord{
					TraceID: *traceID, SpanID: *spanID, ParentSpanID: normalizeBytes(span.ParentSpanId, 8), ServiceName: service,
					ScopeName: scope, OperationName: operation, Kind: spanKind(int(span.Kind)), StartTimeMs: start, EndTimeMs: end,
					DurationMs: float64(max(0, end-start)), Status: status, Attributes: attributesProto(span.Attributes), Resource: cloneMap(resource), Events: events,
				})
			}
		}
	}
	return result
}

func protobufLogs(request *collectorlogsv1.ExportLogsServiceRequest) []model.LogRecord {
	result := make([]model.LogRecord, 0)
	for _, resourceLogs := range request.ResourceLogs {
		resource := attributesProto(resourceLogs.GetResource().GetAttributes())
		service := serviceName(resource)
		for _, scopeLogs := range resourceLogs.ScopeLogs {
			scope := optionalString(scopeLogs.GetScope().GetName())
			for _, record := range scopeLogs.LogRecords {
				attributes := attributesProto(record.Attributes)
				traceID := normalizeBytes(record.TraceId, 16)
				spanID := normalizeBytes(record.SpanId, 8)
				if value, ok := firstAttribute(attributes, "traceId", "trace_id"); ok {
					traceID = NormalizeID(value, 16)
				}
				if value, ok := firstAttribute(attributes, "spanId", "span_id"); ok {
					spanID = NormalizeID(value, 8)
				}
				severity := record.SeverityText
				if severity == "" {
					severity = "INFO"
				}
				timestamp := int64(record.TimeUnixNano / 1_000_000)
				if timestamp == 0 {
					timestamp = int64(record.ObservedTimeUnixNano / 1_000_000)
				}
				result = append(result, model.LogRecord{
					TraceID: traceID, SpanID: spanID, ScopeName: scope, ServiceName: service, SeverityText: severity,
					Body: stringifyProtoValue(record.Body), TimestampMs: timestamp, Attributes: attributes, Resource: cloneMap(resource),
				})
			}
		}
	}
	return result
}

func NormalizeID(value string, expectedBytes int) *string {
	if value == "" {
		return nil
	}
	if len(value) == expectedBytes*2 {
		if decoded, err := hex.DecodeString(value); err == nil && len(decoded) == expectedBytes {
			normalized := strings.ToLower(value)
			return &normalized
		}
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(value)
	if err == nil && len(decoded) == expectedBytes && base64.StdEncoding.EncodeToString(decoded) == value {
		normalized := hex.EncodeToString(decoded)
		return &normalized
	}
	return &value
}

func normalizeBytes(value []byte, expectedBytes int) *string {
	if len(value) == 0 {
		return nil
	}
	if len(value) != expectedBytes {
		encoded := base64.StdEncoding.EncodeToString(value)
		return &encoded
	}
	normalized := hex.EncodeToString(value)
	return &normalized
}

func mapFromJSON(resource *struct {
	Attributes []keyValue `json:"attributes"`
}) map[string]string {
	if resource == nil {
		return map[string]string{}
	}
	return attributesJSON(resource.Attributes)
}

func attributesJSON(values []keyValue) map[string]string {
	result := make(map[string]string, len(values))
	for _, value := range values {
		result[value.Key] = stringifyJSONValue(value.Value)
	}
	return result
}

func attributesProto(values []*commonv1.KeyValue) map[string]string {
	result := make(map[string]string, len(values))
	for _, value := range values {
		result[value.Key] = stringifyProtoValue(value.Value)
	}
	return result
}

func parseJSONValue(value *anyValue) any {
	if value == nil {
		return nil
	}
	if value.StringValue != nil {
		return *value.StringValue
	}
	if value.BoolValue != nil {
		return *value.BoolValue
	}
	if value.IntValue != nil {
		switch n := value.IntValue.(type) {
		case json.Number:
			parsed, _ := n.Float64()
			return parsed
		case string:
			parsed, err := strconv.ParseFloat(n, 64)
			if err != nil {
				return math.NaN()
			}
			return parsed
		default:
			return n
		}
	}
	if value.DoubleValue != nil {
		return *value.DoubleValue
	}
	if value.BytesValue != nil {
		return *value.BytesValue
	}
	if value.ArrayValue != nil {
		result := make([]any, 0, len(value.ArrayValue.Values))
		for i := range value.ArrayValue.Values {
			result = append(result, parseJSONValue(&value.ArrayValue.Values[i]))
		}
		return result
	}
	if value.KVListValue != nil {
		result := make(map[string]any, len(value.KVListValue.Values))
		for _, item := range value.KVListValue.Values {
			result[item.Key] = parseJSONValue(item.Value)
		}
		return result
	}
	return nil
}

func stringifyJSONValue(value *anyValue) string { return stringify(parseJSONValue(value)) }

func protoValue(value *commonv1.AnyValue) any {
	if value == nil {
		return nil
	}
	switch v := value.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return v.StringValue
	case *commonv1.AnyValue_BoolValue:
		return v.BoolValue
	case *commonv1.AnyValue_IntValue:
		return float64(v.IntValue)
	case *commonv1.AnyValue_DoubleValue:
		return v.DoubleValue
	case *commonv1.AnyValue_BytesValue:
		return base64.StdEncoding.EncodeToString(v.BytesValue)
	case *commonv1.AnyValue_ArrayValue:
		result := make([]any, 0, len(v.ArrayValue.Values))
		for _, item := range v.ArrayValue.Values {
			result = append(result, protoValue(item))
		}
		return result
	case *commonv1.AnyValue_KvlistValue:
		result := make(map[string]any, len(v.KvlistValue.Values))
		for _, item := range v.KvlistValue.Values {
			result[item.Key] = protoValue(item.Value)
		}
		return result
	default:
		return nil
	}
}

func stringifyProtoValue(value *commonv1.AnyValue) string { return stringify(protoValue(value)) }

func stringify(value any) string {
	switch value := value.(type) {
	case nil:
		return ""
	case string:
		return value
	case bool:
		return strconv.FormatBool(value)
	case float64:
		if math.IsNaN(value) {
			return "NaN"
		}
		return strconv.FormatFloat(value, 'f', -1, 64)
	case []any:
		parts := make([]string, 0, len(value))
		for _, item := range value {
			if part := stringify(item); part != "" {
				parts = append(parts, part)
			}
		}
		return strings.Join(parts, " ")
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return fmt.Sprint(value)
		}
		return string(encoded)
	}
}

func serviceName(resource map[string]string) string {
	if value := resource["service.name"]; value != "" {
		return value
	}
	if value := resource["service_name"]; value != "" {
		return value
	}
	return "unknown"
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func stringPointer(value string) *string { return &value }

func spanKind(kind int) *string {
	kinds := map[int]string{1: "internal", 2: "server", 3: "client", 4: "producer", 5: "consumer"}
	value, ok := kinds[kind]
	if !ok {
		return nil
	}
	return &value
}

func nanosToMillis(value any) int64 {
	var text string
	switch value := value.(type) {
	case string:
		text = value
	case json.Number:
		text = value.String()
	case float64:
		return int64(math.Floor(value / 1_000_000))
	case nil:
		return 0
	default:
		text = fmt.Sprint(value)
	}
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		return n / 1_000_000
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0
	}
	return int64(math.Floor(f / 1_000_000))
}

func firstAttribute(attributes map[string]string, keys ...string) (string, bool) {
	for _, key := range keys {
		if value, ok := attributes[key]; ok {
			return value, true
		}
	}
	return "", false
}

func cloneMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func traceIDValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
