package otlp

import (
	"encoding/base64"
	"testing"

	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestNormalizeID(t *testing.T) {
	tests := map[string]string{
		"0123456789ABCDEF": "0123456789abcdef",
		base64.StdEncoding.EncodeToString([]byte{1, 35, 69, 103, 137, 171, 205, 239}): "0123456789abcdef",
		"human-span": "human-span",
	}
	for input, expected := range tests {
		actual := NormalizeID(input, 8)
		if actual == nil || *actual != expected {
			t.Fatalf("NormalizeID(%q) = %v, want %q", input, actual, expected)
		}
	}
	if NormalizeID("", 8) != nil {
		t.Fatal("empty ID must normalize to nil")
	}
}

func TestParseJSONTrace(t *testing.T) {
	payload := []byte(`{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"api"}},{"key":"zone","value":{"stringValue":"west"}}]},"scopeSpans":[{"scope":{"name":"test"},"spans":[{"traceId":"trace-1","spanId":"span-1","name":"request","kind":2,"startTimeUnixNano":"1000000000","endTimeUnixNano":"1250000000","attributes":[{"key":"items","value":{"arrayValue":{"values":[{"stringValue":"a"},{"intValue":"2"}]}}}],"status":{"code":2}}]}]}]}`)
	records, err := ParseTraces(payload, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records", len(records))
	}
	record := records[0]
	if record.TraceID != "trace-1" || record.ServiceName != "api" || record.DurationMs != 250 || record.Status != "error" || record.Attributes["items"] != "a 2" {
		t.Fatalf("unexpected record: %#v", record)
	}
}

func TestParseJSONPreservesExplicitEmptyStrings(t *testing.T) {
	tracePayload := []byte(`{"resourceSpans":[{"scopeSpans":[{"scope":{"name":""},"spans":[{"traceId":"trace","spanId":"span","name":"","events":[{"name":""}]}]}]}]}`)
	spans, err := ParseTraces(tracePayload, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 1 || spans[0].OperationName != "" || spans[0].ScopeName == nil || *spans[0].ScopeName != "" || len(spans[0].Events) != 1 || spans[0].Events[0].Name != "" {
		t.Fatalf("explicit empty trace fields were not preserved: %#v", spans)
	}

	logPayload := []byte(`{"resourceLogs":[{"scopeLogs":[{"scope":{"name":""},"logRecords":[{"timeUnixNano":"0","observedTimeUnixNano":"1000000","severityText":""}]}]}]}`)
	logs, err := ParseLogs(logPayload, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 || logs[0].TimestampMs != 0 || logs[0].SeverityText != "" || logs[0].ScopeName == nil || *logs[0].ScopeName != "" {
		t.Fatalf("explicit empty log fields were not preserved: %#v", logs)
	}
}

func TestParseJSONDefaultsMissingFields(t *testing.T) {
	spans, err := ParseTraces([]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"trace","spanId":"span","events":[{}]}]}]}]}`), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 1 || spans[0].OperationName != "unknown" || spans[0].ScopeName != nil || spans[0].Events[0].Name != "event" {
		t.Fatalf("missing trace fields did not use defaults: %#v", spans)
	}
	logs, err := ParseLogs([]byte(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"observedTimeUnixNano":"1000000"}]}]}]}`), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 || logs[0].TimestampMs != 1 || logs[0].SeverityText != "INFO" || logs[0].ScopeName != nil {
		t.Fatalf("missing log fields did not use defaults: %#v", logs)
	}
}

func TestParseProtobufTrace(t *testing.T) {
	request := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{
					{Key: "service.name", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "worker"}}},
				}},
				ScopeSpans: []*tracev1.ScopeSpans{
					{Spans: []*tracev1.Span{
						{
							TraceId: []byte("0123456789abcdef"), SpanId: []byte("01234567"), Name: "job",
							StartTimeUnixNano: 2_000_000_000, EndTimeUnixNano: 2_010_000_000,
						},
					}},
				},
			},
		},
	}
	payload, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	records, err := ParseTraces(payload, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].TraceID != "30313233343536373839616263646566" || records[0].SpanID != "3031323334353637" {
		t.Fatalf("unexpected records: %#v", records)
	}
}
