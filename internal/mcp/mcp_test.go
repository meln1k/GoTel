package mcp

import (
	"reflect"
	"strings"
	"testing"

	"github.com/meln1k/gotel/internal/api"
)

func TestToolContract(t *testing.T) {
	definitions := tools()
	if len(definitions) != 19 {
		t.Fatalf("got %d tools, want 19", len(definitions))
	}
	seen := make(map[string]bool, len(definitions))
	for _, definition := range definitions {
		if !strings.HasPrefix(definition.Name, "gotel_") {
			t.Errorf("tool does not use gotel prefix: %q", definition.Name)
		}
		if seen[definition.Name] {
			t.Errorf("duplicate tool: %q", definition.Name)
		}
		seen[definition.Name] = true
		if definition.InputSchema["type"] != "object" || definition.InputSchema["additionalProperties"] != false {
			t.Errorf("invalid input schema for %q: %#v", definition.Name, definition.InputSchema)
		}
		if definition.Annotations["readOnlyHint"] != true {
			t.Errorf("tool is not read-only: %q", definition.Name)
		}
	}
	for _, required := range []string{
		"gotel_status", "gotel_services", "gotel_facets", "gotel_search_traces", "gotel_get_trace",
		"gotel_get_trace_logs", "gotel_get_trace_spans", "gotel_search_spans", "gotel_get_span", "gotel_get_span_logs",
		"gotel_search_logs", "gotel_search_ai_calls", "gotel_get_ai_call", "gotel_ai_stats", "gotel_traces_stats",
		"gotel_logs_stats", "gotel_docs_index", "gotel_get_doc", "gotel_openapi",
	} {
		if !seen[required] {
			t.Errorf("missing tool: %q", required)
		}
	}
}

func TestToolSchemasAndRequestsComeFromEndpointContract(t *testing.T) {
	definitions := tools()
	byName := make(map[string]tool, len(definitions))
	for _, definition := range definitions {
		byName[definition.Name] = definition
	}
	for _, endpoint := range api.ToolEndpoints() {
		definition, ok := byName[endpoint.Tool.Name]
		if !ok {
			t.Errorf("missing generated tool %q", endpoint.Tool.Name)
			continue
		}
		if !reflect.DeepEqual(definition.InputSchema, toolSchema(endpoint)) {
			t.Errorf("schema for %q did not come from endpoint contract", endpoint.Tool.Name)
		}
	}

	traceLogs, _ := api.EndpointForTool("gotel_get_trace_logs")
	path, query := toolRequest(traceLogs, map[string]any{
		"traceId": "trace/one", "lookback": "2h", "limit": float64(12), "cursor": "next",
	})
	if path != "/api/traces/trace%2Fone/logs" || query.Encode() != "cursor=next&limit=12&lookback=2h" {
		t.Fatalf("unexpected trace log request: path=%q query=%q", path, query.Encode())
	}

	spans, _ := api.EndpointForTool("gotel_search_spans")
	path, query = toolRequest(spans, map[string]any{
		"service": "worker", "attributes": map[string]any{"job.id": "42"},
		"attributeContains": map[string]any{"result": "done"},
	})
	if path != "/api/spans/search" || query.Get("service") != "worker" || query.Get("attr.job.id") != "42" ||
		query.Get("attrContains.result") != "done" {
		t.Fatalf("unexpected span search request: path=%q query=%q", path, query.Encode())
	}

	logStats, _ := api.EndpointForTool("gotel_logs_stats")
	_, query = toolRequest(logStats, map[string]any{"groupBy": "severity", "agg": "ignored"})
	if query.Get("groupBy") != "severity" || query.Get("agg") != "count" {
		t.Fatalf("fixed log aggregate was not applied: %q", query.Encode())
	}
	properties := byName["gotel_logs_stats"].InputSchema["properties"].(map[string]any)
	if properties["agg"] != nil {
		t.Fatalf("fixed argument leaked into tool schema: %#v", properties)
	}
}
