package api

import "net/http"

type Operation string

const (
	Root         Operation = "root"
	Health       Operation = "health"
	IngestTraces Operation = "ingestTraces"
	IngestLogs   Operation = "ingestLogs"
	Services     Operation = "services"
	Traces       Operation = "traces"
	SearchTraces Operation = "searchTraces"
	TraceStats   Operation = "traceStats"
	Trace        Operation = "trace"
	TraceLogs    Operation = "traceLogs"
	TraceSpans   Operation = "traceSpans"
	Span         Operation = "span"
	SpanLogs     Operation = "spanLogs"
	SearchSpans  Operation = "searchSpans"
	Logs         Operation = "logs"
	SearchLogs   Operation = "searchLogs"
	LogStats     Operation = "logStats"
	Docs         Operation = "docs"
	Doc          Operation = "doc"
	Facets       Operation = "facets"
	AICalls      Operation = "aiCalls"
	AICall       Operation = "aiCall"
	AIStats      Operation = "aiStats"
	OpenAPI      Operation = "openapi"
)

type ParameterLocation string

const (
	Query ParameterLocation = "query"
	Path  ParameterLocation = "path"
)

type ParameterType string

const (
	String    ParameterType = "string"
	Number    ParameterType = "number"
	StringMap ParameterType = "stringMap"
)

type Parameter struct {
	Name        string
	Location    ParameterLocation
	Type        ParameterType
	Required    bool
	Enum        []string
	Description string
	QueryPrefix string
	ToolName    string
	ToolHidden  bool
	ToolFixed   string
}

func (parameter Parameter) ArgumentName() string {
	if parameter.ToolName != "" {
		return parameter.ToolName
	}
	return parameter.Name
}

type Tool struct {
	Name        string
	Description string
}

type Endpoint struct {
	Operation         Operation
	Method            string
	Path              string
	Summary           string
	Parameters        []Parameter
	ResponseComponent string
	TextResponse      bool
	JSONRequest       bool
	HasError          bool
	Tool              *Tool
}

func (endpoint Endpoint) Pattern() string {
	if endpoint.Path == "/" {
		return "/"
	}
	return endpoint.Method + " " + endpoint.Path
}

func Endpoints() []Endpoint { return endpoints }

func ToolEndpoints() []Endpoint {
	result := make([]Endpoint, 0)
	for _, endpoint := range endpoints {
		if endpoint.Tool != nil {
			result = append(result, endpoint)
		}
	}
	return result
}

func EndpointForTool(name string) (Endpoint, bool) {
	for _, endpoint := range endpoints {
		if endpoint.Tool != nil && endpoint.Tool.Name == name {
			return endpoint, true
		}
	}
	return Endpoint{}, false
}

func query(name string, parameterType ParameterType, required bool, values ...string) Parameter {
	return Parameter{Name: name, Location: Query, Type: parameterType, Required: required, Enum: values}
}

func path(name string) Parameter {
	return Parameter{Name: name, Location: Path, Type: String, Required: true}
}

func dynamic(name, toolName, prefix, description string) Parameter {
	return Parameter{
		Name: name, Location: Query, Type: StringMap, ToolName: toolName, QueryPrefix: prefix, Description: description,
	}
}

func tool(name, description string) *Tool { return &Tool{Name: name, Description: description} }

func pagingParameters() []Parameter {
	return []Parameter{query("lookback", String, false), query("limit", Number, false), query("cursor", String, false)}
}

func logParameters() []Parameter {
	return []Parameter{
		query("service", String, false), query("severity", String, false), query("traceId", String, false),
		query("spanId", String, false), query("body", String, false),
		dynamic("attr.<key>", "attributes", "attr.", "Match an attribute exactly; replace <key> with the attribute name."),
		dynamic("attrContains.<key>", "attributeContains", "attrContains.", "Match text within an attribute; replace <key> with the attribute name."),
		query("lookback", String, false), query("limit", Number, false), query("cursor", String, false),
	}
}

var endpoints = []Endpoint{
	{Operation: Root, Method: http.MethodGet, Path: "/", Summary: "List Gotel endpoints", TextResponse: true},
	{Operation: Health, Method: http.MethodGet, Path: "/api/health", Summary: "Check server health and identity", ResponseComponent: "Health"},
	{Operation: IngestTraces, Method: http.MethodPost, Path: "/v1/traces", Summary: "Ingest OTLP traces", ResponseComponent: "IngestTraceResponse", JSONRequest: true, HasError: true},
	{Operation: IngestLogs, Method: http.MethodPost, Path: "/v1/logs", Summary: "Ingest OTLP logs", ResponseComponent: "IngestLogResponse", JSONRequest: true, HasError: true},
	{Operation: Services, Method: http.MethodGet, Path: "/api/services", Summary: "List services", ResponseComponent: "ServiceList", HasError: true,
		Tool: tool("gotel_services", "List services with recent telemetry.")},
	{Operation: Traces, Method: http.MethodGet, Path: "/api/traces", Summary: "List traces", ResponseComponent: "TraceSummaryList", HasError: true, Parameters: []Parameter{
		query("service", String, false), query("limit", Number, false), query("lookback", String, false), query("cursor", String, false),
	}},
	{Operation: SearchTraces, Method: http.MethodGet, Path: "/api/traces/search", Summary: "Search traces", ResponseComponent: "TraceSummaryList", HasError: true,
		Tool: tool("gotel_search_traces", "Search recent traces."), Parameters: []Parameter{
			query("service", String, false), query("operation", String, false), query("status", String, false, "ok", "error"),
			query("minDurationMs", Number, false),
			{Name: "aiText", Location: Query, Type: String, ToolHidden: true},
			dynamic("attr.<key>", "attributes", "attr.", "Match an attribute exactly; replace <key> with the attribute name."),
			query("lookback", String, false), query("limit", Number, false), query("cursor", String, false),
		}},
	{Operation: TraceStats, Method: http.MethodGet, Path: "/api/traces/stats", Summary: "Aggregate traces", ResponseComponent: "StatList", HasError: true,
		Tool: tool("gotel_traces_stats", "Aggregate traces."), Parameters: []Parameter{
			query("groupBy", String, true), query("agg", String, true, "count", "avg_duration", "p95_duration", "error_rate"),
			query("service", String, false), query("operation", String, false), query("status", String, false, "ok", "error"),
			query("minDurationMs", Number, false),
			dynamic("attr.<key>", "attributes", "attr.", "Match an attribute exactly; replace <key> with the attribute name."),
			query("lookback", String, false), query("limit", Number, false),
		}},
	{Operation: Trace, Method: http.MethodGet, Path: "/api/traces/{traceId}", Summary: "Get a trace", ResponseComponent: "TraceResponse", HasError: true,
		Tool: tool("gotel_get_trace", "Get one trace and its span tree."), Parameters: []Parameter{path("traceId")}},
	{Operation: TraceLogs, Method: http.MethodGet, Path: "/api/traces/{traceId}/logs", Summary: "List trace logs", ResponseComponent: "LogList", HasError: true,
		Tool: tool("gotel_get_trace_logs", "Get logs correlated with a trace."), Parameters: append([]Parameter{path("traceId")}, pagingParameters()...)},
	{Operation: TraceSpans, Method: http.MethodGet, Path: "/api/traces/{traceId}/spans", Summary: "List trace spans", ResponseComponent: "SpanList", HasError: true,
		Tool: tool("gotel_get_trace_spans", "Get the flat span list for a trace."), Parameters: []Parameter{path("traceId")}},
	{Operation: Span, Method: http.MethodGet, Path: "/api/spans/{spanId}", Summary: "Get a span", ResponseComponent: "SpanResponse", HasError: true,
		Tool: tool("gotel_get_span", "Get one span with trace context."), Parameters: []Parameter{path("spanId")}},
	{Operation: SpanLogs, Method: http.MethodGet, Path: "/api/spans/{spanId}/logs", Summary: "List span logs", ResponseComponent: "LogList", HasError: true,
		Tool: tool("gotel_get_span_logs", "Get logs correlated with a span."), Parameters: append([]Parameter{path("spanId")}, pagingParameters()...)},
	{Operation: SearchSpans, Method: http.MethodGet, Path: "/api/spans/search", Summary: "Search spans", ResponseComponent: "PaginatedSpanList", HasError: true,
		Tool: tool("gotel_search_spans", "Search spans by service, operation, parent, status, or attributes."), Parameters: []Parameter{
			query("service", String, false), query("traceId", String, false), query("operation", String, false),
			query("parentOperation", String, false), query("status", String, false, "ok", "error"),
			dynamic("attr.<key>", "attributes", "attr.", "Match an attribute exactly; replace <key> with the attribute name."),
			dynamic("attrContains.<key>", "attributeContains", "attrContains.", "Match text within an attribute; replace <key> with the attribute name."),
			query("lookback", String, false), query("limit", Number, false),
		}},
	{Operation: Logs, Method: http.MethodGet, Path: "/api/logs", Summary: "Search logs", ResponseComponent: "LogList", HasError: true, Parameters: logParameters()},
	{Operation: SearchLogs, Method: http.MethodGet, Path: "/api/logs/search", Summary: "Search logs", ResponseComponent: "LogList", HasError: true,
		Tool: tool("gotel_search_logs", "Search recent logs."), Parameters: logParameters()},
	{Operation: LogStats, Method: http.MethodGet, Path: "/api/logs/stats", Summary: "Aggregate logs", ResponseComponent: "StatList", HasError: true,
		Tool: tool("gotel_logs_stats", "Count logs by a field."), Parameters: []Parameter{
			query("groupBy", String, true),
			{Name: "agg", Location: Query, Type: String, Required: true, Enum: []string{"count"}, ToolFixed: "count"},
			query("service", String, false), query("traceId", String, false), query("spanId", String, false), query("body", String, false),
			dynamic("attr.<key>", "attributes", "attr.", "Match an attribute exactly; replace <key> with the attribute name."),
			query("lookback", String, false), query("limit", Number, false),
		}},
	{Operation: Docs, Method: http.MethodGet, Path: "/api/docs", Summary: "List agent documentation", ResponseComponent: "DocIndex", HasError: true,
		Tool: tool("gotel_docs_index", "List bundled agent documentation.")},
	{Operation: Doc, Method: http.MethodGet, Path: "/api/docs/{name}", Summary: "Get agent documentation", TextResponse: true, HasError: true,
		Tool: tool("gotel_get_doc", "Read bundled agent documentation."), Parameters: []Parameter{path("name")}},
	{Operation: Facets, Method: http.MethodGet, Path: "/api/facets", Summary: "List facets", ResponseComponent: "FacetList", HasError: true,
		Tool: tool("gotel_facets", "List common trace or log field values."), Parameters: []Parameter{
			query("type", String, true, "traces", "logs"), query("field", String, true),
			{Name: "key", Location: Query, Type: String, ToolHidden: true}, query("service", String, false),
			query("lookback", String, false), query("limit", Number, false),
		}},
	{Operation: AICalls, Method: http.MethodGet, Path: "/api/ai/calls", Summary: "Search AI calls", ResponseComponent: "AiCallList", HasError: true,
		Tool: tool("gotel_search_ai_calls", "Search AI SDK calls."), Parameters: []Parameter{
			query("service", String, false), query("traceId", String, false), query("sessionId", String, false), query("functionId", String, false),
			query("provider", String, false), query("model", String, false), query("operation", String, false),
			query("status", String, false, "ok", "error"), query("minDurationMs", Number, false), query("text", String, false),
			query("lookback", String, false), query("limit", Number, false),
		}},
	{Operation: AICall, Method: http.MethodGet, Path: "/api/ai/calls/{spanId}", Summary: "Get an AI call", ResponseComponent: "AiCallDetailResponse", HasError: true,
		Tool: tool("gotel_get_ai_call", "Get details for one AI call."), Parameters: []Parameter{path("spanId")}},
	{Operation: AIStats, Method: http.MethodGet, Path: "/api/ai/stats", Summary: "Aggregate AI calls", ResponseComponent: "StatList", HasError: true,
		Tool: tool("gotel_ai_stats", "Aggregate AI calls."), Parameters: []Parameter{
			query("groupBy", String, true, "provider", "model", "functionId", "sessionId", "status"),
			query("agg", String, true, "count", "avg_duration", "p95_duration", "total_input_tokens", "total_output_tokens"),
			query("service", String, false), query("traceId", String, false), query("sessionId", String, false), query("functionId", String, false),
			query("provider", String, false), query("model", String, false), query("operation", String, false),
			query("status", String, false, "ok", "error"), query("minDurationMs", Number, false),
			query("lookback", String, false), query("limit", Number, false),
		}},
	{Operation: OpenAPI, Method: http.MethodGet, Path: "/openapi.json", Summary: "Get the HTTP OpenAPI document",
		Tool: tool("gotel_openapi", "Get the HTTP OpenAPI document.")},
}
