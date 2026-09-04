package model

import "time"

type ListMeta struct {
	Limit      int     `json:"limit"`
	Lookback   string  `json:"lookback"`
	Returned   int     `json:"returned"`
	Truncated  bool    `json:"truncated"`
	NextCursor *string `json:"nextCursor"`
}

type TraceSpanEvent struct {
	Name       string            `json:"name"`
	Timestamp  string            `json:"timestamp"`
	Attributes map[string]string `json:"attributes"`
}

type TraceSpan struct {
	SpanID        string            `json:"spanId"`
	ParentSpanID  *string           `json:"parentSpanId"`
	ServiceName   string            `json:"serviceName"`
	ScopeName     *string           `json:"scopeName"`
	Kind          *string           `json:"kind"`
	OperationName string            `json:"operationName"`
	StartTime     string            `json:"startTime"`
	IsRunning     bool              `json:"isRunning"`
	DurationMs    float64           `json:"durationMs"`
	Status        string            `json:"status"`
	Depth         int               `json:"depth"`
	Tags          map[string]string `json:"tags"`
	Warnings      []string          `json:"warnings"`
	Events        []TraceSpanEvent  `json:"events"`
}

type TraceSummary struct {
	TraceID           string   `json:"traceId"`
	ServiceName       string   `json:"serviceName"`
	RootOperationName string   `json:"rootOperationName"`
	StartedAt         string   `json:"startedAt"`
	IsRunning         bool     `json:"isRunning"`
	DurationMs        float64  `json:"durationMs"`
	SpanCount         int      `json:"spanCount"`
	ErrorCount        int      `json:"errorCount"`
	Warnings          []string `json:"warnings"`
}

type Trace struct {
	TraceSummary
	Spans []TraceSpan `json:"spans"`
}

type SpanWithContext struct {
	TraceID             string    `json:"traceId"`
	RootOperationName   string    `json:"rootOperationName"`
	ParentOperationName *string   `json:"parentOperationName"`
	Span                TraceSpan `json:"span"`
}

type Log struct {
	ID           string            `json:"id"`
	Timestamp    string            `json:"timestamp"`
	ServiceName  string            `json:"serviceName"`
	SeverityText string            `json:"severityText"`
	Body         string            `json:"body"`
	TraceID      *string           `json:"traceId"`
	SpanID       *string           `json:"spanId"`
	ScopeName    *string           `json:"scopeName"`
	Attributes   map[string]string `json:"attributes"`
}

type Facet struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

type Stat struct {
	Group string  `json:"group"`
	Value float64 `json:"value"`
	Count int     `json:"count"`
}

type AIUsage struct {
	InputTokens       *float64 `json:"inputTokens"`
	OutputTokens      *float64 `json:"outputTokens"`
	TotalTokens       *float64 `json:"totalTokens"`
	CachedInputTokens *float64 `json:"cachedInputTokens"`
	ReasoningTokens   *float64 `json:"reasoningTokens"`
}

type AICallSummary struct {
	TraceID         string   `json:"traceId"`
	SpanID          string   `json:"spanId"`
	Operation       string   `json:"operation"`
	Service         string   `json:"service"`
	FunctionID      *string  `json:"functionId"`
	Provider        *string  `json:"provider"`
	Model           *string  `json:"model"`
	Status          string   `json:"status"`
	StartedAt       string   `json:"startedAt"`
	DurationMs      float64  `json:"durationMs"`
	SessionID       *string  `json:"sessionId"`
	UserID          *string  `json:"userId"`
	PromptPreview   *string  `json:"promptPreview"`
	ResponsePreview *string  `json:"responsePreview"`
	FinishReason    *string  `json:"finishReason"`
	ToolCallCount   int      `json:"toolCallCount"`
	Usage           *AIUsage `json:"usage"`
}

type AIToolCall struct {
	Name       string   `json:"name"`
	SpanID     *string  `json:"spanId"`
	Status     string   `json:"status"`
	DurationMs *float64 `json:"durationMs"`
}

type AITiming struct {
	MsToFirstChunk           *float64 `json:"msToFirstChunk"`
	MsToFinish               *float64 `json:"msToFinish"`
	AvgOutputTokensPerSecond *float64 `json:"avgOutputTokensPerSecond"`
}

type AICallDetail struct {
	TraceID          string       `json:"traceId"`
	SpanID           string       `json:"spanId"`
	Operation        string       `json:"operation"`
	Service          string       `json:"service"`
	FunctionID       *string      `json:"functionId"`
	Provider         *string      `json:"provider"`
	Model            *string      `json:"model"`
	Status           string       `json:"status"`
	StartedAt        string       `json:"startedAt"`
	DurationMs       float64      `json:"durationMs"`
	SessionID        *string      `json:"sessionId"`
	UserID           *string      `json:"userId"`
	FinishReason     *string      `json:"finishReason"`
	PromptMessages   any          `json:"promptMessages"`
	ResponseText     *string      `json:"responseText"`
	ToolCalls        []AIToolCall `json:"toolCalls"`
	ToolsAvailable   any          `json:"toolsAvailable"`
	ProviderMetadata any          `json:"providerMetadata"`
	Usage            *AIUsage     `json:"usage"`
	Timing           AITiming     `json:"timing"`
	Logs             []Log        `json:"logs"`
}

type SpanRecord struct {
	TraceID, SpanID, ServiceName, OperationName, Status string
	ParentSpanID, ScopeName, Kind                       *string
	StartTimeMs, EndTimeMs                              int64
	DurationMs                                          float64
	Attributes, Resource                                map[string]string
	Events                                              []EventRecord
}

type EventRecord struct {
	Name       string            `json:"name"`
	Timestamp  int64             `json:"timestamp"`
	Attributes map[string]string `json:"attributes"`
}

type LogRecord struct {
	TraceID, SpanID, ScopeName      *string
	ServiceName, SeverityText, Body string
	TimestampMs                     int64
	Attributes, Resource            map[string]string
}

func ISOTime(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z")
}
