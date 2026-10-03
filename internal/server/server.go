package server

import (
	"bytes"
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/meln1k/gotel/internal/api"
	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/otlp"
	"github.com/meln1k/gotel/internal/store"
	"github.com/meln1k/gotel/internal/telemetry"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

//go:embed docs/*.md
var docs embed.FS

type Identity struct {
	PID          int
	URL          string
	Workdir      string
	StartedAt    string
	InstanceID   string
	DatabasePath string
}

type Server struct {
	config           config.Config
	store            *store.Store
	identity         Identity
	handler          http.Handler
	ingestSlots      chan struct{}
	rejectedRequests atomic.Uint64
}

func New(cfg config.Config, telemetryStore *store.Store, identity Identity) *Server {
	server := &Server{config: cfg, store: telemetryStore, identity: identity, ingestSlots: make(chan struct{}, max(0, cfg.MaxConcurrentIngest))}
	server.handler = server.routes()
	return server
}

func (s *Server) Handler() http.Handler { return s.handler }

func (s *Server) Run(ctx context.Context) error {
	if err := s.config.ValidateIngestion(); err != nil {
		return err
	}
	requests, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()
	httpServer := &http.Server{
		Addr:              net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.Port)),
		Handler:           s.handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       30 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return requests },
	}
	errCh := make(chan error, 1)
	go func() { errCh <- httpServer.ListenAndServe() }()
	select {
	case <-ctx.Done():
		s.store.StopAdmission()
		cancelRequests()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownContext); err != nil {
			return errors.Join(err, httpServer.Close())
		}
		return nil
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	handlers := map[api.Operation]http.HandlerFunc{
		api.Root: s.root, api.Health: s.health,
		api.IngestTraces: s.ingestTraces, api.IngestLogs: s.ingestLogs,
		api.Services: s.services, api.Traces: s.traces, api.SearchTraces: s.searchTraces, api.TraceStats: s.traceStats,
		api.Trace: s.trace, api.TraceLogs: s.traceLogs, api.TraceSpans: s.traceSpans,
		api.Span: s.span, api.SpanLogs: s.spanLogs, api.SearchSpans: s.searchSpans,
		api.Logs: s.logs, api.SearchLogs: s.logs, api.LogStats: s.logStats,
		api.Docs: s.docsIndex, api.Doc: s.doc, api.Facets: s.facets,
		api.AICalls: s.aiCalls, api.AICall: s.aiCall, api.AIStats: s.aiStats,
		api.OpenAPI: s.openapi,
	}
	for _, endpoint := range api.Endpoints() {
		handler := handlers[endpoint.Operation]
		if handler == nil {
			panic("missing HTTP handler for " + endpoint.Operation)
		}
		pattern := endpoint.Pattern()
		mux.Handle(pattern, telemetry.HTTPHandler(s.config, pattern, handler))
	}
	return mux
}

func (s *Server) root(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "Gotel local OpenTelemetry server\n\nPOST /v1/traces\nPOST /v1/logs\nGET /api/health\nGET /openapi.json\n")
		return
	}
	writeError(w, http.StatusNotFound, "Not found")
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	persistence := s.store.Stats()
	response := map[string]any{
		"ok": true, "service": "gotel-local-server", "databasePath": s.identity.DatabasePath,
		"databaseBackend": "sqlite",
		"pid":             s.identity.PID, "url": s.identity.URL, "workdir": s.identity.Workdir,
		"startedAt": s.identity.StartedAt, "version": config.Version,
		"ready": persistence.Ready, "persistence": persistence,
		"ingestion":              map[string]any{"decodingRequests": len(s.ingestSlots), "rejectedRequests": s.rejectedRequests.Load()},
		"shutdownTimeoutSeconds": s.config.ShutdownTimeoutSeconds,
	}
	if s.identity.InstanceID != "" {
		response["instanceId"] = s.identity.InstanceID
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) ingestTraces(w http.ResponseWriter, r *http.Request) {
	s.ingest(w, r, "insertedSpans", func(body []byte, protobuf bool) (int, error) {
		records, err := otlp.ParseTraces(body, protobuf)
		if err != nil {
			return 0, errInvalidPayload
		}
		return s.store.IngestSpans(r.Context(), records)
	})
}

func (s *Server) ingestLogs(w http.ResponseWriter, r *http.Request) {
	s.ingest(w, r, "insertedLogs", func(body []byte, protobuf bool) (int, error) {
		records, err := otlp.ParseLogs(body, protobuf)
		if err != nil {
			return 0, errInvalidPayload
		}
		return s.store.IngestLogs(r.Context(), records)
	})
}

var errInvalidPayload = errors.New("invalid OTLP payload")

// The shared slot covers body reading, decoding, and queue admission, not persistence.
func (s *Server) ingest(w http.ResponseWriter, r *http.Request, countField string, admit func([]byte, bool) (int, error)) {
	accepted := false
	defer func() {
		if !accepted {
			s.rejectedRequests.Add(1)
		}
	}()
	select {
	case s.ingestSlots <- struct{}{}:
		defer func() { <-s.ingestSlots }()
	default:
		writeIngestError(w, r, store.ErrOverloaded)
		return
	}
	if r.Context().Err() != nil {
		writeIngestError(w, r, store.ErrClosed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(s.config.MaxRequestBytes)))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeIngestError(w, r, store.ErrRecordTooLarge)
		} else {
			writeIngestError(w, r, errInvalidPayload)
		}
		return
	}
	if r.Context().Err() != nil {
		writeIngestError(w, r, store.ErrClosed)
		return
	}
	protobuf := protobufContent(r.Header.Get("Content-Type"))
	if !protobuf && (!json.Valid(body) || !bytes.HasPrefix(bytes.TrimSpace(body), []byte("{"))) {
		writeIngestError(w, r, errInvalidPayload)
		return
	}
	inserted, err := admit(body, protobuf)
	if err != nil {
		writeIngestError(w, r, err)
		return
	}
	accepted = true
	w.Header().Set("X-GoTel-Acknowledgment", "accepted-not-persisted")
	if protobuf {
		// An empty protobuf message is a valid OTLP Export*ServiceResponse.
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{countField: inserted})
}

func writeIngestError(w http.ResponseWriter, r *http.Request, err error) {
	httpStatus, code, message := http.StatusInternalServerError, int32(13), "Telemetry admission failed"
	switch {
	case errors.Is(err, store.ErrOverloaded), errors.Is(err, store.ErrClosed), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		httpStatus, code, message = http.StatusServiceUnavailable, 8, "Telemetry admission unavailable; retry later"
		w.Header().Set("Retry-After", "1")
	case errors.Is(err, store.ErrRecordTooLarge):
		httpStatus, code, message = http.StatusRequestEntityTooLarge, 8, "Telemetry request or record exceeds configured limits"
	case errors.Is(err, store.ErrInvalidRecord), errors.Is(err, errInvalidPayload):
		httpStatus, code, message = http.StatusBadRequest, 3, "Invalid telemetry request"
	}
	status := &statuspb.Status{Code: code, Message: message}
	var body []byte
	if protobufContent(r.Header.Get("Content-Type")) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		body, _ = proto.Marshal(status)
	} else {
		w.Header().Set("Content-Type", "application/json")
		body, _ = protojson.Marshal(status)
	}
	w.WriteHeader(httpStatus)
	_, _ = w.Write(body)
}

func (s *Server) services(w http.ResponseWriter, r *http.Request) {
	data, err := s.store.ListServices(r.Context())
	writeData(w, data, err)
}

func (s *Server) traces(w http.ResponseWriter, r *http.Request) {
	policy := parseListPolicy(r.URL.Query(), tracePolicy)
	filter := store.TraceFilter{Service: r.URL.Query().Get("service"), SinceMs: policy.since, Cursor: decodeTraceCursor(r.URL.Query().Get("cursor"))}
	s.writeTraceList(w, r, filter, policy)
}

func (s *Server) searchTraces(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	policy := parseListPolicy(query, tracePolicy)
	filter := store.TraceFilter{
		Service: query.Get("service"), Operation: query.Get("operation"), Status: query.Get("status"), AIText: query.Get("aiText"),
		MinDurationMs: optionalFloat(query.Get("minDurationMs")), Attributes: dynamicAttributes(query, "attr."),
		SinceMs: policy.since, Cursor: decodeTraceCursor(query.Get("cursor")),
	}
	s.writeTraceList(w, r, filter, policy)
}

func (s *Server) writeTraceList(w http.ResponseWriter, r *http.Request, filter store.TraceFilter, policy parsedPolicy) {
	data, err := s.store.ListTraceSummaries(r.Context(), filter, policy.limit+1)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	truncated := len(data) > policy.limit
	if truncated {
		data = data[:policy.limit]
	}
	var next *string
	if len(data) > 0 {
		cursor := encodeCursor(map[string]any{"kind": "trace", "startedAt": parseISO(data[len(data)-1].StartedAt), "id": data[len(data)-1].TraceID})
		next = &cursor
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data, "meta": listMeta(policy, len(data), truncated, next)})
}

func (s *Server) trace(w http.ResponseWriter, r *http.Request) {
	data, err := s.store.GetTrace(r.Context(), r.PathValue("traceId"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
	} else if data == nil {
		writeError(w, http.StatusNotFound, "Trace not found")
	} else {
		writeJSON(w, http.StatusOK, map[string]any{"data": data})
	}
}

func (s *Server) traceSpans(w http.ResponseWriter, r *http.Request) {
	data, err := s.store.ListTraceSpans(r.Context(), r.PathValue("traceId"))
	writeData(w, data, err)
}

func (s *Server) span(w http.ResponseWriter, r *http.Request) {
	data, err := s.store.GetSpan(r.Context(), r.PathValue("spanId"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
	} else if data == nil {
		writeError(w, http.StatusNotFound, "Span not found")
	} else {
		writeJSON(w, http.StatusOK, map[string]any{"data": data})
	}
}

func (s *Server) searchSpans(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	policy := parseListPolicy(query, spanPolicy)
	filter := store.SpanFilter{
		Service: query.Get("service"), TraceID: query.Get("traceId"), Operation: query.Get("operation"),
		ParentOperation: query.Get("parentOperation"), Status: query.Get("status"), SinceMs: policy.since,
		Attributes: dynamicAttributes(query, "attr."), AttributeContains: dynamicAttributes(query, "attrContains."),
	}
	data, err := s.store.SearchSpans(r.Context(), filter, policy.limit+1)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	truncated := len(data) > policy.limit
	if truncated {
		data = data[:policy.limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data, "meta": listMeta(policy, len(data), truncated, nil)})
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	s.writeLogList(w, r, store.LogFilter{})
}

func (s *Server) traceLogs(w http.ResponseWriter, r *http.Request) {
	s.writeLogList(w, r, store.LogFilter{TraceID: r.PathValue("traceId")})
}

func (s *Server) spanLogs(w http.ResponseWriter, r *http.Request) {
	s.writeLogList(w, r, store.LogFilter{SpanID: r.PathValue("spanId")})
}

func (s *Server) writeLogList(w http.ResponseWriter, r *http.Request, filter store.LogFilter) {
	query := r.URL.Query()
	policy := parseListPolicy(query, logPolicy)
	if filter.TraceID == "" {
		filter.TraceID = query.Get("traceId")
	}
	if filter.SpanID == "" {
		filter.SpanID = query.Get("spanId")
	}
	filter.Service, filter.Severity, filter.Body = query.Get("service"), query.Get("severity"), query.Get("body")
	filter.Attributes, filter.AttributeContains = dynamicAttributes(query, "attr."), dynamicAttributes(query, "attrContains.")
	filter.SinceMs, filter.Cursor = policy.since, decodeLogCursor(query.Get("cursor"))
	data, err := s.store.SearchLogs(r.Context(), filter, policy.limit+1)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	truncated := len(data) > policy.limit
	if truncated {
		data = data[:policy.limit]
	}
	var next *string
	if len(data) > 0 {
		cursor := encodeCursor(map[string]any{"kind": "log", "timestamp": parseISO(data[len(data)-1].Timestamp), "id": data[len(data)-1].ID})
		next = &cursor
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data, "meta": listMeta(policy, len(data), truncated, next)})
}

func (s *Server) traceStats(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	groupBy, aggregate := query.Get("groupBy"), query.Get("agg")
	if groupBy == "" || !oneOf(aggregate, "count", "avg_duration", "p95_duration", "error_rate") {
		writeError(w, http.StatusBadRequest, "Expected groupBy and agg=count|avg_duration|p95_duration|error_rate")
		return
	}
	policy := parseListPolicy(query, traceStatsPolicy)
	data, err := s.store.TraceStats(r.Context(), groupBy, aggregate, store.TraceFilter{
		Service: query.Get("service"), Operation: query.Get("operation"), Status: query.Get("status"),
		MinDurationMs: optionalFloat(query.Get("minDurationMs")), Attributes: dynamicAttributes(query, "attr."), SinceMs: policy.since,
	}, policy.limit)
	writeData(w, data, err)
}

func (s *Server) logStats(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	groupBy, aggregate := query.Get("groupBy"), query.Get("agg")
	if groupBy == "" || aggregate != "count" {
		writeError(w, http.StatusBadRequest, "Expected groupBy and agg=count")
		return
	}
	policy := parseListPolicy(query, logStatsPolicy)
	data, err := s.store.LogStats(r.Context(), groupBy, store.LogFilter{
		Service: query.Get("service"), TraceID: query.Get("traceId"), SpanID: query.Get("spanId"), Body: query.Get("body"),
		Attributes: dynamicAttributes(query, "attr."), SinceMs: policy.since,
	}, policy.limit)
	writeData(w, data, err)
}

func (s *Server) facets(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	telemetryType, field := query.Get("type"), query.Get("field")
	if !oneOf(telemetryType, "traces", "logs") || field == "" {
		writeError(w, http.StatusBadRequest, "Expected type=traces|logs and field=<name>")
		return
	}
	minutes, normalized := parseLookback(query.Get("lookback"), s.config.TraceLookbackMinutes, 0)
	_ = normalized
	limit := parsePositive(query.Get("limit"), 20)
	data, err := s.store.ListFacets(r.Context(), telemetryType, field, query.Get("key"), query.Get("service"), time.Now().Add(-time.Duration(minutes)*time.Minute).UnixMilli(), limit)
	writeData(w, data, err)
}

func (s *Server) aiCalls(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	policy := parseListPolicy(query, aiPolicy)
	data, err := s.store.SearchAICalls(r.Context(), aiFilter(query, policy.since), policy.limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data, "meta": listMeta(policy, len(data), false, nil)})
}

func (s *Server) aiCall(w http.ResponseWriter, r *http.Request) {
	data, err := s.store.GetAICall(r.Context(), r.PathValue("spanId"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
	} else if data == nil {
		writeError(w, http.StatusNotFound, "AI call not found")
	} else {
		writeJSON(w, http.StatusOK, map[string]any{"data": data})
	}
}

func (s *Server) aiStats(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	groupBy, aggregate := query.Get("groupBy"), query.Get("agg")
	if groupBy == "" || aggregate == "" {
		writeError(w, http.StatusBadRequest, "Expected groupBy and agg parameters")
		return
	}
	if !oneOf(groupBy, "provider", "model", "functionId", "sessionId", "status") || !oneOf(aggregate, "count", "avg_duration", "p95_duration", "total_input_tokens", "total_output_tokens") {
		writeError(w, http.StatusBadRequest, "Invalid groupBy or agg parameter")
		return
	}
	policy := parseListPolicy(query, aiPolicy)
	data, err := s.store.AIStats(r.Context(), groupBy, aggregate, aiFilter(query, policy.since), policy.limit)
	writeData(w, data, err)
}

func aiFilter(query url.Values, since int64) store.AIFilter {
	return store.AIFilter{
		Service: query.Get("service"), TraceID: query.Get("traceId"), SessionID: query.Get("sessionId"),
		FunctionID: query.Get("functionId"), Provider: query.Get("provider"), Model: query.Get("model"),
		Operation: query.Get("operation"), Status: query.Get("status"), Text: query.Get("text"),
		MinDurationMs: optionalFloat(query.Get("minDurationMs")), SinceMs: since,
	}
}

func (s *Server) docsIndex(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"docs": []map[string]string{
		{"name": "debug", "title": "Gotel Debug Workflow", "path": "/api/docs/debug"},
	}})
}

func (s *Server) doc(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name != "debug" {
		writeError(w, http.StatusNotFound, "Unknown doc: "+name+". Available: debug")
		return
	}
	content, err := docs.ReadFile("docs/" + name + ".md")
	if err != nil {
		writeError(w, http.StatusNotFound, "Doc file not found: "+name)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(content)
}

func (s *Server) openapi(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, openAPISpec())
}

type listPolicy struct{ defaultLimit, maxLimit int }
type parsedPolicy struct {
	limit, minutes int
	lookback       string
	since          int64
}

var (
	tracePolicy      = listPolicy{20, 100}
	spanPolicy       = listPolicy{100, 500}
	logPolicy        = listPolicy{100, 500}
	aiPolicy         = listPolicy{20, 500}
	traceStatsPolicy = listPolicy{20, 100}
	logStatsPolicy   = listPolicy{20, 500}
	lookbackPattern  = regexp.MustCompile(`(?i)^(\d+)([mhd])$`)
)

func parseListPolicy(query url.Values, policy listPolicy) parsedPolicy {
	limit := min(parsePositive(query.Get("limit"), policy.defaultLimit), policy.maxLimit)
	minutes, lookback := parseLookback(query.Get("lookback"), 60, 1440)
	return parsedPolicy{limit: limit, minutes: minutes, lookback: lookback, since: time.Now().Add(-time.Duration(minutes) * time.Minute).UnixMilli()}
}

func parseLookback(value string, fallback, maximum int) (int, string) {
	minutes := fallback
	if match := lookbackPattern.FindStringSubmatch(value); match != nil {
		n, _ := strconv.Atoi(match[1])
		switch strings.ToLower(match[2]) {
		case "h":
			n *= 60
		case "d":
			n *= 1440
		}
		if n > 0 {
			minutes = n
		}
	}
	minutes = max(1, minutes)
	if maximum > 0 {
		minutes = min(minutes, maximum)
	}
	return minutes, formatLookback(minutes)
}

func formatLookback(minutes int) string {
	if minutes%1440 == 0 {
		return strconv.Itoa(minutes/1440) + "d"
	}
	if minutes%60 == 0 {
		return strconv.Itoa(minutes/60) + "h"
	}
	return strconv.Itoa(minutes) + "m"
}

func listMeta(policy parsedPolicy, returned int, truncated bool, next *string) map[string]any {
	return map[string]any{"limit": policy.limit, "lookback": policy.lookback, "returned": returned, "truncated": truncated, "nextCursor": next}
}

func dynamicAttributes(query url.Values, prefix string) map[string]string {
	result := make(map[string]string)
	for key, values := range query {
		if strings.HasPrefix(key, prefix) && len(key) > len(prefix) && len(values) > 0 {
			result[strings.TrimPrefix(key, prefix)] = values[len(values)-1]
		}
	}
	return result
}

func encodeCursor(value any) string {
	encoded, _ := json.Marshal(value)
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func decodeTraceCursor(value string) *store.TraceCursor {
	var cursor struct {
		Kind      string `json:"kind"`
		StartedAt int64  `json:"startedAt"`
		ID        string `json:"id"`
	}
	if decodeCursor(value, &cursor) != nil || cursor.Kind != "trace" {
		return nil
	}
	return &store.TraceCursor{StartedAt: cursor.StartedAt, ID: cursor.ID}
}

func decodeLogCursor(value string) *store.LogCursor {
	var cursor struct {
		Kind      string `json:"kind"`
		Timestamp int64  `json:"timestamp"`
		ID        string `json:"id"`
	}
	if decodeCursor(value, &cursor) != nil || cursor.Kind != "log" {
		return nil
	}
	return &store.LogCursor{Timestamp: cursor.Timestamp, ID: cursor.ID}
}

func decodeCursor(value string, target any) error {
	if value == "" {
		return fmt.Errorf("empty cursor")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(decoded, target)
}

func optionalFloat(value string) *float64 {
	if value == "" {
		return nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil
	}
	return &parsed
}

func parsePositive(value string, fallback int) int {
	i := 0
	for i < len(value) && value[i] >= '0' && value[i] <= '9' {
		i++
	}
	if i == 0 {
		return fallback
	}
	parsed, err := strconv.Atoi(value[:i])
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func parseISO(value string) int64 {
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed.UnixMilli()
}

func protobufContent(value string) bool {
	value = strings.ToLower(value)
	return strings.Contains(value, "application/x-protobuf") || strings.Contains(value, "application/protobuf")
}

func oneOf(value string, values ...string) bool {
	for _, candidate := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func writeData(w http.ResponseWriter, data any, err error) {
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func defaultIdentity(cfg config.Config) Identity {
	workdir, _ := os.Getwd()
	return Identity{PID: os.Getpid(), URL: cfg.BaseURL, Workdir: workdir, StartedAt: time.Now().UTC().Format(time.RFC3339Nano), DatabasePath: cfg.DatabasePath}
}
