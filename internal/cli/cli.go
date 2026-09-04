package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/meln1k/gotel/internal/client"
	"github.com/meln1k/gotel/internal/config"
)

func Run(ctx context.Context, cfg config.Config, args []string) (bool, error) {
	if len(args) == 0 {
		return false, nil
	}
	command, commandArgs := args[0], args[1:]
	httpClient := client.New(cfg.QueryURL)
	service := func(index int) string {
		if len(commandArgs) > index && commandArgs[index] != "" {
			return commandArgs[index]
		}
		return cfg.ServiceName
	}
	var result any
	var err error
	switch command {
	case "services":
		result, err = getData(ctx, httpClient, "/api/services", nil)
	case "traces":
		query := url.Values{"service": {service(0)}, "limit": {integerArg(commandArgs, 1, cfg.TraceLimit)}, "lookback": {strconv.Itoa(cfg.TraceLookbackMinutes) + "m"}}
		result, err = getData(ctx, httpClient, "/api/traces", query)
	case "trace":
		if len(commandArgs) < 1 {
			return true, fmt.Errorf("usage: gotel trace <trace-id>")
		}
		result, err = getData(ctx, httpClient, "/api/traces/"+url.PathEscape(commandArgs[0]), nil)
		var httpError *client.HTTPError
		if errors.As(err, &httpError) && httpError.Status == 404 {
			result, err = nil, nil
		}
	case "span":
		if len(commandArgs) < 1 {
			return true, fmt.Errorf("usage: gotel span <span-id>")
		}
		result, err = httpClient.Get(ctx, "/api/spans/"+url.PathEscape(commandArgs[0]), nil)
		var httpError *client.HTTPError
		if errors.As(err, &httpError) {
			result, err = httpError.Body, nil
		}
	case "trace-spans":
		if len(commandArgs) < 1 {
			return true, fmt.Errorf("usage: gotel trace-spans <trace-id>")
		}
		result, err = getData(ctx, httpClient, "/api/traces/"+url.PathEscape(commandArgs[0])+"/spans", nil)
	case "search-spans":
		query := url.Values{"service": {service(0)}, "limit": {strconv.Itoa(cfg.LogLimit)}, "lookback": {strconv.Itoa(cfg.TraceLookbackMinutes) + "m"}}
		filterStart := 1
		if len(commandArgs) > 1 && !attributeToken(commandArgs[1]) && !strings.HasPrefix(commandArgs[1], "parent=") {
			query.Set("operation", commandArgs[1])
			filterStart = 2
		}
		for _, argument := range commandArgs[1:] {
			if strings.HasPrefix(argument, "parent=") {
				query.Set("parentOperation", strings.TrimPrefix(argument, "parent="))
				break
			}
		}
		addAttributeTokens(query, commandArgs[filterStart:])
		result, err = getData(ctx, httpClient, "/api/spans/search", query)
	case "search-traces":
		query := url.Values{"service": {service(0)}, "limit": {strconv.Itoa(cfg.TraceLimit)}, "lookback": {strconv.Itoa(cfg.TraceLookbackMinutes) + "m"}}
		filterStart := 1
		if len(commandArgs) > 1 && !attributeToken(commandArgs[1]) {
			query.Set("operation", commandArgs[1])
			filterStart = 2
		}
		addAttributeTokens(query, commandArgs[filterStart:])
		result, err = getData(ctx, httpClient, "/api/traces/search", query)
	case "trace-stats":
		if len(commandArgs) < 2 || !oneOf(commandArgs[1], "count", "avg_duration", "p95_duration", "error_rate") {
			return true, fmt.Errorf("usage: gotel trace-stats <groupBy> <count|avg_duration|p95_duration|error_rate> [service] [attr.key=value ...]")
		}
		query := url.Values{"groupBy": {commandArgs[0]}, "agg": {commandArgs[1]}, "limit": {"20"}, "lookback": {strconv.Itoa(cfg.TraceLookbackMinutes) + "m"}}
		filterStart := 2
		if len(commandArgs) > 2 && !attributeToken(commandArgs[2]) {
			query.Set("service", commandArgs[2])
			filterStart = 3
		}
		addAttributeTokens(query, commandArgs[filterStart:])
		result, err = getData(ctx, httpClient, "/api/traces/stats", query)
	case "logs":
		query := url.Values{"service": {service(0)}, "limit": {strconv.Itoa(cfg.LogLimit)}, "lookback": {strconv.Itoa(cfg.TraceLookbackMinutes) + "m"}}
		result, err = getData(ctx, httpClient, "/api/logs", query)
	case "search-logs":
		query := url.Values{"service": {service(0)}, "limit": {strconv.Itoa(cfg.LogLimit)}, "lookback": {strconv.Itoa(cfg.TraceLookbackMinutes) + "m"}}
		filterStart := 1
		if len(commandArgs) > 1 && !attributeToken(commandArgs[1]) {
			query.Set("body", commandArgs[1])
			filterStart = 2
		}
		addAttributeTokens(query, commandArgs[filterStart:])
		result, err = getData(ctx, httpClient, "/api/logs/search", query)
	case "log-stats":
		if len(commandArgs) < 1 {
			return true, fmt.Errorf("usage: gotel log-stats <groupBy> [service] [attr.key=value ...]")
		}
		query := url.Values{"groupBy": {commandArgs[0]}, "agg": {"count"}, "limit": {"20"}, "lookback": {strconv.Itoa(cfg.TraceLookbackMinutes) + "m"}}
		filterStart := 1
		if len(commandArgs) > 1 && !attributeToken(commandArgs[1]) {
			query.Set("service", commandArgs[1])
			filterStart = 2
		}
		addAttributeTokens(query, commandArgs[filterStart:])
		result, err = getData(ctx, httpClient, "/api/logs/stats", query)
	case "trace-logs", "span-logs":
		if len(commandArgs) < 1 {
			return true, fmt.Errorf("usage: gotel %s <id>", command)
		}
		path := "/api/traces/" + url.PathEscape(commandArgs[0]) + "/logs"
		if command == "span-logs" {
			path = "/api/spans/" + url.PathEscape(commandArgs[0]) + "/logs"
		}
		result, err = getData(ctx, httpClient, path, url.Values{"limit": {strconv.Itoa(cfg.LogLimit)}, "lookback": {strconv.Itoa(cfg.TraceLookbackMinutes) + "m"}})
	case "facets":
		if len(commandArgs) < 2 || !oneOf(commandArgs[0], "traces", "logs") {
			return true, fmt.Errorf("usage: gotel facets <traces|logs> <field>")
		}
		result, err = getData(ctx, httpClient, "/api/facets", url.Values{"type": {commandArgs[0]}, "field": {commandArgs[1]}, "limit": {"20"}})
	case "instructions":
		fmt.Print(instructions(cfg))
		return true, nil
	case "endpoints":
		result = map[string]string{"baseUrl": cfg.BaseURL, "exporterUrl": cfg.ExporterURL, "logsExporterUrl": cfg.LogsExporterURL, "queryUrl": cfg.QueryURL, "databasePath": cfg.DatabasePath}
	default:
		return false, nil
	}
	if err != nil {
		return true, err
	}
	return true, printJSON(result)
}

func getData(ctx context.Context, httpClient *client.Client, path string, query url.Values) (any, error) {
	response, err := httpClient.Get(ctx, path, query)
	if err != nil {
		return nil, err
	}
	object, ok := response.(map[string]any)
	if !ok {
		return response, nil
	}
	return object["data"], nil
}

func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func integerArg(args []string, index, fallback int) string {
	if len(args) <= index || args[index] == "" {
		return strconv.Itoa(fallback)
	}
	return args[index]
}

func attributeToken(value string) bool {
	return strings.HasPrefix(value, "attr.") && strings.Contains(value, "=")
}

func addAttributeTokens(query url.Values, arguments []string) {
	for _, argument := range arguments {
		if !attributeToken(argument) {
			continue
		}
		key, value, _ := strings.Cut(strings.TrimPrefix(argument, "attr."), "=")
		query.Set("attr."+key, value)
	}
}

func oneOf(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if value == candidate {
			return true
		}
	}
	return false
}

func instructions(cfg config.Config) string {
	return fmt.Sprintf(`Export OpenTelemetry data to Gotel:

  OTEL_EXPORTER_OTLP_ENDPOINT=%s
  OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=%s
  OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=%s
  OTEL_SERVICE_NAME=your-service

Gotel accepts OTLP/HTTP JSON and protobuf.
`, cfg.BaseURL, cfg.ExporterURL, cfg.LogsExporterURL)
}

func Usage() string {
	return `Usage: gotel <command>

Server commands:
  start | daemon          Start or reconnect to the background server
  status                  Show server status
  stop                    Stop the managed server
  restart                 Restart the managed server
  server                  Run the server in the foreground
  mcp                     Run the MCP server over stdio

Query commands:
  services
  traces [service] [limit]
  trace <trace-id>
  span <span-id>
  trace-spans <trace-id>
  search-spans [service] [operation] [parent=<operation>] [attr.key=value ...]
  search-traces [service] [operation] [attr.key=value ...]
  trace-stats <groupBy> <agg> [service] [attr.key=value ...]
  logs [service]
  search-logs [service] [body] [attr.key=value ...]
  log-stats <groupBy> [service] [attr.key=value ...]
  trace-logs <trace-id>
  span-logs <span-id>
  facets <traces|logs> <field>
  instructions
  endpoints
  clear-debug [path]
`
}
