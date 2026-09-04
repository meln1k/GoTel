package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/meln1k/gotel/internal/api"
	"github.com/meln1k/gotel/internal/client"
	"github.com/meln1k/gotel/internal/config"
)

type tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	Annotations map[string]any `json:"annotations"`
}

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

func Run(ctx context.Context, cfg config.Config) error {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var message request
		if err := json.Unmarshal(scanner.Bytes(), &message); err != nil {
			_ = encoder.Encode(rpcError(nil, -32700, "Parse error"))
			continue
		}
		if len(message.ID) == 0 {
			continue
		}
		var result any
		var responseError map[string]any
		switch message.Method {
		case "initialize":
			var parameters struct {
				ProtocolVersion string `json:"protocolVersion"`
			}
			_ = json.Unmarshal(message.Params, &parameters)
			protocolVersion := parameters.ProtocolVersion
			if protocolVersion == "" {
				protocolVersion = "2025-11-25"
			}
			result = map[string]any{
				"protocolVersion": protocolVersion,
				"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
				"serverInfo":      map[string]string{"name": "gotel", "version": config.Version},
				"instructions":    "Query local OpenTelemetry traces and logs. Tools are read-only.",
			}
		case "ping":
			result = map[string]any{}
		case "tools/list":
			result = map[string]any{"tools": tools()}
		case "tools/call":
			var parameters struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			}
			if err := json.Unmarshal(message.Params, &parameters); err != nil {
				responseError = rpcErrorValue(-32602, "Invalid params")
				break
			}
			payload := callTool(ctx, cfg, parameters.Name, parameters.Arguments)
			text, _ := json.Marshal(payload)
			result = map[string]any{
				"content":           []map[string]string{{"type": "text", "text": string(text)}},
				"structuredContent": payload,
			}
		default:
			responseError = rpcErrorValue(-32601, "Method not found")
		}
		response := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(message.ID)}
		if responseError != nil {
			response["error"] = responseError
		} else {
			response["result"] = result
		}
		if err := encoder.Encode(response); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func callTool(parent context.Context, cfg config.Config, name string, arguments map[string]any) any {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	resolution, err := client.Resolve(ctx, cfg)
	if name == "gotel_status" {
		if err != nil {
			return map[string]any{"connected": false, "error": err.Error()}
		}
		cwd, _ := os.Getwd()
		return map[string]any{
			"connected": true, "url": resolution.Client.BaseURL, "version": resolution.Health.Version,
			"workdir": resolution.Health.Workdir, "cwdMatch": within(cwd, resolution.Health.Workdir),
			"instanceCount": resolution.Count, "source": resolution.Source,
		}
	}
	if err != nil {
		return map[string]string{"error": err.Error()}
	}
	httpClient := resolution.Client
	endpoint, ok := api.EndpointForTool(name)
	if !ok {
		return map[string]string{"error": "unknown tool: " + name}
	}
	path, query := toolRequest(endpoint, arguments)
	if endpoint.TextResponse {
		text, getErr := httpClient.GetText(ctx, path)
		if getErr != nil {
			return map[string]string{"error": getErr.Error()}
		}
		return map[string]string{"data": text}
	}
	result, requestErr := httpClient.Get(ctx, path, query)
	if requestErr != nil {
		return map[string]string{"error": requestErr.Error()}
	}
	return result
}

func toolRequest(endpoint api.Endpoint, arguments map[string]any) (string, url.Values) {
	path := endpoint.Path
	query := url.Values{}
	for _, parameter := range endpoint.Parameters {
		if parameter.ToolHidden {
			continue
		}
		if parameter.Location == api.Path {
			placeholder := "{" + parameter.Name + "}"
			path = strings.ReplaceAll(path, placeholder, url.PathEscape(scalar(arguments[parameter.ArgumentName()])))
			continue
		}
		if parameter.ToolFixed != "" {
			query.Set(parameter.Name, parameter.ToolFixed)
			continue
		}
		if parameter.QueryPrefix != "" {
			values, ok := arguments[parameter.ArgumentName()].(map[string]any)
			if !ok {
				continue
			}
			for key, value := range values {
				query.Set(parameter.QueryPrefix+key, scalar(value))
			}
			continue
		}
		value, ok := arguments[parameter.ArgumentName()]
		if ok && value != nil && fmt.Sprint(value) != "" {
			query.Set(parameter.Name, scalar(value))
		}
	}
	return path, query
}

func tools() []tool {
	readOnly := map[string]any{"readOnlyHint": true}
	result := []tool{{
		Name: "gotel_status", Description: "Check connection to a local Gotel server.",
		InputSchema: schema(nil, nil), Annotations: readOnly,
	}}
	for _, endpoint := range api.ToolEndpoints() {
		result = append(result, tool{
			Name: endpoint.Tool.Name, Description: endpoint.Tool.Description,
			InputSchema: toolSchema(endpoint), Annotations: readOnly,
		})
	}
	return result
}

func toolSchema(endpoint api.Endpoint) map[string]any {
	props := make(map[string]any)
	required := make([]string, 0)
	for _, parameter := range endpoint.Parameters {
		if parameter.ToolHidden || parameter.ToolFixed != "" {
			continue
		}
		name := parameter.ArgumentName()
		var property map[string]any
		switch parameter.Type {
		case api.Number:
			property = number()
		case api.StringMap:
			property = attributes()
		default:
			if len(parameter.Enum) != 0 {
				property = enum(parameter.Enum...)
			} else {
				property = text()
			}
		}
		props[name] = property
		if parameter.Required {
			required = append(required, name)
		}
	}
	return schema(props, required)
}

func schema(props map[string]any, required []string) map[string]any {
	if props == nil {
		props = map[string]any{}
	}
	result := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	if len(required) > 0 {
		result["required"] = required
	}
	return result
}

func text() map[string]any   { return map[string]any{"type": "string"} }
func number() map[string]any { return map[string]any{"type": "number"} }
func attributes() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}}
}
func enum(values ...string) map[string]any {
	return map[string]any{"type": "string", "enum": values}
}

func scalar(value any) string {
	switch value := value.(type) {
	case nil:
		return ""
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	case json.Number:
		return value.String()
	default:
		return fmt.Sprint(value)
	}
}

func within(child, parent string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func rpcError(id any, code int, message string) map[string]any {
	return map[string]any{"jsonrpc": "2.0", "id": id, "error": rpcErrorValue(code, message)}
}

func rpcErrorValue(code int, message string) map[string]any {
	return map[string]any{"code": code, "message": message}
}
