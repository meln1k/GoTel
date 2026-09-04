package api

import (
	"strings"
	"testing"
)

func TestEndpointContractIsUniqueAndComplete(t *testing.T) {
	if len(Endpoints()) != 24 {
		t.Fatalf("got %d endpoints, want 24", len(Endpoints()))
	}
	if len(ToolEndpoints()) != 18 {
		t.Fatalf("got %d tool endpoints, want 18", len(ToolEndpoints()))
	}

	operations := make(map[Operation]bool)
	routes := make(map[string]bool)
	tools := make(map[string]bool)
	for _, endpoint := range Endpoints() {
		if endpoint.Operation == "" || operations[endpoint.Operation] {
			t.Errorf("missing or duplicate operation: %q", endpoint.Operation)
		}
		operations[endpoint.Operation] = true
		route := endpoint.Method + " " + endpoint.Path
		if routes[route] {
			t.Errorf("duplicate route: %q", route)
		}
		routes[route] = true

		pathParameters := make(map[string]bool)
		for _, parameter := range endpoint.Parameters {
			if parameter.Name == "" {
				t.Errorf("%s has an unnamed parameter", endpoint.Operation)
			}
			if parameter.Location == Path {
				pathParameters[parameter.Name] = true
				if !parameter.Required || !strings.Contains(endpoint.Path, "{"+parameter.Name+"}") {
					t.Errorf("%s has invalid path parameter %q", endpoint.Operation, parameter.Name)
				}
			}
			if parameter.Type == StringMap && parameter.QueryPrefix == "" {
				t.Errorf("%s map parameter %q has no query prefix", endpoint.Operation, parameter.Name)
			}
		}
		for remaining := endpoint.Path; strings.Contains(remaining, "{"); {
			start := strings.Index(remaining, "{")
			end := strings.Index(remaining[start:], "}")
			if end < 0 {
				t.Fatalf("%s has an invalid path template %q", endpoint.Operation, endpoint.Path)
			}
			name := remaining[start+1 : start+end]
			if !pathParameters[name] {
				t.Errorf("%s path placeholder %q has no parameter", endpoint.Operation, name)
			}
			remaining = remaining[start+end+1:]
		}

		if endpoint.Tool != nil {
			if endpoint.Tool.Name == "" || tools[endpoint.Tool.Name] {
				t.Errorf("missing or duplicate tool: %q", endpoint.Tool.Name)
			}
			tools[endpoint.Tool.Name] = true
		}
	}
}
