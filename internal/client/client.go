package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/registry"
)

type Health struct {
	OK                     bool            `json:"ok"`
	Ready                  bool            `json:"ready"`
	Persistence            json.RawMessage `json:"persistence,omitempty"`
	ShutdownTimeoutSeconds int             `json:"shutdownTimeoutSeconds,omitempty"`
	Service                string          `json:"service"`
	DatabasePath           string          `json:"databasePath"`
	DatabaseBackend        string          `json:"databaseBackend,omitempty"`
	PID                    int             `json:"pid"`
	URL                    string          `json:"url"`
	Workdir                string          `json:"workdir"`
	StartedAt              string          `json:"startedAt"`
	Version                string          `json:"version"`
	InstanceID             string          `json:"instanceId,omitempty"`
}

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

type HTTPError struct {
	Status int
	Body   any
}

func (e *HTTPError) Error() string {
	encoded, _ := json.Marshal(e.Body)
	return fmt.Sprintf("gotel returned HTTP %d: %s", e.Status, encoded)
}

func New(baseURL string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), HTTP: &http.Client{Timeout: 5 * time.Second}}
}

func (c *Client) Health(ctx context.Context) (*Health, error) {
	var health Health
	if err := c.GetInto(ctx, "/api/health", nil, &health); err != nil {
		return nil, err
	}
	if !health.OK || health.Service != "gotel-local-server" {
		return nil, fmt.Errorf("unexpected server identity")
	}
	return &health, nil
}

func (c *Client) Get(ctx context.Context, path string, query url.Values) (any, error) {
	var result any
	if err := c.GetInto(ctx, path, query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) GetInto(ctx context.Context, path string, query url.Values, target any) error {
	requestURL := c.BaseURL + path
	if len(query) > 0 {
		requestURL += "?" + query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return err
	}
	response, err := c.HTTP.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	return decodeResponse(response, target)
}

func (c *Client) GetText(ctx context.Context, path string) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return "", err
	}
	response, err := c.HTTP.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var body any
		_ = json.NewDecoder(response.Body).Decode(&body)
		return "", &HTTPError{Status: response.StatusCode, Body: body}
	}
	content, err := io.ReadAll(response.Body)
	return string(content), err
}

func (c *Client) PostJSON(ctx context.Context, path string, value any) (any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.HTTP.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var result any
	if err := decodeResponse(response, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func decodeResponse(response *http.Response, target any) error {
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var body any
		if err := decoder.Decode(&body); err != nil {
			body = map[string]string{"error": "invalid json"}
		}
		return &HTTPError{Status: response.StatusCode, Body: body}
	}
	return decoder.Decode(target)
}

type Resolution struct {
	Client *Client
	Health *Health
	Source string
	Count  int
}

func Resolve(ctx context.Context, cfg config.Config) (*Resolution, error) {
	if override := strings.TrimSpace(os.Getenv("GOTEL_URL")); override != "" {
		resolved, err := resolveURL(ctx, override)
		if err != nil {
			return nil, fmt.Errorf("GOTEL_URL: %w", err)
		}
		resolved.Source = "environment"
		return resolved, nil
	}
	entries, err := registry.List(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	workdir, _ := os.Getwd()
	type candidate struct {
		entry  registry.Entry
		client *Client
		health *Health
	}
	alive := make([]candidate, 0)
	for _, entry := range entries {
		if !registry.Alive(entry.PID) {
			continue
		}
		candidateClient := New(entry.URL)
		health, healthErr := candidateClient.Health(ctx)
		if healthErr != nil || health.PID != entry.PID || health.InstanceID != entry.InstanceID {
			continue
		}
		alive = append(alive, candidate{entry: entry, client: candidateClient, health: health})
	}
	if len(alive) == 0 {
		return nil, fmt.Errorf("no running Gotel instance found; run gotel start or set GOTEL_URL")
	}
	entryList := make([]registry.Entry, len(alive))
	for i := range alive {
		entryList[i] = alive[i].entry
	}
	if best := registry.BestForWorkdir(entryList, workdir); best != nil {
		for _, candidate := range alive {
			if candidate.entry.PID == best.PID {
				return &Resolution{Client: candidate.client, Health: candidate.health, Source: "workdir", Count: len(alive)}, nil
			}
		}
	}
	if len(alive) == 1 {
		return &Resolution{Client: alive[0].client, Health: alive[0].health, Source: "single", Count: 1}, nil
	}
	return nil, fmt.Errorf("multiple Gotel instances found; set GOTEL_URL")
}

func resolveURL(ctx context.Context, value string) (*Resolution, error) {
	client := New(value)
	health, err := client.Health(ctx)
	if err != nil {
		return nil, err
	}
	return &Resolution{Client: client, Health: health, Count: 1}, nil
}
