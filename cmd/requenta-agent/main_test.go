package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func testAgent(t *testing.T, handler http.HandlerFunc) *Agent {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	return &Agent{Config: Config{Endpoint: server.URL, Namespace: "requenta-system", StatusName: "rq-status", Selector: "requenta.com/pool=true", TokenPath: token}, Client: server.Client()}
}
func TestDiscoveryUsesSelectorAndRotatingToken(t *testing.T) {
	a := testAgent(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer rotated" {
			t.Error("token not reloaded")
		}
		if r.URL.Query().Get("labelSelector") != "requenta.com/pool=true" {
			t.Error("selector missing")
		}
		_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"gpu-1","uid":"u1"},"status":{"allocatable":{"nvidia.com/gpu":"8"},"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"cpu-1"},"status":{}}]}`))
	})
	_ = os.WriteFile(a.Config.TokenPath, []byte("rotated"), 0600)
	nodes, err := a.Discover(context.Background())
	if err != nil || len(nodes) != 2 || nodes[0].GPUCount != 8 || !nodes[0].Ready || nodes[1].GPUCount != 0 {
		t.Fatalf("unexpected inventory: %+v %v", nodes, err)
	}
}
func TestPartialAndMalformedInventoryRejected(t *testing.T) {
	for _, body := range []string{`{"metadata":{"continue":"next"}}`, `{"items":[{"status":{"allocatable":{"nvidia.com/gpu":"1.5"}}}]}`, `invalid`} {
		t.Run(body, func(t *testing.T) {
			a := testAgent(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) })
			if _, err := a.Discover(context.Background()); err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}
func TestPublishOnlyNamedStatusAndNoQualificationClaim(t *testing.T) {
	a := testAgent(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PATCH" || r.URL.Path != "/api/v1/namespaces/requenta-system/configmaps/rq-status" {
			t.Error("wrong write target")
		}
		var body struct {
			Data map[string]string `json:"data"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Data["execution"] != "disabled" || body.Data["qualification"] != "not-performed" {
			t.Error("overclaimed capability")
		}
		_, _ = w.Write([]byte(`{}`))
	})
	if err := a.Publish(context.Background(), []Node{}); err != nil {
		t.Fatal(err)
	}
}
func TestAPIFailureAndUnsafeEndpoint(t *testing.T) {
	a := testAgent(t, func(w http.ResponseWriter, r *http.Request) { http.Error(w, "sensitive upstream response", 403) })
	if _, err := a.Discover(context.Background()); err == nil || err.Error() != "Kubernetes request failed: HTTP 403" {
		t.Fatal(err)
	}
	if _, err := NewAgent(Config{Endpoint: "http://example.test"}); err == nil {
		t.Fatal("HTTP accepted")
	}
}
