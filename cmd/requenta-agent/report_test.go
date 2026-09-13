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

func TestConsoleReportIsScopedAndNeverFollowsRedirects(t *testing.T) {
	token := filepath.Join(t.TempDir(), "token")
	os.WriteFile(token, []byte("connection-only"), 0600)
	requests := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/supplier/inventory" || r.Header.Get("Authorization") != "Bearer connection-only" {
			t.Error("wrong origin or credential")
		}
		var v struct {
			Cluster string `json:"cluster_uid"`
			Nodes   []Node `json:"nodes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&v); err != nil || v.Cluster != "cluster-a" || len(v.Nodes) != 1 {
			t.Error("wrong inventory")
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	if err := postReport(context.Background(), server.URL, token, "cluster-a", []Node{{"gpu-a", "uid-a", 8, true}}, server.Client()); err != nil {
		t.Fatal(err)
	}
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, server.URL+"/api/supplier/inventory", 307)
	}))
	defer redirect.Close()
	if err := postReport(context.Background(), redirect.URL, token, "cluster-a", nil, redirect.Client()); err == nil {
		t.Fatal("redirect must fail")
	}
	if requests != 1 {
		t.Fatal("credential forwarded on redirect")
	}
	if err := postReport(context.Background(), "http://example.test", token, "cluster-a", nil, server.Client()); err == nil {
		t.Fatal("insecure console accepted")
	}
}
