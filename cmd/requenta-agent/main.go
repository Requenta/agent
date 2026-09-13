// requenta-agent inventories explicitly selected Kubernetes nodes. It never provisions workloads.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

type Config struct{ Endpoint, Namespace, StatusName, Selector, TokenPath, CAPath string }
type Agent struct {
	Config Config
	Client *http.Client
}
type Node struct {
	Name     string `json:"name"`
	UID      string `json:"uid"`
	GPUCount int64  `json:"allocatable_gpus"`
	Ready    bool   `json:"ready"`
}
type nodeList struct {
	Metadata struct {
		Continue string `json:"continue"`
	} `json:"metadata"`
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
			UID  string `json:"uid"`
		} `json:"metadata"`
		Status struct {
			Allocatable map[string]string `json:"allocatable"`
			Conditions  []struct {
				Type   string `json:"type"`
				Status string `json:"status"`
			} `json:"conditions"`
		} `json:"status"`
	} `json:"items"`
}

func NewAgent(c Config) (*Agent, error) {
	parsed, err := url.Parse(c.Endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		return nil, errors.New("Kubernetes endpoint must use HTTPS")
	}
	if c.Namespace == "" || c.StatusName == "" || c.Selector == "" {
		return nil, errors.New("namespace, status ConfigMap and node selector are required")
	}
	ca, err := os.ReadFile(c.CAPath)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid Kubernetes CA")
	}
	return &Agent{c, &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (a *Agent) request(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	token, err := os.ReadFile(a.Config.TokenPath)
	if err != nil {
		return nil, err
	} // Read each time: projected credentials rotate.
	req, err := http.NewRequestWithContext(ctx, method, a.Config.Endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+string(bytes.TrimSpace(token)))
	if method == "PATCH" {
		req.Header.Set("Content-Type", "application/merge-patch+json")
	}
	response, err := a.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("Kubernetes request failed: HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 4*1024*1024+1))
	if len(data) > 4*1024*1024 {
		return nil, errors.New("Kubernetes response too large")
	}
	return data, err
}
func (a *Agent) Discover(ctx context.Context) ([]Node, error) {
	data, err := a.request(ctx, "GET", "/api/v1/nodes?limit=200&labelSelector="+url.QueryEscape(a.Config.Selector), nil)
	if err != nil {
		return nil, err
	}
	var list nodeList
	if err = json.Unmarshal(data, &list); err != nil {
		return nil, err
	}
	if list.Metadata.Continue != "" {
		return nil, errors.New("inventory exceeds 200 nodes; narrow the explicit selector")
	}
	nodes := make([]Node, 0, len(list.Items))
	for _, item := range list.Items {
		count := int64(0)
		if raw, ok := item.Status.Allocatable["nvidia.com/gpu"]; ok {
			count, err = strconv.ParseInt(raw, 10, 64)
			if err != nil || count < 0 {
				return nil, errors.New("invalid allocatable GPU count")
			}
		}
		ready := false
		for _, condition := range item.Status.Conditions {
			if condition.Type == "Ready" && condition.Status == "True" {
				ready = true
			}
		}
		nodes = append(nodes, Node{item.Metadata.Name, item.Metadata.UID, count, ready})
	}
	return nodes, nil
}
func (a *Agent) Publish(ctx context.Context, nodes []Node) error {
	inventory, err := json.Marshal(nodes)
	if err != nil {
		return err
	}
	patch, _ := json.Marshal(map[string]any{"data": map[string]string{"mode": "inventory-only", "qualification": "not-performed", "execution": "disabled", "lastInventoryAt": time.Now().UTC().Format(time.RFC3339), "nodes": string(inventory)}})
	_, err = a.request(ctx, "PATCH", "/api/v1/namespaces/"+url.PathEscape(a.Config.Namespace)+"/configmaps/"+url.PathEscape(a.Config.StatusName), patch)
	return err
}
func main() {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if port == "" {
		port = "443"
	}
	endpoint := "https://" + net.JoinHostPort(host, port)
	a, err := NewAgent(Config{endpoint, os.Getenv("POD_NAMESPACE"), os.Getenv("STATUS_CONFIGMAP"), os.Getenv("NODE_SELECTOR"), "/var/run/secrets/kubernetes.io/serviceaccount/token", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"})
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var lastSuccess atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if time.Now().Unix()-lastSuccess.Load() > 150 {
			http.Error(w, "inventory not current", 503)
			return
		}
		w.WriteHeader(200)
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("health server stopped: %v", err)
			stop()
		}
	}()
	scan := func() {
		nodes, err := a.Discover(ctx)
		if err == nil {
			err = a.Publish(ctx, nodes)
		}
		if err == nil && os.Getenv("REQUENTA_ORIGIN") != "" {
			err = a.Report(ctx, nodes)
		}
		if err != nil {
			log.Printf("inventory unavailable: %v", err)
			return
		}
		lastSuccess.Store(time.Now().Unix())
		log.Printf("inventory-only: %d selected nodes; qualification not performed; execution disabled", len(nodes))
	}
	scan()
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
			return
		case <-ticker.C:
			scan()
		}
	}
}

// Report uses a separate HTTP client: Kubernetes credentials never leave its API server.
func (a *Agent) Report(ctx context.Context, nodes []Node) error {
	origin := os.Getenv("REQUENTA_ORIGIN")
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		return errors.New("connection origin must be an HTTPS origin")
	}
	data, err := a.request(ctx, "GET", "/api/v1/namespaces/kube-system", nil)
	if err != nil {
		return err
	}
	var cluster struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	if err = json.Unmarshal(data, &cluster); err != nil {
		return err
	}
	return postReport(ctx, origin, "/var/run/requenta/token", cluster.Metadata.UID, nodes, &http.Client{Timeout: 10 * time.Second})
}
func postReport(ctx context.Context, origin, tokenPath, clusterUID string, nodes []Node, transport *http.Client) error {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		return errors.New("connection origin must be an HTTPS origin")
	}
	secret, err := os.ReadFile(tokenPath)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{"cluster_uid": clusterUID, "nodes": nodes})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", origin+"/api/supplier/inventory", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+string(bytes.TrimSpace(secret)))
	req.Header.Set("Content-Type", "application/json")
	client := *transport
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(req)
	if err != nil {
		return errors.New("console connection unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != 204 {
		return fmt.Errorf("console inventory rejected: HTTP %d", response.StatusCode)
	}
	return nil
}
