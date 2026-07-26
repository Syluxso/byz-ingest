package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type SolrClient struct {
	base       string
	collection string
	http       *http.Client
	commitMs   int
}

func NewSolrClient(baseURL, collection string, commitWithinMs int) *SolrClient {
	if commitWithinMs < 0 {
		commitWithinMs = 1000
	}
	return &SolrClient{
		base:       strings.TrimRight(baseURL, "/"),
		collection: collection,
		commitMs:   commitWithinMs,
		http:       &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *SolrClient) Ping(ctx context.Context) error {
	u := fmt.Sprintf("%s/solr/%s/admin/ping?wt=json", s.base, url.PathEscape(s.collection))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("solr ping status %d", resp.StatusCode)
	}
	return nil
}

func (s *SolrClient) Upsert(ctx context.Context, docs ...SolrDoc) error {
	if len(docs) == 0 {
		return nil
	}
	payload, err := json.Marshal(docs)
	if err != nil {
		return err
	}
	return s.postUpdate(ctx, payload)
}

// PatchFileMeta updates title/path without replacing content (atomic Solr "set").
func (s *SolrClient) PatchFileMeta(ctx context.Context, id, title, path string) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("empty id")
	}
	doc := map[string]any{"id": id}
	if strings.TrimSpace(title) != "" {
		doc["title"] = map[string]string{"set": title}
	}
	if strings.TrimSpace(path) != "" {
		doc["path"] = map[string]string{"set": path}
	}
	payload, err := json.Marshal([]map[string]any{doc})
	if err != nil {
		return err
	}
	return s.postUpdate(ctx, payload)
}

func (s *SolrClient) DeleteByID(ctx context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("empty id")
	}
	payload, err := json.Marshal(map[string]any{
		"delete": map[string]string{"id": id},
	})
	if err != nil {
		return err
	}
	return s.postUpdate(ctx, payload)
}

func (s *SolrClient) postUpdate(ctx context.Context, body []byte) error {
	u := fmt.Sprintf("%s/solr/%s/update?wt=json&commitWithin=%d",
		s.base, url.PathEscape(s.collection), s.commitMs)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("solr update status %d: %s", resp.StatusCode, truncate(string(respBody), 400))
	}
	return nil
}

func truncate(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
