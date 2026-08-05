package main

import (
	"encoding/json"
	"strings"
	"unicode/utf8"
)

// parseMessage turns a Kafka payload into a work item. Never calls Solr.
// Invalid / incomplete messages become workSkip (safe to commit).
func parseMessage(topic string, value []byte) workItem {
	if len(value) == 0 || !json.Valid(value) {
		return workItem{kind: workSkip, skipReason: "empty_or_invalid_json", approxBytes: len(value)}
	}

	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(value, &envelope); err != nil {
		return workItem{kind: workSkip, skipReason: "envelope_decode", approxBytes: len(value)}
	}
	if envelope.Type == "" {
		return workItem{kind: workSkip, skipReason: "missing_type", approxBytes: len(value)}
	}

	switch {
	case topic == "byz.files.file" || strings.Contains(topic, "files"):
		return parseFileEvent(envelope.Type, value)
	case topic == "byz.search.index" || strings.Contains(topic, "search"):
		return parseSearchIndexEvent(envelope.Type, value)
	default:
		return workItem{kind: workSkip, skipReason: "unknown_topic", approxBytes: len(value)}
	}
}

func parseFileEvent(typ string, value []byte) workItem {
	switch typ {
	case "file.created":
		var ev FileLifecycleEvent
		if err := json.Unmarshal(value, &ev); err != nil {
			return workItem{kind: workSkip, skipReason: "file_created_decode", approxBytes: len(value)}
		}
		if ev.FileID == "" || ev.OrganizationID == "" {
			return workItem{kind: workSkip, skipReason: "file_created_missing_ids", approxBytes: len(value)}
		}
		doc := SolrDoc{
			ID:             ev.FileID,
			Title:          ev.Name,
			Content:        buildFileContent(ev),
			OrganizationID: ev.OrganizationID,
			Source:         "file-service",
			Path:           firstNonEmpty(ev.StorageKey, ev.Name),
		}
		if ev.TenantID != nil {
			doc.TenantID = *ev.TenantID
		}
		if ev.UploadedBy != nil {
			doc.UserID = *ev.UploadedBy
		}
		withCodeTokens(&doc)
		return workItem{kind: workUpsert, doc: doc, approxBytes: estimateDocBytes(doc)}

	case "file.updated":
		var ev FileLifecycleEvent
		if err := json.Unmarshal(value, &ev); err != nil {
			return workItem{kind: workSkip, skipReason: "file_updated_decode", approxBytes: len(value)}
		}
		if ev.FileID == "" {
			return workItem{kind: workSkip, skipReason: "file_updated_missing_id", approxBytes: len(value)}
		}
		return workItem{
			kind:       workPatch,
			patchID:    ev.FileID,
			patchTitle: ev.Name,
			patchPath:  firstNonEmpty(ev.StorageKey, ev.Name),
			approxBytes: 256,
		}

	case "file.deleted":
		var ev FileLifecycleEvent
		if err := json.Unmarshal(value, &ev); err != nil {
			return workItem{kind: workSkip, skipReason: "file_deleted_decode", approxBytes: len(value)}
		}
		if ev.FileID == "" {
			return workItem{kind: workSkip, skipReason: "file_deleted_missing_id", approxBytes: len(value)}
		}
		return workItem{kind: workDelete, deleteID: ev.FileID, approxBytes: 64}

	default:
		return workItem{kind: workSkip, skipReason: "file_ignore_type:" + typ, approxBytes: len(value)}
	}
}

func parseSearchIndexEvent(typ string, value []byte) workItem {
	var ev SearchIndexEvent
	if err := json.Unmarshal(value, &ev); err != nil {
		return workItem{kind: workSkip, skipReason: "search_decode", approxBytes: len(value)}
	}

	switch typ {
	case "search.index":
		if ev.DocumentID == "" || ev.OrganizationID == "" {
			return workItem{kind: workSkip, skipReason: "search_index_missing_ids", approxBytes: len(value)}
		}
		doc := SolrDoc{
			ID:             ev.DocumentID,
			Title:          ev.Title,
			Content:        ev.Content,
			OrganizationID: ev.OrganizationID,
			TenantID:       ev.TenantID,
			UserID:         ev.UserID,
			Source:         firstNonEmpty(ev.Source, "search.index"),
			Path:           ev.Path,
			Tags:           ev.Tags,
		}
		if doc.Content == "" && doc.Title != "" {
			doc.Content = doc.Title
		}
		withCodeTokens(&doc)
		return workItem{kind: workUpsert, doc: doc, approxBytes: estimateDocBytes(doc)}

	case "search.delete":
		if ev.DocumentID == "" {
			return workItem{kind: workSkip, skipReason: "search_delete_missing_id", approxBytes: len(value)}
		}
		return workItem{kind: workDelete, deleteID: ev.DocumentID, approxBytes: 64}

	default:
		return workItem{kind: workSkip, skipReason: "search_ignore_type:" + typ, approxBytes: len(value)}
	}
}

func buildFileContent(ev FileLifecycleEvent) string {
	parts := []string{ev.Name, ev.ContentType, ev.StorageKey}
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, " ")
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func estimateDocBytes(d SolrDoc) int {
	// Rough upper bound for batch byte limits (UTF-8 content dominates).
	n := utf8.RuneCountInString(d.Content) + utf8.RuneCountInString(d.Title) + 256
	for _, t := range d.CodeTokens {
		n += len(t)
	}
	if n < 128 {
		n = 128
	}
	return n
}
