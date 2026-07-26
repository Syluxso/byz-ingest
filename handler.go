package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

func handleMessage(ctx context.Context, solr *SolrClient, topic string, value []byte) error {
	if len(value) == 0 || !json.Valid(value) {
		return nil
	}

	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(value, &envelope); err != nil {
		return nil
	}
	if envelope.Type == "" {
		log.Printf("skip topic=%s: missing type", topic)
		return nil
	}

	switch topic {
	case "byz.files.file":
		return handleFileEvent(ctx, solr, envelope.Type, value)
	case "byz.search.index":
		return handleSearchIndexEvent(ctx, solr, envelope.Type, value)
	default:
		// Allow env override topic names via suffix match on known types.
		if strings.Contains(topic, "files") {
			return handleFileEvent(ctx, solr, envelope.Type, value)
		}
		if strings.Contains(topic, "search") {
			return handleSearchIndexEvent(ctx, solr, envelope.Type, value)
		}
		log.Printf("skip unknown topic=%s type=%s", topic, envelope.Type)
		return nil
	}
}

func handleFileEvent(ctx context.Context, solr *SolrClient, typ string, value []byte) error {
	switch typ {
	case "file.created", "file.updated":
		var ev FileLifecycleEvent
		if err := json.Unmarshal(value, &ev); err != nil {
			return fmt.Errorf("decode %s: %w", typ, err)
		}
		if ev.FileID == "" || ev.OrganizationID == "" {
			log.Printf("skip %s: missing fileId/organizationId", typ)
			return nil
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
		if err := solr.Upsert(ctx, doc); err != nil {
			return err
		}
		log.Printf("indexed %s id=%s org=%s name=%q", typ, ev.FileID, ev.OrganizationID, ev.Name)
		return nil

	case "file.deleted":
		var ev FileLifecycleEvent
		if err := json.Unmarshal(value, &ev); err != nil {
			return fmt.Errorf("decode file.deleted: %w", err)
		}
		if ev.FileID == "" {
			log.Printf("skip file.deleted: missing fileId")
			return nil
		}
		if err := solr.DeleteByID(ctx, ev.FileID); err != nil {
			return err
		}
		log.Printf("deleted file id=%s org=%s", ev.FileID, ev.OrganizationID)
		return nil

	default:
		log.Printf("ignore file type=%s", typ)
		return nil
	}
}

func handleSearchIndexEvent(ctx context.Context, solr *SolrClient, typ string, value []byte) error {
	var ev SearchIndexEvent
	if err := json.Unmarshal(value, &ev); err != nil {
		return fmt.Errorf("decode search index event: %w", err)
	}

	switch typ {
	case "search.index":
		if ev.DocumentID == "" || ev.OrganizationID == "" {
			log.Printf("skip search.index: missing documentId/organizationId")
			return nil
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
		if err := solr.Upsert(ctx, doc); err != nil {
			return err
		}
		log.Printf("indexed search.index id=%s org=%s source=%s", ev.DocumentID, ev.OrganizationID, doc.Source)
		return nil

	case "search.delete":
		id := ev.DocumentID
		if id == "" {
			log.Printf("skip search.delete: missing documentId")
			return nil
		}
		if err := solr.DeleteByID(ctx, id); err != nil {
			return err
		}
		log.Printf("deleted search doc id=%s", id)
		return nil

	default:
		log.Printf("ignore search type=%s", typ)
		return nil
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
