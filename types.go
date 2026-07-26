package main

// File lifecycle (byz.files.file) — see events-service docs/EVENTS.md
type FileLifecycleEvent struct {
	EventID         string  `json:"eventId"`
	Type            string  `json:"type"`
	OccurredAt      string  `json:"occurredAt"`
	OrganizationID  string  `json:"organizationId"`
	TenantID        *string `json:"tenantId"`
	FileID          string  `json:"fileId"`
	UploadedBy      *string `json:"uploadedBy"`
	Name            string  `json:"name"`
	ContentType     string  `json:"contentType"`
	SizeBytes       int64   `json:"sizeBytes"`
	ChecksumSha256  string  `json:"checksumSha256"`
	StorageKey      string  `json:"storageKey"`
}

// SearchIndexEvent (byz.search.index) — full-text upsert/delete for extractors / OneDrive sync.
type SearchIndexEvent struct {
	EventID        string   `json:"eventId"`
	Type           string   `json:"type"` // search.index | search.delete
	OccurredAt     string   `json:"occurredAt"`
	OrganizationID string   `json:"organizationId"`
	TenantID       string   `json:"tenantId,omitempty"`
	UserID         string   `json:"userId,omitempty"`
	DocumentID     string   `json:"documentId"`
	Title          string   `json:"title,omitempty"`
	Content        string   `json:"content,omitempty"`
	Source         string   `json:"source,omitempty"`
	Path           string   `json:"path,omitempty"`
	Tags           []string `json:"tags,omitempty"`
}

// SolrDoc matches byz-search field contract.
type SolrDoc struct {
	ID             string   `json:"id"`
	Title          string   `json:"title,omitempty"`
	Content        string   `json:"content,omitempty"`
	OrganizationID string   `json:"organization_id"`
	TenantID       string   `json:"tenant_id,omitempty"`
	UserID         string   `json:"user_id,omitempty"`
	Source         string   `json:"source,omitempty"`
	Path           string   `json:"path,omitempty"`
	Tags           []string `json:"tags,omitempty"`
}
