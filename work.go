package main

// workKind is the Solr operation derived from a Kafka message.
type workKind int

const (
	workSkip workKind = iota
	workUpsert
	workPatch
	workDelete
)

// workItem is a parsed Kafka record ready for Solr (or skip).
type workItem struct {
	kind workKind
	// upsert
	doc SolrDoc
	// patch
	patchID    string
	patchTitle string
	patchPath  string
	// delete
	deleteID string
	// approx JSON size for batch byte limits
	approxBytes int
	// skip reason (logging/metrics)
	skipReason string
}
