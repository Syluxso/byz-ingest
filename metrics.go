package main

import "sync/atomic"

// IngestMetrics is process-wide counters for Phase 1 observability.
type IngestMetrics struct {
	Received  atomic.Int64
	Indexed   atomic.Int64
	Patched   atomic.Int64
	Deleted   atomic.Int64
	Skipped   atomic.Int64
	Retried   atomic.Int64
	DLQ       atomic.Int64
	Batches   atomic.Int64
	FlushErrs atomic.Int64
}

func (m *IngestMetrics) Snapshot() map[string]int64 {
	if m == nil {
		return map[string]int64{}
	}
	return map[string]int64{
		"received":  m.Received.Load(),
		"indexed":   m.Indexed.Load(),
		"patched":   m.Patched.Load(),
		"deleted":   m.Deleted.Load(),
		"skipped":   m.Skipped.Load(),
		"retried":   m.Retried.Load(),
		"dlq":       m.DLQ.Load(),
		"batches":   m.Batches.Load(),
		"flushErrs": m.FlushErrs.Load(),
	}
}
