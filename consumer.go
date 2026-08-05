package main

import (
	"context"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// batchConfig controls Phase-1 flush and retry behaviour.
type batchConfig struct {
	MaxDocs    int
	MaxBytes   int
	MaxWait    time.Duration
	MaxAttempts int
	RetryBase  time.Duration
	RetryMax   time.Duration
}

func defaultBatchConfig() batchConfig {
	return batchConfig{
		MaxDocs:     envInt("INGEST_BATCH_MAX_DOCS", 75),
		MaxBytes:    envInt("INGEST_BATCH_MAX_BYTES", 3_000_000),
		MaxWait:     time.Duration(envInt("INGEST_BATCH_MAX_WAIT_MS", 300)) * time.Millisecond,
		MaxAttempts: envInt("INGEST_MAX_ATTEMPTS", 8),
		RetryBase:   time.Duration(envInt("INGEST_RETRY_BASE_MS", 1000)) * time.Millisecond,
		RetryMax:    time.Duration(envInt("INGEST_RETRY_MAX_MS", 60000)) * time.Millisecond,
	}
}

func runConsumers(ctx context.Context, brokers []string, group string, topics []string, solr *SolrClient, metrics *IngestMetrics) {
	cfg := defaultBatchConfig()
	if cfg.MaxDocs < 1 {
		cfg.MaxDocs = 75
	}
	if cfg.MaxBytes < 1024 {
		cfg.MaxBytes = 3_000_000
	}
	if cfg.MaxWait < 50*time.Millisecond {
		cfg.MaxWait = 300 * time.Millisecond
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 8
	}
	log.Printf("ingest batch maxDocs=%d maxBytes=%d maxWait=%s maxAttempts=%d",
		cfg.MaxDocs, cfg.MaxBytes, cfg.MaxWait, cfg.MaxAttempts)

	for _, topic := range topics {
		t := topic
		go func() {
			if err := consumeTopic(ctx, brokers, group, t, solr, metrics, cfg); err != nil && ctx.Err() == nil {
				log.Printf("consumer topic=%s stopped: %v", t, err)
			}
		}()
	}
}

func consumeTopic(ctx context.Context, brokers []string, group, topic string, solr *SolrClient, metrics *IngestMetrics, cfg batchConfig) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ClientID("byz-ingest"),
		kgo.FetchMaxWait(1*time.Second),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	log.Printf("kafka consumer group=%s topic=%s", group, topic)

	var (
		upsertDocs []SolrDoc
		upsertRecs []*kgo.Record
		upsertBytes int
		lastAdd     time.Time
	)

	flushUpserts := func() {
		if len(upsertDocs) == 0 {
			return
		}
		docs := upsertDocs
		recs := upsertRecs
		upsertDocs = nil
		upsertRecs = nil
		upsertBytes = 0
		lastAdd = time.Time{}

		if err := applyWithRetry(ctx, cfg, metrics, "upsert", len(docs), func() error {
			return solr.Upsert(ctx, docs...)
		}); err != nil {
			// Phase 1 log-DLQ: park and commit so the partition is not blocked.
			logDLQ(topic, recs, err)
			if metrics != nil {
				metrics.DLQ.Add(int64(len(recs)))
				metrics.FlushErrs.Add(1)
			}
		} else {
			if metrics != nil {
				metrics.Indexed.Add(int64(len(docs)))
				metrics.Batches.Add(1)
			}
			log.Printf("batch upsert topic=%s docs=%d", topic, len(docs))
		}
		commitRecs(ctx, client, recs)
	}

	shouldFlush := func() bool {
		if len(upsertDocs) == 0 {
			return false
		}
		if len(upsertDocs) >= cfg.MaxDocs {
			return true
		}
		if upsertBytes >= cfg.MaxBytes {
			return true
		}
		if !lastAdd.IsZero() && time.Since(lastAdd) >= cfg.MaxWait {
			return true
		}
		return false
	}

	for {
		if ctx.Err() != nil {
			flushUpserts()
			return ctx.Err()
		}

		// Time-based flush when idle or between polls.
		if shouldFlush() {
			flushUpserts()
		}

		pollCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		fetches := client.PollFetches(pollCtx)
		cancel()

		if fetches.IsClientClosed() {
			flushUpserts()
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if ctx.Err() != nil {
					flushUpserts()
					return ctx.Err()
				}
				// DeadlineExceeded is normal for short poll.
				if e.Err == context.DeadlineExceeded || e.Err == context.Canceled {
					continue
				}
				log.Printf("kafka fetch topic=%s: %v", e.Topic, e.Err)
			}
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			if metrics != nil {
				metrics.Received.Add(1)
			}
			item := parseMessage(rec.Topic, rec.Value)

			switch item.kind {
			case workSkip:
				if metrics != nil {
					metrics.Skipped.Add(1)
				}
				log.Printf("skip topic=%s offset=%d reason=%s", rec.Topic, rec.Offset, item.skipReason)
				commitRecs(ctx, client, []*kgo.Record{rec})

			case workUpsert:
				// Large single doc: flush pending, then alone.
				if item.approxBytes >= cfg.MaxBytes && len(upsertDocs) > 0 {
					flushUpserts()
				}
				upsertDocs = append(upsertDocs, item.doc)
				upsertRecs = append(upsertRecs, rec)
				upsertBytes += item.approxBytes
				lastAdd = time.Now()
				if item.approxBytes >= cfg.MaxBytes || len(upsertDocs) >= cfg.MaxDocs || upsertBytes >= cfg.MaxBytes {
					flushUpserts()
				}

			case workPatch:
				flushUpserts()
				err := applyWithRetry(ctx, cfg, metrics, "patch", 1, func() error {
					return solr.PatchFileMeta(ctx, item.patchID, item.patchTitle, item.patchPath)
				})
				if err != nil {
					logDLQ(topic, []*kgo.Record{rec}, err)
					if metrics != nil {
						metrics.DLQ.Add(1)
						metrics.FlushErrs.Add(1)
					}
				} else if metrics != nil {
					metrics.Patched.Add(1)
				}
				commitRecs(ctx, client, []*kgo.Record{rec})

			case workDelete:
				flushUpserts()
				err := applyWithRetry(ctx, cfg, metrics, "delete", 1, func() error {
					return solr.DeleteByID(ctx, item.deleteID)
				})
				if err != nil {
					logDLQ(topic, []*kgo.Record{rec}, err)
					if metrics != nil {
						metrics.DLQ.Add(1)
						metrics.FlushErrs.Add(1)
					}
				} else if metrics != nil {
					metrics.Deleted.Add(1)
				}
				commitRecs(ctx, client, []*kgo.Record{rec})
			}
		}
	}
}

func commitRecs(ctx context.Context, client *kgo.Client, recs []*kgo.Record) {
	if len(recs) == 0 {
		return
	}
	if err := client.CommitRecords(ctx, recs...); err != nil {
		log.Printf("commit n=%d: %v", len(recs), err)
	}
}

// applyWithRetry retries transient failures with exponential backoff; returns last error if all attempts fail.
func applyWithRetry(ctx context.Context, cfg batchConfig, metrics *IngestMetrics, op string, n int, fn func() error) error {
	var last error
	backoff := cfg.RetryBase
	if backoff < 100*time.Millisecond {
		backoff = time.Second
	}
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		last = fn()
		if last == nil {
			return nil
		}
		if metrics != nil && attempt < cfg.MaxAttempts {
			metrics.Retried.Add(1)
		}
		log.Printf("solr %s n=%d attempt=%d/%d: %v", op, n, attempt, cfg.MaxAttempts, last)
		if attempt == cfg.MaxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > cfg.RetryMax {
			backoff = cfg.RetryMax
		}
	}
	return last
}

// logDLQ is Phase-1 dead-letter: structured log so ops can find failed offsets (no Kafka DLQ yet).
func logDLQ(topic string, recs []*kgo.Record, err error) {
	for _, rec := range recs {
		if rec == nil {
			continue
		}
		log.Printf("DLQ topic=%s partition=%d offset=%d err=%v value_len=%d",
			topic, rec.Partition, rec.Offset, err, len(rec.Value))
	}
}
