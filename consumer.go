package main

import (
	"context"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func runConsumers(ctx context.Context, brokers []string, group string, topics []string, solr *SolrClient) {
	for _, topic := range topics {
		t := topic
		go func() {
			if err := consumeTopic(ctx, brokers, group, t, solr); err != nil && ctx.Err() == nil {
				log.Printf("consumer topic=%s stopped: %v", t, err)
			}
		}()
	}
}

func consumeTopic(ctx context.Context, brokers []string, group, topic string, solr *SolrClient) error {
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
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("kafka fetch topic=%s: %v", e.Topic, e.Err)
			}
			time.Sleep(time.Second)
			continue
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			for {
				err := handleMessage(ctx, solr, rec.Topic, rec.Value)
				if err == nil {
					break
				}
				log.Printf("handle topic=%s offset=%d: %v (retry)", rec.Topic, rec.Offset, err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}
			if err := client.CommitRecords(ctx, rec); err != nil {
				log.Printf("commit topic=%s offset=%d: %v", rec.Topic, rec.Offset, err)
			}
		}
	}
}
