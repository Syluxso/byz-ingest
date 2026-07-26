package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	port := env("PORT", "8100")
	bind := env("BIND", "0.0.0.0")
	solrURL := env("SOLR_URL", "http://127.0.0.1:8983")
	collection := env("SOLR_COLLECTION", "byz")
	commitMs := envInt("SOLR_COMMIT_WITHIN_MS", 1000)
	bootstrap := env("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
	group := env("KAFKA_GROUP", "byz-ingest")
	topicsCSV := env("KAFKA_TOPICS", "byz.files.file,byz.search.index")

	topics := splitCSV(topicsCSV)
	if len(topics) == 0 {
		log.Fatal("KAFKA_TOPICS empty")
	}

	solr := NewSolrClient(solrURL, collection, commitMs)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runConsumers(ctx, splitCSV(bootstrap), group, topics, solr)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	})
	mux.HandleFunc("/actuator/health", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		status := http.StatusOK
		body := `{"status":"UP"}`
		if err := solr.Ping(pingCtx); err != nil {
			status = http.StatusServiceUnavailable
			body = `{"status":"DOWN","solr":"unreachable"}`
			log.Printf("health solr: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})

	addr := bind + ":" + port
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Printf("byz-ingest listening on http://%s solr=%s/%s topics=%v group=%s",
			addr, solrURL, collection, topics, group)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	log.Printf("shutdown")
}
