package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/MicahParks/keyfunc/v3"
)

func main() {
	port := env("PORT", "8100")
	bind := env("BIND", "0.0.0.0")
	jwksURL := env("IAM_JWKS_URL", "https://iam.byzantineapp.dev/.well-known/jwks.json")
	solrURL := env("SOLR_URL", "http://127.0.0.1:8983")
	collection := env("SOLR_COLLECTION", "byz")
	commitMs := envInt("SOLR_COMMIT_WITHIN_MS", 2000)
	bootstrap := env("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
	group := env("KAFKA_GROUP", "byz-ingest")
	topicsCSV := env("KAFKA_TOPICS", "byz.files.file,byz.search.index")

	topics := splitCSV(topicsCSV)
	if len(topics) == 0 {
		log.Fatal("KAFKA_TOPICS empty")
	}

	logs := NewLogBuffer()
	teeStdLog(logs, "byz-ingest")

	metrics := &IngestMetrics{}
	solr := NewSolrClient(solrURL, collection, commitMs)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	jwks, err := keyfunc.NewDefaultCtx(ctx, []string{jwksURL})
	if err != nil {
		log.Fatalf("jwks: %v", err)
	}

	runConsumers(ctx, splitCSV(bootstrap), group, topics, solr, metrics)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", withCORS(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
	}))
	mux.HandleFunc("/actuator/health", withCORS(func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		status := http.StatusOK
		body := map[string]any{
			"status":  "UP",
			"metrics": metrics.Snapshot(),
		}
		if err := solr.Ping(pingCtx); err != nil {
			status = http.StatusServiceUnavailable
			body["status"] = "DOWN"
			body["solr"] = "unreachable"
			log.Printf("health solr: %v", err)
		}
		writeJSON(w, status, body)
	}))
	mux.HandleFunc("/api/v1/admin/logs", withCORS(withJWT(jwks, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		lines := 200
		if v := r.URL.Query().Get("lines"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				lines = n
			}
		}
		writeJSON(w, http.StatusOK, logs.Tail(lines, r.URL.Query().Get("level")))
	})))

	addr := bind + ":" + port
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Printf("byz-ingest listening on http://%s solr=%s/%s topics=%v group=%s",
			addr, solrURL, collection, topics, group)
		logs.Add("INFO", "byz-ingest", "listening on "+addr, nil)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	// Consumers flush on ctx cancel; give them a moment.
	time.Sleep(200 * time.Millisecond)
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	log.Printf("shutdown metrics=%v", metrics.Snapshot())
}
