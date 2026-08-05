#!/bin/bash
set -euo pipefail
export PORT="${PORT:-8100}"
export BIND="${BIND:-127.0.0.1}"
export SOLR_URL="${SOLR_URL:-http://127.0.0.1:8983}"
export SOLR_COLLECTION="${SOLR_COLLECTION:-byz}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
export KAFKA_GROUP="${KAFKA_GROUP:-byz-ingest}"
export KAFKA_TOPICS="${KAFKA_TOPICS:-byz.files.file,byz.search.index}"
export SOLR_COMMIT_WITHIN_MS="${SOLR_COMMIT_WITHIN_MS:-2000}"
export INGEST_BATCH_MAX_DOCS="${INGEST_BATCH_MAX_DOCS:-75}"
export INGEST_BATCH_MAX_BYTES="${INGEST_BATCH_MAX_BYTES:-3000000}"
export INGEST_BATCH_MAX_WAIT_MS="${INGEST_BATCH_MAX_WAIT_MS:-300}"
export INGEST_MAX_ATTEMPTS="${INGEST_MAX_ATTEMPTS:-8}"

exec /opt/services/byz-ingest/byz-ingest
