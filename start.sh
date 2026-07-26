#!/bin/bash
set -euo pipefail
export PORT="${PORT:-8100}"
export BIND="${BIND:-127.0.0.1}"
export SOLR_URL="${SOLR_URL:-http://127.0.0.1:8983}"
export SOLR_COLLECTION="${SOLR_COLLECTION:-byz}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
export KAFKA_GROUP="${KAFKA_GROUP:-byz-ingest}"
export KAFKA_TOPICS="${KAFKA_TOPICS:-byz.files.file,byz.search.index}"

exec /opt/services/byz-ingest/byz-ingest
