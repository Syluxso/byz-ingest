# Deploy byz-ingest on Linode

## Prereqs

1. Solr + collection `byz` (same as byz-search)
2. Topics `byz.files.file` and `byz.search.index`
3. Kafka reachable on localhost

## Env

```bash
export PORT=8100
export BIND=127.0.0.1
export SOLR_URL=http://127.0.0.1:8983
export SOLR_COLLECTION=byz
export KAFKA_BOOTSTRAP=127.0.0.1:9092
export KAFKA_GROUP=byz-ingest
export KAFKA_TOPICS=byz.files.file,byz.search.index
```

| Item | Value |
|------|--------|
| Deploy dir | `/opt/services/byz-ingest` |
| Binary | `byz-ingest` |
| Supervisor | `byz-ingest` |

## Verify

```bash
curl -s http://127.0.0.1:8100/actuator/health
# Upload a file via file-service → check Solr / byz-search for the file name
```
