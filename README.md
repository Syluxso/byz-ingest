# byz-ingest

Go Kafka → Solr indexer for Byzantine. Pairs with **byz-search** (same collection / field names). No database.

| Setting | Default |
|---------|---------|
| Health HTTP | `8100` |
| Solr | `http://127.0.0.1:8983` / collection `byz` |
| Consumer group | `byz-ingest` |

## Topics consumed

| Topic | Types handled |
|-------|----------------|
| `byz.files.file` | `file.created` → upsert metadata; `file.updated` → patch title/path only; `file.deleted` → delete |
| `byz.search.index` | `search.index` → upsert full-text doc; `search.delete` → delete |

File lifecycle events carry **metadata only** (no extracted text). Ingest indexes `title=name` and `content` from name + contentType + storageKey so files are findable until a text extractor publishes `search.index`.

## Solr fields

Same contract as [byz-search](../byz-search/README.md): `id`, `title`, `content`, `code_tokens`, `organization_id`, `tenant_id`, `user_id`, `source`, `path`, `tags`.

`code_tokens` = path/API-shaped snippets extracted from title/content/path so queries like `/notifications/{id}` work.

## Health / admin

- `GET /healthz` — process up  
- `GET /actuator/health` — UP only if Solr ping succeeds  
- `GET /api/v1/admin/logs` — in-memory log tail (JWT; Admin Logs UI)

## Batching (Phase 1)

Kafka records are buffered and flushed to Solr as **JSON arrays** (fewer HTTP calls).

| Env | Default | Meaning |
|-----|---------|---------|
| `INGEST_BATCH_MAX_DOCS` | `75` | Max upserts per Solr request |
| `INGEST_BATCH_MAX_BYTES` | `3000000` | Approx content budget per batch |
| `INGEST_BATCH_MAX_WAIT_MS` | `300` | Max wait before flush under light load |
| `INGEST_MAX_ATTEMPTS` | `8` | Retries per batch/op before log-DLQ |
| `INGEST_RETRY_BASE_MS` | `1000` | Initial backoff |
| `INGEST_RETRY_MAX_MS` | `60000` | Backoff cap |

- **Upserts** (`search.index`, `file.created`) are batched.
- **Patch / delete** flush pending upserts first, then run alone (ordering).
- After max attempts: **log `DLQ topic=… offset=…`** and **commit** so a poison message does not block the partition forever (Phase 1; Kafka DLQ topic later).
- Health: `GET /actuator/health` includes `metrics` counters (`received`, `indexed`, `skipped`, `dlq`, `batches`, …).

## Config

| Env | Default |
|-----|---------|
| `PORT` | `8100` |
| `BIND` | `0.0.0.0` |
| `IAM_JWKS_URL` | `https://iam.byzantineapp.dev/.well-known/jwks.json` |
| `SOLR_URL` | `http://127.0.0.1:8983` |
| `SOLR_COLLECTION` | `byz` |
| `SOLR_COMMIT_WITHIN_MS` | `2000` |
| `KAFKA_BOOTSTRAP` | `127.0.0.1:9092` |
| `KAFKA_GROUP` | `byz-ingest` |
| `KAFKA_TOPICS` | `byz.files.file,byz.search.index` |

## Run

```bash
export SOLR_URL=http://127.0.0.1:8983
export KAFKA_BOOTSTRAP=127.0.0.1:9092
go run .
```

## Deploy

See [DEPLOY-LINODE.md](DEPLOY-LINODE.md). Create topics via byz-events bootstrap (includes `byz.search.index`).
