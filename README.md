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

Same contract as [byz-search](../byz-search/README.md): `id`, `title`, `content`, `organization_id`, `tenant_id`, `user_id`, `source`, `path`, `tags`.

## Health / admin

- `GET /healthz` — process up  
- `GET /actuator/health` — UP only if Solr ping succeeds  
- `GET /api/v1/admin/logs` — in-memory log tail (JWT; Admin Logs UI)

## Config

| Env | Default |
|-----|---------|
| `PORT` | `8100` |
| `BIND` | `0.0.0.0` |
| `IAM_JWKS_URL` | `https://iam.byzantineapp.dev/.well-known/jwks.json` |
| `SOLR_URL` | `http://127.0.0.1:8983` |
| `SOLR_COLLECTION` | `byz` |
| `SOLR_COMMIT_WITHIN_MS` | `1000` |
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
