# Confluence CDC → Databricks Mosaic AI Vector Search

```
Confluence webhook (page created/updated/trashed/removed)
   │
   ▼
API Gateway ──(direct integration)──► SQS (+DLQ)
                                        │
                                        ▼
                              Lambda: ingest  ── fetch full page via Confluence REST
                                        │
                                        ▼
                         S3  raw/space=KEY/dt=YYYY-MM-DD/<page>-<msg>.json
                                        │  (S3 event)
                                        ▼
                         Lambda: trigger_job ──► Databricks job (queued, debounced)
                                                   ├─ bronze      Auto Loader → confluence_bronze (full body, append-only log)
                                                   ├─ silver      chunk + per-page delete/insert → confluence_silver_chunks (CDF on)
                                                   └─ vector_sync create/sync Delta Sync index (managed embeddings)
```

## Layout
| Path | Purpose |
|---|---|
| `infra/template.yaml` | SAM: API GW→SQS, DLQ, S3, both Lambdas, IAM |
| `src/lambdas/ingest` | SQS consumer: verifies HMAC, fetches page, writes JSON to S3 |
| `src/lambdas/trigger_job` | S3 → Databricks `run-now` (skips if a run is already queued) |
| `src/rag_pipeline` | Wheel: `bronze`, `silver`, `vector_sync` entry points + chunking |
| `databricks.yml`, `resources/` | Databricks Asset Bundle (serverless job, 3 tasks) |
| `scripts/` | UC setup SQL, initial backfill, retrieval smoke test |
| `tests/` | Unit tests (`make test`) |

## Deploy order
1. **Unity Catalog**: run `scripts/setup_uc.sql`.
2. **Secrets Manager**: `{"email","api_token"}` for Confluence, `{"host","token"}` for Databricks.
3. **Databricks**: set `raw_path` and workspace host, `databricks bundle deploy -t dev`, note the job id.
4. **AWS**: `make deploy-aws` (pass job id, secrets ARNs, base URL, space keys). Register the created bucket's `raw/` prefix as a UC external location.
5. **Backfill** existing pages: `python scripts/backfill_space.py ...`
6. **Confluence**: Settings → Webhooks → URL = `WebhookUrl` output, events page created/updated/trashed/removed/restored, secret = `WebhookSecret`.
7. Query: `python scripts/query_index.py "your question"`.

## Design notes
- **Bronze** is an append-only event log; **silver** is current state (latest version of each page, chunked).
- Silver update is *delete all chunks of the page → insert new chunks*, so shrinking pages and deletions are handled. Deletes flow to the index through Change Data Feed.
- Within a batch, the latest event per page wins, so out-of-order delivery of rapid edits is safe. The ingest Lambda fetches the page at processing time, so you always index the freshest content.
- Webhook payloads don't contain page bodies, hence the REST fetch. If the fetch 404s, the event is converted to a delete.
