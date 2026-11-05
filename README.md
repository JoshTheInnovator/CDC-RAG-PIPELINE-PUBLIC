# CDC-RAG-PIPELINE

#Overview

This project keeps a RAG knowledge base continuously in sync with a Confluence space. Instead of re-crawling and re-embedding every document on a schedule, it uses change data capture. When a page is created, updated, trashed or removed in Confluence, only that page is reprocessed and the vector index is updated. Answers from the RAG application then reflect recent edits within minutes, and the embedding cost stays proportional to what actually changed.

#How it works

Capture. A Confluence webhook sends page events to an AWS API Gateway endpoint. The endpoint writes straight into an SQS queue, so the webhook gets a fast response and no events are lost if downstream systems are slow. A dead-letter queue catches repeated failures.
Enrich and land. A Lambda consumes the queue and verifies the webhook’s HMAC signature. Webhooks carry only the page ID and metadata, so the Lambda fetches the full page content from the Confluence REST API. It writes each event as a JSON file to S3. Deletions are recorded as delete events.
Trigger. Each new S3 file fires a second Lambda that starts a Databricks job. It skips the trigger if a run is already queued, so a burst of edits produces one run rather than dozens.
Bronze layer. Auto Loader ingests the raw JSON into a Delta table. It is an append-only log of every change, with the complete document body on each record, so history can be replayed or audited.
Silver layer. The latest event per page is converted from Confluence HTML to clean text and split into overlapping chunks. For each affected page the old chunks are deleted and the new ones inserted, so edits, shrinking pages and deletions are all handled. Change Data Feed is enabled on this table.
Vector index. A Mosaic AI Vector Search Delta Sync index sits on the silver table and generates embeddings with a managed model. Each job run triggers a sync, so only the changed chunks are re-embedded.


#Key features

Incremental and event-driven: updates arrive in minutes, with no full re-index.
Handles deletes and edits correctly: removed pages disappear from search results.
Resilient: SQS buffering, a DLQ, partial-batch failure reporting, and idempotent delete-then-insert logic.
Secure: HMAC webhook verification, secrets in AWS Secrets Manager, and a private S3 bucket.
Backfill support: a one-off script loads existing pages before the webhook is switched on.
Deployable as code: AWS infrastructure in a SAM template and the Databricks job in an Asset Bundle, with unit tests for the chunking and webhook logic.


#Tech stack

AWS API Gateway, SQS, Lambda, S3 and Secrets Manager; Confluence REST API and webhooks; Databricks (Auto Loader, Structured Streaming, Delta Lake with Change Data Feed, Unity Catalog, serverless Jobs); Mosaic AI Vector Search; Python, BeautifulSoup and LangChain text splitters; AWS SAM and Databricks Asset Bundles.

If you want a shorter version for a resume or GitHub tagline, or a more detailed architecture write-up, I can produce either.
