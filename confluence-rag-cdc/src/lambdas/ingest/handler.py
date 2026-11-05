"""SQS -> (fetch full page from Confluence) -> S3 JSON-lines event file."""
import hashlib
import hmac
import json
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache

import boto3

import confluence

log = logging.getLogger()
log.setLevel(logging.INFO)

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")

# Confluence webhook event -> normalised event type
EVENT_MAP = {
    "page_created": "page_upserted",
    "page_updated": "page_upserted",
    "page_published": "page_upserted",
    "page_restored": "page_upserted",
    "page_moved": "page_upserted",
    "page_removed": "page_deleted",
    "page_trashed": "page_deleted",
}


@lru_cache(maxsize=1)
def _confluence_creds() -> dict:
    return json.loads(
        secrets.get_secret_value(SecretId=os.environ["CONFLUENCE_SECRET_ARN"])["SecretString"]
    )


def verify_signature(body: str, signature: str | None, secret: str) -> bool:
    if not secret:
        return True
    if not signature:
        return False
    expected = "sha256=" + hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def normalize_event(payload: dict):
    """Return (event_type, page_id, space_key) or None when the event is not relevant."""
    event_type = EVENT_MAP.get(payload.get("event", ""))
    page = payload.get("page") or {}
    if not event_type or not page.get("id"):
        return None
    return event_type, str(page["id"]), page.get("spaceKey")


def _write(record: dict, message_id: str):
    now = datetime.now(timezone.utc)
    key = (
        f"raw/space={record['space_key']}/dt={now:%Y-%m-%d}/"
        f"{record['page_id']}-{message_id}.json"
    )
    s3.put_object(
        Bucket=os.environ["RAW_BUCKET"],
        Key=key,
        Body=(json.dumps(record) + "\n").encode(),  # single JSON line (Auto Loader friendly)
        ContentType="application/json",
    )
    log.info("wrote s3://%s/%s", os.environ["RAW_BUCKET"], key)


def process(rec: dict):
    body = rec["body"]
    sig = (rec.get("messageAttributes", {}).get("signature") or {}).get("stringValue")
    if not verify_signature(body, sig, os.environ.get("WEBHOOK_SECRET", "")):
        log.warning("bad signature, dropping message %s", rec["messageId"])
        return

    payload = json.loads(body)
    norm = normalize_event(payload)
    if norm is None:
        log.info("ignoring event %s", payload.get("event"))
        return
    event_type, page_id, space_key = norm

    allowed = [s.strip() for s in os.environ.get("SPACE_KEYS", "").split(",") if s.strip()]
    if allowed and space_key and space_key not in allowed:
        return

    received_at = datetime.now(timezone.utc).isoformat()
    base = {
        "event_id": rec["messageId"],
        "source_event": payload.get("event"),
        "received_at": received_at,
        "page_id": page_id,
    }

    page = None
    if event_type == "page_upserted":
        c = _confluence_creds()
        page = confluence.get_page(
            os.environ["CONFLUENCE_BASE_URL"], page_id, c["email"], c["api_token"]
        )
        if page is None:  # deleted between webhook and fetch -> treat as delete
            event_type = "page_deleted"

    if page:
        if allowed and page["space_key"] not in allowed:
            return
        record = {**base, "event_type": event_type, **page}
    else:
        record = {
            **base,
            "event_type": "page_deleted",
            "space_key": space_key or "unknown",
            "title": (payload.get("page") or {}).get("title"),
            "version": None, "last_modified": None, "url": None, "body_html": None,
        }
    _write(record, rec["messageId"])


def handler(event, context):
    failures = []
    for rec in event["Records"]:
        try:
            process(rec)
        except Exception:
            log.exception("failed message %s", rec["messageId"])
            failures.append({"itemIdentifier": rec["messageId"]})
    return {"batchItemFailures": failures}
