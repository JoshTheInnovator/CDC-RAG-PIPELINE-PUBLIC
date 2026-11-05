"""One-off initial load: emit a synthetic 'page_upserted' event to S3 for every page in a space.
CDC webhooks only capture changes, so run this once before enabling the webhook.

  python scripts/backfill_space.py --space ENG --bucket <RawBucketName> \
      --base-url https://acme.atlassian.net --email me@acme.com --token $CONF_TOKEN
"""
import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "lambdas", "ingest"))
import confluence  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--space", required=True)
    p.add_argument("--bucket", required=True)
    p.add_argument("--base-url", required=True)
    p.add_argument("--email", required=True)
    p.add_argument("--token", required=True)
    a = p.parse_args()

    s3 = boto3.client("s3")
    n = 0
    for pid in confluence.list_space_page_ids(a.base_url, a.space, a.email, a.token):
        page = confluence.get_page(a.base_url, pid, a.email, a.token)
        if not page:
            continue
        now = datetime.now(timezone.utc)
        eid = str(uuid.uuid4())
        rec = {"event_id": eid, "event_type": "page_upserted", "source_event": "backfill",
               "received_at": now.isoformat(), **page}
        s3.put_object(Bucket=a.bucket,
                      Key=f"raw/space={a.space}/dt={now:%Y-%m-%d}/{pid}-{eid}.json",
                      Body=(json.dumps(rec) + "\n").encode())
        n += 1
    print(f"backfilled {n} pages")


if __name__ == "__main__":
    main()
