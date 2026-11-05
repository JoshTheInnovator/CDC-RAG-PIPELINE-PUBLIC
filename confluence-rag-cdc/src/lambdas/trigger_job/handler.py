"""S3 ObjectCreated -> trigger the Databricks job (debounced)."""
import json
import logging
import os
import urllib.request
from functools import lru_cache

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)
secrets = boto3.client("secretsmanager")


@lru_cache(maxsize=1)
def _creds() -> dict:
    return json.loads(
        secrets.get_secret_value(SecretId=os.environ["DATABRICKS_SECRET_ARN"])["SecretString"]
    )


def _api(method: str, path: str, body: dict | None = None) -> dict:
    c = _creds()
    req = urllib.request.Request(
        f"{c['host'].rstrip('/')}{path}",
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"Authorization": f"Bearer {c['token']}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read() or "{}")


def handler(event, context):
    job_id = int(os.environ["DATABRICKS_JOB_ID"])

    # Debounce: if a run is already waiting in the queue it will pick up this file too
    # (Auto Loader lists the path when it starts). A RUNNING run might have missed it,
    # so only QUEUED / PENDING runs count as "covered".
    runs = _api("GET", f"/api/2.1/jobs/runs/list?job_id={job_id}&active_only=true").get("runs", [])
    if any(r["state"]["life_cycle_state"] in ("QUEUED", "PENDING") for r in runs):
        log.info("run already queued, skipping")
        return {"triggered": False}

    resp = _api("POST", "/api/2.1/jobs/run-now", {"job_id": job_id})
    log.info("triggered run %s", resp.get("run_id"))
    return {"triggered": True, "run_id": resp.get("run_id")}
