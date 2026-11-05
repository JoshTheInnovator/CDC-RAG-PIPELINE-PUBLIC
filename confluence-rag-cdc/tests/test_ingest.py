import hashlib
import hmac
import os

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
from handler import normalize_event, verify_signature  # noqa: E402


def test_normalize():
    p = {"event": "page_updated", "page": {"id": 12, "spaceKey": "ENG"}}
    assert normalize_event(p) == ("page_upserted", "12", "ENG")
    assert normalize_event({"event": "page_trashed", "page": {"id": "1"}})[0] == "page_deleted"
    assert normalize_event({"event": "comment_created", "page": {"id": "1"}}) is None


def test_signature():
    body, secret = '{"a":1}', "s3cret"
    sig = "sha256=" + hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
    assert verify_signature(body, sig, secret)
    assert not verify_signature(body, "sha256=bad", secret)
    assert verify_signature(body, None, "")
