"""Minimal Confluence Cloud REST client (stdlib only, so no Lambda layer needed)."""
import base64
import json
import urllib.error
import urllib.request


def _auth_header(email: str, api_token: str) -> dict:
    raw = base64.b64encode(f"{email}:{api_token}".encode()).decode()
    return {"Authorization": f"Basic {raw}", "Accept": "application/json"}


def _get(url: str, email: str, api_token: str):
    req = urllib.request.Request(url, headers=_auth_header(email, api_token))
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def get_page(base_url: str, page_id: str, email: str, api_token: str):
    """Return normalised page dict, or None if page no longer exists / is trashed."""
    url = f"{base_url}/wiki/rest/api/content/{page_id}?expand=body.storage,version,space"
    data = _get(url, email, api_token)
    if data is None or data.get("status") in ("trashed", "deleted"):
        return None
    links = data.get("_links", {})
    return {
        "page_id": str(data["id"]),
        "space_key": data["space"]["key"],
        "title": data["title"],
        "version": data["version"]["number"],
        "last_modified": data["version"]["when"],
        "url": links.get("base", f"{base_url}/wiki") + links.get("webui", ""),
        "body_html": data["body"]["storage"]["value"],
    }


def list_space_page_ids(base_url: str, space_key: str, email: str, api_token: str):
    """Yield every current page id in a space (used by the backfill script)."""
    url = f"{base_url}/wiki/rest/api/space/{space_key}/content/page?limit=100"
    while url:
        data = _get(url, email, api_token) or {}
        for r in data.get("results", []):
            yield str(r["id"])
        nxt = data.get("_links", {}).get("next")
        url = f"{base_url}/wiki{nxt}" if nxt else None
