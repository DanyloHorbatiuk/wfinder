import json
from datetime import datetime

from curl_cffi import requests as cf_requests

from storage.loader import save_file_record
from storage.minio import save_object
from utils.logger import get_logger
from utils.time import utcnow

logger = get_logger(__name__)

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.5",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
}
REQUEST_TIMEOUT = 15


def fetch_and_save_single(name: str, url: str) -> dict:
    """Raises on any failure (HTTP, invalid JSON, MinIO, DB) so the Airflow task
    actually fails and retries (D5, SPEC §6.1) instead of silently reporting success."""
    logger.info(f"fetching {name} url...")
    content = _fetch_source_content(name, url)
    fetched_at = utcnow()
    file_name = f"{name}_{fetched_at.strftime('%Y%m%dT%H%M%SZ')}"
    load_file_and_meta(file_name, content, fetched_at)
    logger.info(f"file and meta {file_name} finished successfully")
    return {"name": name, "status": "success"}


def _fetch_source_content(name: str, url: str) -> str:
    if name == "softserve":
        return _fetch_softserve_all_pages(url)
    response = cf_requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    text = response.text
    json.loads(text)  # validate before it ever reaches MinIO (D5, SPEC §6.1)
    return text


def _fetch_softserve_all_pages(url: str) -> str:
    """SoftServe's search endpoint is paginated (meta.total/last_page/per_page).
    Follow links.next until it's null and merge every page's data[] into one
    snapshot before it's saved to MinIO (I-01, SPEC §6.1)."""
    all_data = []
    next_url = url
    while next_url:
        response = cf_requests.get(next_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = json.loads(response.text)
        all_data.extend(payload.get("data", []))
        next_url = (payload.get("links") or {}).get("next")
    return json.dumps({"data": all_data})


def load_file_and_meta(f_name: str, content: str, fetched_at: datetime) -> None:
    logger.info(f"loading file {f_name} to minio bucket...")
    meta = save_object(f_name, content)
    logger.info(f"file {f_name} saved to minio bucket")
    logger.debug(meta)
    logger.info(f"saving file {f_name} metadata to database...")
    save_file_record(
        bucket=meta["bucket"],
        key=meta["key"],
        source=f_name.split("_")[0],
        etag=meta["etag"],
        size_bytes=meta["size_bytes"],
        fetched_at=fetched_at,
    )
    logger.info(f"file metadata {f_name} saved to database")
