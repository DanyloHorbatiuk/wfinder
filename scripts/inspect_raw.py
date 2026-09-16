"""Inspect the latest raw snapshot of each source in MinIO.

For each configured source, prints the JSON key tree (types, list lengths),
samples of text fields longer than 50 characters, and any pagination /
total-count fields found. Used for I-01 (SPEC §8.1, §15).

Usage: python -m scripts.inspect_raw
"""
import json
from typing import Any

from storage.minio import BUCKET, get_s3_client

SOURCES = ["epam", "softserve"]

LONG_TEXT_MIN_LEN = 50
LIST_SAMPLE_SIZE = 2
MAX_TREE_DEPTH = 10

PAGINATION_KEYS = {
    "total", "totalcount", "total_count", "totalitems", "total_items",
    "count", "page", "pagenumber", "page_number", "pagesize", "page_size",
    "totalpages", "total_pages", "hasmore", "has_more", "next", "nextpage",
    "offset", "limit",
}


def latest_object_key(s3_client, source: str) -> str | None:
    prefix = f"raw/{source}_"
    paginator = s3_client.get_paginator("list_objects_v2")
    latest = None
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if latest is None or obj["LastModified"] > latest["LastModified"]:
                latest = obj
    return latest["Key"] if latest else None


def build_key_tree(value: Any, path: str = "$", depth: int = 0) -> list[str]:
    lines = []
    indent = "  " * depth
    if depth > MAX_TREE_DEPTH:
        lines.append(f"{indent}{path}: ... (max depth reached)")
        return lines
    if isinstance(value, dict):
        lines.append(f"{indent}{path}: object ({len(value)} keys)")
        for key, val in value.items():
            lines.extend(build_key_tree(val, f"{path}.{key}", depth + 1))
    elif isinstance(value, list):
        lines.append(f"{indent}{path}: array (len={len(value)})")
        if value:
            lines.extend(build_key_tree(value[0], f"{path}[0]", depth + 1))
    else:
        lines.append(f"{indent}{path}: {type(value).__name__} = {value!r}")
    return lines


def find_long_text_fields(value: Any, path: str = "$") -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    if isinstance(value, dict):
        for key, val in value.items():
            found.extend(find_long_text_fields(val, f"{path}.{key}"))
    elif isinstance(value, list):
        for i, item in enumerate(value[:LIST_SAMPLE_SIZE]):
            found.extend(find_long_text_fields(item, f"{path}[{i}]"))
    elif isinstance(value, str) and len(value) > LONG_TEXT_MIN_LEN:
        found.append((path, value))
    return found


def find_pagination_fields(value: Any, path: str = "$") -> list[tuple[str, Any]]:
    found: list[tuple[str, Any]] = []
    if isinstance(value, dict):
        for key, val in value.items():
            if key.lower() in PAGINATION_KEYS:
                found.append((f"{path}.{key}", val))
            if isinstance(val, (dict, list)):
                found.extend(find_pagination_fields(val, f"{path}.{key}"))
    elif isinstance(value, list):
        for i, item in enumerate(value[:LIST_SAMPLE_SIZE]):
            found.extend(find_pagination_fields(item, f"{path}[{i}]"))
    return found


def build_report(source: str, raw: dict) -> str:
    lines = [f"{'=' * 20} {source} {'=' * 20}"]

    lines.append("\n-- key tree --")
    lines.extend(build_key_tree(raw))

    lines.append("\n-- text fields longer than 50 chars (sample) --")
    long_fields = find_long_text_fields(raw)
    if not long_fields:
        lines.append("none found")
    for path, text in long_fields[:20]:
        preview = text[:120] + ("..." if len(text) > 120 else "")
        lines.append(f"{path}: {preview!r}")

    lines.append("\n-- pagination / total-count fields --")
    pagination = find_pagination_fields(raw)
    if not pagination:
        lines.append("none found")
    for path, val in pagination:
        lines.append(f"{path} = {val!r}")

    return "\n".join(lines)


def main() -> None:
    s3_client = get_s3_client()
    for source in SOURCES:
        key = latest_object_key(s3_client, source)
        if key is None:
            print(f"\n{source}: no raw/ objects found in MinIO bucket '{BUCKET}'")
            continue
        response = s3_client.get_object(Bucket=BUCKET, Key=key)
        raw = json.loads(response["Body"].read())
        print(f"\nusing {key}")
        print(build_report(source, raw))


if __name__ == "__main__":
    main()
