"""Read Spark custom listener output from local paths or GCS."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def is_gcs_uri(path: str) -> bool:
    return path.startswith("gs://")


def normalize_dir(path: str) -> str:
    normalized = path.strip()
    if not normalized:
        return normalized
    if is_gcs_uri(normalized):
        return normalized if normalized.endswith("/") else f"{normalized}/"
    return str(Path(normalized).as_posix().rstrip("/") + "/")


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    bucket = parsed.netloc
    blob_path = parsed.path.lstrip("/")
    return bucket, blob_path


def read_text(path: str) -> str:
    if is_gcs_uri(path):
        from google.cloud import storage

        bucket_name, blob_path = parse_gcs_uri(path)
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(blob_path)
        return blob.download_as_text(encoding="utf-8")
    return Path(path).read_text(encoding="utf-8")


def write_text(path: str, content: str) -> None:
    if is_gcs_uri(path):
        from google.cloud import storage

        bucket_name, blob_path = parse_gcs_uri(path)
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(blob_path)
        blob.upload_from_string(content, content_type="text/plain; charset=utf-8")
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")


def append_text(path: str, content: str) -> None:
    if is_gcs_uri(path):
        from google.cloud import storage

        bucket_name, blob_path = parse_gcs_uri(path)
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(blob_path)
        if blob.exists():
            existing = blob.download_as_text(encoding="utf-8")
            content = existing + content
        blob.upload_from_string(content, content_type="text/plain; charset=utf-8")
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(content)


def list_files(prefix: str) -> list[str]:
    normalized = normalize_dir(prefix)
    if is_gcs_uri(normalized):
        from google.cloud import storage

        bucket_name, blob_prefix = parse_gcs_uri(normalized)
        client = storage.Client()
        blobs = client.list_blobs(bucket_name, prefix=blob_prefix)
        return [f"gs://{bucket_name}/{blob.name}" for blob in blobs if not blob.name.endswith("/")]
    root = Path(normalized)
    if not root.is_dir():
        return []
    return [str(path) for path in sorted(root.rglob("*")) if path.is_file()]


def read_json(path: str) -> dict[str, Any]:
    payload = read_text(path)
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def read_jsonl(path: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line in read_text(path).splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        item = json.loads(stripped)
        if isinstance(item, dict):
            records.append(item)
    return records


def load_metrics_bundle(metrics_output_dir: str) -> tuple[dict[str, Any], list[str]]:
    """Load listener artifacts from a metrics output directory."""
    files = list_files(metrics_output_dir)
    by_name: dict[str, list[str]] = {}
    for file_path in files:
        name = Path(urlparse(file_path).path if is_gcs_uri(file_path) else file_path).name
        by_name.setdefault(name, []).append(file_path)

    bundle: dict[str, Any] = {
        "application_info": None,
        "stages": [],
        "tasks": [],
        "executors": [],
        "jobs": [],
        "openlineage": [],
        "query_plans": [],
    }
    files_read: list[str] = []

    app_paths = by_name.get("application_info.json") or []
    if app_paths:
        bundle["application_info"] = read_json(app_paths[0])
        files_read.append(app_paths[0])

    for key, filename in (
        ("stages", "stages.jsonl"),
        ("tasks", "tasks.jsonl"),
        ("executors", "executors.jsonl"),
        ("jobs", "jobs.jsonl"),
        ("openlineage", "openlineage.jsonl"),
        ("query_plans", "query_plans.jsonl"),
    ):
        for file_path in by_name.get(filename, []):
            bundle[key].extend(read_jsonl(file_path))
            files_read.append(file_path)

    return bundle, files_read
