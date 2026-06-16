"""Dataproc Jobs API helpers."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .models import DataprocJobSnapshot


def fetch_dataproc_job(
    *,
    project_id: str,
    region: str,
    job_id: str,
    access_token: str | None = None,
    timeout_sec: int = 30,
) -> DataprocJobSnapshot:
    """Fetch Dataproc job metadata via REST (Composer ADC when token omitted)."""
    if not access_token:
        access_token = _default_access_token()
    encoded_job = urllib.parse.quote(job_id, safe="")
    url = (
        f"https://dataproc.googleapis.com/v1/projects/{project_id}"
        f"/regions/{region}/jobs/{encoded_job}"
    )
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        payload = json.loads(response.read().decode("utf-8"))
    yarn_ids: list[str] = []
    status = payload.get("status") or {}
    driver = payload.get("driverControlFiles") or {}
    placement = payload.get("placement") or {}
    for item in payload.get("yarnApplications") or []:
        if isinstance(item, dict) and item.get("applicationId"):
            yarn_ids.append(str(item["applicationId"]))
    return DataprocJobSnapshot(
        job_id=job_id,
        state=str(status.get("state") or ""),
        placement_cluster_name=str(placement.get("clusterName") or ""),
        driver_output_uri=str(driver.get("driverOutputUri") or payload.get("driverOutputResourceUri") or ""),
        yarn_application_ids=yarn_ids,
    )


def _default_access_token() -> str:
    try:
        import google.auth
        import google.auth.transport.requests

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        credentials.refresh(google.auth.transport.requests.Request())
        token = credentials.token
        if not token:
            raise RuntimeError("empty access token")
        return token
    except Exception as exc:
        raise RuntimeError(
            "Unable to obtain GCP credentials for Dataproc API. "
            "Run on Composer or set GOOGLE_APPLICATION_CREDENTIALS."
        ) from exc


def extract_dataproc_job_id(xcom_value: Any) -> str | None:
    """Normalize DataprocSubmitJobOperator XCom return value to a job id string."""
    if xcom_value is None:
        return None
    if isinstance(xcom_value, str):
        return xcom_value
    if isinstance(xcom_value, dict):
        for key in ("job_id", "reference", "id"):
            if key in xcom_value and isinstance(xcom_value[key], str):
                return xcom_value[key]
        ref = xcom_value.get("reference")
        if isinstance(ref, dict) and isinstance(ref.get("jobId"), str):
            return ref["jobId"]
    return str(xcom_value)
