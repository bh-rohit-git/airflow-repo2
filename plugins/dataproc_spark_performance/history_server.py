"""Spark History Server REST client."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .models import SparkApplicationSnapshot, StageMetrics


class SparkHistoryServerClient:
    def __init__(self, base_url: str, *, timeout_sec: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec

    def _get_json(self, path: str) -> Any:
        url = f"{self.base_url}{path}"
        request = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=self.timeout_sec) as response:
            return json.loads(response.read().decode("utf-8"))

    def list_applications(self) -> list[dict[str, Any]]:
        payload = self._get_json("/api/v1/applications")
        if isinstance(payload, list):
            return payload
        return []

    def resolve_application_id(
        self,
        *,
        preferred_ids: list[str] | None = None,
        name_hint: str | None = None,
    ) -> str | None:
        if preferred_ids:
            known = {app.get("id") for app in self.list_applications()}
            for app_id in preferred_ids:
                if app_id in known:
                    return app_id
        apps = self.list_applications()
        if name_hint:
            for app in apps:
                if name_hint in (app.get("name") or ""):
                    return app.get("id")
        return apps[0].get("id") if apps else None

    def fetch_application_snapshot(self, application_id: str) -> SparkApplicationSnapshot:
        app_meta = self._get_json(f"/api/v1/applications/{urllib.parse.quote(application_id, safe='')}")
        stages_payload = self._get_json(
            f"/api/v1/applications/{urllib.parse.quote(application_id, safe='')}/stages"
        )
        stages: list[StageMetrics] = []
        if isinstance(stages_payload, list):
            for stage in stages_payload:
                if not isinstance(stage, dict):
                    continue
                stages.append(
                    StageMetrics(
                        stage_id=int(stage.get("stageId") or 0),
                        name=str(stage.get("name") or ""),
                        num_tasks=int(stage.get("numTasks") or 0),
                        duration_ms=int(stage.get("executorRunTime") or 0),
                        input_bytes=int(stage.get("inputBytes") or 0),
                        output_bytes=int(stage.get("outputBytes") or 0),
                        shuffle_read_bytes=int(stage.get("shuffleReadBytes") or 0),
                        shuffle_write_bytes=int(stage.get("shuffleWriteBytes") or 0),
                        disk_bytes_spilled=int(stage.get("diskBytesSpilled") or 0),
                        executor_run_time_ms=int(stage.get("executorRunTime") or 0),
                        peak_execution_memory=int(stage.get("peakExecutionMemory") or 0),
                    )
                )
        duration_ms = 0
        if isinstance(app_meta, list) and app_meta:
            app_meta = app_meta[0]
        if isinstance(app_meta, dict):
            duration_ms = int(app_meta.get("duration") or 0)
            app_name = str(app_meta.get("name") or "")
        else:
            app_name = ""
        return SparkApplicationSnapshot(
            application_id=application_id,
            name=app_name,
            duration_ms=duration_ms,
            stages=stages,
        )
