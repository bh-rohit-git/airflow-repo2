"""Data models for Spark metrics reports."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SparkMetricsReport:
    """Structured report covering telemetry, lineage, and performance."""

    dag_id: str | None
    run_id: str | None
    upstream_task_id: str | None
    metrics_output_dir: str
    report_output_dir: str
    telemetry: dict[str, Any] = field(default_factory=dict)
    lineage: dict[str, Any] = field(default_factory=dict)
    performance: dict[str, Any] = field(default_factory=dict)
    issues: list[dict[str, Any]] = field(default_factory=list)
    files_read: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "upstream_task_id": self.upstream_task_id,
            "metrics_output_dir": self.metrics_output_dir,
            "report_output_dir": self.report_output_dir,
            "telemetry": self.telemetry,
            "lineage": self.lineage,
            "performance": self.performance,
            "issues": self.issues,
            "files_read": self.files_read,
        }
