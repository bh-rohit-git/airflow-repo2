"""Orchestrate Spark metrics collection for Airflow tasks."""

from __future__ import annotations

import json
from typing import Any

from dataproc_spark_metrics.analyzer import analyze_metrics_bundle
from dataproc_spark_metrics.reader import append_text, load_metrics_bundle, normalize_dir
from dataproc_spark_metrics.reporter import write_reports


def collect_spark_metrics(
    *,
    metrics_output_dir: str,
    report_output_dir: str | None = None,
    upstream_task_id: str | None = None,
    dag_id: str | None = None,
    run_id: str | None = None,
    task_telemetry_log_path: str | None = None,
) -> dict[str, Any]:
    """Load listener artifacts, analyze them, and write JSON/HTML reports."""
    metrics_dir = normalize_dir(metrics_output_dir)
    report_dir = normalize_dir(report_output_dir or metrics_dir)
    bundle, files_read = load_metrics_bundle(metrics_dir)
    report = analyze_metrics_bundle(
        bundle,
        dag_id=dag_id,
        run_id=run_id,
        upstream_task_id=upstream_task_id,
        metrics_output_dir=metrics_dir,
        report_output_dir=report_dir,
        files_read=files_read,
    )
    artifact_paths = write_reports(report, report_output_dir=report_dir)
    result = {
        "status": "ok" if files_read else "no_metrics_found",
        "metrics_output_dir": metrics_dir,
        "report_output_dir": report_dir,
        "files_read": files_read,
        "report_paths": artifact_paths,
        "issue_count": len(report.issues),
        "telemetry": report.telemetry,
        "lineage": {
            "pipeline_id": report.lineage.get("pipeline_id"),
            "flow_id": report.lineage.get("flow_id"),
            "input_count": len(report.lineage.get("inputs") or []),
            "output_count": len(report.lineage.get("outputs") or []),
        },
        "performance": report.performance,
    }
    if task_telemetry_log_path:
        append_text(
            task_telemetry_log_path,
            json.dumps(
                {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "upstream_task_id": upstream_task_id,
                    "metrics_output_dir": metrics_dir,
                    "report_paths": artifact_paths,
                    "issue_count": len(report.issues),
                    "performance": report.performance,
                    "lineage": result["lineage"],
                },
                sort_keys=True,
            )
            + "\n",
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def run_collect_spark_metrics(**kwargs: Any) -> dict[str, Any]:
    """Airflow ``python_callable`` entry point."""
    return collect_spark_metrics(
        metrics_output_dir=str(kwargs["metrics_output_dir"]),
        report_output_dir=kwargs.get("report_output_dir"),
        upstream_task_id=kwargs.get("upstream_task_id"),
        dag_id=kwargs.get("dag_id"),
        run_id=kwargs.get("run_id"),
        task_telemetry_log_path=kwargs.get("task_telemetry_log_path"),
    )
