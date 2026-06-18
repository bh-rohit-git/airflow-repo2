"""Analyze Spark listener metrics into telemetry, lineage, and performance sections."""

from __future__ import annotations

from typing import Any

from dataproc_spark_metrics.models import SparkMetricsReport


def _sum_field(records: list[dict[str, Any]], field: str) -> int:
    total = 0
    for record in records:
        value = record.get(field)
        if isinstance(value, (int, float)):
            total += int(value)
    return total


def _max_field(records: list[dict[str, Any]], field: str) -> float:
    values = [record.get(field) for record in records if isinstance(record.get(field), (int, float))]
    return float(max(values)) if values else 0.0


def _extract_openlineage_datasets(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    inputs: list[dict[str, Any]] = []
    outputs: list[dict[str, Any]] = []
    seen_inputs: set[tuple[str, str]] = set()
    seen_outputs: set[tuple[str, str]] = set()

    for event in events:
        for item in event.get("inputs") or []:
            if not isinstance(item, dict):
                continue
            namespace = str(item.get("namespace") or "")
            name = str(item.get("name") or "")
            key = (namespace, name)
            if key in seen_inputs:
                continue
            seen_inputs.add(key)
            inputs.append({"namespace": namespace, "name": name, "facets": item.get("facets") or {}})
        for item in event.get("outputs") or []:
            if not isinstance(item, dict):
                continue
            namespace = str(item.get("namespace") or "")
            name = str(item.get("name") or "")
            key = (namespace, name)
            if key in seen_outputs:
                continue
            seen_outputs.add(key)
            outputs.append({"namespace": namespace, "name": name, "facets": item.get("facets") or {}})
    return inputs, outputs


def _extract_sql_queries(events: list[dict[str, Any]]) -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()
    for event in events:
        job = event.get("job") if isinstance(event.get("job"), dict) else {}
        facets = job.get("facets") if isinstance(job.get("facets"), dict) else {}
        sql = facets.get("sql") if isinstance(facets.get("sql"), dict) else {}
        query = sql.get("query")
        if isinstance(query, str) and query.strip() and query not in seen:
            seen.add(query)
            queries.append(query)
    return queries


def _collect_stage_issues(stages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for stage in stages:
        if not stage.get("issue_detected"):
            continue
        issues.append(
            {
                "scope": "stage",
                "stage_id": stage.get("stageId"),
                "name": stage.get("name"),
                "issue_type": stage.get("issue_type") or "unknown",
                "skew_ratio": stage.get("skew_ratio"),
                "spill_ratio": stage.get("spill_ratio"),
                "memory_bytes_spilled": stage.get("memoryBytesSpilled"),
            }
        )
    return issues


def _collect_task_issues(tasks: list[dict[str, Any]], *, limit: int = 20) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for task in tasks:
        if not task.get("issue_detected"):
            continue
        issues.append(
            {
                "scope": "task",
                "task_id": task.get("taskId"),
                "stage_id": task.get("stageId"),
                "executor_id": task.get("executorId"),
                "duration_ms": task.get("duration"),
                "memory_bytes_spilled": task.get("memoryBytesSpilled"),
                "network_bytes": task.get("network_bytes"),
            }
        )
    issues.sort(key=lambda item: int(item.get("duration_ms") or 0), reverse=True)
    return issues[:limit]


def analyze_metrics_bundle(
    bundle: dict[str, Any],
    *,
    dag_id: str | None,
    run_id: str | None,
    upstream_task_id: str | None,
    metrics_output_dir: str,
    report_output_dir: str,
    files_read: list[str],
) -> SparkMetricsReport:
    application_info = bundle.get("application_info") if isinstance(bundle.get("application_info"), dict) else {}
    stages = [item for item in bundle.get("stages") or [] if isinstance(item, dict)]
    tasks = [item for item in bundle.get("tasks") or [] if isinstance(item, dict)]
    executors = [item for item in bundle.get("executors") or [] if isinstance(item, dict)]
    jobs = [item for item in bundle.get("jobs") or [] if isinstance(item, dict)]
    openlineage = [item for item in bundle.get("openlineage") or [] if isinstance(item, dict)]

    telemetry = {
        "app_name": application_info.get("app_name"),
        "app_id": application_info.get("app_id"),
        "spark_version": application_info.get("spark_version"),
        "start_time": application_info.get("start_time"),
        "end_time": application_info.get("end_time"),
        "duration_sec": application_info.get("duration_sec"),
        "executor_count": len(executors),
        "stage_count": len(stages),
        "task_count": len(tasks),
        "job_count": len(jobs),
        "openlineage_event_count": len(openlineage),
    }

    inputs, outputs = _extract_openlineage_datasets(openlineage)
    lineage = {
        "pipeline_id": application_info.get("pipeline_id") or dag_id,
        "flow_id": application_info.get("flow_id") or upstream_task_id,
        "pipeline_name": application_info.get("pipeline_name") or dag_id,
        "inputs": inputs,
        "outputs": outputs,
        "sql_queries": _extract_sql_queries(openlineage),
        "tables_from_stages": sorted(
            {
                table
                for stage in stages
                for table in (stage.get("table_names") or [])
                if isinstance(table, str) and table
            }
        ),
    }

    total_stage_duration_ms = _sum_field(stages, "duration")
    performance = {
        "total_stage_duration_ms": total_stage_duration_ms,
        "total_executor_run_time_ms": _sum_field(stages, "executorRunTime"),
        "total_input_bytes": _sum_field(stages, "inputBytes"),
        "total_output_bytes": _sum_field(stages, "outputBytes"),
        "total_shuffle_read_bytes": _sum_field(stages, "shuffleTotalBytesRead"),
        "total_shuffle_write_bytes": _sum_field(stages, "shuffleBytesWritten"),
        "total_spill_bytes": _sum_field(stages, "memoryBytesSpilled"),
        "peak_execution_memory": _max_field(stages, "peakExecutionMemory"),
        "total_jvm_gc_time_ms": _sum_field(executors, "jvmGCTime"),
        "slowest_stage_ms": _max_field(stages, "duration"),
        "stage_issue_count": sum(1 for stage in stages if stage.get("issue_detected")),
        "task_issue_count": sum(1 for task in tasks if task.get("issue_detected")),
    }

    issues = _collect_stage_issues(stages)
    issues.extend(_collect_task_issues(tasks))

    return SparkMetricsReport(
        dag_id=dag_id,
        run_id=run_id,
        upstream_task_id=upstream_task_id,
        metrics_output_dir=metrics_output_dir,
        report_output_dir=report_output_dir,
        telemetry=telemetry,
        lineage=lineage,
        performance=performance,
        issues=issues,
        files_read=files_read,
    )
