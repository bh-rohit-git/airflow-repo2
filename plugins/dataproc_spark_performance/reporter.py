"""Markdown performance report writer for Dataproc Spark runs."""

from __future__ import annotations

from pathlib import Path

from dataproc_spark_performance.models import SparkPerformanceReport


def write_spark_performance_report(report: SparkPerformanceReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = [
        f"# Spark Performance Report: {report.run_key}",
        "",
        "## Run metadata",
        f"- **DAG ID**: {report.dag_id}",
        f"- **Task ID**: {report.task_id}",
        f"- **Airflow run ID**: {report.airflow_run_id}",
        f"- **Dataproc job ID**: {report.dataproc_job_id}",
        f"- **Spark application ID**: {report.spark_application_id or 'n/a'}",
        f"- **Project / region**: {report.project_id} / {report.region}",
        f"- **Cluster**: {report.cluster_name}",
        "",
        "## Summary",
    ]
    if not report.findings:
        lines.append("No performance issues detected from History Server stage metrics.")
    else:
        critical = sum(1 for f in report.findings if f.severity == "critical")
        warning = sum(1 for f in report.findings if f.severity == "warning")
        lines.append(f"- **Findings**: {len(report.findings)} ({critical} critical, {warning} warning)")
    lines.extend(["", "## Findings", ""])
    if report.findings:
        lines.append("| Severity | Category | Recommendation |")
        lines.append("| --- | --- | --- |")
        for finding in report.findings:
            lines.append(
                f"| {finding.severity.upper()} | {finding.category.value} | {finding.recommendation} |"
            )
    else:
        lines.append("_No findings._")
    lines.extend(["", "## Stage metrics", ""])
    if report.stages:
        lines.append(
            "| Stage | Tasks | Duration (ms) | Shuffle write | Shuffle read | Spill |"
        )
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
        for stage in report.stages:
            lines.append(
                f"| {stage.name or stage.stage_id} | {stage.num_tasks} | {stage.duration_ms} | "
                f"{stage.shuffle_write_bytes} | {stage.shuffle_read_bytes} | {stage.disk_bytes_spilled} |"
            )
    else:
        lines.append("_No stage metrics collected._")
    if report.metadata:
        lines.extend(["", "## Diagnostics", ""])
        for key, value in sorted(report.metadata.items()):
            lines.append(f"- **{key}**: {value}")
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path
