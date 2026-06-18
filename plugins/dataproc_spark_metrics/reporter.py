"""Render Spark metrics reports as JSON and HTML."""

from __future__ import annotations

import html
import json
from typing import Any

from dataproc_spark_metrics.models import SparkMetricsReport
from dataproc_spark_metrics.reader import normalize_dir, write_text


def _format_bytes(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    size = float(value)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{value} B"


def _render_list(items: list[Any]) -> str:
    if not items:
        return "<p>None</p>"
    rows = []
    for item in items:
        if isinstance(item, dict):
            label = ", ".join(
                str(item.get(key))
                for key in ("namespace", "name", "issue_type", "stage_id", "task_id")
                if item.get(key) is not None
            )
            rows.append(f"<li>{html.escape(label or json.dumps(item, sort_keys=True))}</li>")
        else:
            rows.append(f"<li>{html.escape(str(item))}</li>")
    return f"<ul>{''.join(rows)}</ul>"


def render_html_report(report: SparkMetricsReport) -> str:
    task_label = report.upstream_task_id or "spark_job"
    perf = report.performance
    telemetry = report.telemetry
    lineage = report.lineage
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Spark Metrics Report - {html.escape(task_label)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2937; }}
    h1, h2 {{ color: #111827; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 24px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 8px 12px; text-align: left; }}
    th {{ background: #f3f4f6; }}
    .issue {{ color: #b45309; }}
  </style>
</head>
<body>
  <h1>Spark Metrics Report</h1>
  <p><strong>DAG:</strong> {html.escape(str(report.dag_id or '-'))}</p>
  <p><strong>Run:</strong> {html.escape(str(report.run_id or '-'))}</p>
  <p><strong>Task:</strong> {html.escape(task_label)}</p>
  <p><strong>Metrics dir:</strong> {html.escape(report.metrics_output_dir)}</p>

  <h2>Telemetry</h2>
  <table>
    <tr><th>Application</th><td>{html.escape(str(telemetry.get('app_name') or '-'))}</td></tr>
    <tr><th>App ID</th><td>{html.escape(str(telemetry.get('app_id') or '-'))}</td></tr>
    <tr><th>Spark version</th><td>{html.escape(str(telemetry.get('spark_version') or '-'))}</td></tr>
    <tr><th>Duration (sec)</th><td>{html.escape(str(telemetry.get('duration_sec') or '-'))}</td></tr>
    <tr><th>Stages / Tasks / Executors</th><td>{telemetry.get('stage_count', 0)} / {telemetry.get('task_count', 0)} / {telemetry.get('executor_count', 0)}</td></tr>
  </table>

  <h2>Lineage</h2>
  <table>
    <tr><th>Pipeline ID</th><td>{html.escape(str(lineage.get('pipeline_id') or '-'))}</td></tr>
    <tr><th>Flow ID</th><td>{html.escape(str(lineage.get('flow_id') or '-'))}</td></tr>
    <tr><th>Pipeline name</th><td>{html.escape(str(lineage.get('pipeline_name') or '-'))}</td></tr>
  </table>
  <h3>Inputs</h3>
  {_render_list(lineage.get('inputs') or [])}
  <h3>Outputs</h3>
  {_render_list(lineage.get('outputs') or [])}
  <h3>SQL queries</h3>
  {_render_list(lineage.get('sql_queries') or [])}

  <h2>Performance</h2>
  <table>
    <tr><th>Total stage duration</th><td>{perf.get('total_stage_duration_ms', 0)} ms</td></tr>
    <tr><th>Shuffle read</th><td>{_format_bytes(perf.get('total_shuffle_read_bytes'))}</td></tr>
    <tr><th>Shuffle write</th><td>{_format_bytes(perf.get('total_shuffle_write_bytes'))}</td></tr>
    <tr><th>Input bytes</th><td>{_format_bytes(perf.get('total_input_bytes'))}</td></tr>
    <tr><th>Output bytes</th><td>{_format_bytes(perf.get('total_output_bytes'))}</td></tr>
    <tr><th>Spill bytes</th><td>{_format_bytes(perf.get('total_spill_bytes'))}</td></tr>
    <tr><th>JVM GC time</th><td>{perf.get('total_jvm_gc_time_ms', 0)} ms</td></tr>
    <tr><th>Stage issues</th><td class="issue">{perf.get('stage_issue_count', 0)}</td></tr>
    <tr><th>Task issues</th><td class="issue">{perf.get('task_issue_count', 0)}</td></tr>
  </table>

  <h2>Detected issues</h2>
  {_render_list(report.issues)}
</body>
</html>
"""


def write_reports(
    report: SparkMetricsReport,
    *,
    report_output_dir: str,
) -> dict[str, str]:
    prefix = normalize_dir(report_output_dir)
    task_label = report.upstream_task_id or "spark_job"
    json_path = f"{prefix}{task_label}_spark_metrics_report.json"
    html_path = f"{prefix}{task_label}_spark_metrics_report.html"
    write_text(json_path, json.dumps(report.to_dict(), indent=2, sort_keys=True))
    write_text(html_path, render_html_report(report))
    return {"json_report": json_path, "html_report": html_path}
