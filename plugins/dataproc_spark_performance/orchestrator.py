"""Orchestrate Dataproc job + Spark History Server performance analysis."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bh_migrate.dataproc.spark_performance.analyzer import analyze_stage_metrics
from bh_migrate.dataproc.spark_performance.dataproc_jobs import (
    extract_dataproc_job_id,
    fetch_dataproc_job,
)
from bh_migrate.dataproc.spark_performance.history_server import SparkHistoryServerClient
from bh_migrate.dataproc.spark_performance.models import SparkPerformanceReport
from bh_migrate.dataproc.spark_performance.reporter import write_spark_performance_report


def run_spark_performance_analysis(
    *,
    project_id: str,
    region: str,
    cluster_name: str,
    upstream_spark_task_id: str,
    dag_id: str,
    airflow_run_id: str,
    ti,
    spark_history_server_url: str | None = None,
    performance_report_dir: str | None = None,
    dataproc_job_id: str | None = None,
) -> str:
    """Analyze one upstream Dataproc submit task and write a Markdown report."""
    job_id = dataproc_job_id or extract_dataproc_job_id(ti.xcom_pull(task_ids=upstream_spark_task_id))
    if not job_id:
        raise ValueError(
            f"No Dataproc job id in XCom for upstream task '{upstream_spark_task_id}'"
        )

    run_key = f"{dag_id}__{airflow_run_id}__{upstream_spark_task_id}"
    metadata: dict[str, Any] = {"upstream_task_id": upstream_spark_task_id}
    job = fetch_dataproc_job(project_id=project_id, region=region, job_id=job_id)
    metadata["dataproc_job_state"] = job.state
    metadata["driver_output_uri"] = job.driver_output_uri

    stages = []
    application_id = ""
    if spark_history_server_url:
        client = SparkHistoryServerClient(spark_history_server_url)
        application_id = client.resolve_application_id(preferred_ids=job.yarn_application_ids) or ""
        if application_id:
            snapshot = client.fetch_application_snapshot(application_id)
            stages = snapshot.stages
            metadata["spark_application_name"] = snapshot.name
        else:
            metadata["history_server_warning"] = "No Spark application matched in History Server"
    else:
        metadata["history_server_warning"] = "spark_history_server_url not configured"

    findings = analyze_stage_metrics(stages, run_id=run_key)
    report = SparkPerformanceReport(
        run_key=run_key,
        dag_id=dag_id,
        task_id=upstream_spark_task_id,
        airflow_run_id=airflow_run_id,
        dataproc_job_id=job_id,
        spark_application_id=application_id,
        project_id=project_id,
        region=region,
        cluster_name=cluster_name,
        findings=findings,
        stages=stages,
        metadata=metadata,
    )
    report_dir = Path(
        performance_report_dir
        or "/home/airflow/gcs/logs/performance_reports"
    )
    output_path = report_dir / dag_id / airflow_run_id / f"{upstream_spark_task_id}.md"
    write_spark_performance_report(report, output_path)
    return str(output_path)


def analyze_dataproc_spark_job(
    *,
    upstream_spark_task_ids: list[str],
    project_id: str,
    region: str,
    cluster_name: str,
    spark_history_server_url: str | None = None,
    performance_report_dir: str | None = None,
    **context,
) -> list[str]:
    """Airflow PythonOperator entrypoint: analyze all upstream Dataproc submit tasks."""
    ti = context["ti"]
    dag_run = context.get("dag_run")
    dag_id = ti.dag_id
    airflow_run_id = getattr(dag_run, "run_id", None) or context.get("run_id") or "manual"
    report_paths: list[str] = []
    for upstream_task_id in upstream_spark_task_ids:
        report_paths.append(
            run_spark_performance_analysis(
                project_id=project_id,
                region=region,
                cluster_name=cluster_name,
                upstream_spark_task_id=upstream_task_id,
                dag_id=dag_id,
                airflow_run_id=str(airflow_run_id),
                ti=ti,
                spark_history_server_url=spark_history_server_url,
                performance_report_dir=performance_report_dir,
            )
        )
    return report_paths
