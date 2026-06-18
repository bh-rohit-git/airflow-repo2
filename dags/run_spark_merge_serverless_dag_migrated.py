from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
from airflow.sdk import Param
import copy
from airflow.providers.standard.operators.python import PythonOperator
from dataproc_spark_metrics.orchestrator import run_collect_spark_metrics


def _coerce_dataproc_int(value):
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return value


def _duration_seconds_value(value, default=1800):
    if isinstance(value, int):
        return value
    if isinstance(value, dict) and "seconds" in value:
        return int(value["seconds"])
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.endswith("s"):
            stripped = stripped[:-1].strip()
        if stripped.isdigit():
            return int(stripped)
    return default


def _coerce_duration_proto(value, default=1800):
    return {"seconds": _duration_seconds_value(value, default)}


_MISPLACED_GCE_METADATA_KEYS = frozenset(
    {
        "spark_listener_jar_uri",
        "spark_metrics_output_prefix",
        "performance_report_dir",
        "task_telemetry_log_path",
    }
)


def _strip_misplaced_gce_metadata(config):
    gce = config.get("gce_cluster_config")
    if not isinstance(gce, dict):
        return config
    metadata = gce.get("metadata")
    if not isinstance(metadata, dict):
        return config
    stripped = {
        key: value
        for key, value in metadata.items()
        if key not in _MISPLACED_GCE_METADATA_KEYS
    }
    if stripped != metadata:
        gce = dict(gce)
        if stripped:
            gce["metadata"] = stripped
        else:
            gce.pop("metadata", None)
        config = dict(config)
        config["gce_cluster_config"] = gce
    return config


def normalize_dataproc_cluster_config_types(config):
    normalized = _strip_misplaced_gce_metadata(copy.deepcopy(config))
    for role in ("master_config", "worker_config"):
        section = normalized.get(role)
        if not isinstance(section, dict):
            continue
        if "num_instances" in section:
            section["num_instances"] = _coerce_dataproc_int(section["num_instances"])
        disk = section.get("disk_config")
        if isinstance(disk, dict) and "boot_disk_size_gb" in disk:
            disk["boot_disk_size_gb"] = _coerce_dataproc_int(disk["boot_disk_size_gb"])
    lifecycle = normalized.get("lifecycle_config")
    if isinstance(lifecycle, dict):
        if "idle_delete_ttl" in lifecycle:
            lifecycle["idle_delete_ttl"] = _coerce_duration_proto(
                lifecycle["idle_delete_ttl"]
            )
        if "auto_delete_ttl" in lifecycle:
            lifecycle["auto_delete_ttl"] = _coerce_duration_proto(
                lifecycle["auto_delete_ttl"]
            )
    return normalized


class TypedDataprocCreateClusterOperator(DataprocCreateClusterOperator):
    def execute(self, context):
        if isinstance(self.cluster_config, dict):
            self.cluster_config = normalize_dataproc_cluster_config_types(
                self.cluster_config
            )
        return super().execute(context)


from pathlib import Path
import yaml

_ENV_CONFIG_PATH = Path(__file__).resolve().parent.parent / "plugins" / "config_dev.yml"
_DAG_SIZING_PATH = (
    Path(__file__).resolve().parent.parent
    / "plugins"
    / "run_spark_merge_serverless_dag_migrated_cluster_config.yml"
)
if not _ENV_CONFIG_PATH.is_file():
    raise FileNotFoundError(
        "config_dev.yml not found under plugins/. Provision it in the target environment (e.g. gs://<composer_bucket>/plugins/config_dev.yml)."
    )
if not _DAG_SIZING_PATH.is_file():
    raise FileNotFoundError(
        "run_spark_merge_serverless_dag_migrated_cluster_config.yml not found under plugins/. Upload it to gs://<composer_bucket>/plugins/run_spark_merge_serverless_dag_migrated_cluster_config.yml."
    )
with _ENV_CONFIG_PATH.open(encoding="utf-8") as _env_config_file:
    _ENV_CONFIG = yaml.safe_load(_env_config_file)
with _DAG_SIZING_PATH.open(encoding="utf-8") as _dag_sizing_file:
    _DAG_SIZING = yaml.safe_load(_dag_sizing_file)
_gce_metadata = (
    (_ENV_CONFIG.get("dataproc_cluster_config") or {}).get("gce_cluster_config") or {}
).get("metadata") or {}
for _cfg_key in (
    "spark_listener_jar_uri",
    "spark_metrics_output_prefix",
    "performance_report_dir",
    "task_telemetry_log_path",
):
    if _cfg_key not in _ENV_CONFIG and _cfg_key in _gce_metadata:
        _ENV_CONFIG[_cfg_key] = _gce_metadata[_cfg_key]
GCP_PROJECT_ID = _ENV_CONFIG["gcp_project_id"]
GCP_REGION = _ENV_CONFIG["gcp_region"]
DATABRICKS_HOST = _ENV_CONFIG.get("databricks_host", "")
DATABRICKS_TOKEN = _ENV_CONFIG.get("databricks_token", "")
DATAPROC_CLUSTER_NAME = _DAG_SIZING["dataproc_cluster_name"]
_BASE_CLUSTER_CONFIG = normalize_dataproc_cluster_config_types(
    _ENV_CONFIG["dataproc_cluster_config"]
)
_SIZING = normalize_dataproc_cluster_config_types(
    {
        "master_config": _DAG_SIZING["master_config"],
        "worker_config": _DAG_SIZING["worker_config"],
    }
)
DATAPROC_CLUSTER_CONFIG = {
    **_BASE_CLUSTER_CONFIG,
    "master_config": _SIZING["master_config"],
    "worker_config": _SIZING["worker_config"],
}
SPARK_LISTENER_JAR_URI = _ENV_CONFIG.get(
    "spark_listener_jar_uri", "gs://YOUR_BUCKET/jars/plan-listener-assembly-1.0.jar"
)
SPARK_METRICS_OUTPUT_PREFIX = _ENV_CONFIG.get(
    "spark_metrics_output_prefix", "gs://YOUR_BUCKET/spark-metrics/"
)
PERFORMANCE_REPORT_DIR = _ENV_CONFIG.get(
    "performance_report_dir", "gs://YOUR_BUCKET/spark-reports/"
)
TASK_TELEMETRY_LOG_PATH = _ENV_CONFIG.get(
    "task_telemetry_log_path", "gs://YOUR_BUCKET/telemetry/task_metrics.jsonl"
)
default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}
with DAG(
    dag_id="run_spark_merge_serverless_dag_migrated",
    default_args=default_args,
    description="Trigger Spark merge job on Databricks serverless (JAR task)",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["databricks", "spark", "serverless"],
    params={
        "input_path": Param(
            default="gs://databricks-8259550742932689-unitycatalog/8259550742932689/incoming/customer_events_merge.csv",
            type="string",
            description="GCS path to the input CSV file or folder",
        ),
        "jar_file_uris": Param(
            default=[
                "/Volumes/databricks-migrate-activity/schema1/jars/spark-merge-job-assembly.jar"
            ],
            type="array",
            description="JAR file URIs for the Dataproc Spark job (job JAR + listener JAR)",
        ),
        "spark_job_args": Param(
            default=[
                "--input-path",
                "{{ dag_run.conf.get('input_path', params.input_path) }}",
                "--target-table",
                "databricks-migrate-activity.schema1.customer_events",
                "--merge-key",
                "id",
            ],
            type="array",
            description="Spark job CLI args for the Dataproc submit task (override via dag_run.conf)",
        ),
    },
    render_template_as_native_obj=True,
) as dag:
    create_cluster = TypedDataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        cluster_name=DATAPROC_CLUSTER_NAME,
        cluster_config=DATAPROC_CLUSTER_CONFIG,
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        cluster_name=DATAPROC_CLUSTER_NAME,
        trigger_rule="all_done",
    )
    run_spark_merge = DataprocSubmitJobOperator(
        task_id="run_spark_merge",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "spark_job": {
                "main_class": "com.example.merge.Main",
                "jar_file_uris": [
                    "{{ (dag_run.conf.get('jar_file_uris') or params.jar_file_uris)[0] }}",
                    SPARK_LISTENER_JAR_URI,
                ],
                "args": "{{ dag_run.conf.get('spark_job_args') or params.spark_job_args }}",
                "properties": {
                    "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.1",
                    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                    "spark.submit.deployMode": "cluster",
                    "spark.yarn.appMasterEnv.DATABRICKS_HOST": DATABRICKS_HOST,
                    "spark.yarn.appMasterEnv.DATABRICKS_TOKEN": DATABRICKS_TOKEN,
                    "spark.costanalyzer.outputDir": SPARK_METRICS_OUTPUT_PREFIX
                    + "{{ dag.dag_id }}/{{ run_id }}/{{ ti.task_id }}/",
                    "spark.sql.queryExecutionListeners": "ai.bighammer.spark.listener.SQLAndMetricsCaptureListener",
                    "spark.openlineage.metrics.enabled": "true",
                    "spark.sql.adaptive.enabled": "true",
                    "spark.pipeline.id": "{{ dag.dag_id }}",
                    "spark.flow.id": "{{ ti.task_id }}",
                    "spark.pipeline.name": "{{ dag.dag_id }}",
                },
            },
        },
    )
    collect_spark_metrics_run_spark_merge = PythonOperator(
        task_id="collect_spark_metrics_run_spark_merge",
        python_callable=run_collect_spark_metrics,
        op_kwargs={
            "metrics_output_dir": SPARK_METRICS_OUTPUT_PREFIX
            + "{{ dag.dag_id }}/{{ run_id }}/run_spark_merge/",
            "report_output_dir": PERFORMANCE_REPORT_DIR
            + "{{ dag.dag_id }}/{{ run_id }}/",
            "upstream_task_id": "run_spark_merge",
            "dag_id": "{{ dag.dag_id }}",
            "run_id": "{{ run_id }}",
            "task_telemetry_log_path": TASK_TELEMETRY_LOG_PATH,
        },
    )
    create_cluster >> run_spark_merge
    run_spark_merge >> collect_spark_metrics_run_spark_merge
    collect_spark_metrics_run_spark_merge >> delete_cluster
