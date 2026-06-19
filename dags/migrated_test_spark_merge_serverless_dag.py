from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator
from airflow.sdk import Param
import copy

def _coerce_dataproc_int(value):
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return value

def _duration_seconds_value(value, default=1800):
    if isinstance(value, int):
        return value
    if isinstance(value, dict) and 'seconds' in value:
        return int(value['seconds'])
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.endswith('s'):
            stripped = stripped[:-1].strip()
        if stripped.isdigit():
            return int(stripped)
    return default

def _coerce_duration_proto(value, default=1800):
    return {'seconds': _duration_seconds_value(value, default)}

def normalize_dataproc_cluster_config_types(config):
    normalized = copy.deepcopy(config)
    for role in ('master_config', 'worker_config'):
        section = normalized.get(role)
        if not isinstance(section, dict):
            continue
        if 'num_instances' in section:
            section['num_instances'] = _coerce_dataproc_int(section['num_instances'])
        disk = section.get('disk_config')
        if isinstance(disk, dict) and 'boot_disk_size_gb' in disk:
            disk['boot_disk_size_gb'] = _coerce_dataproc_int(disk['boot_disk_size_gb'])
    lifecycle = normalized.get('lifecycle_config')
    if isinstance(lifecycle, dict):
        if 'idle_delete_ttl' in lifecycle:
            lifecycle['idle_delete_ttl'] = _coerce_duration_proto(lifecycle['idle_delete_ttl'])
        if 'auto_delete_ttl' in lifecycle:
            lifecycle['auto_delete_ttl'] = _coerce_duration_proto(lifecycle['auto_delete_ttl'])
    return normalized

class TypedDataprocCreateClusterOperator(DataprocCreateClusterOperator):

    def execute(self, context):
        if isinstance(self.cluster_config, dict):
            self.cluster_config = normalize_dataproc_cluster_config_types(self.cluster_config)
        return super().execute(context)
from pathlib import Path
import yaml
_ENV_CONFIG_PATH = Path(__file__).resolve().parent.parent / 'plugins' / 'config_dev.yml'
_DAG_SIZING_PATH = Path(__file__).resolve().parent.parent / 'plugins' / 'migrated_test_spark_merge_serverless_dag_cluster_config.yml'
if not _ENV_CONFIG_PATH.is_file():
    raise FileNotFoundError('config_dev.yml not found under plugins/. Provision it in the target environment (e.g. gs://<composer_bucket>/plugins/config_dev.yml).')
if not _DAG_SIZING_PATH.is_file():
    raise FileNotFoundError('migrated_test_spark_merge_serverless_dag_cluster_config.yml not found under plugins/. Upload it to gs://<composer_bucket>/plugins/migrated_test_spark_merge_serverless_dag_cluster_config.yml.')
with _ENV_CONFIG_PATH.open(encoding='utf-8') as _env_config_file:
    _ENV_CONFIG = yaml.safe_load(_env_config_file)
with _DAG_SIZING_PATH.open(encoding='utf-8') as _dag_sizing_file:
    _DAG_SIZING = yaml.safe_load(_dag_sizing_file)
GCP_PROJECT_ID = _ENV_CONFIG['gcp_project_id']
GCP_REGION = _ENV_CONFIG['gcp_region']
DATABRICKS_HOST = _ENV_CONFIG.get('databricks_host', '')
DATABRICKS_TOKEN = _ENV_CONFIG.get('databricks_token', '')
DATAPROC_CLUSTER_NAME = _DAG_SIZING['dataproc_cluster_name']
_BASE_CLUSTER_CONFIG = normalize_dataproc_cluster_config_types(_ENV_CONFIG['dataproc_cluster_config'])
_SIZING = normalize_dataproc_cluster_config_types({'master_config': _DAG_SIZING['master_config'], 'worker_config': _DAG_SIZING['worker_config']})
DATAPROC_CLUSTER_CONFIG = {**_BASE_CLUSTER_CONFIG, 'master_config': _SIZING['master_config'], 'worker_config': _SIZING['worker_config']}
default_args = {'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}
with DAG(dag_id='migrated_test_spark_merge_serverless_dag', default_args=default_args, description='Trigger Spark merge job on Databricks serverless (JAR task)', schedule=None, start_date=datetime(2025, 1, 1), catchup=False, tags=['databricks', 'spark', 'serverless'], params={'input_path': Param(default='gs://databricks-8259550742932689-unitycatalog/8259550742932689/incoming/customer_events_merge.csv', type='string', description='GCS path to the input CSV file or folder'), 'target_table': Param(default='databricks-migrate-activity.schema1.customer_events', type='string', description='Target table'), 'jar_file_uris': Param(default=['/Volumes/databricks-migrate-activity/schema1/jars/spark-merge-job-assembly.jar'], type='array', description='JAR file URIs for the Dataproc Spark job')}, render_template_as_native_obj=True) as dag:
    run_spark_merge = DataprocSubmitJobOperator(task_id='run_spark_merge', project_id=GCP_PROJECT_ID, region=GCP_REGION, job={'placement': {'cluster_name': DATAPROC_CLUSTER_NAME}, 'spark_job': {'main_class': 'com.example.merge.Main', 'jar_file_uris': ["{{ (dag_run.conf.get('jar_file_uris') or params.jar_file_uris)[0] }}"], 'args': ['--input-path', "{{ dag_run.conf.get('input_path', params.input_path) }}", '--target-table', "{{ dag_run.conf.get('target_table', params.target_table) }}", '--merge-key', 'id'], 'properties': {'spark.jars.packages': 'io.delta:delta-spark_2.12:3.2.1', 'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog', 'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension', 'spark.submit.deployMode': 'cluster', 'spark.yarn.appMasterEnv.DATABRICKS_HOST': DATABRICKS_HOST, 'spark.yarn.appMasterEnv.DATABRICKS_TOKEN': DATABRICKS_TOKEN}}})
create_cluster = TypedDataprocCreateClusterOperator(task_id='create_cluster', project_id=GCP_PROJECT_ID, region=GCP_REGION, cluster_name=DATAPROC_CLUSTER_NAME, cluster_config=DATAPROC_CLUSTER_CONFIG)
delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', project_id=GCP_PROJECT_ID, region=GCP_REGION, cluster_name=DATAPROC_CLUSTER_NAME, trigger_rule='all_done')
create_cluster >> run_spark_merge
run_spark_merge >> delete_cluster
