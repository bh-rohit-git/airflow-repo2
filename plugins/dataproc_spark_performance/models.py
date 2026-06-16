"""Models for Dataproc Spark performance analysis."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class HotspotCategory(str, Enum):
    SHUFFLE = "shuffle"
    SKEW = "skew"
    SPILL = "spill"
    PARTITIONING = "partitioning"


class PerformanceFinding(BaseModel):
    finding_id: str
    run_id: str
    category: HotspotCategory
    severity: Literal["info", "warning", "critical"]
    evidence: dict[str, Any] = Field(default_factory=dict)
    recommendation: str


class StageMetrics(BaseModel):
    stage_id: int
    name: str = ""
    num_tasks: int = 0
    duration_ms: int = 0
    input_bytes: int = 0
    output_bytes: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    disk_bytes_spilled: int = 0
    executor_run_time_ms: int = 0
    peak_execution_memory: int = 0


class SparkApplicationSnapshot(BaseModel):
    application_id: str
    name: str = ""
    duration_ms: int = 0
    stages: list[StageMetrics] = Field(default_factory=list)


class DataprocJobSnapshot(BaseModel):
    job_id: str
    state: str = ""
    placement_cluster_name: str = ""
    driver_output_uri: str = ""
    yarn_application_ids: list[str] = Field(default_factory=list)


class SparkPerformanceReport(BaseModel):
    run_key: str
    dag_id: str = ""
    task_id: str = ""
    airflow_run_id: str = ""
    dataproc_job_id: str = ""
    spark_application_id: str = ""
    project_id: str = ""
    region: str = ""
    cluster_name: str = ""
    findings: list[PerformanceFinding] = Field(default_factory=list)
    stages: list[StageMetrics] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
