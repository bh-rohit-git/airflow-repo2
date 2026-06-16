"""Runtime Spark stage performance rules."""

from __future__ import annotations

import uuid
from typing import Iterable

from .models import HotspotCategory, PerformanceFinding, StageMetrics

_SHUFFLE_WRITE_WARN_BYTES = 256 * 1024 * 1024
_SPILL_WARN_BYTES = 1
_SKEW_RATIO_WARN = 3.0


def analyze_stage_metrics(
    stages: Iterable[StageMetrics],
    *,
    run_id: str,
) -> list[PerformanceFinding]:
    findings: list[PerformanceFinding] = []
    stage_list = list(stages)
    if not stage_list:
        findings.append(
            PerformanceFinding(
                finding_id=f"spark-{uuid.uuid4()}",
                run_id=run_id,
                category=HotspotCategory.PARTITIONING,
                severity="info",
                evidence={"stage_count": 0},
                recommendation=(
                    "No Spark stages were returned from the History Server. "
                    "Confirm spark.eventLog.enabled and spark.eventLog.dir are set on the cluster."
                ),
            )
        )
        return findings

    total_duration = sum(stage.duration_ms for stage in stage_list)
    for stage in sorted(stage_list, key=lambda s: s.duration_ms, reverse=True):
        if stage.disk_bytes_spilled > _SPILL_WARN_BYTES:
            findings.append(
                PerformanceFinding(
                    finding_id=f"spark-{uuid.uuid4()}",
                    run_id=run_id,
                    category=HotspotCategory.SPILL,
                    severity="critical",
                    evidence={
                        "stage_id": stage.stage_id,
                        "stage_name": stage.name,
                        "disk_bytes_spilled": stage.disk_bytes_spilled,
                    },
                    recommendation=(
                        f"Stage '{stage.name}' spilled {stage.disk_bytes_spilled} bytes to disk. "
                        "Increase executor memory or spark.memory.fraction, reduce wide shuffles, "
                        "or filter data earlier."
                    ),
                )
            )
        if stage.shuffle_write_bytes >= _SHUFFLE_WRITE_WARN_BYTES:
            findings.append(
                PerformanceFinding(
                    finding_id=f"spark-{uuid.uuid4()}",
                    run_id=run_id,
                    category=HotspotCategory.SHUFFLE,
                    severity="warning",
                    evidence={
                        "stage_id": stage.stage_id,
                        "stage_name": stage.name,
                        "shuffle_write_bytes": stage.shuffle_write_bytes,
                    },
                    recommendation=(
                        f"Stage '{stage.name}' wrote {stage.shuffle_write_bytes} shuffle bytes. "
                        "Consider repartitioning, bucketing, broadcast joins for small tables, "
                        "or pre-aggregation before joins."
                    ),
                )
            )
        if stage.num_tasks > 1 and stage.executor_run_time_ms > 0:
            avg_task_ms = stage.executor_run_time_ms / max(stage.num_tasks, 1)
            if stage.duration_ms > avg_task_ms * _SKEW_RATIO_WARN * stage.num_tasks * 0.5:
                findings.append(
                    PerformanceFinding(
                        finding_id=f"spark-{uuid.uuid4()}",
                        run_id=run_id,
                        category=HotspotCategory.SKEW,
                        severity="warning",
                        evidence={
                            "stage_id": stage.stage_id,
                            "stage_name": stage.name,
                            "num_tasks": stage.num_tasks,
                            "duration_ms": stage.duration_ms,
                        },
                        recommendation=(
                            f"Stage '{stage.name}' may have skewed tasks (long stage vs average task time). "
                            "Enable AQE (spark.sql.adaptive.enabled), salt skewed keys, or rebalance partitions."
                        ),
                    )
                )

    if total_duration > 0:
        slowest = max(stage_list, key=lambda s: s.duration_ms)
        if slowest.duration_ms / total_duration >= 0.5 and len(stage_list) > 1:
            findings.append(
                PerformanceFinding(
                    finding_id=f"spark-{uuid.uuid4()}",
                    run_id=run_id,
                    category=HotspotCategory.PARTITIONING,
                    severity="info",
                    evidence={
                        "stage_id": slowest.stage_id,
                        "stage_name": slowest.name,
                        "share_of_runtime": slowest.duration_ms / total_duration,
                    },
                    recommendation=(
                        f"Stage '{slowest.name}' dominates runtime. Profile its operators in the Spark UI "
                        "and check partition counts and join strategy."
                    ),
                )
            )
    return findings
