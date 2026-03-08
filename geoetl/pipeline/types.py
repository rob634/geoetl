"""Pydantic models for pipeline orchestration."""

from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


class ItemStatus(str, Enum):
    SUCCESS = "success"
    SKIPPED = "skipped"
    FAILED = "failed"


class ItemResult(BaseModel):
    """Result of processing a single pipeline item."""

    key: str
    status: ItemStatus
    time_seconds: float = 0.0
    input_path: Optional[str] = None
    output_path: Optional[str] = None
    input_size_mb: Optional[float] = None
    output_size_mb: Optional[float] = None
    error: Optional[str] = None
    metadata: dict = Field(default_factory=dict)


class ProgressSnapshot(BaseModel):
    """Point-in-time snapshot of pipeline progress."""

    total: int
    processed: int
    successful: int
    skipped: int
    failed: int
    elapsed_seconds: float
    rate_per_second: float
    eta_seconds: Optional[float]
    cumulative_stats: dict[str, float] = Field(default_factory=dict)


class PipelineSummary(BaseModel):
    """Final summary of a completed pipeline run."""

    pipeline_name: str
    started_at: datetime
    finished_at: datetime
    total_time_seconds: float
    total: int
    successful: int
    skipped: int
    failed: int
    cumulative_stats: dict[str, float] = Field(default_factory=dict)
    results: list[ItemResult] = Field(default_factory=list)


class PipelineConfig(BaseModel):
    """Configuration for pipeline execution."""

    skip_existing: bool = False
    dry_run: bool = False
    checkpoint_path: Optional[Path] = None
    log_path: Optional[Path] = None
    status_interval: int = Field(default=50, ge=1)
