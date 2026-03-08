"""Pipeline orchestrator: combines progress, checkpointing, and logging."""

import logging
import time
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import TypeVar

from geoetl.pipeline.checkpoint import CheckpointManager
from geoetl.pipeline.progress import ProgressMonitor
from geoetl.pipeline.types import (
    ItemResult,
    ItemStatus,
    PipelineConfig,
    PipelineSummary,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


def run_pipeline(
    name: str,
    items: Sequence[T],
    process_fn: Callable[[T], ItemResult],
    key_fn: Callable[[T], str],
    config: PipelineConfig | None = None,
    output_exists_fn: Callable[[T], bool] | None = None,
    cumulative_keys: list[str] | None = None,
) -> PipelineSummary:
    """Run a sequential processing pipeline with progress, checkpointing, and logging.

    Args:
        name: Pipeline name (used in logs and summary).
        items: Sequence of items to process.
        process_fn: Function that processes one item and returns an ItemResult.
        key_fn: Function that extracts a unique key from an item.
        config: Pipeline configuration (skip_existing, dry_run, checkpoint, logging).
        output_exists_fn: Optional function to check if output already exists for an item.
            Used when config.skip_existing is True.
        cumulative_keys: Metadata keys to sum across results for cumulative stats.

    Returns:
        PipelineSummary with counts, timing, and per-item results.
    """
    config = config or PipelineConfig()
    items = list(items)
    results: list[ItemResult] = []
    started_at = datetime.now(timezone.utc)

    # Set up checkpoint manager
    checkpoint: CheckpointManager | None = None
    if config.checkpoint_path:
        checkpoint = CheckpointManager(config.checkpoint_path)
        checkpoint.load()

    monitor = ProgressMonitor(
        total=len(items),
        desc=name,
        cumulative_keys=cumulative_keys or [],
        status_interval=config.status_interval,
    )

    logger.info(
        "Pipeline '%s' starting: %d items (skip_existing=%s, dry_run=%s)",
        name, len(items), config.skip_existing, config.dry_run,
    )

    try:
        for item in items:
            key = key_fn(item)
            t0 = time.monotonic()

            # Skip if already checkpointed
            if checkpoint and checkpoint.is_done(key):
                result = ItemResult(key=key, status=ItemStatus.SKIPPED, time_seconds=0.0)
                results.append(result)
                monitor.update(result)
                continue

            # Skip if output already exists
            if config.skip_existing and output_exists_fn and output_exists_fn(item):
                result = ItemResult(key=key, status=ItemStatus.SKIPPED, time_seconds=0.0)
                results.append(result)
                monitor.update(result)
                if checkpoint:
                    checkpoint.mark_done(key)
                continue

            # Dry run: skip actual processing
            if config.dry_run:
                result = ItemResult(key=key, status=ItemStatus.SKIPPED, time_seconds=0.0)
                results.append(result)
                monitor.update(result)
                continue

            # Process the item
            try:
                result = process_fn(item)
                result.time_seconds = time.monotonic() - t0
            except Exception as e:
                result = ItemResult(
                    key=key,
                    status=ItemStatus.FAILED,
                    time_seconds=time.monotonic() - t0,
                    error=str(e),
                )

            results.append(result)
            monitor.update(result)

            if checkpoint and result.status == ItemStatus.SUCCESS:
                checkpoint.mark_done(key)

    finally:
        monitor.close()

    finished_at = datetime.now(timezone.utc)
    snap = monitor.snapshot()

    summary = PipelineSummary(
        pipeline_name=name,
        started_at=started_at,
        finished_at=finished_at,
        total_time_seconds=snap.elapsed_seconds,
        total=snap.total,
        successful=snap.successful,
        skipped=snap.skipped,
        failed=snap.failed,
        cumulative_stats=snap.cumulative_stats,
        results=results,
    )

    # Write JSON log
    if config.log_path:
        _write_log(summary, config.log_path)

    logger.info(
        "Pipeline '%s' finished: %d ok, %d skipped, %d failed (%.1fs)",
        name, summary.successful, summary.skipped, summary.failed,
        summary.total_time_seconds,
    )

    return summary


def summarize_results(
    name: str,
    results: list[ItemResult],
    log_path: Path | None = None,
) -> PipelineSummary:
    """Build a PipelineSummary from a list of ItemResults (e.g. from parallel_map).

    Args:
        name: Pipeline name.
        results: List of ItemResult from processing.
        log_path: Optional path to write JSON log.

    Returns:
        PipelineSummary.
    """
    now = datetime.now(timezone.utc)
    total_time = sum(r.time_seconds for r in results)
    successful = sum(1 for r in results if r.status == ItemStatus.SUCCESS)
    skipped = sum(1 for r in results if r.status == ItemStatus.SKIPPED)
    failed = sum(1 for r in results if r.status == ItemStatus.FAILED)

    summary = PipelineSummary(
        pipeline_name=name,
        started_at=now,
        finished_at=now,
        total_time_seconds=total_time,
        total=len(results),
        successful=successful,
        skipped=skipped,
        failed=failed,
        results=results,
    )

    if log_path:
        _write_log(summary, log_path)

    return summary


def _write_log(summary: PipelineSummary, log_path: Path) -> None:
    """Write pipeline summary to a JSON log file."""
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(summary.model_dump_json(indent=2))
    logger.info("Pipeline log written: %s", log_path)
