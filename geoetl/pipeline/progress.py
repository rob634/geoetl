"""Pipeline progress monitoring with tqdm and periodic status logging."""

import logging
import time

from tqdm import tqdm

from geoetl.pipeline.types import ItemResult, ItemStatus, ProgressSnapshot

logger = logging.getLogger(__name__)


class ProgressMonitor:
    """Tracks pipeline progress, cumulative stats, and periodic status logging.

    Args:
        total: Total number of items to process.
        desc: Description for the progress bar.
        cumulative_keys: Metadata keys to sum across results (e.g. ["output_size_mb"]).
        status_interval: Log a status line every N items.
    """

    def __init__(
        self,
        total: int,
        desc: str = "Processing",
        cumulative_keys: list[str] | None = None,
        status_interval: int = 50,
    ):
        self.total = total
        self.cumulative_keys = cumulative_keys or []
        self.status_interval = status_interval

        self._successful = 0
        self._skipped = 0
        self._failed = 0
        self._cumulative: dict[str, float] = {k: 0.0 for k in self.cumulative_keys}
        self._start_time = time.monotonic()
        self._pbar = tqdm(total=total, desc=desc)

    @property
    def processed(self) -> int:
        return self._successful + self._skipped + self._failed

    def update(self, result: ItemResult) -> None:
        """Record a completed item and advance the progress bar."""
        if result.status == ItemStatus.SUCCESS:
            self._successful += 1
        elif result.status == ItemStatus.SKIPPED:
            self._skipped += 1
        else:
            self._failed += 1

        for key in self.cumulative_keys:
            if key in result.metadata:
                self._cumulative[key] += result.metadata[key]

        self._pbar.update(1)

        if self.processed % self.status_interval == 0:
            snap = self.snapshot()
            logger.info(
                "Progress: %d/%d (%.0f/s) | ok=%d skip=%d fail=%d | ETA %.0fs",
                snap.processed,
                snap.total,
                snap.rate_per_second,
                snap.successful,
                snap.skipped,
                snap.failed,
                snap.eta_seconds or 0,
            )

    def snapshot(self) -> ProgressSnapshot:
        """Return a point-in-time snapshot of progress."""
        elapsed = time.monotonic() - self._start_time
        rate = self.processed / elapsed if elapsed > 0 else 0.0
        remaining = self.total - self.processed
        eta = remaining / rate if rate > 0 else None

        return ProgressSnapshot(
            total=self.total,
            processed=self.processed,
            successful=self._successful,
            skipped=self._skipped,
            failed=self._failed,
            elapsed_seconds=elapsed,
            rate_per_second=rate,
            eta_seconds=eta,
            cumulative_stats=dict(self._cumulative),
        )

    def close(self) -> None:
        """Close the progress bar."""
        self._pbar.close()
