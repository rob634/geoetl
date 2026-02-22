"""Spawn-context multiprocessing pool for GDAL/rasterio fork-safety."""

import logging
import multiprocessing as mp
from collections.abc import Callable, Iterable
from typing import TypeVar

from tqdm import tqdm

from geoetl.config import BatchConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


def _worker_init():
    """Initialize worker process -- forces GDAL to re-init in spawned process."""
    import rasterio  # noqa: F401

    logger.debug("Worker %s initialized", mp.current_process().name)


def parallel_map(
    func: Callable,
    items: Iterable,
    config: BatchConfig | None = None,
    desc: str = "Processing",
) -> list:
    """Apply func to each item, optionally in parallel using spawn context.

    Uses mp.get_context("spawn") which is critical for GDAL/rasterio
    fork-safety. Each worker re-initializes its own GDAL environment.

    Falls back to sequential execution when max_workers=1 (useful for debugging).

    Args:
        func: Function to apply to each item (must be picklable for parallel mode).
        items: Iterable of items to process.
        config: Batch configuration (max_workers, chunk_size).
        desc: Description for the progress bar.

    Returns:
        List of results in input order.
    """
    config = config or BatchConfig()
    items = list(items)

    if len(items) == 0:
        return []

    # Single-worker mode: no pool overhead, easier debugging
    if config.max_workers == 1:
        logger.info("Running %d items sequentially", len(items))
        return [func(item) for item in tqdm(items, desc=desc)]

    # Spawn context is critical: GDAL/rasterio are NOT fork-safe
    ctx = mp.get_context("spawn")
    logger.info(
        "Running %d items with %d workers (spawn context)",
        len(items), config.max_workers,
    )

    with ctx.Pool(processes=config.max_workers, initializer=_worker_init) as pool:
        results = list(tqdm(
            pool.imap(func, items, chunksize=config.chunk_size),
            total=len(items),
            desc=desc,
        ))

    return results
