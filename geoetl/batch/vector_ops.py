"""Parallel vector validation and conversion."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from geoetl.config import BatchConfig
from geoetl.batch.pool import parallel_map

logger = logging.getLogger(__name__)


@dataclass
class _VectorTask:
    input_path: Path
    output_dir: Path
    target_crs: str


def _validate_vector_worker(task: _VectorTask) -> Path:
    """Worker function for parallel vector validation."""
    from geoetl.vector.converters import load_vector
    from geoetl.vector.validation import validate_geometries

    gdf = load_vector(task.input_path)
    gdf, report = validate_geometries(gdf, target_crs=task.target_crs)

    output_path = task.output_dir / f"{task.input_path.stem}_validated.gpkg"
    gdf.to_file(output_path, driver="GPKG")

    logger.info(
        "Validated %s: %d -> %d rows",
        task.input_path.name, report.input_count, report.output_count,
    )
    return output_path


def batch_validate_vectors(
    input_paths: list[Path],
    output_dir: Path,
    target_crs: str = "EPSG:4326",
    config: Optional[BatchConfig] = None,
) -> list[Path]:
    """Validate and clean multiple vector files in parallel.

    Args:
        input_paths: List of input vector file paths.
        output_dir: Directory for validated output files.
        target_crs: Target CRS for reprojection.
        config: Batch processing configuration.

    Returns:
        List of paths to validated GeoPackage files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    tasks = [
        _VectorTask(input_path=p, output_dir=output_dir, target_crs=target_crs)
        for p in input_paths
    ]

    return parallel_map(_validate_vector_worker, tasks, config=config, desc="Validating vectors")
