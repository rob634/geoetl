"""Tile scheme generation and window-based tile extraction."""

import logging
from pathlib import Path

import rasterio
from rasterio.windows import Window

from geoetl.config import TilingConfig
from geoetl.raster.types import TileSpec

logger = logging.getLogger(__name__)


def calculate_tile_grid(
    raster_path: Path,
    config: TilingConfig | None = None,
) -> list[TileSpec]:
    """Generate a grid of TileSpecs covering the entire raster.

    Args:
        raster_path: Path to the source raster.
        config: Tiling configuration (tile_size, overlap).

    Returns:
        List of TileSpec objects defining the tile grid.
    """
    config = config or TilingConfig()
    tile_size = config.tile_size
    overlap = config.overlap
    step = tile_size - overlap

    specs: list[TileSpec] = []

    with rasterio.open(raster_path) as src:
        row = 0
        row_idx = 0
        while row < src.height:
            col = 0
            col_idx = 0
            h = min(tile_size, src.height - row)
            while col < src.width:
                w = min(tile_size, src.width - col)
                window = Window(col, row, w, h)
                bounds = rasterio.windows.bounds(window, src.transform)

                specs.append(TileSpec(
                    col_off=col,
                    row_off=row,
                    width=w,
                    height=h,
                    tile_id=f"tile_r{row_idx}_c{col_idx}",
                    bounds=(bounds[0], bounds[1], bounds[2], bounds[3]),
                ))
                col += step
                col_idx += 1
            row += step
            row_idx += 1

    logger.info(
        "Generated %d tiles (%dx%d, overlap=%d) for %s",
        len(specs), tile_size, tile_size, overlap, raster_path.name,
    )
    return specs


def calculate_optimal_tile_size(
    raster_path: Path,
    target_memory_mb: float = 512.0,
) -> int:
    """Calculate tile size that fits within a target memory budget.

    Args:
        raster_path: Path to the source raster.
        target_memory_mb: Maximum memory per tile in MB.

    Returns:
        Optimal tile size in pixels (clamped to 256-4096).
    """
    with rasterio.open(raster_path) as src:
        dtype_bytes = {"uint8": 1, "int8": 1, "uint16": 2, "int16": 2,
                       "uint32": 4, "int32": 4, "float32": 4, "float64": 8}
        bpp = dtype_bytes.get(str(src.dtypes[0]), 8)
        bytes_per_pixel = bpp * src.count
        target_bytes = target_memory_mb * 1024 * 1024
        # tile_size^2 * bytes_per_pixel * 3 (safety factor) <= target_bytes
        max_pixels = target_bytes / (bytes_per_pixel * 3)
        tile_size = int(max_pixels ** 0.5)
        # Clamp and round to nearest 256
        tile_size = max(256, min(4096, (tile_size // 256) * 256))
        return tile_size


def extract_tile(
    raster_path: Path,
    spec: TileSpec,
    output_dir: Path,
) -> Path:
    """Extract a single tile from a raster using windowed reading.

    Args:
        raster_path: Path to the source raster.
        spec: Tile specification.
        output_dir: Directory to write the tile file.

    Returns:
        Path to the extracted tile.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{spec.tile_id}.tif"

    window = Window(spec.col_off, spec.row_off, spec.width, spec.height)
    with rasterio.open(raster_path) as src:
        data = src.read(window=window)
        profile = src.profile.copy()
        profile.update(
            width=spec.width,
            height=spec.height,
            transform=src.window_transform(window),
        )
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(data)

    return output_path


def extract_all_tiles(
    raster_path: Path,
    tile_grid: list[TileSpec],
    output_dir: Path,
) -> list[Path]:
    """Extract all tiles from a raster sequentially.

    For parallel extraction, use batch.raster_ops.batch_extract_tiles().
    """
    paths = []
    for spec in tile_grid:
        paths.append(extract_tile(raster_path, spec, output_dir))
    return paths
