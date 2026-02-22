"""Cloud Optimized GeoTIFF creation via rio-cogeo."""

import logging
from pathlib import Path
from typing import Optional

from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from geoetl.config import COGConfig, COGQuality, RasterType
from geoetl.exceptions import COGCreationError
from geoetl.raster.validation import validate_raster

logger = logging.getLogger(__name__)

# Compression profiles keyed by (raster_type, quality)
_COMPRESSION_PROFILES: dict[tuple[RasterType, COGQuality], dict] = {
    # RGB
    (RasterType.RGB, COGQuality.VISUALIZATION): {
        "driver": "GTiff", "compress": "JPEG", "quality": 85,
    },
    (RasterType.RGB, COGQuality.ANALYSIS): {
        "driver": "GTiff", "compress": "DEFLATE", "predictor": "2",
    },
    (RasterType.RGB, COGQuality.ARCHIVE): {
        "driver": "GTiff", "compress": "LZW", "predictor": "2",
    },
    # RGBA -- WebP supports alpha
    (RasterType.RGBA, COGQuality.VISUALIZATION): {
        "driver": "GTiff", "compress": "WEBP", "quality": 85,
    },
    (RasterType.RGBA, COGQuality.ANALYSIS): {
        "driver": "GTiff", "compress": "DEFLATE", "predictor": "2",
    },
    (RasterType.RGBA, COGQuality.ARCHIVE): {
        "driver": "GTiff", "compress": "LZW", "predictor": "2",
    },
    # DEM
    (RasterType.DEM, COGQuality.VISUALIZATION): {
        "driver": "GTiff", "compress": "DEFLATE", "predictor": "2",
    },
    (RasterType.DEM, COGQuality.ANALYSIS): {
        "driver": "GTiff", "compress": "DEFLATE", "predictor": "2",
    },
    (RasterType.DEM, COGQuality.ARCHIVE): {
        "driver": "GTiff", "compress": "LERC_DEFLATE",
    },
    # Categorical
    (RasterType.CATEGORICAL, COGQuality.VISUALIZATION): {
        "driver": "GTiff", "compress": "DEFLATE",
    },
    (RasterType.CATEGORICAL, COGQuality.ANALYSIS): {
        "driver": "GTiff", "compress": "DEFLATE",
    },
    (RasterType.CATEGORICAL, COGQuality.ARCHIVE): {
        "driver": "GTiff", "compress": "LZW",
    },
    # Multispectral
    (RasterType.MULTISPECTRAL, COGQuality.VISUALIZATION): {
        "driver": "GTiff", "compress": "DEFLATE", "predictor": "2",
    },
    (RasterType.MULTISPECTRAL, COGQuality.ANALYSIS): {
        "driver": "GTiff", "compress": "DEFLATE", "predictor": "2",
    },
    (RasterType.MULTISPECTRAL, COGQuality.ARCHIVE): {
        "driver": "GTiff", "compress": "LERC_DEFLATE",
    },
    # Panchromatic
    (RasterType.PANCHROMATIC, COGQuality.VISUALIZATION): {
        "driver": "GTiff", "compress": "JPEG", "quality": 85,
    },
    (RasterType.PANCHROMATIC, COGQuality.ANALYSIS): {
        "driver": "GTiff", "compress": "DEFLATE", "predictor": "2",
    },
    (RasterType.PANCHROMATIC, COGQuality.ARCHIVE): {
        "driver": "GTiff", "compress": "LZW", "predictor": "2",
    },
}

# Resampling by raster type for overview generation
_OVERVIEW_RESAMPLING: dict[RasterType, str] = {
    RasterType.RGB: "bilinear",
    RasterType.RGBA: "bilinear",
    RasterType.DEM: "bilinear",
    RasterType.CATEGORICAL: "nearest",
    RasterType.MULTISPECTRAL: "bilinear",
    RasterType.PANCHROMATIC: "bilinear",
    RasterType.UNKNOWN: "nearest",
}


def _get_compression_profile(raster_type: RasterType, quality: COGQuality) -> dict:
    """Get the compression profile for a given raster type and quality."""
    key = (raster_type, quality)
    if key in _COMPRESSION_PROFILES:
        return _COMPRESSION_PROFILES[key].copy()
    # Fallback: DEFLATE
    return {"driver": "GTiff", "compress": "DEFLATE", "predictor": "2"}


def create_cog(
    input_path: Path,
    output_path: Path,
    quality: COGQuality = COGQuality.ANALYSIS,
    raster_type: Optional[RasterType] = None,
    config: Optional[COGConfig] = None,
) -> Path:
    """Create a Cloud Optimized GeoTIFF from an input raster.

    Args:
        input_path: Path to the source raster.
        output_path: Path for the output COG.
        quality: Compression quality tier.
        raster_type: Override auto-detected raster type.
        config: COG creation configuration.

    Returns:
        Path to the created COG.
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    config = config or COGConfig(quality=quality)

    if not input_path.exists():
        raise COGCreationError(f"Input file not found: {input_path}")

    # Auto-detect raster type if not provided
    if raster_type is None:
        info = validate_raster(input_path)
        raster_type = info.raster_type
        logger.info("Auto-detected raster type: %s", raster_type.value)

    compression = _get_compression_profile(raster_type, quality)
    resampling = _OVERVIEW_RESAMPLING.get(raster_type, "nearest")

    # Build the COG profile
    profile = cog_profiles.get("deflate")
    profile.update(compression)
    profile.update(blockxsize=config.blocksize, blockysize=config.blocksize)

    # Determine whether to process in memory
    file_size_mb = input_path.stat().st_size / (1024 * 1024)
    in_memory = file_size_mb < config.max_memory_mb

    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Creating COG: %s -> %s [%s, %s, %s]",
        input_path.name,
        output_path.name,
        raster_type.value,
        quality.value,
        compression.get("compress", "DEFLATE"),
    )

    try:
        cog_translate(
            str(input_path),
            str(output_path),
            profile,
            overview_level=config.overview_level,
            overview_resampling=resampling,
            config={"GDAL_TIFF_OVR_BLOCKSIZE": str(config.blocksize)},
            in_memory=in_memory,
            quiet=True,
        )
    except Exception as e:
        raise COGCreationError(f"COG creation failed for {input_path}: {e}") from e

    logger.info("COG created: %s (%.1f MB)", output_path.name, output_path.stat().st_size / (1024 * 1024))
    return output_path
