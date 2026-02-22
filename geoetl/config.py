"""Configuration models and enums for geoetl."""

from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class StorageTier(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class RasterType(str, Enum):
    RGB = "rgb"
    RGBA = "rgba"
    DEM = "dem"
    CATEGORICAL = "categorical"
    MULTISPECTRAL = "multispectral"
    PANCHROMATIC = "panchromatic"
    UNKNOWN = "unknown"


class COGQuality(str, Enum):
    VISUALIZATION = "visualization"
    ANALYSIS = "analysis"
    ARCHIVE = "archive"


class COGConfig(BaseModel):
    quality: COGQuality = COGQuality.ANALYSIS
    blocksize: int = Field(default=512, ge=256, le=2048)
    overview_level: int = Field(default=6, ge=0, le=12)
    nodata: Optional[float] = None
    max_memory_mb: int = Field(default=4096)


class TilingConfig(BaseModel):
    tile_size: int = Field(default=1024, ge=256, le=4096)
    overlap: int = Field(default=0, ge=0)
    output_format: str = "tif"


class BatchConfig(BaseModel):
    max_workers: int = Field(default=4, ge=1, le=32)
    chunk_size: int = Field(default=1, ge=1)


class GeoETLConfig(BaseSettings):
    base_dir: Path = Field(default=Path.home() / "geodata")
    bronze_dir: Optional[Path] = None
    silver_dir: Optional[Path] = None
    gold_dir: Optional[Path] = None
    cog: COGConfig = COGConfig()
    tiling: TilingConfig = TilingConfig()
    batch: BatchConfig = BatchConfig()
    log_level: str = "INFO"

    model_config = {"env_prefix": "GEOETL_"}

    def model_post_init(self, __context):
        if self.bronze_dir is None:
            self.bronze_dir = self.base_dir / "bronze"
        if self.silver_dir is None:
            self.silver_dir = self.base_dir / "silver"
        if self.gold_dir is None:
            self.gold_dir = self.base_dir / "gold"
