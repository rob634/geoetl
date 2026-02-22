"""Pydantic models for raster data."""

from dataclasses import dataclass
from typing import Optional

from pydantic import BaseModel

from geoetl.config import RasterType


class CRSInfo(BaseModel):
    epsg: Optional[int]
    wkt: str
    is_geographic: bool
    is_projected: bool


class BandStats(BaseModel):
    band: int
    min: float
    max: float
    mean: float
    std: float
    nodata: Optional[float]
    null_percent: float


class RasterInfo(BaseModel):
    path: str
    raster_type: RasterType
    crs: CRSInfo
    width: int
    height: int
    band_count: int
    dtype: str
    bands: list[BandStats]
    memory_estimate_mb: float
    is_valid: bool
    errors: list[str] = []


@dataclass
class TileSpec:
    col_off: int
    row_off: int
    width: int
    height: int
    tile_id: str
    bounds: tuple  # (left, bottom, right, top) in CRS units
