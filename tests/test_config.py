"""Tests for geoetl.config."""

from pathlib import Path

from geoetl.config import (
    BatchConfig,
    COGConfig,
    COGQuality,
    GeoETLConfig,
    RasterType,
    StorageTier,
    TilingConfig,
)


def test_storage_tier_values():
    assert StorageTier.BRONZE.value == "bronze"
    assert StorageTier.SILVER.value == "silver"
    assert StorageTier.GOLD.value == "gold"


def test_raster_type_values():
    assert RasterType.RGB.value == "rgb"
    assert RasterType.DEM.value == "dem"
    assert RasterType.MULTISPECTRAL.value == "multispectral"


def test_cog_quality_values():
    assert COGQuality.VISUALIZATION.value == "visualization"
    assert COGQuality.ANALYSIS.value == "analysis"
    assert COGQuality.ARCHIVE.value == "archive"


def test_cog_config_defaults():
    cfg = COGConfig()
    assert cfg.quality == COGQuality.ANALYSIS
    assert cfg.blocksize == 512
    assert cfg.overview_level == 6
    assert cfg.max_memory_mb == 4096


def test_tiling_config_defaults():
    cfg = TilingConfig()
    assert cfg.tile_size == 1024
    assert cfg.overlap == 0
    assert cfg.output_format == "tif"


def test_batch_config_defaults():
    cfg = BatchConfig()
    assert cfg.max_workers == 4
    assert cfg.chunk_size == 1


def test_geoetl_config_defaults():
    cfg = GeoETLConfig()
    assert cfg.base_dir == Path.home() / "geodata"
    assert cfg.bronze_dir == Path.home() / "geodata" / "bronze"
    assert cfg.silver_dir == Path.home() / "geodata" / "silver"
    assert cfg.gold_dir == Path.home() / "geodata" / "gold"
    assert cfg.log_level == "INFO"


def test_geoetl_config_custom_base(tmp_path):
    cfg = GeoETLConfig(base_dir=tmp_path / "mydata")
    assert cfg.bronze_dir == tmp_path / "mydata" / "bronze"
    assert cfg.silver_dir == tmp_path / "mydata" / "silver"
    assert cfg.gold_dir == tmp_path / "mydata" / "gold"


def test_geoetl_config_custom_tier_dirs(tmp_path):
    cfg = GeoETLConfig(
        base_dir=tmp_path,
        bronze_dir=tmp_path / "raw",
        silver_dir=tmp_path / "clean",
        gold_dir=tmp_path / "prod",
    )
    assert cfg.bronze_dir == tmp_path / "raw"
    assert cfg.silver_dir == tmp_path / "clean"
    assert cfg.gold_dir == tmp_path / "prod"
