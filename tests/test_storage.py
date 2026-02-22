"""Tests for geoetl.storage.manager."""

from pathlib import Path

import pytest

from geoetl.config import GeoETLConfig, StorageTier
from geoetl.exceptions import StorageError
from geoetl.storage.manager import StorageManager


@pytest.fixture
def storage(tmp_path):
    config = GeoETLConfig(base_dir=tmp_path / "geodata")
    return StorageManager(config)


@pytest.fixture
def sample_file(tmp_path):
    f = tmp_path / "test_data.csv"
    f.write_text("id,value\n1,hello\n2,world\n")
    return f


def test_init_creates_tier_dirs(storage):
    for tier in StorageTier:
        assert storage.tier_path(tier).exists()


def test_ingest(storage, sample_file):
    dest = storage.ingest(sample_file, "test_dataset")
    assert dest.exists()
    assert dest.parent.name == "test_dataset"
    assert dest.read_text() == sample_file.read_text()


def test_ingest_missing_file(storage, tmp_path):
    with pytest.raises(StorageError, match="not found"):
        storage.ingest(tmp_path / "nonexistent.csv", "test")


def test_promote(storage, sample_file):
    bronze_path = storage.ingest(sample_file, "ds1")
    silver_path = storage.promote(
        bronze_path, StorageTier.BRONZE, StorageTier.SILVER, "ds1"
    )
    assert silver_path.exists()
    assert "silver" in str(silver_path)
    assert silver_path.read_text() == sample_file.read_text()


def test_promote_same_tier(storage, sample_file):
    bronze_path = storage.ingest(sample_file, "ds1")
    with pytest.raises(StorageError, match="same tier"):
        storage.promote(bronze_path, StorageTier.BRONZE, StorageTier.BRONZE, "ds1")


def test_list_files(storage, sample_file):
    storage.ingest(sample_file, "ds1")
    files = storage.list_files(StorageTier.BRONZE, "ds1")
    assert len(files) == 1
    assert files[0].name == "test_data.csv"


def test_list_files_empty(storage):
    files = storage.list_files(StorageTier.GOLD, "nonexistent")
    assert files == []


def test_list_files_pattern(storage, sample_file, tmp_path):
    storage.ingest(sample_file, "ds1")
    tif = tmp_path / "raster.tif"
    tif.write_bytes(b"fake tif data")
    storage.ingest(tif, "ds1")

    csv_files = storage.list_files(StorageTier.BRONZE, "ds1", "*.csv")
    assert len(csv_files) == 1

    tif_files = storage.list_files(StorageTier.BRONZE, "ds1", "*.tif")
    assert len(tif_files) == 1


def test_file_hash(storage, sample_file):
    h = storage.file_hash(sample_file)
    assert isinstance(h, str)
    assert len(h) == 64  # SHA-256 hex digest


def test_register(storage, sample_file):
    dest = storage.ingest(sample_file, "ds1")
    record = storage.register(dest, "ds1", StorageTier.BRONZE)
    assert record.filename == "test_data.csv"
    assert record.dataset == "ds1"
    assert record.tier == StorageTier.BRONZE
    assert record.source_hash is not None
