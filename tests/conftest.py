"""Shared test fixtures."""

import tempfile
from pathlib import Path

import pytest

from geoetl.config import GeoETLConfig


@pytest.fixture
def tmp_geodata(tmp_path):
    """Provide a temporary geodata directory."""
    return tmp_path / "geodata"


@pytest.fixture
def config(tmp_geodata):
    """Provide a GeoETLConfig pointing to a temp directory."""
    return GeoETLConfig(base_dir=tmp_geodata)
