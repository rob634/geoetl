"""Bronze/Silver/Gold local filesystem storage manager."""

import hashlib
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from geoetl.config import GeoETLConfig, StorageTier
from geoetl.exceptions import StorageError

logger = logging.getLogger(__name__)


class FileRecord(BaseModel):
    filename: str
    dataset: str
    tier: StorageTier
    path: str
    ingested_at: str
    processed_at: Optional[str] = None
    source_hash: Optional[str] = None
    metadata: dict = {}


class StorageManager:
    """Manages Bronze/Silver/Gold local filesystem tiers."""

    def __init__(self, config: Optional[GeoETLConfig] = None):
        self.config = config or GeoETLConfig()
        self._ensure_dirs()

    def _ensure_dirs(self):
        for tier in StorageTier:
            self.tier_path(tier).mkdir(parents=True, exist_ok=True)

    def tier_path(self, tier: StorageTier) -> Path:
        mapping = {
            StorageTier.BRONZE: self.config.bronze_dir,
            StorageTier.SILVER: self.config.silver_dir,
            StorageTier.GOLD: self.config.gold_dir,
        }
        return mapping[tier]

    def dataset_path(self, tier: StorageTier, dataset_name: str) -> Path:
        path = self.tier_path(tier) / dataset_name
        path.mkdir(parents=True, exist_ok=True)
        return path

    def ingest(self, source: Path, dataset_name: str) -> Path:
        """Copy a file into the Bronze tier."""
        if not source.exists():
            raise StorageError(f"Source file not found: {source}")

        dest_dir = self.dataset_path(StorageTier.BRONZE, dataset_name)
        dest = dest_dir / source.name

        shutil.copy2(source, dest)
        logger.info("Ingested %s -> %s", source.name, dest)
        return dest

    def promote(
        self,
        source: Path,
        from_tier: StorageTier,
        to_tier: StorageTier,
        dataset_name: str,
    ) -> Path:
        """Copy a file from one tier to another."""
        if not source.exists():
            raise StorageError(f"Source file not found: {source}")
        if from_tier == to_tier:
            raise StorageError(f"Cannot promote within the same tier: {from_tier.value}")

        dest_dir = self.dataset_path(to_tier, dataset_name)
        dest = dest_dir / source.name

        shutil.copy2(source, dest)
        logger.info(
            "Promoted %s: %s -> %s",
            source.name,
            from_tier.value,
            to_tier.value,
        )
        return dest

    def list_files(
        self,
        tier: StorageTier,
        dataset_name: str = "",
        pattern: str = "*",
    ) -> list[Path]:
        """List files in a tier, optionally filtered by dataset and glob pattern."""
        if dataset_name:
            search_dir = self.tier_path(tier) / dataset_name
        else:
            search_dir = self.tier_path(tier)

        if not search_dir.exists():
            return []
        return sorted(search_dir.rglob(pattern))

    def file_hash(self, path: Path) -> str:
        """Compute SHA-256 hash of a file."""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    def register(self, path: Path, dataset_name: str, tier: StorageTier) -> FileRecord:
        """Create a FileRecord for a stored file."""
        return FileRecord(
            filename=path.name,
            dataset=dataset_name,
            tier=tier,
            path=str(path),
            ingested_at=datetime.now(timezone.utc).isoformat(),
            source_hash=self.file_hash(path),
        )
