"""Crash-safe checkpoint manager for pipeline resume."""

import json
import logging
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Tracks completed item keys on disk for checkpoint/resume.

    Uses atomic writes (write to temp file, then rename) to prevent
    corruption if the process crashes mid-write.

    Args:
        path: Path to the checkpoint JSON file.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self._done: set[str] = set()

    def load(self) -> set[str]:
        """Load completed keys from disk. Returns the set of done keys."""
        if self.path.exists():
            data = json.loads(self.path.read_text())
            self._done = set(data)
            logger.info("Loaded checkpoint: %d items already done", len(self._done))
        else:
            self._done = set()
        return set(self._done)

    def is_done(self, key: str) -> bool:
        """Check if an item has already been processed."""
        return key in self._done

    def mark_done(self, key: str) -> None:
        """Mark an item as done and persist to disk atomically."""
        self._done.add(key)
        self._save()

    def _save(self) -> None:
        """Write checkpoint to disk via atomic rename."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            dir=self.path.parent,
            suffix=".tmp",
        )
        try:
            with open(fd, "w") as f:
                json.dump(sorted(self._done), f)
            Path(tmp_path).replace(self.path)
        except BaseException:
            Path(tmp_path).unlink(missing_ok=True)
            raise
