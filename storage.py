"""
storage.py - File storage abstraction layer
Supports local filesystem now, designed for easy migration to cloud storage (Azure Blob, S3).
"""

import os
import shutil
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_DIR = os.environ.get("DATA_DIR", "data")
UPLOADS_ROOT = os.path.join(DATA_DIR, "uploads")
PENDING_DIR = "_pending"


class LocalFileStorage:
    """Local filesystem storage for uploaded documents."""

    def __init__(self, root_dir=None):
        self.root = Path(root_dir or UPLOADS_ROOT)
        self.root.mkdir(parents=True, exist_ok=True)

    def save_pending(self, session_id, doc_type, source_path):
        """
        Save an uploaded file to pending storage.
        Returns the pending storage key (relative path).
        Overwrites any existing file for the same session_id + doc_type.
        """
        source = Path(source_path)
        ext = source.suffix.lower() or '.bin'
        dest_dir = self.root / PENDING_DIR / session_id
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Remove any existing file for this doc_type (handles re-uploads)
        for existing in dest_dir.glob(f"{doc_type}.*"):
            existing.unlink()

        dest_filename = f"{doc_type}{ext}"
        dest_path = dest_dir / dest_filename
        shutil.copy2(str(source), str(dest_path))

        key = f"{PENDING_DIR}/{session_id}/{dest_filename}"
        logger.info(f"Saved pending file: {key}")
        return key

    def finalize(self, session_id, course_slug, submission_id):
        """
        Move files from pending to permanent storage.
        Returns a dict of {doc_type: storage_key} for files that were moved.
        """
        pending_dir = self.root / PENDING_DIR / session_id
        if not pending_dir.exists():
            logger.warning(f"No pending directory for session {session_id}")
            return {}

        final_dir = self.root / course_slug / str(submission_id)
        final_dir.mkdir(parents=True, exist_ok=True)

        file_keys = {}
        for file_path in pending_dir.iterdir():
            if file_path.is_file():
                doc_type = file_path.stem  # e.g., "PHOTO", "ID", "LETTER"
                dest = final_dir / file_path.name
                shutil.move(str(file_path), str(dest))
                key = f"{course_slug}/{submission_id}/{file_path.name}"
                file_keys[doc_type] = key
                logger.info(f"Finalized file: {key}")

        # Clean up empty pending directory
        try:
            pending_dir.rmdir()
        except OSError:
            pass

        return file_keys

    def get_path(self, key):
        """Get the absolute filesystem path for a storage key."""
        if not key:
            return None
        full_path = self.root / key
        if full_path.exists():
            return str(full_path)
        return None

    def delete(self, key):
        """Delete a file by storage key."""
        if not key:
            return False
        full_path = self.root / key
        try:
            if full_path.exists():
                full_path.unlink()
                logger.info(f"Deleted file: {key}")
                return True
        except Exception as e:
            logger.error(f"Error deleting file {key}: {e}")
        return False

    def delete_submission_files(self, course_slug, submission_id):
        """Delete all files for a submission."""
        sub_dir = self.root / course_slug / str(submission_id)
        if sub_dir.exists():
            shutil.rmtree(str(sub_dir))
            logger.info(f"Deleted submission files: {course_slug}/{submission_id}")

    def cleanup_stale_pending(self, max_age_hours=24):
        """Remove pending directories older than max_age_hours."""
        pending_root = self.root / PENDING_DIR
        if not pending_root.exists():
            return

        cutoff = time.time() - (max_age_hours * 3600)
        removed = 0

        for session_dir in pending_root.iterdir():
            if session_dir.is_dir():
                try:
                    mtime = session_dir.stat().st_mtime
                    if mtime < cutoff:
                        shutil.rmtree(str(session_dir))
                        removed += 1
                except Exception as e:
                    logger.error(f"Error cleaning up {session_dir}: {e}")

        if removed:
            logger.info(f"Cleaned up {removed} stale pending upload(s)")


# Singleton instance
_storage = None


def get_storage():
    """Get the storage instance (creates on first call)."""
    global _storage
    if _storage is None:
        backend = os.environ.get("STORAGE_BACKEND", "local")
        if backend == "local":
            _storage = LocalFileStorage()
        else:
            raise ValueError(f"Unknown storage backend: {backend}. Supported: local")
    return _storage
