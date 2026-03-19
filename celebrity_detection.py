"""
celebrity_detection.py - Celebrity face detection module using InsightFace

This module detects if an uploaded photo matches a known celebrity face.
Uses InsightFace's ArcFace model for 512-dimensional face embeddings.

Usage:
    from celebrity_detection import CelebrityDetector

    # Initialize at startup (loads/computes embeddings)
    CelebrityDetector.initialize()

    # Check a photo for celebrity match
    result = CelebrityDetector.detect_celebrity(rgb_image_array)
    if result["detected"]:
        print(f"Celebrity detected: {result['celebrity_name']}")
"""

import os
import logging
import pickle
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import numpy as np
import cv2

logger = logging.getLogger(__name__)

# Configuration
CELEBRITY_REFERENCE_DIR = "celebrity_reference"
CELEBRITY_EMBEDDINGS_CACHE = "models/celebrity_embeddings.pkl"
FACE_SIMILARITY_THRESHOLD = 0.4  # Cosine similarity threshold (higher = stricter, 0.35-0.45 recommended)
MIN_REFERENCE_IMAGES = 3  # Minimum images needed per celebrity

# Try to import insightface
try:
    import insightface
    from insightface.app import FaceAnalysis
    _INSIGHTFACE_AVAILABLE = True
    logger.info("InsightFace library loaded successfully")
except ImportError:
    insightface = None
    FaceAnalysis = None
    _INSIGHTFACE_AVAILABLE = False
    logger.warning("InsightFace not available. Celebrity detection disabled. "
                   "Install with: pip install insightface onnxruntime")


def compute_cosine_similarity(embedding1: np.ndarray, embedding2: np.ndarray) -> float:
    """Compute cosine similarity between two embeddings."""
    dot_product = np.dot(embedding1, embedding2)
    norm1 = np.linalg.norm(embedding1)
    norm2 = np.linalg.norm(embedding2)
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return float(dot_product / (norm1 * norm2))


class CelebrityDetector:
    """
    Singleton class for celebrity face detection using InsightFace.
    Loads celebrity embeddings once at startup and reuses across requests.
    """

    _instance = None
    _embeddings: Dict[str, Dict] = {}  # {celebrity_id: {name, embeddings[]}}
    _face_analyzer = None
    _loaded = False
    _cache_version = "2.0"  # Updated for InsightFace

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CelebrityDetector, cls).__new__(cls)
        return cls._instance

    @classmethod
    def initialize(cls, reference_dir: str = None,
                   cache_path: str = None,
                   force_rebuild: bool = False) -> bool:
        """
        Load or compute celebrity face embeddings.

        Args:
            reference_dir: Path to celebrity reference images folder
            cache_path: Path to cached embeddings file
            force_rebuild: If True, recompute embeddings even if cache exists

        Returns:
            bool: True if initialization successful
        """
        if not _INSIGHTFACE_AVAILABLE:
            logger.warning("Cannot initialize: InsightFace library not available")
            return False

        if cls._loaded and not force_rebuild:
            logger.info("Celebrity detector already initialized")
            return True

        reference_dir = reference_dir or CELEBRITY_REFERENCE_DIR
        cache_path = cache_path or CELEBRITY_EMBEDDINGS_CACHE

        # Initialize InsightFace analyzer
        try:
            cls._face_analyzer = FaceAnalysis(
                name='buffalo_l',  # Use buffalo_l model (good balance of speed/accuracy)
                providers=['CPUExecutionProvider']  # Use CPU (GPU: CUDAExecutionProvider)
            )
            cls._face_analyzer.prepare(ctx_id=0, det_size=(640, 640))
            logger.info("InsightFace analyzer initialized")
        except Exception as e:
            logger.error(f"Failed to initialize InsightFace: {e}")
            return False

        # Try to load from cache first
        if not force_rebuild and cls._load_cached_embeddings(cache_path):
            # Check for new/removed celebrity folders and sync incrementally
            cls._sync_embeddings(reference_dir, cache_path)
            cls._loaded = True
            logger.info(f"Celebrity detection ready: {len(cls._embeddings)} celebrities")
            return True

        # Check if reference directory exists
        if not os.path.isdir(reference_dir):
            logger.warning(f"Celebrity reference directory not found: {reference_dir}")
            logger.info("Create the directory and add celebrity subfolders with images")
            return False

        # Compute embeddings from reference images
        cls._embeddings = cls._compute_embeddings(reference_dir)

        if not cls._embeddings:
            logger.warning("No celebrity embeddings computed. Check reference images.")
            return False

        # Save to cache
        cls._save_embeddings_cache(cache_path)
        cls._loaded = True

        logger.info(f"Celebrity detection initialized: {len(cls._embeddings)} celebrities")
        return True

    @classmethod
    def _load_cached_embeddings(cls, cache_path: str) -> bool:
        """Load embeddings from cache file."""
        try:
            if not os.path.exists(cache_path):
                logger.info("No embeddings cache found")
                return False

            with open(cache_path, 'rb') as f:
                cache_data = pickle.load(f)

            # Validate cache structure
            if cache_data.get('version') != cls._cache_version:
                logger.info("Cache version mismatch, will rebuild")
                return False

            cls._embeddings = cache_data.get('celebrities', {})

            if not cls._embeddings:
                logger.info("Cache is empty, will rebuild")
                return False

            logger.info(f"Loaded embeddings cache: {len(cls._embeddings)} celebrities")
            return True

        except Exception as e:
            logger.warning(f"Failed to load embeddings cache: {e}")
            return False

    @classmethod
    def _compute_single_celebrity(cls, celeb_folder: Path) -> Optional[Dict]:
        """
        Compute face embeddings for a single celebrity folder.

        Args:
            celeb_folder: Path to celebrity folder containing images

        Returns:
            Dict with celebrity data or None if insufficient images
        """
        folder_name = celeb_folder.name

        # Extract celebrity name from folder (e.g., "c001_salman_khan" -> "Salman Khan")
        parts = folder_name.split('_', 1)
        if len(parts) == 2:
            celeb_id = parts[0]
            celeb_name = parts[1].replace('_', ' ').title()
        else:
            celeb_id = folder_name
            celeb_name = folder_name.replace('_', ' ').title()

        # Load all images and compute embeddings
        celeb_embeddings = []
        image_count = 0

        for img_file in celeb_folder.iterdir():
            if img_file.suffix.lower() not in ['.jpg', '.jpeg', '.png', '.bmp', '.webp']:
                continue

            try:
                # Load image
                img = cv2.imread(str(img_file))
                if img is None:
                    logger.debug(f"Could not load {img_file.name}")
                    continue

                # Convert BGR to RGB
                img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

                # Detect faces and get embeddings
                faces = cls._face_analyzer.get(img_rgb)

                if not faces:
                    logger.debug(f"No face found in {img_file.name}")
                    continue

                # Use the largest face (by bounding box area)
                largest_face = max(faces, key=lambda f: (f.bbox[2] - f.bbox[0]) * (f.bbox[3] - f.bbox[1]))

                if largest_face.embedding is not None:
                    # Normalize embedding
                    embedding = largest_face.embedding / np.linalg.norm(largest_face.embedding)
                    celeb_embeddings.append(embedding)
                    image_count += 1

            except Exception as e:
                logger.warning(f"Error processing {img_file}: {e}")
                continue

        # Only return if we have enough embeddings
        if len(celeb_embeddings) >= MIN_REFERENCE_IMAGES:
            return {
                'name': celeb_name,
                'id': celeb_id,
                'image_count': image_count,
                'embeddings': celeb_embeddings
            }
        else:
            logger.warning(f"  - {celeb_name}: Only {len(celeb_embeddings)} images "
                          f"(need {MIN_REFERENCE_IMAGES}+)")
            return None

    @classmethod
    def _sync_embeddings(cls, reference_dir: str, cache_path: str) -> bool:
        """
        Sync embeddings with reference directory (incremental update).
        - Add embeddings for new celebrity folders
        - Remove embeddings for deleted folders
        - Preserve existing embeddings

        Returns:
            bool: True if changes were made, False if already in sync
        """
        reference_path = Path(reference_dir)

        if not reference_path.exists():
            return False

        # Get current folders in reference directory
        current_folders = {f.name for f in reference_path.iterdir() if f.is_dir()}

        # Get cached celebrity folders
        cached_folders = set(cls._embeddings.keys())

        # Find new and removed folders
        new_folders = current_folders - cached_folders
        removed_folders = cached_folders - current_folders

        if not new_folders and not removed_folders:
            logger.info("Celebrity database is up to date")
            return False  # No changes

        logger.info(f"Syncing celebrity database: +{len(new_folders)} new, -{len(removed_folders)} removed")

        # Remove deleted celebrities
        for folder in removed_folders:
            celeb_name = cls._embeddings[folder].get('name', folder)
            del cls._embeddings[folder]
            logger.info(f"  - Removed: {celeb_name}")

        # Add new celebrities
        for folder in new_folders:
            celeb_path = reference_path / folder
            celeb_data = cls._compute_single_celebrity(celeb_path)
            if celeb_data:
                cls._embeddings[folder] = celeb_data
                logger.info(f"  + Added: {celeb_data['name']} ({celeb_data['image_count']} images)")

        # Save updated cache
        cls._save_embeddings_cache(cache_path)
        logger.info(f"Celebrity database synced: {len(cls._embeddings)} total celebrities")
        return True  # Changes made

    @classmethod
    def _compute_embeddings(cls, reference_dir: str) -> Dict[str, Dict]:
        """
        Compute face embeddings for all celebrities in reference directory.

        Expected folder structure:
        celebrity_reference/
        ├── c001_salman_khan/
        │   ├── img_01.jpg
        │   └── ...
        ├── c002_shah_rukh_khan/
        │   └── ...
        """
        embeddings = {}
        reference_path = Path(reference_dir)

        logger.info(f"Computing embeddings from: {reference_path}")

        # Iterate through celebrity folders
        for celeb_folder in reference_path.iterdir():
            if not celeb_folder.is_dir():
                continue

            celeb_data = cls._compute_single_celebrity(celeb_folder)
            if celeb_data:
                embeddings[celeb_folder.name] = celeb_data
                logger.info(f"  + {celeb_data['name']}: {celeb_data['image_count']} images")

        return embeddings

    @classmethod
    def _save_embeddings_cache(cls, cache_path: str) -> bool:
        """Save computed embeddings to cache file."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)

            cache_data = {
                'version': cls._cache_version,
                'created_at': datetime.now().isoformat(),
                'threshold': FACE_SIMILARITY_THRESHOLD,
                'celebrity_count': len(cls._embeddings),
                'celebrities': cls._embeddings
            }

            with open(cache_path, 'wb') as f:
                pickle.dump(cache_data, f)

            logger.info(f"Saved embeddings cache to: {cache_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to save embeddings cache: {e}")
            return False

    @classmethod
    def detect_celebrity(cls, image_array: np.ndarray) -> Dict:
        """
        Check if image contains a celebrity face.

        Args:
            image_array: RGB image as numpy array

        Returns:
            Dict with:
                - detected: bool
                - celebrity_name: str or None
                - celebrity_id: str or None
                - confidence: float (0.0-1.0, higher = more confident)
                - similarity: float (cosine similarity)
                - message: str
        """
        result = {
            "detected": False,
            "celebrity_name": None,
            "celebrity_id": None,
            "confidence": 0.0,
            "similarity": 0.0,
            "message": ""
        }

        if not cls.is_available():
            result["message"] = "Celebrity detection unavailable"
            return result

        if len(cls._embeddings) == 0:
            result["message"] = "No celebrity database loaded"
            return result

        # Validate image array
        if image_array is None or len(image_array.shape) != 3:
            result["message"] = "Invalid image format"
            return result

        try:
            # Detect faces in uploaded image
            faces = cls._face_analyzer.get(image_array)

            if not faces:
                result["message"] = "No face detected in image"
                return result

            # Get the largest face
            largest_face = max(faces, key=lambda f: (f.bbox[2] - f.bbox[0]) * (f.bbox[3] - f.bbox[1]))

            if largest_face.embedding is None:
                result["message"] = "Could not encode face"
                return result

            # Normalize uploaded face embedding
            uploaded_embedding = largest_face.embedding / np.linalg.norm(largest_face.embedding)

            # Compare against all celebrity embeddings
            best_match = None
            best_similarity = -1.0

            for celeb_folder, celeb_data in cls._embeddings.items():
                celeb_embeddings = celeb_data['embeddings']

                # Compare with all reference images for this celebrity
                similarities = [
                    compute_cosine_similarity(uploaded_embedding, celeb_emb)
                    for celeb_emb in celeb_embeddings
                ]
                max_similarity = max(similarities)

                if max_similarity > best_similarity:
                    best_similarity = max_similarity
                    best_match = {
                        'folder': celeb_folder,
                        'id': celeb_data['id'],
                        'name': celeb_data['name'],
                        'similarity': max_similarity
                    }

            # Check if best match exceeds threshold
            if best_match and best_similarity > FACE_SIMILARITY_THRESHOLD:
                result["detected"] = True
                result["celebrity_name"] = best_match['name']
                result["celebrity_id"] = best_match['id']
                result["similarity"] = float(best_similarity)
                result["confidence"] = float(best_similarity)  # Use similarity as confidence
                result["message"] = f"Face matches to a known popular figure"

                logger.warning(f"Celebrity match: {best_match['name']} (similarity: {best_similarity:.3f})")
            else:
                result["message"] = "No celebrity match found"
                logger.debug(f"No celebrity match (best similarity: {best_similarity:.3f})")

            return result

        except Exception as e:
            logger.error(f"Celebrity detection error: {e}")
            result["message"] = f"Detection error: {str(e)}"
            return result

    @classmethod
    def is_available(cls) -> bool:
        """Check if celebrity detection is available and loaded."""
        return _INSIGHTFACE_AVAILABLE and cls._loaded and cls._face_analyzer is not None

    @classmethod
    def get_celebrity_count(cls) -> int:
        """Return number of celebrities in database."""
        return len(cls._embeddings)

    @classmethod
    def get_celebrity_list(cls) -> List[str]:
        """Return list of celebrity names in database."""
        return [data['name'] for data in cls._embeddings.values()]

    @classmethod
    def rebuild_cache(cls, reference_dir: str = None, cache_path: str = None) -> bool:
        """Force rebuild of celebrity embeddings cache."""
        return cls.initialize(reference_dir, cache_path, force_rebuild=True)
