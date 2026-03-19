"""
model_manager.py - Singleton class to manage ML model loading and caching
Loads models once at startup and reuses across requests for performance
"""

import joblib
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class ModelManager:
    """
    Singleton pattern to load and cache ML models
    Ensures models are loaded only once and reused across all requests
    """

    _instance = None
    _ml_model = None
    _outlier_model = None
    _feature_names = None
    _models_loaded = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ModelManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    def initialize(cls, model_dir: str = "models"):
        """
        Load models from disk at application startup

        Args:
            model_dir: Directory containing the model files
        """
        if cls._models_loaded:
            logger.info("Models already loaded, skipping re-initialization")
            return

        model_path = Path(model_dir)

        try:
            # Load ML classifier
            classifier_file = model_path / "document_classifier.pkl"
            if not classifier_file.exists():
                raise FileNotFoundError(f"Classifier not found: {classifier_file}")

            cls._ml_model = joblib.load(classifier_file)
            logger.info(f"✓ Loaded ML classifier from {classifier_file}")

            # Load outlier detector
            outlier_file = model_path / "outlier_detector.pkl"
            if not outlier_file.exists():
                raise FileNotFoundError(f"Outlier detector not found: {outlier_file}")

            cls._outlier_model = joblib.load(outlier_file)
            logger.info(f"✓ Loaded outlier detector from {outlier_file}")

            # Load feature names
            feature_names_file = model_path / "feature_names.pkl"
            if feature_names_file.exists():
                cls._feature_names = joblib.load(feature_names_file)
                logger.info(
                    f"✓ Loaded feature names: {cls._feature_names}"
                )
            else:
                # Fallback to default feature names
                cls._feature_names = [
                    "aspect_ratio",
                    "content_density",
                    "edge_density",
                ]
                logger.warning(
                    "Feature names file not found, using defaults: "
                    f"{cls._feature_names}"
                )

            cls._models_loaded = True
            logger.info("✓ All models loaded successfully")

            # Initialize celebrity detection (optional - graceful failure)
            try:
                from celebrity_detection import CelebrityDetector
                if CelebrityDetector.initialize():
                    logger.info(f"✓ Celebrity detection initialized "
                               f"({CelebrityDetector.get_celebrity_count()} celebrities)")
                else:
                    logger.warning("⚠ Celebrity detection not available (no reference images)")
            except ImportError as e:
                logger.warning(f"⚠ Celebrity detection module not available: {e}")
            except Exception as e:
                logger.warning(f"⚠ Celebrity detection initialization failed: {e}")

        except Exception as e:
            logger.error(f"✗ Failed to load models: {str(e)}")
            raise

    @classmethod
    def get_ml_model(cls):
        """Get the ML classifier model"""
        if not cls._models_loaded:
            raise RuntimeError("Models not initialized. Call initialize() first.")
        return cls._ml_model

    @classmethod
    def get_outlier_model(cls):
        """Get the outlier detector model"""
        if not cls._models_loaded:
            raise RuntimeError("Models not initialized. Call initialize() first.")
        return cls._outlier_model

    @classmethod
    def get_feature_names(cls):
        """Get feature names list"""
        if not cls._models_loaded:
            raise RuntimeError("Models not initialized. Call initialize() first.")
        return cls._feature_names

    @classmethod
    def is_ready(cls) -> bool:
        """Check if models are loaded and ready"""
        return cls._models_loaded

    @classmethod
    def is_celebrity_detection_ready(cls) -> bool:
        """Check if celebrity detection is available and loaded"""
        try:
            from celebrity_detection import CelebrityDetector
            return CelebrityDetector.is_available()
        except ImportError:
            return False
