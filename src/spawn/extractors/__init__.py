"""
Metadata extractors for SPAwn.

This package contains metadata extractors for different file types.
"""

import logging
from typing import List, Type

from spawn.metadata import MetadataExtractor, register_extractor

logger = logging.getLogger(__name__)


def register_builtin_extractors() -> None:
    """Register all built-in metadata extractors."""
    # Import and register all extractors
    try:
        from spawn.extractors.text import TextMetadataExtractor
        register_extractor(TextMetadataExtractor)
    except ImportError:
        logger.debug("TextMetadataExtractor not available")

    try:
        from spawn.extractors.image import ImageMetadataExtractor
        register_extractor(ImageMetadataExtractor)
    except ImportError:
        logger.debug("ImageMetadataExtractor not available")

    try:
        from spawn.extractors.pdf import PDFMetadataExtractor
        register_extractor(PDFMetadataExtractor)
    except ImportError:
        logger.debug("PDFMetadataExtractor not available")

    try:
        from spawn.extractors.audio import AudioMetadataExtractor
        register_extractor(AudioMetadataExtractor)
    except ImportError:
        logger.debug("AudioMetadataExtractor not available")

    try:
        from spawn.extractors.video import VideoMetadataExtractor
        register_extractor(VideoMetadataExtractor)
    except ImportError:
        logger.debug("VideoMetadataExtractor not available")