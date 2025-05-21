"""
Metadata extractors for SPAwn.

This package contains metadata extractors for different file types.
"""

import logging

from spawn.extractors.metadata import register_extractor

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
        from spawn.extractors.tabular import TabularMetadataExtractor

        register_extractor(TabularMetadataExtractor)
    except ImportError:
        logger.debug("TabularMetadataExtractor not available")

    try:
        from spawn.extractors.hdf import HDFMetadataExtractor

        register_extractor(HDFMetadataExtractor)
    except ImportError:
        logger.debug("HDFMetadataExtractor not available")

    try:
        from spawn.extractors.pdf import PDFMetadataExtractor

        register_extractor(PDFMetadataExtractor)
    except ImportError:
        logger.debug("PDFMetadataExtractor not available")

    try:
        from spawn.extractors.python import PythonMetadataExtractor

        register_extractor(PythonMetadataExtractor)
    except ImportError:
        logger.debug("PythonMetadataExtractor not available")

    # try:
    #     from spawn.extractors.audio import AudioMetadataExtractor
    #     register_extractor(AudioMetadataExtractor)
    # except ImportError:
    #     logger.debug("AudioMetadataExtractor not available")

    # try:
    #     from spawn.extractors.video import VideoMetadataExtractor
    #     register_extractor(VideoMetadataExtractor)
    # except ImportError:
    #     logger.debug("VideoMetadataExtractor not available")
