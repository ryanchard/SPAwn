"""
Metadata extraction for SPAwn.

This module provides functionality for extracting metadata from files.
"""

import json
import logging
import mimetypes
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

from spawn.config import config

logger = logging.getLogger(__name__)


class MetadataExtractor(ABC):
    """Base class for metadata extractors."""

    # List of file extensions this extractor can handle
    supported_extensions: List[str] = []

    # List of MIME types this extractor can handle
    supported_mime_types: List[str] = []

    @staticmethod
    def add_common_metadata(file_path: Path) -> Dict[str, Any]:
        """
        Add common file metadata to the extraction results.

        This method adds standardized file metadata that should be
        consistent across all extractors.

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of common file metadata.
        """
        stat = file_path.stat()

        return {
            "file": {
                "filename": file_path.name,
                "directory": str(file_path.parent),
                "extension": file_path.suffix.lower(),
                "size_bytes": stat.st_size,
            }
        }

    @classmethod
    def can_handle(cls, file_path: Path) -> bool:
        """
        Check if this extractor can handle the given file.

        Args:
            file_path: Path to the file.

        Returns:
            True if this extractor can handle the file, False otherwise.
        """
        # Check file extension
        if (
            cls.supported_extensions
            and file_path.suffix.lower() in cls.supported_extensions
        ):
            return True

        # Check MIME type
        if cls.supported_mime_types:
            mime_type, _ = mimetypes.guess_type(str(file_path))
            if mime_type and any(
                mime_type.startswith(t) for t in cls.supported_mime_types
            ):
                return True

        return False

    @abstractmethod
    def extract(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from a file.

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of metadata.
        """
        pass


class BasicMetadataExtractor(MetadataExtractor):
    """Extract basic metadata from any file."""

    supported_extensions = []  # Handle all extensions
    supported_mime_types = []  # Handle all MIME types

    def extract(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract basic metadata from a file.

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of basic metadata.
        """
        # Get common file metadata
        metadata = MetadataExtractor.add_common_metadata(file_path)

        stat = file_path.stat()
        mime_type, encoding = mimetypes.guess_type(str(file_path))

        # Add additional metadata
        metadata.update(
            {
                "path": str(file_path),
                "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "accessed_at": datetime.fromtimestamp(stat.st_atime).isoformat(),
                "mime_type": mime_type or "application/octet-stream",
                "encoding": encoding,
            }
        )

        return metadata


# Registry of metadata extractors
_extractors: List[Type[MetadataExtractor]] = [BasicMetadataExtractor]


def register_extractor(extractor_class: Type[MetadataExtractor]) -> None:
    """
    Register a metadata extractor.

    Args:
        extractor_class: The extractor class to register.
    """
    if extractor_class not in _extractors:
        _extractors.append(extractor_class)
        logger.debug(f"Registered metadata extractor: {extractor_class.__name__}")


def get_extractors_for_file(file_path: Path) -> List[Type[MetadataExtractor]]:
    """
    Get all extractors that can handle the given file.

    Args:
        file_path: Path to the file.

    Returns:
        List of extractor classes that can handle the file.
    """
    return [ext for ext in _extractors if ext.can_handle(file_path)]


def extract_metadata(file_path: Path) -> Dict[str, Any]:
    """
    Extract metadata from a file using all available extractors.

    Args:
        file_path: Path to the file.

    Returns:
        Dictionary of metadata.
    """
    metadata = {}

    # Add common file metadata
    common_metadata = MetadataExtractor.add_common_metadata(file_path)
    metadata.update(common_metadata)

    # Get extractors for this file
    extractors = get_extractors_for_file(file_path)

    # Apply each extractor
    for extractor_class in extractors:
        try:
            extractor = extractor_class()
            extracted_data = extractor.extract(file_path)

            # If the extractor already added file metadata in a different format,
            # we want to preserve our standardized format
            if "file" in metadata and "file" in extracted_data:
                file_metadata = extracted_data.pop("file")
                # Merge any additional file metadata that doesn't conflict with our standard fields
                for key, value in file_metadata.items():
                    if key not in metadata["file"]:
                        metadata["file"][key] = value

            metadata.update(extracted_data)
        except Exception as e:
            logger.error(
                f"Error extracting metadata with {extractor_class.__name__}: {e}"
            )

    return metadata


def save_metadata_to_json(
    file_path_or_metadata: Union[Path, Dict[str, Any]],
    metadata: Optional[Dict[str, Any]] = None,
    output_dir: Optional[Path] = None,
) -> Path:
    """
    Save metadata to a JSON file.

    This function can be called in two ways:
    1. With a file path and its metadata: save_metadata_to_json(file_path, metadata, output_dir)
    2. With a dictionary of file paths to metadata: save_metadata_to_json(metadata_dict, output_dir=output_dir)

    Args:
        file_path_or_metadata: Either a Path object pointing to the file, or a dictionary mapping file paths to metadata.
        metadata: Dictionary of metadata to save (only used when file_path_or_metadata is a Path).
        output_dir: Directory to save the JSON file(s) in. If None, uses the config value.

    Returns:
        Path to the saved JSON file.
    """
    # Determine output directory
    if output_dir is None:
        output_dir = config.get("metadata", {}).get("json_dir")
        output_dir = Path(output_dir).expanduser().absolute()
    else:
        output_dir = Path(output_dir).expanduser().absolute()

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check if we're saving a single file's metadata or multiple files
    if isinstance(file_path_or_metadata, dict) and metadata is None:
        # We're saving multiple files' metadata
        metadata_dict = file_path_or_metadata
        json_filename = "SPAwn_metadata.json"
        json_path = output_dir / json_filename

        try:
            with open(json_path, "w") as f:
                json.dump(metadata_dict, f, indent=2, default=str)
            logger.debug(
                f"Saved metadata for {len(metadata_dict)} files to {json_path}"
            )
            return json_path
        except Exception as e:
            logger.error(f"Error saving metadata to {json_path}: {e}")
            raise
    else:
        # We're saving a single file's metadata
        file_path = file_path_or_metadata
        if not isinstance(file_path, Path):
            file_path = Path(file_path)

        # Create JSON filename based on original filename
        json_filename = f"{file_path.stem}_metadata.json"
        json_path = output_dir / json_filename

        try:
            with open(json_path, "w") as f:
                json.dump(metadata, f, indent=2, default=str)
            logger.debug(f"Saved metadata for {file_path} to {json_path}")
            return json_path
        except Exception as e:
            logger.error(f"Error saving metadata to {json_path}: {e}")
            raise


# Import and register additional extractors
try:
    from spawn.extractors import register_builtin_extractors

    register_builtin_extractors()
except ImportError:
    logger.debug("No additional extractors found")
