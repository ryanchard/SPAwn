"""
JSON file metadata extractor for SPAwn.

This module provides functionality for extracting metadata from JSON files.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from spawn.metadata import MetadataExtractor

logger = logging.getLogger(__name__)


class JSONMetadataExtractor(MetadataExtractor):
    """Extract metadata from JSON files."""

    supported_extensions = [".json"]
    supported_mime_types = ["application/json"]

    def __init__(self, max_content_length: int = 10000000):
        """
        Initialize the JSON metadata extractor.

        Args:
            max_content_length: Maximum number of bytes to read from the file.
        """
        self.max_content_length = max_content_length

    def extract(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from a JSON file.

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of metadata.
        """
        # Get common file metadata
        metadata = self.add_common_metadata(file_path)

        try:
            # Read file content
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read(self.max_content_length)

            # Parse JSON
            json_data = json.loads(content)

            # Extract JSON structure metadata
            metadata["json_valid"] = True
            metadata["json_structure"] = self._analyze_structure(json_data)
            metadata["json_root_keys"] = self._get_root_keys(json_data)
            metadata["json_root_key_count"] = len(metadata["json_root_keys"])
            metadata["json_depth"] = self._calculate_depth(json_data)
            metadata["json_size"] = len(content)
            
            # Add a preview (truncated if necessary)
            preview = content[:1000] if len(content) > 1000 else content
            metadata["content_preview"] = preview

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {file_path}: {e}")
            metadata["json_valid"] = False
            metadata["json_error"] = str(e)
        except Exception as e:
            logger.error(f"Error extracting JSON metadata from {file_path}: {e}")

        return metadata

    def _analyze_structure(self, data: Any) -> Dict[str, Any]:
        """
        Analyze the structure of JSON data.

        Args:
            data: The JSON data.

        Returns:
            Dictionary describing the structure.
        """
        if isinstance(data, dict):
            return {
                "type": "object",
                "key_count": len(data),
                "sample_keys": list(data.keys())[:5] if data else [],
            }
        elif isinstance(data, list):
            return {
                "type": "array",
                "length": len(data),
                "sample_item_types": self._get_sample_types(data),
            }
        elif isinstance(data, str):
            return {"type": "string", "length": len(data)}
        elif isinstance(data, (int, float)):
            return {"type": "number"}
        elif isinstance(data, bool):
            return {"type": "boolean"}
        elif data is None:
            return {"type": "null"}
        else:
            return {"type": str(type(data).__name__)}

    def _get_sample_types(self, array_data: List[Any]) -> List[str]:
        """
        Get sample types from an array.

        Args:
            array_data: The array data.

        Returns:
            List of type names.
        """
        types = set()
        for item in array_data[:5]:  # Sample first 5 items
            if isinstance(item, dict):
                types.add("object")
            elif isinstance(item, list):
                types.add("array")
            elif isinstance(item, str):
                types.add("string")
            elif isinstance(item, (int, float)):
                types.add("number")
            elif isinstance(item, bool):
                types.add("boolean")
            elif item is None:
                types.add("null")
            else:
                types.add(str(type(item).__name__))
        return list(types)

    def _get_root_keys(self, data: Any) -> List[str]:
        """
        Get root keys from JSON data.

        Args:
            data: The JSON data.

        Returns:
            List of root keys.
        """
        if isinstance(data, dict):
            return list(data.keys())
        return []

    def _calculate_depth(self, data: Any, current_depth: int = 0) -> int:
        """
        Calculate the maximum depth of the JSON structure.

        Args:
            data: The JSON data.
            current_depth: The current depth.

        Returns:
            Maximum depth.
        """
        if isinstance(data, dict):
            if not data:
                return current_depth
            return max(
                self._calculate_depth(value, current_depth + 1) for value in data.values()
            )
        elif isinstance(data, list):
            if not data:
                return current_depth
            return max(
                self._calculate_depth(item, current_depth + 1) for item in data
            )
        else:
            return current_depth