"""
YAML file metadata extractor for SPAwn.

This module provides functionality for extracting metadata from YAML files.
"""

import logging
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from spawn.metadata import MetadataExtractor

logger = logging.getLogger(__name__)


class YAMLMetadataExtractor(MetadataExtractor):
    """Extract metadata from YAML files."""

    supported_extensions = [".yaml", ".yml"]
    supported_mime_types = ["application/x-yaml", "text/yaml"]

    def __init__(self, max_content_length: int = 10000000):
        """
        Initialize the YAML metadata extractor.

        Args:
            max_content_length: Maximum number of bytes to read from the file.
        """
        self.max_content_length = max_content_length

    def extract(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from a YAML file.

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

            # Parse YAML
            yaml_data = yaml.safe_load(content)

            # Extract YAML structure metadata
            metadata["yaml_valid"] = True
            metadata["yaml_structure"] = self._analyze_structure(yaml_data)
            metadata["yaml_root_keys"] = self._get_root_keys(yaml_data)
            metadata["yaml_root_key_count"] = len(metadata["yaml_root_keys"])
            metadata["yaml_depth"] = self._calculate_depth(yaml_data)
            metadata["yaml_size"] = len(content)
            
            # Add a preview (truncated if necessary)
            preview = content[:1000] if len(content) > 1000 else content
            metadata["content_preview"] = preview

        except yaml.YAMLError as e:
            logger.error(f"Invalid YAML in {file_path}: {e}")
            metadata["yaml_valid"] = False
            metadata["yaml_error"] = str(e)
        except Exception as e:
            logger.error(f"Error extracting YAML metadata from {file_path}: {e}")

        return metadata

    def _analyze_structure(self, data: Any) -> Dict[str, Any]:
        """
        Analyze the structure of YAML data.

        Args:
            data: The YAML data.

        Returns:
            Dictionary describing the structure.
        """
        if isinstance(data, dict):
            return {
                "type": "mapping",
                "key_count": len(data),
                "sample_keys": list(data.keys())[:5] if data else [],
            }
        elif isinstance(data, list):
            return {
                "type": "sequence",
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
        Get sample types from a sequence.

        Args:
            array_data: The sequence data.

        Returns:
            List of type names.
        """
        types = set()
        for item in array_data[:5]:  # Sample first 5 items
            if isinstance(item, dict):
                types.add("mapping")
            elif isinstance(item, list):
                types.add("sequence")
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
        Get root keys from YAML data.

        Args:
            data: The YAML data.

        Returns:
            List of root keys.
        """
        if isinstance(data, dict):
            return list(data.keys())
        return []

    def _calculate_depth(self, data: Any, current_depth: int = 0) -> int:
        """
        Calculate the maximum depth of the YAML structure.

        Args:
            data: The YAML data.
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