"""
HDF file metadata extractor for SPAwn.

This module provides functionality for extracting metadata from HDF files
(Hierarchical Data Format), commonly used for storing large scientific datasets.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from spawn.metadata import MetadataExtractor

logger = logging.getLogger(__name__)


class HDFMetadataExtractor(MetadataExtractor):
    """Extract metadata from HDF files (HDF4, HDF5)."""

    supported_extensions = [
        ".h5",
        ".hdf",
        ".hdf5",
        ".he5",
        ".h4",
        ".hdf4",
    ]
    supported_mime_types = [
        "application/x-hdf",
        "application/x-hdf5",
    ]

    def __init__(
        self, max_datasets_to_sample: int = 10, max_attrs_per_dataset: int = 20
    ):
        """
        Initialize the HDF metadata extractor.

        Args:
            max_datasets_to_sample: Maximum number of datasets to sample for metadata.
            max_attrs_per_dataset: Maximum number of attributes to extract per dataset.
        """
        self.max_datasets_to_sample = max_datasets_to_sample
        self.max_attrs_per_dataset = max_attrs_per_dataset

    def extract(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from an HDF file.

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of metadata.
        """
        # Get common file metadata
        metadata = self.add_common_metadata(file_path)

        try:
            # Try to import h5py, which is required for HDF5 file handling
            try:
                import h5py

                has_h5py = True
            except ImportError:
                has_h5py = False
                metadata["error"] = (
                    "h5py library not available for HDF5 file processing"
                )
                return metadata

            if not has_h5py:
                return metadata

            # Extract metadata using h5py
            with h5py.File(file_path, "r") as hdf_file:
                # Get basic file info
                metadata["format"] = "HDF5"
                metadata["root_groups"] = list(hdf_file.keys())

                # Extract file attributes
                file_attrs = {}
                for attr_name, attr_value in hdf_file.attrs.items():
                    # Convert numpy arrays to lists for JSON serialization
                    if hasattr(attr_value, "tolist"):
                        attr_value = attr_value.tolist()
                    file_attrs[attr_name] = str(attr_value)

                metadata["file_attributes"] = file_attrs

                # Extract dataset structure
                datasets_info = {}
                self._extract_group_info(hdf_file, datasets_info, "", 0)

                metadata["datasets"] = datasets_info

                # Count total datasets and estimate total size
                total_datasets = 0
                total_size_bytes = 0

                for path, info in datasets_info.items():
                    if info.get("type") == "dataset":
                        total_datasets += 1
                        total_size_bytes += info.get("size_bytes", 0)

                metadata["total_datasets"] = total_datasets
                metadata["total_size_bytes"] = total_size_bytes

        except Exception as e:
            logger.error(f"Error extracting HDF metadata from {file_path}: {e}")
            metadata["error"] = str(e)

        return metadata

    def _extract_group_info(self, group, info_dict, path_prefix, depth, max_depth=3):
        """
        Recursively extract information about groups and datasets in an HDF5 file.

        Args:
            group: h5py Group object
            info_dict: Dictionary to store the extracted information
            path_prefix: Current path prefix for nested items
            depth: Current recursion depth
            max_depth: Maximum recursion depth
        """
        # Avoid going too deep in the hierarchy
        if depth > max_depth:
            return

        # Limit the number of datasets we process
        datasets_processed = 0

        # Process all items in the group
        for name, item in group.items():
            # Create the full path for this item
            item_path = f"{path_prefix}/{name}" if path_prefix else name

            # Check if it's a dataset or a group
            if hasattr(item, "items"):  # It's a group
                # Add group info
                info_dict[item_path] = {
                    "type": "group",
                    "num_items": len(item),
                    "attributes": self._extract_attributes(item),
                }

                # Recursively process this group
                self._extract_group_info(
                    item, info_dict, item_path, depth + 1, max_depth
                )
            else:  # It's a dataset
                # Skip if we've processed enough datasets
                if datasets_processed >= self.max_datasets_to_sample:
                    continue

                datasets_processed += 1

                # Extract dataset info
                dataset_info = {
                    "type": "dataset",
                    "shape": str(item.shape),
                    "dtype": str(item.dtype),
                    "size_bytes": item.size * item.dtype.itemsize,
                    "attributes": self._extract_attributes(item),
                }

                # Add dataset statistics if it's numeric and not too large
                if item.dtype.kind in "iuf" and item.size < 1000:
                    try:
                        import numpy as np

                        data = item[()]
                        dataset_info["statistics"] = {
                            "min": float(np.min(data)),
                            "max": float(np.max(data)),
                            "mean": float(np.mean(data)),
                            "std": float(np.std(data)),
                        }
                    except Exception as e:
                        logger.debug(
                            f"Could not compute statistics for {item_path}: {e}"
                        )

                info_dict[item_path] = dataset_info

    def _extract_attributes(self, item):
        """
        Extract attributes from an HDF5 item (group or dataset).

        Args:
            item: h5py Group or Dataset object

        Returns:
            Dictionary of attributes
        """
        attributes = {}

        # Limit the number of attributes we extract
        attr_count = 0

        for attr_name, attr_value in item.attrs.items():
            if attr_count >= self.max_attrs_per_dataset:
                break

            attr_count += 1

            # Convert numpy arrays to lists for JSON serialization
            if hasattr(attr_value, "tolist"):
                try:
                    attr_value = attr_value.tolist()
                except:
                    attr_value = str(attr_value)

            # Convert bytes to strings
            if isinstance(attr_value, bytes):
                try:
                    attr_value = attr_value.decode("utf-8")
                except:
                    attr_value = str(attr_value)

            attributes[attr_name] = attr_value

        return attributes
