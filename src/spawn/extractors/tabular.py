"""
Tabular data metadata extractor for SPAwn.

This module provides functionality for extracting metadata from tabular data files
such as CSV, Excel, and other structured data formats.
"""

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from spawn.metadata import MetadataExtractor

logger = logging.getLogger(__name__)


class TabularMetadataExtractor(MetadataExtractor):
    """Extract metadata from tabular data files."""

    supported_extensions = [
        ".csv",
        ".tsv",
        ".xlsx",
        ".xls",
        ".ods",
        ".json",
        ".xml",
    ]
    supported_mime_types = [
        "text/csv",
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.oasis.opendocument.spreadsheet",
    ]

    def __init__(self, max_rows_to_sample: int = 1000):
        """
        Initialize the tabular data metadata extractor.

        Args:
            max_rows_to_sample: Maximum number of rows to sample for metadata extraction.
        """
        self.max_rows_to_sample = max_rows_to_sample

    def extract(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from a tabular data file.

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of metadata.
        """
        metadata = {}

        try:
            # Handle different file types
            if file_path.suffix.lower() in [".csv", ".tsv"]:
                metadata.update(self._extract_from_delimited(file_path))
            elif file_path.suffix.lower() in [".xlsx", ".xls", ".ods"]:
                metadata.update(self._extract_from_spreadsheet(file_path))
            elif file_path.suffix.lower() == ".json":
                metadata.update(self._extract_from_json(file_path))
            elif file_path.suffix.lower() == ".xml":
                metadata.update(self._extract_from_xml(file_path))

        except Exception as e:
            logger.error(f"Error extracting tabular metadata from {file_path}: {e}")

        return metadata

    def _extract_from_delimited(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from a delimited text file (CSV, TSV).

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of metadata.
        """
        metadata = {}

        try:
            # Determine delimiter based on file extension
            delimiter = "," if file_path.suffix.lower() == ".csv" else "\t"

            # Read a sample of the file
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                # Read header
                header = f.readline().strip()
                columns = header.split(delimiter)

                # Sample rows
                sample_rows = []
                for _ in range(
                    min(self.max_rows_to_sample, 10)
                ):  # Just read a few rows for basic info
                    line = f.readline()
                    if not line:
                        break
                    sample_rows.append(line.strip().split(delimiter))

                # Count total rows (approximate for large files)
                f.seek(0)
                row_count = sum(1 for _ in f)

            # Extract metadata
            metadata["column_count"] = len(columns)
            metadata["row_count"] = row_count
            metadata["columns"] = columns

            # Detect column types based on sample
            metadata["column_types"] = self._detect_column_types(columns, sample_rows)

            # Calculate basic statistics
            if sample_rows:
                metadata["sample_statistics"] = self._calculate_statistics(
                    columns, sample_rows
                )

        except Exception as e:
            logger.error(f"Error extracting from delimited file {file_path}: {e}")

        return metadata

    def _extract_from_spreadsheet(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from a spreadsheet file (Excel, ODS).

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of metadata.
        """
        metadata = {}

        try:
            # For actual implementation, you would use libraries like:
            # - openpyxl for .xlsx
            # - xlrd for .xls
            # - pyexcel for .ods

            # This is a placeholder that returns basic file info
            metadata["format"] = file_path.suffix.lower()[1:]  # Remove the dot
            metadata["note"] = (
                "Full spreadsheet metadata extraction requires additional libraries"
            )
            metadata["sheets"] = ["Sheet metadata would be listed here"]

        except Exception as e:
            logger.error(f"Error extracting from spreadsheet {file_path}: {e}")

        return metadata

    def _extract_from_json(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from a JSON file that contains tabular data.

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of metadata.
        """
        metadata = {}

        try:
            import json

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Check if it's an array of objects (tabular format)
            if isinstance(data, list) and data and isinstance(data[0], dict):
                # Extract column names from the first object
                columns = list(data[0].keys())

                metadata["format"] = "json"
                metadata["row_count"] = len(data)
                metadata["column_count"] = len(columns)
                metadata["columns"] = columns

                # Sample the data
                sample = data[: min(self.max_rows_to_sample, len(data))]

                # Convert to rows for type detection
                sample_rows = []
                for item in sample:
                    row = [item.get(col, None) for col in columns]
                    sample_rows.append(row)

                # Detect column types
                metadata["column_types"] = self._detect_column_types(
                    columns, sample_rows
                )

                # Calculate statistics
                metadata["sample_statistics"] = self._calculate_statistics(
                    columns, sample_rows
                )
            else:
                metadata["format"] = "json"
                metadata["structure"] = "non-tabular"
                metadata["note"] = "JSON file does not contain tabular data"

        except Exception as e:
            logger.error(f"Error extracting from JSON {file_path}: {e}")

        return metadata

    def _extract_from_xml(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from an XML file that might contain tabular data.

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of metadata.
        """
        metadata = {}

        try:
            # For actual implementation, you would use libraries like:
            # - xml.etree.ElementTree or lxml

            metadata["format"] = "xml"
            metadata["note"] = "XML metadata extraction requires additional processing"

        except Exception as e:
            logger.error(f"Error extracting from XML {file_path}: {e}")

        return metadata

    def _detect_column_types(
        self, columns: List[str], sample_rows: List[List[str]]
    ) -> Dict[str, str]:
        """
        Detect the data types of columns based on sample data.

        Args:
            columns: List of column names.
            sample_rows: Sample of data rows.

        Returns:
            Dictionary mapping column names to detected types.
        """
        column_types = {}

        # No samples, can't detect types
        if not sample_rows:
            return {col: "unknown" for col in columns}

        for i, col in enumerate(columns):
            # Get all values for this column from the sample
            values = [row[i] if i < len(row) else None for row in sample_rows]

            # Detect type
            column_types[col] = self._detect_value_type(values)

        return column_types

    def _detect_value_type(self, values: List[Any]) -> str:
        """
        Detect the data type of a list of values.

        Args:
            values: List of values.

        Returns:
            Detected data type as a string.
        """
        # Filter out None and empty values
        non_empty = [v for v in values if v is not None and v != ""]

        if not non_empty:
            return "empty"

        # Check if all values are numeric
        numeric_count = 0
        for v in non_empty:
            try:
                float(v)
                numeric_count += 1
            except (ValueError, TypeError):
                pass

        if numeric_count == len(non_empty):
            # Check if all are integers
            try:
                if all(float(v) == int(float(v)) for v in non_empty):
                    return "integer"
                return "float"
            except (ValueError, TypeError):
                return "float"

        # Check for dates
        date_patterns = [
            r"\d{4}-\d{2}-\d{2}",  # ISO format
            r"\d{2}/\d{2}/\d{4}",  # MM/DD/YYYY
            r"\d{2}-\d{2}-\d{4}",  # MM-DD-YYYY
        ]

        date_count = 0
        for v in non_empty:
            if isinstance(v, str) and any(
                re.match(pattern, v) for pattern in date_patterns
            ):
                date_count += 1

        if date_count == len(non_empty):
            return "date"

        # Check for boolean values
        bool_values = ["true", "false", "yes", "no", "1", "0", "t", "f", "y", "n"]
        if all(str(v).lower() in bool_values for v in non_empty):
            return "boolean"

        # Default to string
        return "string"

    def _calculate_statistics(
        self, columns: List[str], sample_rows: List[List[str]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Calculate basic statistics for numeric columns.

        Args:
            columns: List of column names.
            sample_rows: Sample of data rows.

        Returns:
            Dictionary of statistics for each column.
        """
        stats = {}

        for i, col in enumerate(columns):
            # Get all values for this column from the sample
            values = [row[i] if i < len(row) else None for row in sample_rows]

            col_stats = {
                "count": len(values),
                "null_count": values.count(None) + values.count(""),
            }

            # Calculate numeric statistics if possible
            numeric_values = []
            for v in values:
                try:
                    if v is not None and v != "":
                        numeric_values.append(float(v))
                except (ValueError, TypeError):
                    pass

            if numeric_values:
                col_stats["min"] = min(numeric_values)
                col_stats["max"] = max(numeric_values)
                col_stats["mean"] = sum(numeric_values) / len(numeric_values)

                # Calculate median
                sorted_values = sorted(numeric_values)
                mid = len(sorted_values) // 2
                if len(sorted_values) % 2 == 0:
                    col_stats["median"] = (
                        sorted_values[mid - 1] + sorted_values[mid]
                    ) / 2
                else:
                    col_stats["median"] = sorted_values[mid]

            stats[col] = col_stats

        return stats
