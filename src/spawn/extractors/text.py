"""
Text file metadata extractor for SPAwn.

This module provides functionality for extracting metadata from text files.
"""

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from spawn.metadata import MetadataExtractor

logger = logging.getLogger(__name__)


class TextMetadataExtractor(MetadataExtractor):
    """Extract metadata from text files."""

    supported_extensions = [
        ".txt",
        ".md",
        ".rst",
        ".csv",
        ".json",
        ".xml",
        ".html",
        ".htm",
        ".yaml",
        ".yml",
    ]
    supported_mime_types = ["text/"]

    def __init__(self, max_content_length: int = 10000):
        """
        Initialize the text metadata extractor.

        Args:
            max_content_length: Maximum number of characters to read from the file.
        """
        self.max_content_length = max_content_length

    def extract(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from a text file.

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of metadata.
        """
        metadata = {}

        try:
            # Read file content
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read(self.max_content_length)

            # Extract basic text metadata
            metadata["content_preview"] = (
                content[:1000] if len(content) > 1000 else content
            )
            metadata["line_count"] = content.count("\n") + 1
            metadata["word_count"] = len(re.findall(r"\b\w+\b", content))
            metadata["char_count"] = len(content)

            # Try to detect language (simple heuristic)
            metadata["language"] = self._detect_language(content)

            # Extract keywords (simple implementation)
            metadata["keywords"] = self._extract_keywords(content)

        except Exception as e:
            logger.error(f"Error extracting text metadata from {file_path}: {e}")

        return metadata

    def _detect_language(self, content: str) -> str:
        """
        Detect the language of the text (simple heuristic).

        Args:
            content: The text content.

        Returns:
            Detected language code.
        """
        # This is a very simple heuristic and should be replaced with a proper language detection library
        # like langdetect or fasttext in a production environment

        # Count common words in different languages
        english_words = ["the", "and", "is", "in", "to", "of", "that", "for"]
        spanish_words = ["el", "la", "es", "en", "y", "de", "que", "por"]
        french_words = ["le", "la", "est", "en", "et", "de", "que", "pour"]

        # Count occurrences
        english_count = sum(
            1
            for word in re.findall(r"\b\w+\b", content.lower())
            if word in english_words
        )
        spanish_count = sum(
            1
            for word in re.findall(r"\b\w+\b", content.lower())
            if word in spanish_words
        )
        french_count = sum(
            1
            for word in re.findall(r"\b\w+\b", content.lower())
            if word in french_words
        )

        # Determine language
        if english_count > spanish_count and english_count > french_count:
            return "en"
        elif spanish_count > english_count and spanish_count > french_count:
            return "es"
        elif french_count > english_count and french_count > spanish_count:
            return "fr"
        else:
            return "unknown"

    def _extract_keywords(self, content: str, max_keywords: int = 10) -> List[str]:
        """
        Extract keywords from the text.

        Args:
            content: The text content.
            max_keywords: Maximum number of keywords to extract.

        Returns:
            List of keywords.
        """
        # This is a simple implementation and should be replaced with a proper keyword extraction
        # algorithm in a production environment

        # Remove common stop words
        stop_words = {
            "the",
            "and",
            "is",
            "in",
            "to",
            "of",
            "that",
            "for",
            "on",
            "with",
            "as",
            "this",
            "by",
        }

        # Extract words
        words = re.findall(r"\b\w{3,}\b", content.lower())

        # Count word frequencies
        word_counts = {}
        for word in words:
            if word not in stop_words:
                word_counts[word] = word_counts.get(word, 0) + 1

        # Sort by frequency
        sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)

        # Return top keywords
        return [word for word, count in sorted_words[:max_keywords]]
