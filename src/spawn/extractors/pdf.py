"""
PDF metadata extractor for SPAwn.

This module provides functionality for extracting metadata from PDF files.
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from spawn.metadata import MetadataExtractor

logger = logging.getLogger(__name__)


class PDFMetadataExtractor(MetadataExtractor):
    """Extract metadata from PDF files."""

    supported_extensions = [
        ".pdf",
    ]
    supported_mime_types = [
        "application/pdf",
    ]

    def __init__(self, extract_text: bool = True, max_pages_to_extract: int = 5):
        """
        Initialize the PDF metadata extractor.

        Args:
            extract_text: Whether to extract text content from the PDF.
            max_pages_to_extract: Maximum number of pages to extract text from.
        """
        self.extract_text = extract_text
        self.max_pages_to_extract = max_pages_to_extract

    def extract(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from a PDF file.

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of metadata.
        """
        metadata = {}

        try:
            # Try to import PyPDF2, which is required for PDF processing
            try:
                import PyPDF2
                has_pypdf2 = True
            except ImportError:
                has_pypdf2 = False
                metadata["error"] = "PyPDF2 library not available for PDF processing"
                return metadata

            if not has_pypdf2:
                return metadata

            # Open the PDF file
            with open(file_path, "rb") as f:
                pdf_reader = PyPDF2.PdfReader(f)
                
                # Basic PDF information
                metadata["page_count"] = len(pdf_reader.pages)
                
                # Extract document information
                if pdf_reader.metadata:
                    doc_info = pdf_reader.metadata
                    
                    # Extract common metadata fields
                    if doc_info.get("/Title"):
                        metadata["title"] = doc_info.get("/Title")
                    if doc_info.get("/Author"):
                        metadata["author"] = doc_info.get("/Author")
                    if doc_info.get("/Subject"):
                        metadata["subject"] = doc_info.get("/Subject")
                    if doc_info.get("/Keywords"):
                        metadata["keywords"] = doc_info.get("/Keywords")
                    if doc_info.get("/Producer"):
                        metadata["producer"] = doc_info.get("/Producer")
                    if doc_info.get("/Creator"):
                        metadata["creator"] = doc_info.get("/Creator")
                    
                    # Extract creation and modification dates
                    if doc_info.get("/CreationDate"):
                        creation_date = self._parse_pdf_date(doc_info.get("/CreationDate"))
                        if creation_date:
                            metadata["creation_date"] = creation_date
                    
                    if doc_info.get("/ModDate"):
                        mod_date = self._parse_pdf_date(doc_info.get("/ModDate"))
                        if mod_date:
                            metadata["modification_date"] = mod_date
                
                # Extract text content if requested
                if self.extract_text:
                    text_content = ""
                    page_count = min(len(pdf_reader.pages), self.max_pages_to_extract)
                    
                    for i in range(page_count):
                        try:
                            page = pdf_reader.pages[i]
                            text_content += page.extract_text() + "\n\n"
                        except Exception as e:
                            logger.debug(f"Error extracting text from page {i+1}: {e}")
                    
                    # Add text content to metadata
                    if text_content:
                        # Truncate if too long
                        if len(text_content) > 10000:
                            metadata["text_preview"] = text_content[:10000] + "..."
                        else:
                            metadata["text_preview"] = text_content
                        
                        # Calculate text statistics
                        metadata["word_count"] = len(re.findall(r'\b\w+\b', text_content))
                        metadata["char_count"] = len(text_content)
                
                # Extract form fields if present
                if hasattr(pdf_reader, 'get_fields') and callable(getattr(pdf_reader, 'get_fields')):
                    fields = pdf_reader.get_fields()
                    if fields:
                        form_fields = []
                        for field_name, field_value in fields.items():
                            form_fields.append({
                                "name": field_name,
                                "type": type(field_value).__name__,
                            })
                        metadata["form_fields"] = form_fields
                        metadata["is_form"] = True
                
                # Check for encryption
                metadata["is_encrypted"] = pdf_reader.is_encrypted
                
                # Check for images (simple check)
                has_images = False
                for i in range(min(3, len(pdf_reader.pages))):  # Check first 3 pages
                    page = pdf_reader.pages[i]
                    if "/XObject" in page:
                        xobject = page["/XObject"]
                        if xobject:
                            has_images = True
                            break
                
                metadata["has_images"] = has_images

        except Exception as e:
            logger.error(f"Error extracting PDF metadata from {file_path}: {e}")
            metadata["error"] = str(e)

        return metadata

    def _parse_pdf_date(self, date_string: str) -> Optional[str]:
        """
        Parse PDF date format to ISO format.

        Args:
            date_string: PDF date string (e.g., "D:20201231235959+00'00'")

        Returns:
            ISO formatted date string or None if parsing fails.
        """
        try:
            # Remove 'D:' prefix if present
            if date_string.startswith("D:"):
                date_string = date_string[2:]
            
            # Basic format: YYYYMMDDHHmmSS
            if len(date_string) >= 14:
                year = int(date_string[0:4])
                month = int(date_string[4:6])
                day = int(date_string[6:8])
                hour = int(date_string[8:10])
                minute = int(date_string[10:12])
                second = int(date_string[12:14])
                
                # Create datetime object
                dt = datetime(year, month, day, hour, minute, second)
                
                # Parse timezone if present
                if len(date_string) > 14 and date_string[14] in ['+', '-', 'Z']:
                    # For simplicity, we're not adjusting for timezone here
                    pass
                
                return dt.isoformat()
            
            return None
        except Exception as e:
            logger.debug(f"Error parsing PDF date '{date_string}': {e}")
            return None