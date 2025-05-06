"""
Globus Search integration for SPAwn.

This module provides functionality for publishing metadata to Globus Search.
"""

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import requests

from spawn.config import config

logger = logging.getLogger(__name__)


class GlobusSearchClient:
    """Client for interacting with Globus Search."""

    def __init__(
        self,
        index_uuid: str,
        auth_token: Optional[str] = None,
        base_url: str = "https://search.api.globus.org/v1",
    ):
        """
        Initialize the Globus Search client.

        Args:
            index_uuid: UUID of the Globus Search index.
            auth_token: Globus Auth token with search.ingest scope.
                If None, uses the token from config or environment.
            base_url: Globus Search API base URL.
        """
        self.index_uuid = index_uuid
        self.auth_token = auth_token or config.globus_auth_token
        self.base_url = base_url
        
        if not self.auth_token:
            logger.warning("No Globus Auth token provided. Some operations may fail.")
    
    def _get_headers(self) -> Dict[str, str]:
        """
        Get headers for Globus Search API requests.

        Returns:
            Dictionary of headers.
        """
        headers = {
            "Content-Type": "application/json",
        }
        
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        
        return headers
    
    def ingest_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ingest a single entry into Globus Search.

        Args:
            entry: Entry to ingest.

        Returns:
            Response from the Globus Search API.

        Raises:
            ValueError: If the ingest fails.
        """
        url = f"{self.base_url}/ingest/{self.index_uuid}"
        
        # Ensure entry has required fields
        if "subject" not in entry:
            raise ValueError("Entry must have a 'subject' field")
        
        # Create ingest document
        ingest_doc = {
            "ingest_type": "GMetaEntry",
            "ingest_data": {
                "gmeta": [entry],
            },
        }
        
        response = requests.post(
            url,
            headers=self._get_headers(),
            json=ingest_doc,
        )
        
        if response.status_code != 200:
            raise ValueError(f"Failed to ingest entry: {response.json().get('error', response.text)}")
        
        return response.json()
    
    def ingest_entries(self, entries: List[Dict[str, Any]], batch_size: int = 100) -> Dict[str, Any]:
        """
        Ingest multiple entries into Globus Search.

        Args:
            entries: List of entries to ingest.
            batch_size: Number of entries to ingest in a single batch.

        Returns:
            Dictionary with counts of successful and failed ingest operations.

        Raises:
            ValueError: If the ingest fails.
        """
        url = f"{self.base_url}/ingest/{self.index_uuid}"
        
        # Process entries in batches
        success_count = 0
        failed_count = 0
        
        for i in range(0, len(entries), batch_size):
            batch = entries[i:i+batch_size]
            
            # Create ingest document
            ingest_doc = {
                "ingest_type": "GMetaList",
                "ingest_data": {
                    "gmeta": batch,
                },
            }
            
            try:
                response = requests.post(
                    url,
                    headers=self._get_headers(),
                    json=ingest_doc,
                )
                
                if response.status_code != 200:
                    logger.error(f"Failed to ingest batch: {response.json().get('error', response.text)}")
                    failed_count += len(batch)
                else:
                    success_count += len(batch)
                    
                    # Add a small delay to avoid rate limiting
                    time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error ingesting batch: {e}")
                failed_count += len(batch)
        
        return {
            "success": success_count,
            "failed": failed_count,
        }
    
    def get_entry(self, subject: str) -> Optional[Dict[str, Any]]:
        """
        Get an entry from Globus Search.

        Args:
            subject: Subject of the entry to get.

        Returns:
            Entry if found, None otherwise.
        """
        url = f"{self.base_url}/get_entry/{self.index_uuid}/{subject}"
        
        response = requests.get(
            url,
            headers=self._get_headers(),
        )
        
        if response.status_code != 200:
            logger.error(f"Failed to get entry: {response.json().get('error', response.text)}")
            return None
        
        result = response.json()
        
        if "gmeta" in result and len(result["gmeta"]) > 0:
            return result["gmeta"][0]
        
        return None
    
    def delete_entry(self, subject: str) -> bool:
        """
        Delete an entry from Globus Search.

        Args:
            subject: Subject of the entry to delete.

        Returns:
            True if the entry was deleted, False otherwise.
        """
        url = f"{self.base_url}/delete_by_subject/{self.index_uuid}"
        
        data = {
            "subjects": [subject],
        }
        
        response = requests.post(
            url,
            headers=self._get_headers(),
            json=data,
        )
        
        if response.status_code != 200:
            logger.error(f"Failed to delete entry: {response.json().get('error', response.text)}")
            return False
        
        return True


def metadata_to_gmeta_entry(
    file_path: Path,
    metadata: Dict[str, Any],
    subject_prefix: str = "file://",
    visible_to: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Convert file metadata to a GMetaEntry.

    Args:
        file_path: Path to the file.
        metadata: Dictionary of metadata.
        subject_prefix: Prefix to use for the subject.
        visible_to: List of Globus Auth identities that can see this entry.

    Returns:
        GMetaEntry dictionary.
    """
    # Create subject from file path
    subject = f"{subject_prefix}{file_path}"
    
    # Create GMetaEntry
    entry = {
        "subject": subject,
        "visible_to": visible_to or ["public"],
        "content": metadata,
    }
    
    return entry


def publish_metadata(
    file_paths: List[Path],
    index_uuid: str,
    auth_token: Optional[str] = None,
    batch_size: int = 100,
    subject_prefix: str = "file://",
    visible_to: Optional[List[str]] = None,
) -> Dict[str, int]:
    """
    Publish metadata to Globus Search.

    Args:
        file_paths: List of file paths to publish metadata for.
        index_uuid: UUID of the Globus Search index.
        auth_token: Globus Auth token with search.ingest scope.
        batch_size: Number of entries to ingest in a single batch.
        subject_prefix: Prefix to use for the subject.
        visible_to: List of Globus Auth identities that can see these entries.

    Returns:
        Dictionary with counts of successful and failed publish operations.
    """
    from spawn.metadata import extract_metadata
    
    # Create Globus Search client
    client = GlobusSearchClient(
        index_uuid=index_uuid,
        auth_token=auth_token,
    )
    
    # Extract metadata and create GMetaEntries
    entries = []
    
    for file_path in file_paths:
        try:
            # Extract metadata
            metadata = extract_metadata(file_path)
            
            # Convert to GMetaEntry
            entry = metadata_to_gmeta_entry(
                file_path=file_path,
                metadata=metadata,
                subject_prefix=subject_prefix,
                visible_to=visible_to,
            )
            
            entries.append(entry)
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
    
    # Ingest entries
    return client.ingest_entries(entries, batch_size=batch_size)