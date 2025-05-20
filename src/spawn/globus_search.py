"""
Globus Search integration for SPAwn.

This module provides functionality for publishing metadata to Globus Search.
"""

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import globus_sdk

from globus_sdk import SearchClient
from globus_sdk import UserApp, ClientApp

import requests

from spawn.config import config

logger = logging.getLogger(__name__)


class GlobusSearchClient:
    """Client for interacting with Globus Search."""

    def __init__(
        self,
        index_uuid: str,
    ):
        """
        Initialize the Globus Search client.

        Args:
            index_uuid: UUID of the Globus Search index.
        """
        self.index_uuid = index_uuid

        app = UserApp("SPAwn CLI App", client_id="367628a1-4b6a-4176-82bd-422f071d1adc")
        app.add_scope_requirements(
            {"search": [globus_sdk.scopes.SearchScopes.make_mutable("all")]}
        )
        self.search_client = SearchClient(app=app)

    def _get_headers(self) -> Dict[str, str]:
        """
        Get headers for Globus Search API requests.

        Returns:
            Dictionary of headers.
        """
        headers = {
            "Content-Type": "application/json",
        }

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

        response = self.search_client.ingest(self.index_uuid, ingest_doc)

        if response.status_code != 200:
            raise ValueError(
                f"Failed to ingest entry: {response.json().get('error', response.text)}"
            )

        return response.json()

    def ingest_entries(
        self, entries: List[Dict[str, Any]], batch_size: int = 100
    ) -> Dict[str, Any]:
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

        # Process entries in batches
        success_count = 0
        failed_count = 0

        for i in range(0, len(entries), batch_size):
            batch = entries[i : i + batch_size]

            # Create ingest document
            ingest_doc = {
                "ingest_type": "GMetaList",
                "ingest_data": {
                    "gmeta": batch,
                },
            }

            try:
                response = self.search_client.ingest(self.index_uuid, ingest_doc)

                logger.info(response)

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
            logger.error(
                f"Failed to get entry: {response.json().get('error', response.text)}"
            )
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
            logger.error(
                f"Failed to delete entry: {response.json().get('error', response.text)}"
            )
            return False

        return True


def metadata_to_gmeta_entry(
    file_path: str,
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
    metadata: Dict[str, str],
    index_uuid: str,
    batch_size: int = 100,
    subject_prefix: str = "file://",
    visible_to: Optional[List[str]] = None,
) -> Dict[str, int]:
    """
    Publish metadata to Globus Search.

    Args:
        metadata: The metadata to be published.
        index_uuid: UUID of the Globus Search index.
        search_client: Globus SearchClient.
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
    )

    # Extract metadata and create GMetaEntries
    entries = []

    # Convert to GMetaEntry
    for k, v in metadata.items():
        entry = metadata_to_gmeta_entry(
            file_path=k,
            metadata=v,
            subject_prefix=subject_prefix,
            visible_to=visible_to,
        )

        entries.append(entry)

    # Ingest entries
    return client.ingest_entries(entries, batch_size=batch_size)
